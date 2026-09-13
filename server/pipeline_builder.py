#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""pipeline_builder.py — Pipecat pipeline construction for the HVAC Voice Agent.

Constructs a PipelineWorker for each inbound Twilio WebSocket connection.
The pipeline uses:
  - Deepgram Nova-3 for STT (with punctuation for faster LLM parsing)
  - OpenAI GPT-4o-mini for reasoning (temp=0.1 to minimize hallucination)
  - Cartesia Sonic for ultra-low latency TTS
  - Silero VAD tuned for high-noise HVAC environments
  - MCP tools registered from a pre-cached schema
"""

from fastapi import WebSocket
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)
from pipecat.workers.runner import WorkerRunner

from config import settings
from error_handlers import attach_error_handlers

# ---------------------------------------------------------------------------
# HVAC Dispatcher System Prompt
# ---------------------------------------------------------------------------
# Kept inline here for single-file visibility. Extract to prompts.py if it
# grows beyond a few paragraphs.
SYSTEM_PROMPT = (
    "You are a professional AI dispatcher for an HVAC emergency service company "
    "located in 6th of October City, Egypt. Your name is Sara. "
    "You handle inbound calls from homeowners and businesses experiencing "
    "heating, ventilation, or air conditioning issues.\n\n"
    "CALL FLOW:\n"
    "1. Greet the caller warmly and introduce yourself.\n"
    "2. Ask what HVAC issue they are experiencing.\n"
    "3. Triage the urgency (emergency leak, no cooling, routine maintenance, etc.).\n"
    "4. Collect the caller's name and confirm their phone number.\n"
    "5. Ask for their preferred appointment date and time.\n"
    "6. Use the 'check_availability' tool first to verify the technician is free at the requested slot.\n"
    "7. If available, use the 'check_and_book_slot' tool to book the appointment.\n"
    "8. If the tool returns SUCCESS, confirm the details with the caller.\n"
    "9. If a tool returns an ERROR, explain the issue and offer alternatives.\n\n"
    "RULES:\n"
    "- Be highly concise. Your responses will be spoken aloud.\n"
    "- Never use emojis, bullet points, or markdown formatting.\n"
    "- Never invent availability. Always use the booking tool.\n"
    "- Appointments must be on the hour (e.g., 2 PM, not 2:30 PM).\n"
    "- Business hours are 8 AM to 6 PM, Saturday through Thursday.\n"
    "- If the caller provides an invalid time, politely ask them to choose another.\n"
    "- If a slot is taken, apologize and suggest the next available hour.\n"
    "- Speak in the same language the caller uses (Arabic or English).\n"
    "- Valid technician IDs are: TECH_01, TECH_02, TECH_03, TECH_04, TECH_05.\n"
    "  Use the most relevant one. Do NOT make up technician IDs.\n"
)


async def build_and_run_pipeline(
    websocket: WebSocket,
    stream_sid: str,
    call_sid: str,
    mcp_client,
):
    """Constructs and runs the Pipecat PipelineWorker for a single inbound call.

    This function is invoked once per Twilio WebSocket connection. It sets up
    the full STT → LLM → TTS cascade, registers MCP tools, attaches error
    handlers, and runs the pipeline until the call ends.

    Args:
        websocket: The accepted FastAPI WebSocket from Twilio.
        stream_sid: Twilio's unique identifier for this media stream.
        call_sid: Twilio's unique identifier for this phone call.
        mcp_client: The persistent MCPClient instance, cached during FastAPI
            lifespan to avoid per-call connection overhead.
    """
    logger.info(f"Building pipeline for call {call_sid} (stream {stream_sid})")

    # -----------------------------------------------------------------------
    # 1. Telephony Transport & Serializer
    # -----------------------------------------------------------------------
    # Twilio sends audio encoded as 8kHz PCMU (μ-law). The TwilioFrameSerializer
    # handles the automatic resampling to 16kHz PCM required by Pipecat frames.
    serializer = TwilioFrameSerializer(
        stream_sid=stream_sid,
        call_sid=call_sid,
        account_sid=settings.twilio_sid,
        auth_token=settings.twilio_auth,
    )

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,  # Strict False for raw Twilio RTP streams
            serializer=serializer,
        ),
    )

    # -----------------------------------------------------------------------
    # 2. Speech-to-Text (Deepgram Nova-3)
    # -----------------------------------------------------------------------
    # punctuate=True and smart_format=True offload parsing logic from the LLM,
    # allowing it to interpret semantic boundaries faster and reducing TTFT.
    stt = DeepgramSTTService(
        api_key=settings.deepgram_key,
        settings=DeepgramSTTService.Settings(
            model="nova-3",
            language="en-US",
            smart_format=True,
            punctuate=True,
        ),
    )

    # -----------------------------------------------------------------------
    # 3. Large Language Model (OpenAI GPT-4o-mini)
    # -----------------------------------------------------------------------
    # Temperature 0.1 minimizes CRM data hallucination while retaining
    # enough flexibility for natural conversation.
    llm = OpenAILLMService(
        api_key=settings.openai_key,
        settings=OpenAILLMService.Settings(
            model="gpt-4o-mini",
            temperature=0.1,
        ),
    )

    # Register MCP tools — this creates the actual MCP proxy handlers
    mcp_tools = await mcp_client.register_tools(llm)
    logger.info(
        f"Registered MCP tools: {[t.name for t in mcp_tools.standard_tools]}"
    )

    # Wrap each registered handler with timeout + error catching and detailed logging
    import asyncio

    from pipecat.frames.frames import TTSSpeakFrame

    def make_tool_wrapper(original_handler):
        async def wrapped_handler(params):
            async def speak_delay():
                try:
                    await asyncio.sleep(5.0)
                    logger.info(f"Tool '{params.function_name}' execution taking longer than 5s, playing loading message")
                    await params.llm.push_frame(TTSSpeakFrame("Bear with me, it's loading."))
                except asyncio.CancelledError:
                    pass

            delay_task = asyncio.create_task(speak_delay())

            # Intercept and log the tool results/failures via the result callback
            original_callback = params.result_callback

            async def wrapped_callback(result):
                logger.info(f"MCP Tool '{params.function_name}' result callback invoked. Result: {result}")
                await original_callback(result)

            params.result_callback = wrapped_callback

            logger.info(
                f"LLM invoking MCP Tool '{params.function_name}' (call ID: {params.tool_call_id}) "
                f"with arguments: {params.arguments}"
            )

            try:
                result = await original_handler(params)
                return result
            except Exception as e:
                logger.error(f"Error executing tool wrapper for '{params.function_name}': {e}")
                err_msg = "ERROR: hmm.. it seems like I can't access the calendar right now, can I give you a call in a few minutes to check?"
                await original_callback(err_msg)
            finally:
                delay_task.cancel()
                try:
                    await delay_task
                except asyncio.CancelledError:
                    pass

        return wrapped_handler

    for tool_name, registry_item in llm._functions.items():
        if registry_item.handler is not None:
            original = registry_item.handler
            registry_item.handler = make_tool_wrapper(original)
            logger.info(f"Wrapped tool '{tool_name}' with timeout/error handler and logging")

    # -----------------------------------------------------------------------
    # 4. Text-to-Speech (Cartesia Sonic)
    # -----------------------------------------------------------------------
    tts = CartesiaTTSService(
        api_key=settings.cartesia_key,
        settings=CartesiaTTSService.Settings(
            # Localized persona voice ID — ensure it supports Arabic phonetics
            voice="71a7ad14-091c-4e8e-a314-022ece01c121",
        ),
    )

    # -----------------------------------------------------------------------
    # 5. Conversation Context & VAD Aggregation
    # -----------------------------------------------------------------------
    import datetime
    current_date_str = datetime.datetime.now().strftime("%A, %B %d, %Y")
    today_context = f"\n\nToday is {current_date_str}."

    context = LLMContext(
        messages=[{"role": "system", "content": SYSTEM_PROMPT + today_context}],
        tools=mcp_tools,
    )

    # VAD is attached to the aggregator, isolating speech detection logic from
    # the transport. Parameters are tuned for noisy HVAC environments:
    #   - confidence=0.85: Aggressive threshold filters rhythmic mechanical noise
    #   - start_secs=0.25: Requires sustained vocalization, prevents transient pops
    #   - stop_secs=0.20: Fast termination for conversational velocity
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=SileroVADAnalyzer(
                params=VADParams(
                    confidence=0.85,
                    start_secs=0.25,
                    stop_secs=0.20,
                )
            )
        ),
    )

    # -----------------------------------------------------------------------
    # 6. Pipeline Construction
    # -----------------------------------------------------------------------
    pipeline = Pipeline(
        [
            transport.input(),       # Yields InputAudioRawFrame from Twilio
            stt,                     # Consumes audio, yields TranscriptionFrame
            user_aggregator,         # Buffers transcriptions, yields LLMContextFrame
            llm,                     # Consumes context, yields LLMTextFrame / Function Calls
            tts,                     # Consumes text, yields OutputAudioRawFrame
            transport.output(),      # Serializes to PCMU and writes to Twilio WS
            assistant_aggregator,    # Appends synthetic text to context history
        ]
    )

    # -----------------------------------------------------------------------
    # 7. Worker & Runner Execution
    # -----------------------------------------------------------------------
    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    # Attach specialized error and lifecycle handlers for graceful degradation
    attach_error_handlers(worker)

    # The WorkerRunner manages the AsyncQueueBus and execution state.
    # handle_sigint=False because FastAPI/uvicorn handles signals.
    runner = WorkerRunner(handle_sigint=False)
    await runner.run(worker)

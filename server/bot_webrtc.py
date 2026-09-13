#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""bot_webrtc.py — WebRTC-based test runner for the HVAC Voice Agent.

Allows local testing of the voice agent directly in your web browser
using your microphone and speakers, bypassing Twilio.

Runs with:
    uv run bot_webrtc.py
"""

import os

from loguru import logger
from mcp.client.session_group import SseServerParameters
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import create_transport
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.mcp_service import MCPClient
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.daily.transport import DailyParams

from config import settings
from error_handlers import attach_error_handlers
from pipeline_builder import SYSTEM_PROMPT


async def run_bot(
    transport: BaseTransport,
    runner_args: RunnerArguments,
) -> None:
    """Runs the voice bot session using the selected WebRTC transport."""
    logger.info("Starting WebRTC voice bot session...")

    # 1. Establish connection to the FastMCP server over SSE
    mcp_url = str(settings.MCP_SERVER_URL)
    logger.info(f"Connecting to FastMCP server at {mcp_url}...")

    async with MCPClient(
        server_params=SseServerParameters(url=mcp_url)
    ) as mcp_client:

        # 2. Speech-to-Text (Deepgram)
        stt = DeepgramSTTService(
            api_key=settings.deepgram_key,
            settings=DeepgramSTTService.Settings(
                model="nova-3",
                language="en-US",
                smart_format=True,
                punctuate=True,
            ),
        )

        # 3. Large Language Model (OpenAI GPT-4o-mini)
        llm = OpenAILLMService(
            api_key=settings.openai_key,
            settings=OpenAILLMService.Settings(
                model="gpt-4o-mini",
                temperature=0.1,
            ),
        )

        # Register MCP tools — this creates the actual MCP proxy handlers
        # (get_tools_schema() only returns schemas WITHOUT handlers)
        mcp_tools = await mcp_client.register_tools(llm)
        logger.info(
            f"Registered MCP tools: {[t.name for t in mcp_tools.standard_tools]}"
        )

        # Now wrap each registered handler with timeout + error catching and detailed logging
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

        # Post-registration wrapping: access the internal registry, wrap handlers
        for tool_name, registry_item in llm._functions.items():
            if registry_item.handler is not None:
                original = registry_item.handler
                registry_item.handler = make_tool_wrapper(original)
                logger.info(f"Wrapped tool '{tool_name}' with timeout/error handler and logging")

        # 4. Text-to-Speech (Cartesia)
        tts = CartesiaTTSService(
            api_key=settings.cartesia_key,
            settings=CartesiaTTSService.Settings(
                voice="71a7ad14-091c-4e8e-a314-022ece01c121",
            ),
        )

        # 5. Context & Aggregators (High-noise VAD parameters)
        import datetime
        current_date_str = datetime.datetime.now().strftime("%A, %B %d, %Y")
        today_context = f"\n\nToday is {current_date_str}."

        context = LLMContext(
            messages=[{"role": "system", "content": SYSTEM_PROMPT + today_context}],
            tools=mcp_tools,
        )

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

        # 6. Pipeline Construction
        pipeline = Pipeline(
            [
                transport.input(),
                stt,
                user_aggregator,
                llm,
                tts,
                transport.output(),
                assistant_aggregator,
            ]
        )

        worker = PipelineWorker(
            pipeline,
            params=PipelineParams(
                enable_metrics=True,
                enable_usage_metrics=True,
            ),
        )

        # Attach gracefully degrading error handlers
        attach_error_handlers(worker)

        # Main RTVI startup trigger (kicks off conversation)
        @worker.rtvi.event_handler("on_client_ready")
        async def on_client_ready(rtvi):
            from pipecat.frames.frames import LLMRunFrame
            await worker.queue_frames([LLMRunFrame()])

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport, client):
            logger.info("Client disconnected, stopping bot...")
            from pipecat.frames.frames import EndFrame
            await worker.queue_frames([EndFrame()])

        # Run the Pipecat worker
        from pipecat.workers.runner import WorkerRunner
        runner = WorkerRunner(handle_sigint=False)
        await runner.run(worker)


async def bot(runner_args: RunnerArguments):
    """Main bot entrypoint for the development runner."""
    transport_params = {
        "daily": lambda: DailyParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
        ),
        "webrtc": lambda: TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
        ),
    }

    # Automatically resolves transport depending on connection URL/arguments
    transport = await create_transport(runner_args, transport_params)
    await run_bot(transport, runner_args)


if __name__ == "__main__":
    from pipecat.runner.run import main
    main()

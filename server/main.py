#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""main.py — FastAPI entrypoint for the HVAC Voice Agent.

Exposes two endpoints:
  - POST /twiml — Returns TwiML XML to route inbound SIP calls to a WebSocket.
  - WS /ws/stream — Accepts the Twilio Media Stream WebSocket, parses the
    'start' event, and delegates to the Pipecat pipeline.

The FastAPI lifespan event establishes a persistent SSE connection to the
FastMCP server and caches the tool schema, avoiding a 1-2 second TCP
handshake penalty during every inbound call.
"""

import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, WebSocket
from loguru import logger

from pipecat.services.mcp_service import MCPClient
from mcp.client.session_group import SseServerParameters

from config import settings
from pipeline_builder import build_and_run_pipeline

# ---------------------------------------------------------------------------
# Module-level state for the cached MCP connection
# ---------------------------------------------------------------------------
mcp_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Establishes the SSE connection to the FastMCP server during startup.

    Caching the schema here prevents a 1-2 second TCP handshake latency
    penalty during the initialization of every inbound Twilio call.
    The connection is held open for the lifetime of the FastAPI application
    and closed cleanly on shutdown.
    """
    global mcp_client

    logger.info(
        f"Initializing persistent MCP Client connection to {settings.MCP_SERVER_URL}..."
    )

    mcp_client = MCPClient(
        server_params=SseServerParameters(url=str(settings.MCP_SERVER_URL))
    )
    await mcp_client.start()

    logger.info("MCP Client connected and ready.")

    yield

    # Shutdown: close the persistent MCP connection
    logger.info("Shutting down MCP Client connection...")
    await mcp_client.close()


app = FastAPI(
    title="HVAC AI Voice Agent",
    description="Pipecat-powered inbound voice dispatcher for HVAC emergency booking",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Twilio Webhook — Returns TwiML to upgrade the call to a WebSocket stream
# ---------------------------------------------------------------------------
@app.post("/twiml")
async def twilio_webhook(request: Request):
    """Provides the TwiML response to route the inbound SIP call to WebSocket.

    Twilio calls this endpoint when a call comes in (configured via the Twilio
    Console or TwiML Bin). The response tells Twilio to open a bidirectional
    WebSocket Media Stream to our /ws/stream endpoint.
    """
    host = request.headers.get("host")
    ws_url = f"wss://{host}/ws/stream"

    twiml_response = f"""<?xml version="1.0" encoding="UTF-8"?>
    <Response>
        <Connect>
            <Stream url="{ws_url}" />
        </Connect>
        <Pause length="40"/>
    </Response>"""

    return Response(content=twiml_response, media_type="application/xml")


# ---------------------------------------------------------------------------
# Twilio Media Stream WebSocket
# ---------------------------------------------------------------------------
@app.websocket("/ws/stream")
async def twilio_media_stream(websocket: WebSocket):
    """Handles the Twilio WebSocket connection.

    Negotiates the 'start' event to extract streamSid and callSid,
    then delegates full media routing to the Pipecat PipelineWorker.
    """
    await websocket.accept()

    stream_sid = None
    call_sid = None

    try:
        # Await the 'start' event from Twilio to extract routing identifiers.
        # Twilio sends: connected → start → media (continuous) → stop
        while True:
            message = await websocket.receive_text()
            data = json.loads(message)

            if data["event"] == "start":
                stream_sid = data["start"]["streamSid"]
                call_sid = data["start"]["callSid"]
                logger.info(
                    f"Twilio stream started. StreamSID: {stream_sid}, "
                    f"CallSID: {call_sid}"
                )
                break

    except Exception as e:
        logger.error(f"WebSocket closed before start event: {e}")
        await websocket.close()
        return

    # Delegate full media routing to the Pipecat WorkerRunner.
    # Each call gets its own pipeline instance.
    await build_and_run_pipeline(
        websocket=websocket,
        stream_sid=stream_sid,
        call_sid=call_sid,
        mcp_client=mcp_client,
    )


# ---------------------------------------------------------------------------
# Health check for deployment probes
# ---------------------------------------------------------------------------
@app.get("/health")
async def health_check():
    """Simple health check endpoint for load balancers and deployment probes."""
    return {"status": "healthy", "mcp_connected": mcp_client is not None}

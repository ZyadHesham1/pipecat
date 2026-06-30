#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""error_handlers.py — Graceful degradation for the HVAC Voice Agent.

When an unhandled exception occurs within a FrameProcessor, Pipecat emits an
ErrorFrame. Left unhandled, this severs the Twilio WebSocket unceremoniously,
resulting in a dropped call.

This module intercepts the fatal error, cancels the pipeline, injects a
localized fallback TTS message (Arabic/English for the 6th of October City
demographic), and dispatches an EndFrame to gracefully tear down the connection.
"""

from loguru import logger
from pipecat.frames.frames import EndFrame, ErrorFrame, TTSSpeakFrame
from pipecat.pipeline.worker import PipelineWorker


def attach_error_handlers(worker: PipelineWorker):
    """Binds lifecycle and error interceptors to the PipelineWorker.

    Provides a safety net to ensure graceful degradation of the caller
    experience even during total cascading failures of the reasoning or
    CRM layer.

    Args:
        worker: The PipelineWorker instance to attach handlers to.
    """

    @worker.event_handler("on_pipeline_error")
    async def on_pipeline_error(worker: PipelineWorker, frame: ErrorFrame):
        """Intercepts fatal pipeline errors and plays a localized fallback
        message before terminating the Twilio connection.
        """
        logger.error(f"Critical Pipeline Failure detected: {frame.error}")

        # Localized fallback message for the 6th of October City demographic.
        # Note: Ensure the selected Cartesia voice supports Arabic phonetics.
        fallback_text = (
            "نعتذر، نظام الحجز لدينا يواجه عطلاً فنياً حالياً. "
            "سنقوم بحفظ رقمك ومعاودة الاتصال بك في أقرب وقت. "
            "We apologize, our scheduling system is currently offline. "
            "We have logged your number and will call you back shortly."
        )

        try:
            # 1. Immediately cancel current processing to flush any hallucinated
            # LLM text or corrupted audio chunks stuck in the buffer.
            await worker.cancel()

            # 2. Queue a direct TTS frame.
            # Because the pipeline is in an error state, we bypass the LLM entirely.
            # This frame flows directly to the CartesiaTTSService for synthesis.
            await worker.queue_frame(TTSSpeakFrame(fallback_text))

            # 3. Enqueue the terminal EndFrame.
            # The EndFrame is processed sequentially AFTER the TTSSpeakFrame
            # concludes, ensuring the audio fully plays out to the Twilio caller
            # before the ASGI application closes the WebSocket connection.
            await worker.queue_frame(EndFrame())

        except Exception as e:
            # Absolute fallback if the TTS processor itself is the source of
            # the crash. Force shutdown immediately.
            logger.critical(
                f"Failed to synthesize fallback audio. Forcing shutdown. Error: {e}"
            )
            await worker.queue_frame(EndFrame())

    @worker.event_handler("on_pipeline_finished")
    async def on_pipeline_finished(worker: PipelineWorker, frame):
        """Executes final cleanup routines.

        Fires when EndFrame, StopFrame, or CancelFrame concludes execution.
        Use this hook to close local database handles, release dangling locks,
        or emit telemetry.
        """
        logger.info(
            f"Pipeline execution completed via terminal frame: {type(frame).__name__}"
        )

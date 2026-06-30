#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""mcp_server.py — FastMCP server for HVAC appointment booking.

This is a standalone process that exposes MCP tools over Server-Sent Events
(SSE). The Pipecat main application connects to this server during its
FastAPI lifespan event and caches the tool schemas.

Key responsibilities:
  - Strict Pydantic validation of booking requests (future dates, on-the-hour
    times, business hours enforcement)
  - Redis pessimistic locking (SET NX PX 300000) to prevent double-booking
    during concurrent emergency calls
  - Proxying confirmed bookings to the Frappe CRM backend

Run independently:
    uv run python mcp_server.py
"""

import datetime
import os
import uuid

from dotenv import load_dotenv

load_dotenv()

import httpx
import redis.asyncio as redis
from fastmcp import Context, FastMCP
from loguru import logger
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# FastMCP Server Initialization
# ---------------------------------------------------------------------------
mcp = FastMCP("HVAC_Dispatch_MCP_Server")

# ---------------------------------------------------------------------------
# Redis Connection Pool
# ---------------------------------------------------------------------------
# Requires low-latency network routing. The decode_responses=True flag
# ensures all Redis responses come back as Python strings, not bytes.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ---------------------------------------------------------------------------
# Frappe CRM Configuration
# ---------------------------------------------------------------------------
FRAPPE_CRM_URL = os.getenv("FRAPPE_CRM_URL", "https://zyad-crm.k.frappe.cloud")
FRAPPE_CRM_API_KEY = os.getenv("FRAPPE_CRM_API_KEY", "")
FRAPPE_CRM_API_SECRET = os.getenv("FRAPPE_CRM_API_SECRET", "")


# ---------------------------------------------------------------------------
# Pydantic Validation Schema
# ---------------------------------------------------------------------------
class SlotBookingRequest(BaseModel):
    """Strict validation schema enforcing HVAC business rules before execution.

    Every field is validated before the Redis lock is even attempted. If the
    LLM provides bad data, the validation error is returned as a programmatic
    string that the LLM can use to ask the caller for clarification.
    """

    technician_id: str = Field(
        ...,
        min_length=1,
        description="The ID of the technician (e.g., 'TECH_01').",
    )
    customer_name: str = Field(
        ...,
        min_length=2,
        description="The full name of the customer requesting service.",
    )
    customer_phone: str = Field(
        ...,
        min_length=7,
        description="The customer's phone number for callback.",
    )
    proposed_date: datetime.date = Field(
        ...,
        description="The date to book in YYYY-MM-DD format.",
    )
    proposed_time: datetime.time = Field(
        ...,
        description="The time to book in HH:MM format (24-hour clock).",
    )
    service_type: str = Field(
        default="emergency_repair",
        description=(
            "Type of HVAC service. Must be one of: "
            "'emergency_repair', 'maintenance', 'installation', "
            "'inspection', 'duct_cleaning'."
        ),
    )
    issue_description: str = Field(
        default="",
        max_length=500,
        description="Brief description of the HVAC issue reported by the caller.",
    )

    @field_validator("proposed_date")
    @classmethod
    def date_must_be_future(cls, v):
        """Rejects past dates. Returns an actionable error for the LLM."""
        if v < datetime.date.today():
            raise ValueError(
                "The proposed date cannot be in the past. "
                "Instruct the user to provide a future date."
            )
        return v

    @field_validator("proposed_time")
    @classmethod
    def time_must_be_aligned(cls, v):
        """Enforces on-the-hour slots within business hours (08:00–18:00).

        HVAC appointments are strict 60-minute blocks starting on the hour.
        """
        if v.minute != 0:
            raise ValueError(
                "Appointments must be booked exactly on the hour "
                "(e.g., 14:00, not 14:30). Round to the nearest hour "
                "and ask the user to confirm the new time."
            )

        if v.hour < 8 or v.hour >= 18:
            raise ValueError(
                "Emergency dispatch hours are restricted to 08:00 – 18:00. "
                "Apologize to the user and request a time within this "
                "operating window."
            )
        return v

    @field_validator("service_type")
    @classmethod
    def service_type_must_be_valid(cls, v):
        """Ensures the service type is one of the allowed categories."""
        allowed = {
            "emergency_repair",
            "maintenance",
            "installation",
            "inspection",
            "duct_cleaning",
        }
        if v not in allowed:
            raise ValueError(
                f"Invalid service type '{v}'. Must be one of: "
                f"{', '.join(sorted(allowed))}. "
                "Ask the caller to clarify what type of service they need."
            )
        return v


# ---------------------------------------------------------------------------
# MCP Tool: check_and_book_slot
# ---------------------------------------------------------------------------
@mcp.tool(
    name="check_and_book_slot",
    description=(
        "Validates availability, acquires a pessimistic lock, and books a "
        "technician slot in the Frappe CRM. Returns SUCCESS on booking or "
        "a structured ERROR string explaining why the booking failed."
    ),
)
async def check_and_book_slot(
    technician_id: str,
    customer_name: str,
    customer_phone: str,
    proposed_date: str,
    proposed_time: str,
    service_type: str = "emergency_repair",
    issue_description: str = "",
    ctx: Context = None,
) -> str:
    """Executes pessimistic locking via Redis, then proxies to Frappe CRM.

    The flow:
      1. Validate all inputs with Pydantic (catches hallucinated dates, etc.)
      2. Acquire a Redis lock on the specific technician+date+time slot (300s TTL)
      3. If locked by another caller, return ERROR with alternative suggestion
      4. Post the booking to Frappe CRM
      5. On success, leave the lock to expire naturally (5-min propagation buffer)
      6. On CRM failure, release the lock immediately
    """

    # -----------------------------------------------------------------------
    # Step 1: Pydantic Validation
    # -----------------------------------------------------------------------
    # If validation fails, the error message is returned directly to the LLM
    # so it can ask the caller for corrections.
    try:
        request = SlotBookingRequest(
            technician_id=technician_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
            proposed_date=proposed_date,
            proposed_time=proposed_time,
            service_type=service_type,
            issue_description=issue_description,
        )
    except Exception as e:
        logger.warning(f"Validation failed: {e}")
        return f"VALIDATION_ERROR: {e}"

    time_str = request.proposed_time.strftime("%H:%M")
    date_str = request.proposed_date.strftime("%Y-%m-%d")

    # -----------------------------------------------------------------------
    # Step 2: Acquire Redis Pessimistic Lock
    # -----------------------------------------------------------------------
    # Construct a deterministic lock key based on the resource dimensions.
    lock_key = f"lock:tech:{request.technician_id}:{date_str}:{time_str}"

    # Generate a unique token for this lock holder. This prevents a client
    # from accidentally releasing a lock held by another caller.
    lock_token = str(uuid.uuid4())

    # SET NX PX 300000:
    #   NX = only set if key doesn't exist (pessimistic lock acquisition)
    #   PX = 300,000 ms (300 second / 5 minute TTL dead-man's switch)
    lock_acquired = await redis_client.set(
        lock_key, lock_token, nx=True, px=300000
    )

    if not lock_acquired:
        # The slot is currently being booked by another dispatcher.
        # Provide actionable feedback so the LLM can pivot the conversation.
        logger.warning(f"Lock collision detected for {lock_key}")
        next_hour = (request.proposed_time.hour + 1) % 24
        # Clamp suggestion to business hours
        if next_hour < 8:
            next_hour = 8
        if next_hour >= 18:
            return (
                f"ERROR: SLOT_TAKEN. The slot {time_str} on {date_str} is "
                "currently locked by another dispatcher. No more slots are "
                "available today. Apologize and offer to book for the next "
                "business day at 08:00."
            )
        return (
            f"ERROR: SLOT_TAKEN. The slot {time_str} on {date_str} is "
            "currently locked and being booked by another dispatcher. "
            f"Apologize to the caller and offer {next_hour:02d}:00 instead."
        )

    # -----------------------------------------------------------------------
    # Step 3: Proxy to Frappe CRM
    # -----------------------------------------------------------------------
    try:
        async with httpx.AsyncClient() as client:
            crm_payload = {
                "technician": request.technician_id,
                "customer_name": request.customer_name,
                "customer_phone": request.customer_phone,
                "schedule_date": date_str,
                "schedule_time": time_str,
                "service_type": request.service_type,
                "issue_description": request.issue_description,
                "status": "Confirmed",
            }

            headers = {
                "Authorization": f"token {FRAPPE_CRM_API_KEY}:{FRAPPE_CRM_API_SECRET}",
                "Content-Type": "application/json",
            }

            response = await client.post(
                f"{FRAPPE_CRM_URL}/api/resource/TechnicianSchedule",
                json=crm_payload,
                headers=headers,
                timeout=10.0,
            )

            if response.status_code == 200:
                logger.info(
                    f"Booking confirmed: {request.technician_id} on "
                    f"{date_str} at {time_str} for {request.customer_name}"
                )
                return (
                    f"SUCCESS: Appointment booked for {request.customer_name} "
                    f"on {date_str} at {time_str} with technician "
                    f"{request.technician_id} for {request.service_type}. "
                    "Confirm the booking details with the caller."
                )
            elif response.status_code == 409:
                # Frappe rejects due to a database-level conflict.
                # Release the Redis lock explicitly.
                await _safe_unlock(lock_key, lock_token)
                return (
                    "ERROR: CRM_CONFLICT. The CRM reports a scheduling conflict "
                    "for this slot. Ask the user for another time."
                )
            else:
                await _safe_unlock(lock_key, lock_token)
                return "ERROR: hmm.. it seems like I can't access the calendar right now, can I give you a call in a few minutes to check?"

    except httpx.RequestError as e:
        # Network failure to CRM. Release lock to prevent capacity blocking.
        await _safe_unlock(lock_key, lock_token)
        logger.error(f"CRM network error: {e}")
        return "ERROR: hmm.. it seems like I can't access the calendar right now, can I give you a call in a few minutes to check?"

    # Note: Upon a successful HTTP 200 booking, we deliberately DO NOT delete
    # the lock key. Allowing the 5-minute TTL to expire naturally provides a
    # buffer window for Frappe CRM to propagate the database write to
    # read-replicas, ensuring subsequent LLM queries see the slot as occupied.


# ---------------------------------------------------------------------------
# Safe Unlock via Lua Script
# ---------------------------------------------------------------------------
# This Lua script atomically checks that the lock is still owned by us
# (via the unique token) before deleting it. This prevents accidentally
# releasing a lock that was acquired by a different caller after our TTL
# expired.
UNLOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


async def _safe_unlock(lock_key: str, lock_token: str):
    """Safely releases a Redis lock only if we still hold it.

    Uses a Lua script for atomic check-and-delete to prevent releasing
    a lock that has been acquired by another caller after our TTL expired.
    """
    try:
        await redis_client.eval(UNLOCK_SCRIPT, 1, lock_key, lock_token)
    except Exception as e:
        logger.error(f"Failed to release lock {lock_key}: {e}")


# ---------------------------------------------------------------------------
# Server Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport="sse")

#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""config.py — Strict environment validation for the HVAC Voice Agent.

Uses pydantic-settings to validate all required environment variables at startup.
If any critical secret is missing or malformed, the application fails immediately
with a clear error message, preventing silent runtime failures.
"""

from pydantic import SecretStr, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class ApplicationConfig(BaseSettings):
    """Validates all required environment variables for the Pipecat/FastMCP ecosystem.

    Fails immediately upon instantiation if the environment is misconfigured.
    This enforces a "fail fast" contract — no partial boots allowed.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Twilio (Telephony Transport) ---
    TWILIO_ACCOUNT_SID: SecretStr
    TWILIO_AUTH_TOKEN: SecretStr

    # --- AI Service Providers ---
    DEEPGRAM_API_KEY: SecretStr
    OPENAI_API_KEY: SecretStr
    CARTESIA_API_KEY: SecretStr

    # --- Infrastructure ---
    REDIS_URL: str = "redis://localhost:6379/0"
    MCP_SERVER_URL: HttpUrl

    # --- Frappe CRM ---
    FRAPPE_CRM_API_KEY: SecretStr
    FRAPPE_CRM_API_SECRET: SecretStr
    FRAPPE_CRM_URL: HttpUrl = "https://crm.hvac-business.local"

    # --- Convenience Properties ---
    # Avoids calling .get_secret_value() repeatedly in the pipeline code.

    @property
    def twilio_sid(self) -> str:
        return self.TWILIO_ACCOUNT_SID.get_secret_value()

    @property
    def twilio_auth(self) -> str:
        return self.TWILIO_AUTH_TOKEN.get_secret_value()

    @property
    def deepgram_key(self) -> str:
        return self.DEEPGRAM_API_KEY.get_secret_value()

    @property
    def openai_key(self) -> str:
        return self.OPENAI_API_KEY.get_secret_value()

    @property
    def cartesia_key(self) -> str:
        return self.CARTESIA_API_KEY.get_secret_value()

    @property
    def frappe_auth_header(self) -> str:
        """Returns the Frappe API token header value."""
        key = self.FRAPPE_CRM_API_KEY.get_secret_value()
        secret = self.FRAPPE_CRM_API_SECRET.get_secret_value()
        return f"token {key}:{secret}"


# Singleton — validated once at module import time.
# If this line throws, the application never starts.
settings = ApplicationConfig()

"""Validated environment configuration shared by application composition roots."""

from typing import Literal, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PlatformSettings(BaseSettings):
    """Platform settings with safe local defaults and secret-aware representations."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_PLATFORM_",
        case_sensitive=False,
        extra="ignore",
    )

    environment: Literal["development", "test", "production"] = "development"
    service_name: str = Field(default="agent-platform", min_length=1, max_length=100)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_json: bool = True

    database_url: SecretStr = SecretStr(
        "postgresql+asyncpg://agent_platform:local-postgres-change-me@127.0.0.1:5432/agent_platform"
    )
    redis_url: SecretStr = SecretStr("redis://127.0.0.1:6379/0")

    s3_endpoint: str = "http://127.0.0.1:9000"
    s3_access_key: SecretStr = SecretStr("local-minio")
    s3_secret_key: SecretStr = SecretStr("local-minio-change-me")
    s3_bucket: str = Field(default="agent-platform", pattern=r"^[a-z0-9][a-z0-9.-]+$")

    gateway_url: str = "http://127.0.0.1:4000"
    gateway_api_key: SecretStr = SecretStr("sk-local-litellm-change-me")

    max_turns: int = Field(default=24, ge=1, le=100)
    max_tool_calls: int = Field(default=64, ge=1, le=1_000)
    max_semantic_retries: int = Field(default=3, ge=0, le=10)
    max_run_seconds: int = Field(default=1_800, ge=60, le=86_400)

    command_timeout_seconds: int = Field(default=120, ge=1, le=900)
    max_command_timeout_seconds: int = Field(default=900, ge=1, le=3_600)
    max_command_output_bytes: int = Field(default=1_048_576, ge=1_024, le=104_857_600)

    lease_duration_seconds: int = Field(default=20, ge=5, le=300)
    heartbeat_interval_seconds: int = Field(default=5, ge=1, le=60)
    lease_scan_interval_seconds: int = Field(default=2, ge=1, le=60)

    @field_validator("s3_endpoint", "gateway_url")
    @classmethod
    def validate_http_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("URL must use http or https")
        return value.rstrip("/")

    @model_validator(mode="after")
    def validate_invariants(self) -> Self:
        if self.command_timeout_seconds > self.max_command_timeout_seconds:
            raise ValueError("command_timeout_seconds may not exceed max_command_timeout_seconds")
        if self.heartbeat_interval_seconds >= self.lease_duration_seconds:
            raise ValueError("heartbeat interval must be shorter than the lease duration")
        return self

    def redaction_values(self) -> tuple[str, ...]:
        """Return configured secret values for the logging redactor."""

        return (
            self.database_url.get_secret_value(),
            self.redis_url.get_secret_value(),
            self.s3_access_key.get_secret_value(),
            self.s3_secret_key.get_secret_value(),
            self.gateway_api_key.get_secret_value(),
        )

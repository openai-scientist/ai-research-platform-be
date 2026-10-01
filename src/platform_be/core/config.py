from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=tuple([f"{chr(46)}env.local"]),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "AI Research Platform API"
    app_env: Literal["local", "test", "staging", "production"] = "local"
    debug: bool = False
    api_prefix: str = "/api/v1"
    database_url: str = "postgresql+asyncpg://platform:platform@localhost:5432/platform"
    cors_allowed_origins: str = "http://localhost:3000,http://localhost:5173"
    firebase_project_id: str = "ai-research-platform-4ceb9"
    firebase_client_email: str | None = None
    firebase_private_key: SecretStr | None = None
    session_cookie_name: str = "platform_session"
    session_signing_secret: SecretStr = SecretStr("local-only-session-secret-change-me-32")
    cookie_secure: bool = False
    cookie_samesite: Literal["lax", "strict", "none"] = "lax"
    session_idle_minutes: int = Field(default=7 * 24 * 60, ge=5, le=30 * 24 * 60)
    session_absolute_days: int = Field(default=30, ge=1, le=30)
    recent_auth_seconds: int = Field(default=300, ge=60, le=1800)
    auth_session_rate_limit: int = Field(default=10, ge=1, le=1000)
    auth_session_rate_window_seconds: int = Field(default=60, ge=1, le=3600)
    request_max_body_bytes: int = Field(default=1_048_576, ge=1024, le=10_485_760)

    @field_validator("cors_allowed_origins")
    @classmethod
    def validate_origins(cls, value: str) -> str:
        origins = [origin.strip() for origin in value.split(",") if origin.strip()]
        if not origins or any("*" in origin for origin in origins):
            raise ValueError("cors_allowed_origins must contain explicit origins")
        return ",".join(origins)

    @field_validator("session_signing_secret")
    @classmethod
    def validate_secret_length(cls, value: SecretStr) -> SecretStr:
        if len(value.get_secret_value()) < 32:
            raise ValueError("session_signing_secret must be at least 32 characters")
        return value

    @field_validator("firebase_client_email", mode="before")
    @classmethod
    def normalize_client_email(cls, value: object) -> object:
        return None if isinstance(value, str) and not value.strip() else value

    @field_validator("firebase_private_key", mode="before")
    @classmethod
    def normalize_private_key(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        if isinstance(value, SecretStr) and not value.get_secret_value().strip():
            return None
        return value

    @model_validator(mode="after")
    def validate_deployment_security(self) -> "Settings":
        if bool(self.firebase_client_email) != bool(self.firebase_private_key):
            raise ValueError(
                "firebase_client_email and firebase_private_key must be configured together"
            )
        if self.cookie_samesite == "none" and not self.cookie_secure:
            raise ValueError("SameSite=None cookies require Secure")
        if self.app_env == "production":
            if not self.cookie_secure:
                raise ValueError("Production requires secure session cookies")
            if self.session_signing_secret.get_secret_value().startswith("local-only-"):
                raise ValueError("Production requires a non-development session signing secret")
            if any(not origin.startswith("https://") for origin in self.allowed_origins):
                raise ValueError("Production CORS origins must use HTTPS")
        return self

    @property
    def allowed_origins(self) -> list[str]:
        return [origin.strip().rstrip("/") for origin in self.cors_allowed_origins.split(",")]


@lru_cache
def get_settings() -> Settings:
    return Settings()

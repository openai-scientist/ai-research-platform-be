from decimal import Decimal
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
    # Uploaded datasets and run artifacts live in a file store; only "local" exists so far.
    storage_backend: Literal["local"] = "local"
    storage_local_root: str = "var/storage"
    dataset_max_upload_bytes: int = Field(default=52_428_800, ge=1024, le=1_073_741_824)
    artifact_max_upload_bytes: int = Field(default=52_428_800, ge=1024, le=1_073_741_824)
    # Popper is a separate service. Runs cannot start until its base URL is configured.
    popper_base_url: str | None = None
    popper_service_key: SecretStr | None = None
    popper_callback_key: SecretStr | None = None
    popper_timeout_seconds: float = Field(default=30, ge=1, le=300)
    # Address Popper uses to call this API back, without the API prefix.
    public_base_url: str = "http://localhost:8000"
    run_default_budget_usd: Decimal = Field(default=Decimal("5"), gt=0)
    run_max_budget_usd: Decimal = Field(default=Decimal("20"), gt=0)

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

    @field_validator("popper_base_url", "popper_service_key", "popper_callback_key", mode="before")
    @classmethod
    def normalize_blank_popper_setting(cls, value: object) -> object:
        if isinstance(value, SecretStr):
            value = value.get_secret_value()
        return None if isinstance(value, str) and not value.strip() else value

    @model_validator(mode="after")
    def validate_deployment_security(self) -> "Settings":
        if self.run_default_budget_usd > self.run_max_budget_usd:
            raise ValueError("run_default_budget_usd must not exceed run_max_budget_usd")
        if self.popper_base_url and not (self.popper_service_key and self.popper_callback_key):
            raise ValueError(
                "popper_service_key and popper_callback_key are required with popper_base_url"
            )
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
            for key in (self.popper_service_key, self.popper_callback_key):
                if key is not None and len(key.get_secret_value()) < 32:
                    raise ValueError("Production Popper service keys need at least 32 characters")
        return self

    @property
    def allowed_origins(self) -> list[str]:
        return [origin.strip().rstrip("/") for origin in self.cors_allowed_origins.split(",")]


@lru_cache
def get_settings() -> Settings:
    return Settings()

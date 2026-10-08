from decimal import Decimal
from functools import lru_cache
from typing import Literal

from cryptography.fernet import Fernet
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
    session_cookie_name: str = "platform_session"
    session_signing_secret: SecretStr = SecretStr("local-only-session-secret-change-me-32")
    cookie_secure: bool = False
    cookie_samesite: Literal["lax", "strict", "none"] = "lax"
    session_idle_minutes: int = Field(default=7 * 24 * 60, ge=5, le=30 * 24 * 60)
    session_absolute_days: int = Field(default=30, ge=1, le=30)
    # Cost of hashing a password: scrypt works on 2**n blocks. Tests lower it to stay fast.
    password_scrypt_log2_n: int = Field(default=15, ge=4, le=17)
    auth_session_rate_limit: int = Field(default=10, ge=1, le=1000)
    auth_session_rate_window_seconds: int = Field(default=60, ge=1, le=3600)
    # The endpoints that take or send a one-time code share their own limit, same window.
    auth_code_rate_limit: int = Field(default=20, ge=1, le=1000)
    # One-time codes emailed for sign-up and password reset.
    otp_ttl_minutes: int = Field(default=10, ge=1, le=60)
    otp_max_attempts: int = Field(default=5, ge=1, le=10)
    otp_resend_cooldown_seconds: int = Field(default=60, ge=0, le=3600)
    otp_max_sends_per_hour: int = Field(default=5, ge=1, le=20)
    # Wrong codes, counted across reissued codes, before the purpose is locked.
    otp_lock_after_failures: int = Field(default=10, ge=5, le=50)
    otp_lock_minutes: int = Field(default=60, ge=5, le=1440)
    request_max_body_bytes: int = Field(default=1_048_576, ge=1024, le=10_485_760)
    # Uploaded and produced files live in a file store: a local directory or a Cloudflare R2 bucket.
    storage_backend: Literal["local", "r2"] = "local"
    storage_local_root: str = "var/storage"
    r2_account_id: str | None = None
    # Only for buckets with a jurisdiction (EU, FedRAMP), whose endpoint differs from the default.
    r2_endpoint_url: str | None = None
    r2_bucket: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: SecretStr | None = None
    dataset_max_upload_bytes: int = Field(default=52_428_800, ge=1024, le=1_073_741_824)
    artifact_max_upload_bytes: int = Field(default=52_428_800, ge=1024, le=1_073_741_824)
    project_file_max_upload_bytes: int = Field(default=52_428_800, ge=1024, le=1_073_741_824)
    avatar_max_upload_bytes: int = Field(default=4_194_304, ge=65_536, le=5_242_880)
    # Popper is a separate service. Runs cannot start until its base URL is configured.
    popper_base_url: str | None = None
    popper_service_key: SecretStr | None = None
    popper_callback_key: SecretStr | None = None
    popper_timeout_seconds: float = Field(default=30, ge=1, le=300)
    # Address Popper uses to call this API back, without the API prefix.
    public_base_url: str = "http://localhost:8080"
    run_default_budget_usd: Decimal = Field(default=Decimal("5"), gt=0)
    run_max_budget_usd: Decimal = Field(default=Decimal("20"), gt=0)
    # Email goes through Resend. Without a key, local and test write each message to the log.
    resend_api_key: SecretStr | None = None
    email_from: str = "AI Research Platform <no-reply@beyond8.io.vn>"
    email_timeout_seconds: float = Field(default=10, ge=1, le=30)
    # Address of the frontend, put in emails as the sign-in link. No link when empty.
    app_url: str | None = None
    # Sign-in with Google. Off unless these three and app_url are all set. The redirect URI
    # is this API's /auth/google/callback, exactly as registered in the Google Cloud console.
    google_oauth_client_id: str | None = None
    google_oauth_client_secret: SecretStr | None = None
    google_oauth_redirect_uri: str | None = None
    # Google Sheets and Drive connections use the same client with a redirect URI of their
    # own: this API's /connections/google/callback. Off unless it, the client, app_url and
    # connection_secret_key are all set.
    google_oauth_connections_redirect_uri: str | None = None
    # Public browser key and Cloud project number for the Google Drive file picker.
    google_picker_api_key: SecretStr | None = None
    google_picker_app_id: str | None = Field(default=None, pattern=r"^\d+$")
    # How long an admin waits before sending a user's sign-in details again.
    invite_resend_cooldown_seconds: int = Field(default=60, ge=0, le=3600)
    # How long a project invitation can be accepted after it was last sent.
    project_invite_ttl_hours: int = Field(default=24, ge=1, le=168)
    # A Platform Admin created at startup when both are set. For development only: the
    # password is a known value, so production refuses these settings.
    default_admin_email: str | None = None
    default_admin_password: SecretStr | None = None
    # Data connections to external databases. Without a key the feature is off: nothing can be
    # stored or tested, but existing rows can still be listed.
    connection_secret_key: SecretStr | None = None
    # Lets a connection point at a loopback or private address. Local development and tests only.
    connection_allow_private_hosts: bool = False
    connection_connect_timeout_seconds: float = Field(default=10, ge=1, le=60)
    connection_query_timeout_seconds: int = Field(default=60, ge=1, le=600)
    # An import reads a whole table, so it gets longer than one query does.
    connection_import_timeout_seconds: int = Field(default=300, ge=1, le=3600)
    connection_max_concurrent_queries: int = Field(default=4, ge=1, le=32)
    # How many of those one project, and one user, may hold: a slow server cannot take them all.
    connection_max_concurrent_per_owner: int = Field(default=2, ge=1, le=32)
    # Calls to external databases one user may start per auth_session_rate_window_seconds.
    connection_probe_rate_limit: int = Field(default=30, ge=1, le=1000)
    # Reads through a saved connection (browsing, previews) one user may start per window.
    connection_query_rate_limit: int = Field(default=120, ge=1, le=10000)
    connection_preview_max_rows: int = Field(default=100, ge=1, le=1000)
    # The most one BigQuery query may scan, and so be billed for, in bytes. BigQuery bills at
    # least 10 MiB for each table a query reads, so a lower limit would refuse every query.
    connection_bigquery_max_bytes_billed: int = Field(default=1024**3, ge=10 * 1024**2)

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

    @field_validator(
        "popper_base_url",
        "popper_service_key",
        "popper_callback_key",
        "r2_account_id",
        "r2_endpoint_url",
        "r2_bucket",
        "r2_access_key_id",
        "r2_secret_access_key",
        "resend_api_key",
        "app_url",
        "google_oauth_client_id",
        "google_oauth_client_secret",
        "google_oauth_redirect_uri",
        "google_oauth_connections_redirect_uri",
        "google_picker_api_key",
        "google_picker_app_id",
        "default_admin_email",
        "default_admin_password",
        "connection_secret_key",
        mode="before",
    )
    @classmethod
    def normalize_blank_optional_setting(cls, value: object) -> object:
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
        if self.storage_backend == "r2" and not (
            self.r2_bucket
            and self.r2_access_key_id
            and self.r2_secret_access_key
            and (self.r2_account_id or self.r2_endpoint_url)
        ):
            raise ValueError(
                "storage_backend=r2 requires r2_bucket, r2_access_key_id, r2_secret_access_key, "
                "and r2_account_id (or r2_endpoint_url)"
            )
        if self.app_env in ("staging", "production") and self.resend_api_key is None:
            raise ValueError("resend_api_key is required in staging and production")
        if (self.default_admin_email is None) != (self.default_admin_password is None):
            raise ValueError("default_admin_email and default_admin_password go together")
        if self.default_admin_email and self.app_env in ("staging", "production"):
            raise ValueError("A default admin account is for local development only")
        if self.connection_secret_key:
            try:
                Fernet(self.connection_secret_key.get_secret_value().encode())
            except ValueError:
                raise ValueError(
                    "connection_secret_key must be a Fernet key (32 url-safe base64 bytes)"
                ) from None
        if self.connection_allow_private_hosts and self.app_env in ("staging", "production"):
            raise ValueError("connection_allow_private_hosts is for local development only")
        if self.cookie_samesite == "none" and not self.cookie_secure:
            raise ValueError("SameSite=None cookies require Secure")
        if self.app_env == "production":
            if not self.cookie_secure:
                raise ValueError("Production requires secure session cookies")
            if self.session_signing_secret.get_secret_value().startswith("local-only-"):
                raise ValueError("Production requires a non-development session signing secret")
            if self.password_scrypt_log2_n < 15:
                raise ValueError("Production requires password_scrypt_log2_n of at least 15")
            if any(not origin.startswith("https://") for origin in self.allowed_origins):
                raise ValueError("Production CORS origins must use HTTPS")
            if self.app_url and not self.app_url.startswith("https://"):
                raise ValueError("Production app_url must use HTTPS")
            for key in (self.popper_service_key, self.popper_callback_key):
                if key is not None and len(key.get_secret_value()) < 32:
                    raise ValueError("Production Popper service keys need at least 32 characters")
        return self

    @property
    def r2_endpoint(self) -> str:
        return (
            self.r2_endpoint_url or f"https://{self.r2_account_id}.r2.cloudflarestorage.com"
        ).rstrip("/")

    @property
    def allowed_origins(self) -> list[str]:
        return [origin.strip().rstrip("/") for origin in self.cors_allowed_origins.split(",")]


@lru_cache
def get_settings() -> Settings:
    return Settings()

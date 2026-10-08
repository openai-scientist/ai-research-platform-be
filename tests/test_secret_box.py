import pytest
from cryptography.fernet import Fernet
from pydantic import ValidationError

from platform_be.core.config import Settings
from platform_be.services.secret_box import SecretBox, SecretBoxError
from tests.conftest import CONNECTION_KEY


def test_a_sealed_secret_opens_only_with_the_same_key() -> None:
    box = SecretBox(CONNECTION_KEY)
    secret = {"password": "p@ss wörd"}

    sealed = box.seal(secret)

    assert "p@ss" not in sealed
    assert box.open(sealed) == secret
    # Each seal uses a fresh IV, so equal secrets do not produce equal rows.
    assert box.seal(secret) != sealed
    other = SecretBox(Fernet.generate_key().decode())
    with pytest.raises(SecretBoxError):
        other.open(sealed)
    with pytest.raises(SecretBoxError):
        box.open("not-a-token")


def test_connection_settings_are_checked_at_startup(monkeypatch) -> None:
    monkeypatch.delenv("CONNECTION_SECRET_KEY", raising=False)
    monkeypatch.delenv("CONNECTION_ALLOW_PRIVATE_HOSTS", raising=False)
    assert Settings(_env_file=None).connection_secret_key is None
    assert Settings(_env_file=None, connection_secret_key=CONNECTION_KEY).connection_secret_key
    # A blank value from an env file means "not configured", not a bad key.
    assert Settings(_env_file=None, connection_secret_key="  ").connection_secret_key is None
    with pytest.raises(ValidationError, match="must be a Fernet key"):
        Settings(_env_file=None, connection_secret_key="too-short")

    deployed = {
        "cookie_secure": True,
        "session_signing_secret": "a-real-session-signing-secret-of-40-chars",
        "cors_allowed_origins": "https://app.example.com",
        "resend_api_key": "re_key",
    }
    assert Settings(_env_file=None, app_env="test", connection_allow_private_hosts=True)
    for env in ("staging", "production"):
        with pytest.raises(ValidationError, match="local development only"):
            Settings(_env_file=None, app_env=env, connection_allow_private_hosts=True, **deployed)

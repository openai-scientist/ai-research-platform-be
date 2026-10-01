from pathlib import Path
from types import SimpleNamespace

import firebase_admin
import pytest
from pydantic import SecretStr, ValidationError

from platform_be.auth import tokens
from platform_be.core.config import Settings


def test_firebase_service_account_fields_must_be_configured_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("FIREBASE_CLIENT_EMAIL", "FIREBASE_PRIVATE_KEY"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValidationError, match="must be configured together"):
        Settings(firebase_client_email="firebase-admin@example.iam.gserviceaccount.com")


def test_settings_load_firebase_service_account_from_local_environment_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("FIREBASE_PROJECT_ID", "FIREBASE_CLIENT_EMAIL", "FIREBASE_PRIVATE_KEY"):
        monkeypatch.delenv(name, raising=False)
    local_env = tmp_path / f"{chr(46)}env.local"
    local_env.write_text(
        "FIREBASE_PROJECT_ID=ai-research-platform-4ceb9\n"
        "FIREBASE_CLIENT_EMAIL=firebase-admin@example.iam.gserviceaccount.com\n"
        "FIREBASE_PRIVATE_KEY='line-one\\nline-two'\n",
        encoding="utf-8",
    )

    settings = Settings()

    assert settings.firebase_project_id == "ai-research-platform-4ceb9"
    assert settings.firebase_client_email == "firebase-admin@example.iam.gserviceaccount.com"
    assert settings.firebase_private_key is not None
    assert settings.firebase_private_key.get_secret_value() == "line-one\\nline-two"


def test_firebase_admin_uses_service_account_settings_and_expands_newlines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        firebase_project_id="ai-research-platform-4ceb9",
        firebase_client_email="firebase-admin@example.iam.gserviceaccount.com",
        firebase_private_key=SecretStr(
            "-----BEGIN PRIVATE KEY-----\\nprivate-key-body\\n-----END PRIVATE KEY-----\\n"
        ),
    )
    app = SimpleNamespace(name="fake-firebase-app")
    captured: dict[str, object] = {}
    credential_sentinel = object()

    def get_app(name: str) -> object:
        assert name == settings.firebase_project_id
        raise ValueError("No app")

    def certificate(service_account: dict[str, str]) -> object:
        captured["service_account"] = service_account
        return credential_sentinel

    def initialize_app(credential: object, *, options: dict[str, str], name: str) -> object:
        captured["credential"] = credential
        captured["options"] = options
        captured["name"] = name
        return app

    monkeypatch.setattr(firebase_admin, "get_app", get_app)
    monkeypatch.setattr(firebase_admin, "initialize_app", initialize_app)
    monkeypatch.setattr(tokens.credentials, "Certificate", certificate)

    verifier = tokens.FirebaseTokenVerifier(settings)

    assert verifier._get_app() is app
    service_account = captured["service_account"]
    assert isinstance(service_account, dict)
    assert service_account["project_id"] == "ai-research-platform-4ceb9"
    assert service_account["client_email"] == "firebase-admin@example.iam.gserviceaccount.com"
    assert service_account["private_key"] == (
        "-----BEGIN PRIVATE KEY-----\nprivate-key-body\n-----END PRIVATE KEY-----\n"
    )
    assert captured["credential"] is credential_sentinel

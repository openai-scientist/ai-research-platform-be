import json
import logging

import httpx
import pytest
from pydantic import ValidationError

from platform_be.core.config import Settings
from platform_be.services.email_sender import (
    RESEND_URL,
    LogEmailSender,
    ResendEmailSender,
    build_email_sender,
)
from tests.conftest import Harness

API_KEY = "re_test_key_that_must_never_be_logged"
SENDER = "AI Research Platform <no-reply@beyond8.io.vn>"
MESSAGE = {
    "to": "dat@example.com",
    "subject": "Your account",
    "text": "Sign in with dat",
    "html": "<p>Sign in with dat</p>",
}


def resend(handler) -> ResendEmailSender:
    return ResendEmailSender(
        API_KEY, SENDER, timeout_seconds=1, transport=httpx.MockTransport(handler)
    )


def settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)


@pytest.mark.asyncio
async def test_resend_gets_the_message_and_the_key() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"id": "49a3999c"})

    assert await resend(handler).send(**MESSAGE) is True
    (request,) = seen
    assert request.method == "POST"
    assert str(request.url) == RESEND_URL
    assert request.headers["authorization"] == f"Bearer {API_KEY}"
    assert json.loads(request.content) == {
        "from": SENDER,
        "to": ["dat@example.com"],
        "subject": "Your account",
        "text": "Sign in with dat",
        "html": "<p>Sign in with dat</p>",
    }


@pytest.mark.asyncio
async def test_a_failed_send_is_logged_without_the_message(caplog) -> None:
    def refused(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            json={"name": "validation_error", "message": "Invalid `to`: dat@example.com"},
        )

    def broken(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="<html>dat@example.com</html>")

    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    with caplog.at_level(logging.INFO):
        for handler in (refused, broken, unreachable):
            assert await resend(handler).send(**MESSAGE) is False

    errors = [record for record in caplog.records if record.name == "platform_be.email"]
    assert [record.levelname for record in errors] == ["ERROR"] * 3
    messages = [record.getMessage() for record in errors]
    assert messages == [
        "email was refused: status 422, error validation_error",
        "email was refused: status 500, error unknown",
        "email was not sent: ConnectTimeout",
    ]
    # Neither the recipient, the message nor the key reaches the log, from any logger.
    logged = caplog.text
    for secret in ("dat@example.com", "Sign in with dat", API_KEY):
        assert secret not in logged


@pytest.mark.asyncio
async def test_without_a_key_the_message_goes_to_the_log(caplog) -> None:
    sender = build_email_sender(settings(resend_api_key="  "))
    assert isinstance(sender, LogEmailSender)
    with caplog.at_level(logging.INFO, logger="platform_be.email"):
        assert await sender.send(**MESSAGE) is True
    assert "dat@example.com" in caplog.text
    assert "Sign in with dat" in caplog.text

    assert isinstance(build_email_sender(settings(resend_api_key=API_KEY)), ResendEmailSender)


def test_deployed_settings_need_the_key_and_a_safe_link() -> None:
    deployed = {
        "cookie_secure": True,
        "session_signing_secret": "a-real-session-signing-secret-of-40-chars",
        "cors_allowed_origins": "https://app.example.com",
    }
    for env in ("staging", "production"):
        with pytest.raises(ValidationError, match="resend_api_key is required"):
            settings(app_env=env, **deployed)
        assert settings(app_env=env, resend_api_key=API_KEY, **deployed).app_url is None

    with pytest.raises(ValidationError, match="app_url must use HTTPS"):
        settings(
            app_env="production",
            resend_api_key=API_KEY,
            app_url="http://app.example.com",
            **deployed,
        )
    production = settings(
        app_env="production", resend_api_key=API_KEY, app_url="https://app.example.com", **deployed
    )
    assert production.app_url == "https://app.example.com"
    # A blank value from Compose means "not set".
    assert settings(app_url="").app_url is None
    assert API_KEY not in repr(production)


def test_the_app_gets_a_sender(harness: Harness) -> None:
    assert harness.app.state.email_sender is harness.emails

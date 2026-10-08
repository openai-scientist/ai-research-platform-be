"""Transactional email: sent through Resend, or written to the log on a developer machine.

Callers commit their transaction first and send afterwards, so a message never describes
something the database then rolled back.
"""

import asyncio
import logging
from typing import Protocol

import httpx
from fastapi import Request

from platform_be.core.config import Settings

logger = logging.getLogger("platform_be.email")

RESEND_URL = "https://api.resend.com/emails"


class EmailSender(Protocol):
    async def send(self, *, to: str, subject: str, text: str, html: str) -> bool:
        """Send one message. True when it was accepted; never raises."""


class ResendEmailSender:
    def __init__(
        self,
        api_key: str,
        sender: str,
        *,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._headers = {"Authorization": f"Bearer {api_key}"}
        self._sender = sender
        self._timeout = timeout_seconds
        self._transport = transport

    async def send(self, *, to: str, subject: str, text: str, html: str) -> bool:
        payload = {
            "from": self._sender,
            "to": [to],
            "subject": subject,
            "text": text,
            "html": html,
        }
        try:
            async with (
                asyncio.timeout(self._timeout),
                httpx.AsyncClient(
                    headers=self._headers, timeout=self._timeout, transport=self._transport
                ) as client,
            ):
                response = await client.post(RESEND_URL, json=payload)
        except Exception as exc:
            # Callers have already committed, so nothing may escape from here. The recipient
            # and the message stay out of the log.
            logger.error("email was not sent: %s", type(exc).__name__)
            return False
        if response.is_success:
            return True
        logger.error(
            "email was refused: status %s, error %s", response.status_code, _error_name(response)
        )
        return False


def _error_name(response: httpx.Response) -> str:
    """The provider's short error name, never its free text, which may quote the request."""
    try:
        name = response.json().get("name")
    except Exception:
        return "unknown"
    if isinstance(name, str) and len(name) <= 64 and name.isascii():
        if name.replace("_", "").isalnum():
            return name
    return "unknown"


class LogEmailSender:
    """Writes the message to the log instead of sending it; for local work without a key."""

    async def send(self, *, to: str, subject: str, text: str, html: str) -> bool:
        logger.info("email not sent, no provider key: to %s, subject %s\n%s", to, subject, text)
        return True


def build_email_sender(settings: Settings) -> EmailSender:
    if settings.resend_api_key is None:
        # Settings validation allows a missing key only in local and test.
        logger.info("email sender: log")
        return LogEmailSender()
    logger.info("email sender: resend")
    return ResendEmailSender(
        settings.resend_api_key.get_secret_value(),
        settings.email_from,
        timeout_seconds=settings.email_timeout_seconds,
    )


def get_email_sender(request: Request) -> EmailSender:
    return request.app.state.email_sender

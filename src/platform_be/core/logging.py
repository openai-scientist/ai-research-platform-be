import json
import logging
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    """Emit structured records without serializing request contents."""

    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("request_id", "method", "path", "status_code", "duration_ms"):
            if hasattr(record, key):
                data[key] = getattr(record, key)
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data, ensure_ascii=False, default=str)


# Where Google sends the browser back with a one-use code: sign-in, and Drive access.
_GOOGLE_CALLBACKS = ("/auth/google/callback", "/connections/google/callback")


class _HideGoogleCallbackQuery(logging.Filter):
    """Cut the query string from the server's access line for the Google callbacks.

    Uvicorn prints the full request target, and those carry Google's one-use code.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if (
            isinstance(args, tuple)
            and len(args) == 5
            and isinstance(args[2], str)
            and any(path in args[2] for path in _GOOGLE_CALLBACKS)
        ):
            record.args = (*args[:2], args[2].split("?", 1)[0], *args[3:])
        return True


class _HideOutgoingQuery(logging.Filter):
    """Cut the query string and the server's own words from the line httpx logs per request.

    The query can hold what a user asked an outside server for, and the reason phrase is
    whatever that server chose to send.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and len(args) == 5 and isinstance(args[3], int):
            url = str(args[1]).split("?", 1)[0]
            record.args = (args[0], url, args[2], args[3], "")
        return True


def hide_outgoing_queries() -> None:
    outgoing = logging.getLogger("httpx")
    if not any(isinstance(item, _HideOutgoingQuery) for item in outgoing.filters):
        outgoing.addFilter(_HideOutgoingQuery())


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    # The storage SDK logs every request at debug level, signed headers included.
    for name in ("boto3", "botocore", "s3transfer", "urllib3", "httpx", "httpcore", "hpack"):
        logging.getLogger(name).setLevel(logging.INFO)
    hide_outgoing_queries()
    access = logging.getLogger("uvicorn.access")
    if not any(isinstance(item, _HideGoogleCallbackQuery) for item in access.filters):
        access.addFilter(_HideGoogleCallbackQuery())

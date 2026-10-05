from dataclasses import dataclass


@dataclass(slots=True)
class APIError(Exception):
    status_code: int
    code: str
    message: str
    # The session cookie is dead; tell the browser to drop it with the error response.
    clear_session_cookie: bool = False
    # Seconds the caller should wait; sent as the Retry-After header.
    retry_after: int | None = None

    def __str__(self) -> str:
        return self.message

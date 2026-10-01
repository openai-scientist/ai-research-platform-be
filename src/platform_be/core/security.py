import hashlib
import hmac
import secrets


def new_session_secret() -> str:
    return secrets.token_urlsafe(48)


def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def csrf_token(session_secret: str, signing_secret: str) -> str:
    return hmac.new(
        signing_secret.encode("utf-8"), session_secret.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def csrf_matches(session_secret: str, signing_secret: str, candidate: str | None) -> bool:
    if not candidate:
        return False
    expected = csrf_token(session_secret, signing_secret)
    return hmac.compare_digest(expected, candidate)

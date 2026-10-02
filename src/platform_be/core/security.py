import base64
import hashlib
import hmac
import secrets


def new_session_secret() -> str:
    return secrets.token_urlsafe(48)


def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def hash_password(password: str, log2_n: int) -> str:
    """Hash a password with scrypt. The result carries its own salt and cost parameters."""
    salt = secrets.token_bytes(16)
    derived = _scrypt(password, salt, log2_n, _SCRYPT_R, _SCRYPT_P)
    encoded = (base64.b64encode(part).decode("ascii") for part in (salt, derived))
    return "$".join(("scrypt", str(log2_n), str(_SCRYPT_R), str(_SCRYPT_P), *encoded))


def verify_password(password: str, stored: str | None) -> bool:
    """Check a password against a stored hash, in constant time. No hash matches nothing."""
    try:
        scheme, log2_n, r, p, salt, expected = (stored or "").split("$")
        if scheme != "scrypt":
            return False
        derived = _scrypt(password, base64.b64decode(salt), int(log2_n), int(r), int(p))
        return hmac.compare_digest(derived, base64.b64decode(expected))
    except ValueError:
        return False


_SCRYPT_R = 8
_SCRYPT_P = 3


def _scrypt(password: str, salt: bytes, log2_n: int, r: int, p: int) -> bytes:
    return hashlib.scrypt(
        password.encode("utf-8"), salt=salt, n=2**log2_n, r=r, p=p, maxmem=2**28, dklen=32
    )


def csrf_token(session_secret: str, signing_secret: str) -> str:
    return hmac.new(
        signing_secret.encode("utf-8"), session_secret.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def service_key_matches(expected: str | None, candidate: str | None) -> bool:
    """Compare a shared service key in constant time. No configured key matches nothing."""
    if not expected or not candidate:
        return False
    return hmac.compare_digest(expected.encode("utf-8"), candidate.encode("utf-8"))


def csrf_matches(session_secret: str, signing_secret: str, candidate: str | None) -> bool:
    if not candidate:
        return False
    expected = csrf_token(session_secret, signing_secret)
    return hmac.compare_digest(expected, candidate)

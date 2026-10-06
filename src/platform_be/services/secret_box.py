import json
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from fastapi import Request


class SecretBoxError(Exception):
    """A stored secret cannot be opened, for example after the key was replaced."""


class SecretBox:
    """Encrypts the credentials of a data connection before they reach the database."""

    def __init__(self, key: str) -> None:
        self._fernet = Fernet(key.encode())

    def seal(self, secret: dict[str, Any]) -> str:
        return self._fernet.encrypt(json.dumps(secret).encode()).decode()

    def open(self, ciphertext: str) -> dict[str, Any]:
        try:
            return json.loads(self._fernet.decrypt(ciphertext.encode()))
        except InvalidToken as exc:
            raise SecretBoxError("The stored secret cannot be decrypted") from exc


def get_secret_box(request: Request) -> SecretBox | None:
    """None until CONNECTION_SECRET_KEY is set; routes that store credentials answer 503."""
    return request.app.state.secret_box

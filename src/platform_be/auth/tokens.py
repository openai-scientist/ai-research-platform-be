from typing import Protocol

import firebase_admin
from firebase_admin import auth, credentials
from firebase_admin.exceptions import FirebaseError
from google.auth.exceptions import GoogleAuthError

from platform_be.core.config import Settings


class TokenVerifier(Protocol):
    def verify(self, id_token: str) -> dict[str, object]: ...


class FirebaseTokenRejected(Exception):
    """The presented token is invalid, expired, revoked, or belongs to a disabled user."""


class FirebaseUnavailable(Exception):
    """Firebase could not be reached to verify the presented token."""


class FirebaseTokenVerifier:
    """Verify Firebase ID tokens; Firebase Admin is initialized on first use."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._app: firebase_admin.App | None = None

    def _get_app(self) -> firebase_admin.App:
        if self._app is not None:
            return self._app
        try:
            self._app = firebase_admin.get_app(self.settings.firebase_project_id)
        except ValueError:
            try:
                credential = self._service_account_credential()
                self._app = firebase_admin.initialize_app(
                    credential,
                    options={"projectId": self.settings.firebase_project_id},
                    name=self.settings.firebase_project_id,
                )
            except (FirebaseError, GoogleAuthError, ValueError) as exc:
                raise FirebaseUnavailable from exc
        return self._app

    def _service_account_credential(self) -> credentials.Base:
        if not self.settings.firebase_client_email or not self.settings.firebase_private_key:
            return credentials.ApplicationDefault()
        private_key = self.settings.firebase_private_key.get_secret_value().replace("\\n", "\n")
        return credentials.Certificate(
            {
                "type": "service_account",
                "project_id": self.settings.firebase_project_id,
                "private_key": private_key,
                "client_email": self.settings.firebase_client_email,
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        )

    def verify(self, id_token: str) -> dict[str, object]:
        try:
            return auth.verify_id_token(id_token, check_revoked=True, app=self._get_app())
        except (
            auth.InvalidIdTokenError,
            auth.ExpiredIdTokenError,
            auth.RevokedIdTokenError,
            auth.UserDisabledError,
            auth.UserNotFoundError,
        ) as exc:
            raise FirebaseTokenRejected from exc
        except (FirebaseError, GoogleAuthError) as exc:
            raise FirebaseUnavailable from exc

"""The emails about signing in: one-time codes and the notice of a changed password.

Each function returns ``(subject, text, html)``. Anyone can make the Platform send these
to any address, so they carry no text a user typed: the greeting is the address itself.
A code is in the body only, never in the subject, which shows in notifications.
"""

from datetime import datetime

from platform_be.services.invite_emails import PRODUCT, _Message, _render, local_time

DO_NOT_SHARE = f"Do not share this code with anyone. The {PRODUCT} never asks for it."


def _until(expires_at: datetime, minutes: int) -> str:
    return f"{local_time(expires_at)}, in {minutes} minutes"


def verification_code(
    email: str, code: str, expires_at: datetime, minutes: int
) -> tuple[str, str, str]:
    text, html = _render(
        _Message(
            greeting_name=email,
            intro=f"Use the code below to confirm your email address on the {PRODUCT}.",
            details=[("Verification code", code), ("Valid until", _until(expires_at, minutes))],
            steps=[
                "Go back to the page that asked for the code.",
                "Enter the 6 digits. The code works once.",
            ],
            notes=[DO_NOT_SHARE],
            reason=(
                f"You received this email because an account for {email} is waiting for "
                "its email to be confirmed. If that was not you, ignore this email: the "
                "account cannot be used without the code."
            ),
        )
    )
    return f"Your {PRODUCT} verification code", text, html


def password_reset_code(
    email: str, code: str, expires_at: datetime, minutes: int
) -> tuple[str, str, str]:
    text, html = _render(
        _Message(
            greeting_name=email,
            intro=f"Use the code below to set a new password on the {PRODUCT}.",
            details=[("Password reset code", code), ("Valid until", _until(expires_at, minutes))],
            steps=[
                "Go back to the page that asked for the code.",
                "Enter the 6 digits and your new password. The code works once.",
                "Sign in with the new password. Every other device is signed out.",
            ],
            notes=[DO_NOT_SHARE],
            reason=(
                f"You received this email because a password reset was requested for {email}. "
                "If that was not you, ignore this email: your password stays the same."
            ),
        )
    )
    return f"Your {PRODUCT} password reset code", text, html


def password_changed(email: str, changed_at: datetime) -> tuple[str, str, str]:
    text, html = _render(
        _Message(
            greeting_name=email,
            intro=f"The password of your {PRODUCT} account was changed.",
            details=[("Account", email), ("Changed at", local_time(changed_at))],
            steps=[
                "If you changed it yourself, you do not need to do anything.",
                'If you did not, choose "Forgot password" on the sign-in page to set a new '
                "password, and tell an administrator.",
            ],
            reason=f"You received this email because the password for {email} was changed.",
        )
    )
    return f"Your {PRODUCT} password was changed", text, html

"""One-time codes sent by email: issuing them, checking them, and the limits on both.

Every function expects the caller to hold the lock on the user row, so two requests for
one user never run here at the same time. The caller also owns the transaction: after a
failed check it must commit before raising, or the counters are rolled back with the error.
"""

import hmac
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import as_utc
from platform_be.core.config import Settings
from platform_be.core.security import new_otp_code, new_session_secret, otp_digest, token_digest
from platform_be.models.identity import EmailOtp, User

SEND_WINDOW = timedelta(hours=1)


class OtpPurpose(StrEnum):
    VERIFY_EMAIL = "verify_email"
    RESET_PASSWORD = "reset_password"


async def _locked_row(db: AsyncSession, user: User, purpose: OtpPurpose) -> EmailOtp | None:
    return await db.scalar(
        select(EmailOtp)
        .where(EmailOtp.user_id == user.id, EmailOtp.purpose == purpose)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def _digest(settings: Settings, user: User, purpose: OtpPurpose, code: str) -> str:
    return otp_digest(settings.session_signing_secret.get_secret_value(), purpose, user.id, code)


async def issue_code(
    db: AsyncSession,
    settings: Settings,
    user: User,
    purpose: OtpPurpose,
    *,
    now: datetime | None = None,
) -> str | None:
    """Store a new code and return it, or None when no code may be sent right now.

    None means the purpose is locked, the last code went out a moment ago, or this hour's
    codes are used up. A new code replaces the previous one.
    """
    now = now or datetime.now(UTC)
    row = await _locked_row(db, user, purpose)
    if row is None:
        row = EmailOtp(
            user_id=user.id,
            purpose=purpose,
            expires_at=now,
            sent_at=now,
            send_window_started_at=now,
            attempts=0,
            failed_attempts=0,
            send_count=0,
        )
        db.add(row)
    else:
        if row.locked_until is not None and as_utc(row.locked_until) > now:
            return None
        waited = (now - as_utc(row.sent_at)).total_seconds()
        if waited < settings.otp_resend_cooldown_seconds:
            return None
        if now - as_utc(row.send_window_started_at) >= SEND_WINDOW:
            row.send_window_started_at = now
            row.send_count = 0
        if row.send_count >= settings.otp_max_sends_per_hour:
            return None
    code = new_otp_code()
    row.code_digest = _digest(settings, user, purpose, code)
    row.reset_token_digest = None
    row.reset_token_expires_at = None
    row.expires_at = now + timedelta(minutes=settings.otp_ttl_minutes)
    row.attempts = 0
    row.sent_at = now
    row.send_count += 1
    await db.flush()
    return code


async def check_code(
    db: AsyncSession,
    settings: Settings,
    user: User,
    purpose: OtpPurpose,
    code: str,
    *,
    now: datetime | None = None,
) -> bool:
    """True once for the right code. Every check is counted, right or wrong."""
    now = now or datetime.now(UTC)
    row = await _locked_row(db, user, purpose)
    if (
        row is None
        or row.code_digest is None
        or as_utc(row.expires_at) <= now
        or (row.locked_until is not None and as_utc(row.locked_until) > now)
        or row.attempts >= settings.otp_max_attempts
    ):
        return False
    row.attempts += 1
    matches = hmac.compare_digest(row.code_digest, _digest(settings, user, purpose, code))
    if matches:
        row.code_digest = None
        row.failed_attempts = 0
    else:
        row.failed_attempts += 1
        if row.failed_attempts >= settings.otp_lock_after_failures:
            row.locked_until = now + timedelta(minutes=settings.otp_lock_minutes)
            row.code_digest = None
            row.failed_attempts = 0
    await db.flush()
    return matches


async def clear_code(db: AsyncSession, user: User, purpose: OtpPurpose) -> None:
    """Withdraw the current code. The counters stay."""
    await db.execute(
        update(EmailOtp)
        .where(EmailOtp.user_id == user.id, EmailOtp.purpose == purpose)
        .values(code_digest=None, reset_token_digest=None, reset_token_expires_at=None)
        .execution_options(synchronize_session=False)
    )


async def verify_reset_code(
    db: AsyncSession, settings: Settings, user: User, code: str
) -> str | None:
    """Consume the OTP and grant a short-lived, one-use password reset token."""
    if not await check_code(db, settings, user, OtpPurpose.RESET_PASSWORD, code):
        return None
    row = await _locked_row(db, user, OtpPurpose.RESET_PASSWORD)
    token = new_session_secret()
    row.reset_token_digest = token_digest(token)
    row.reset_token_expires_at = datetime.now(UTC) + timedelta(minutes=settings.otp_ttl_minutes)
    await db.flush()
    return token


async def consume_reset_token(db: AsyncSession, user: User, token: str) -> bool:
    """Consume a verified reset grant while the caller holds the user lock."""
    row = await _locked_row(db, user, OtpPurpose.RESET_PASSWORD)
    if (
        row is None
        or row.reset_token_digest is None
        or row.reset_token_expires_at is None
        or as_utc(row.reset_token_expires_at) <= datetime.now(UTC)
        or not hmac.compare_digest(row.reset_token_digest, token_digest(token))
    ):
        return False
    row.reset_token_digest = None
    row.reset_token_expires_at = None
    await db.flush()
    return True

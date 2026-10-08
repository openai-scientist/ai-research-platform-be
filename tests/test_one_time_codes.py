from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from platform_be.models.identity import EmailOtp, User
from platform_be.services.one_time_codes import OtpPurpose, check_code, clear_code, issue_code
from tests.conftest import Harness

START = datetime(2026, 10, 5, 9, 0, tzinfo=UTC)
VERIFY, RESET = OtpPurpose.VERIFY_EMAIL, OtpPurpose.RESET_PASSWORD


async def new_user(harness: Harness, email: str = "owner@example.com") -> User:
    async with harness.factory() as db:
        user = User(email=email, email_normalized=email)
        db.add(user)
        await db.commit()
    return user


async def issue(harness: Harness, user: User, at: datetime, purpose=VERIFY) -> str | None:
    """Each call uses its own session, so every row is read back as the database returns it."""
    async with harness.factory() as db:
        code = await issue_code(db, harness.settings, user, purpose, now=at)
        await db.commit()
    return code


async def check(harness: Harness, user: User, code: str, at: datetime, purpose=VERIFY) -> bool:
    async with harness.factory() as db:
        result = await check_code(db, harness.settings, user, purpose, code, now=at)
        await db.commit()
    return result


async def stored(harness: Harness, user: User, purpose=VERIFY) -> EmailOtp:
    async with harness.factory() as db:
        return await db.scalar(
            select(EmailOtp).where(EmailOtp.user_id == user.id, EmailOtp.purpose == purpose)
        )


def wrong(code: str) -> str:
    return f"{(int(code) + 1) % 10**6:06d}"


def minutes(count: float) -> datetime:
    return START + timedelta(minutes=count)


@pytest.mark.asyncio
async def test_a_code_is_six_digits_and_works_once(harness: Harness) -> None:
    user = await new_user(harness)
    code = await issue(harness, user, START)
    assert code is not None and len(code) == 6 and code.isdigit()
    row = await stored(harness, user)
    # What is stored is a digest: the code cannot be read from the database.
    assert len(row.code_digest) == 64 and code not in row.code_digest

    assert await check(harness, user, code, minutes(1)) is True
    assert await check(harness, user, code, minutes(1)) is False
    # The row stays, so the limits keep applying: no second code inside the cooldown.
    assert (await stored(harness, user)).code_digest is None
    assert await issue(harness, user, START + timedelta(seconds=59)) is None
    assert await issue(harness, user, START + timedelta(seconds=60)) is not None


@pytest.mark.asyncio
async def test_a_code_expires_and_is_spent_after_five_checks(harness: Harness) -> None:
    user = await new_user(harness)
    code = await issue(harness, user, START)
    assert await check(harness, user, code, minutes(10)) is False
    assert await check(harness, user, code, minutes(9.9)) is True

    code = await issue(harness, user, minutes(20))
    for _ in range(5):
        assert await check(harness, user, wrong(code), minutes(21)) is False
    # The sixth check fails even with the right code.
    assert await check(harness, user, code, minutes(21)) is False
    assert (await stored(harness, user)).failed_attempts == 5


@pytest.mark.asyncio
async def test_a_new_code_replaces_the_old_one_and_sends_are_capped(harness: Harness) -> None:
    user = await new_user(harness)
    first = await issue(harness, user, START)
    assert await check(harness, user, wrong(first), START) is False
    second = await issue(harness, user, minutes(2))
    assert second is not None
    row = await stored(harness, user)
    # Wrong checks are remembered across codes; checks against the code start again.
    assert (row.failed_attempts, row.attempts, row.send_count) == (1, 0, 2)
    if first != second:
        assert await check(harness, user, first, minutes(3)) is False
    assert await check(harness, user, second, minutes(3)) is True
    assert (await stored(harness, user)).failed_attempts == 0

    for sent in range(3, 6):
        assert await issue(harness, user, minutes(sent * 2)) is not None
    # Five an hour: the sixth waits for the window to end.
    assert await issue(harness, user, minutes(30)) is None
    assert await issue(harness, user, minutes(59)) is None
    assert await issue(harness, user, minutes(60)) is not None
    assert (await stored(harness, user)).send_count == 1


@pytest.mark.asyncio
async def test_ten_wrong_checks_lock_the_purpose_for_an_hour(harness: Harness) -> None:
    user = await new_user(harness)
    code = await issue(harness, user, START)
    for _ in range(5):
        assert await check(harness, user, wrong(code), START) is False
    code = await issue(harness, user, minutes(2))
    for _ in range(4):
        assert await check(harness, user, wrong(code), minutes(2)) is False
    assert (await stored(harness, user)).locked_until is None
    assert await check(harness, user, wrong(code), minutes(2)) is False

    row = await stored(harness, user)
    assert row.code_digest is None and row.failed_attempts == 0
    # While locked: the right code fails and no new code is issued.
    assert await check(harness, user, code, minutes(3)) is False
    assert await issue(harness, user, minutes(30)) is None
    assert await issue(harness, user, minutes(61.9)) is None
    # The other purpose has its own counters.
    assert await issue(harness, user, minutes(3), RESET) is not None

    code = await issue(harness, user, minutes(62))
    assert code is not None
    assert await check(harness, user, code, minutes(62)) is True


@pytest.mark.asyncio
async def test_a_code_belongs_to_one_user_and_one_purpose(harness: Harness) -> None:
    owner = await new_user(harness)
    other = await new_user(harness, "other@example.com")
    code = await issue(harness, owner, START)
    assert await issue(harness, other, START) is not None
    assert await issue(harness, owner, START, RESET) is not None
    other_code_digest = (await stored(harness, other)).code_digest

    assert await check(harness, owner, code, START, RESET) is False
    # Even if another row held this user's digest, it would not match there.
    assert (await stored(harness, owner)).code_digest != other_code_digest
    assert await check(harness, owner, "12345", START) is False
    assert await check(harness, await new_user(harness, "none@example.com"), code, START) is False

    async with harness.factory() as db:
        await clear_code(db, owner, VERIFY)
        await db.commit()
    assert await check(harness, owner, code, START) is False
    # Withdrawing a code leaves the cooldown in place.
    assert await issue(harness, owner, START + timedelta(seconds=30)) is None

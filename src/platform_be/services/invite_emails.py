"""The wording of the emails the Platform sends. Each function returns ``(subject, text, html)``.

Every message has the same parts in the same order: a greeting, what happened, the facts
as labelled lines, what to do next, and why the person received it. The plain-text and
HTML bodies are built from the same parts, so they never say different things.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from html import escape

PRODUCT = "AI Research Platform"

# Vietnam has no daylight saving time, so a fixed offset is exact.
VIETNAM_TIME = timezone(timedelta(hours=7))


def local_time(moment: datetime) -> str:
    """A moment as the readers of these emails see it on their clock."""
    if moment.tzinfo is None:
        # SQLite returns naive datetimes.
        moment = moment.replace(tzinfo=UTC)
    return f"{moment.astimezone(VIETNAM_TIME):%d %b %Y, %H:%M} (Vietnam time, GMT+7)"


ROLE_NAMES = {
    "project_manager": "Project Manager",
    "researcher": "Researcher",
    "reviewer": "Reviewer",
}


@dataclass
class _Message:
    greeting_name: str
    intro: str
    # Labelled facts, shown one per line.
    details: list[tuple[str, str]]
    # What the reader does next, in order.
    steps: list[str]
    reason: str
    notes: list[str] = field(default_factory=list)


def _one_line(value: str) -> str:
    """A value safe for a subject line: no line breaks, no runs of spaces."""
    return " ".join(value.split())


def _render(message: _Message) -> tuple[str, str]:
    footer = [message.reason, "This is an automated message. Please do not reply to it."]

    lines = [f"Hello {message.greeting_name},", "", message.intro, ""]
    lines += [f"{label}: {value}" for label, value in message.details]
    lines += ["", "What to do next:"]
    lines += [f"{number}. {step}" for number, step in enumerate(message.steps, start=1)]
    for note in message.notes:
        lines += ["", note]
    lines += ["", "--", *footer, PRODUCT]
    text = "\n".join(lines) + "\n"

    def row(label: str, value: str) -> str:
        shown = escape(value)
        if value.startswith(("http://", "https://")):
            shown = f'<a href="{escape(value, quote=True)}">{shown}</a>'
        return (
            f'<tr><td style="padding:4px 16px 4px 0;color:#555">{escape(label)}</td>'
            f'<td style="padding:4px 0"><strong>{shown}</strong></td></tr>'
        )

    html = (
        '<div style="font-family:Arial,Helvetica,sans-serif;font-size:15px;line-height:1.5;'
        'color:#111;max-width:560px">'
        f"<p>Hello {escape(message.greeting_name)},</p>"
        f"<p>{escape(message.intro)}</p>"
        f'<table style="border-collapse:collapse">'
        f"{''.join(row(label, value) for label, value in message.details)}</table>"
        "<p><strong>What to do next</strong></p>"
        f"<ol>{''.join(f'<li>{escape(step)}</li>' for step in message.steps)}</ol>"
        f"{''.join(f'<p>{escape(note)}</p>' for note in message.notes)}"
        '<hr style="border:none;border-top:1px solid #ddd">'
        f'<p style="font-size:13px;color:#555">{"<br>".join(escape(line) for line in footer)}'
        f"<br>{PRODUCT}</p>"
        "</div>"
    )
    return text, html


def _sign_in_step(app_url: str | None, how: str = "") -> str:
    return f"Open {app_url or f'the {PRODUCT}'} and sign in{how}."


def account_invite(
    email: str, display_name: str, temporary_password: str, app_url: str | None
) -> tuple[str, str, str]:
    details = [("Email", email), ("Password", temporary_password)]
    if app_url:
        details.append(("Sign-in page", app_url))
    text, html = _render(
        _Message(
            greeting_name=display_name,
            intro=f"An administrator created an account for you on the {PRODUCT}.",
            details=details,
            steps=[
                _sign_in_step(app_url, " with the details above"),
                "Choose your own password when asked. You cannot use the account before that.",
            ],
            notes=["This password stops working once you have chosen your own."],
            reason=(
                f"You received this email because an account was created for {email}. "
                "If you did not expect it, you can ignore it."
            ),
        )
    )
    return f"Your {PRODUCT} account: sign-in details", text, html


def project_invite(
    *,
    recipient_name: str,
    project_name: str,
    role: str,
    inviter_name: str,
    expires_at: datetime,
    expires_hours: int,
    app_url: str | None,
) -> tuple[str, str, str]:
    role_name = ROLE_NAMES.get(role, role)
    details = [
        ("Project", project_name),
        ("Your role", role_name),
        ("Invited by", inviter_name),
        ("Valid until", f"{local_time(expires_at)}, in {expires_hours} hours"),
    ]
    if app_url:
        details.append(("Sign-in page", app_url))
    text, html = _render(
        _Message(
            greeting_name=recipient_name,
            intro=f"{inviter_name} invited you to a project on the {PRODUCT}.",
            details=details,
            steps=[
                _sign_in_step(app_url),
                "Open your invitations and choose Accept or Decline.",
            ],
            notes=[
                "You have no access to the project until you accept. After the time above "
                "the invitation can no longer be accepted, and the project manager has to "
                "send it again."
            ],
            reason=(
                "You received this email because a project manager invited your account. "
                "If you do not want to join, decline the invitation or ignore this email."
            ),
        )
    )
    return f'Invitation to the project "{_one_line(project_name)}" on the {PRODUCT}', text, html

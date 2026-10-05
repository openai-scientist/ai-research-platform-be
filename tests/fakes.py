from decimal import Decimal
from typing import IO, Any
from uuid import UUID

from platform_be.services.popper_client import PopperNotFound, PopperRunState


class FakeEmailSender:
    """Keeps every message instead of sending it."""

    def __init__(self) -> None:
        self.sent: list[dict[str, str]] = []
        # Set to False to answer like a provider that refused the message.
        self.accept = True

    async def send(self, *, to: str, subject: str, text: str, html: str) -> bool:
        if self.accept:
            self.sent.append({"to": to, "subject": subject, "text": text, "html": html})
        return self.accept


class FakePopperClient:
    """Stands in for Popper: remembers what it was asked and answers from memory."""

    def __init__(self) -> None:
        self.runs: dict[str, PopperRunState] = {}
        self.started: list[dict[str, Any]] = []
        self.reviews: list[dict[str, Any]] = []
        # Set to an exception instance to make the next call fail with it.
        self.fail_with: Exception | None = None
        # When True, the run is recorded and the call then fails, like a lost response.
        self.record_before_failing = False

    def _maybe_fail(self) -> None:
        if self.fail_with is not None:
            error, self.fail_with = self.fail_with, None
            raise error

    async def start_run(
        self,
        *,
        platform_run_id: UUID,
        research_markdown: str,
        dataset: IO[bytes],
        dataset_filename: str,
        budget_usd: Decimal,
        auto_review: bool,
        callback_url: str,
    ) -> str:
        if not self.record_before_failing:
            self._maybe_fail()
        popper_run_id = f"popper-{platform_run_id}"
        self.started.append(
            {
                "platform_run_id": platform_run_id,
                "research_markdown": research_markdown,
                "dataset": dataset.read(),
                "dataset_filename": dataset_filename,
                "budget_usd": budget_usd,
                "auto_review": auto_review,
                "callback_url": callback_url,
            }
        )
        self.runs[popper_run_id] = PopperRunState(popper_run_id=popper_run_id, status="running")
        self._maybe_fail()
        return popper_run_id

    async def get_run(self, popper_run_id: str) -> PopperRunState:
        self._maybe_fail()
        if popper_run_id not in self.runs:
            raise PopperNotFound(popper_run_id)
        return self.runs[popper_run_id]

    async def find_run(self, platform_run_id: UUID) -> PopperRunState | None:
        self._maybe_fail()
        return self.runs.get(f"popper-{platform_run_id}")

    async def submit_review(
        self, popper_run_id: str, *, review_sequence: int, decision: dict[str, Any]
    ) -> None:
        self._maybe_fail()
        self.reviews.append(
            {"popper_run_id": popper_run_id, "review_sequence": review_sequence, **decision}
        )

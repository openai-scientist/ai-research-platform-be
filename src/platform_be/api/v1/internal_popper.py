"""Endpoints Popper calls to report on a run. Authenticated by a service key, not a session."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok
from platform_be.core.security import service_key_matches
from platform_be.db.session import get_db
from platform_be.models.research import (
    FINISHED_RUN_STATUSES,
    ResearchRun,
    RunArtifact,
    RunEvent,
    RunGate,
)
from platform_be.services.access import lock_project_scope
from platform_be.services.file_store import FileStore, get_file_store, iter_file, safe_filename
from platform_be.services.runs import ingest_popper_state, normalize_popper_status
from platform_be.services.run_event_stream import run_events_changed

ArtifactKind = Literal["paper_pdf", "paper_tex", "figure", "results", "other"]

# The stored content type comes from the extension, never from what the sender claims.
_CONTENT_TYPES = {
    ".pdf": "application/pdf",
    ".tex": "application/x-tex",
    ".png": "image/png",
    ".svg": "image/svg+xml",
    ".json": "application/json",
    ".csv": "text/csv",
    ".md": "text/markdown",
    ".txt": "text/plain",
}


def require_popper_service(request: Request) -> None:
    settings: Settings = request.app.state.settings
    expected = settings.popper_callback_key
    if not service_key_matches(
        expected.get_secret_value() if expected else None, request.headers.get("X-Service-Key")
    ):
        raise APIError(401, "SERVICE_KEY_INVALID", "A valid service key is required")


router = APIRouter(
    prefix="/internal/popper",
    tags=["popper callbacks"],
    dependencies=[Depends(require_popper_service)],
    responses={
        401: {"model": ErrorResponse, "description": "The service key is missing or wrong"},
        404: {"model": ErrorResponse, "description": "The run was not found"},
    },
)


class RunStatusReport(BaseModel):
    status: str = Field(
        max_length=120,
        description=(
            "`running`, `awaiting_review`, `completed`, `budget_exceeded`, or `failed` "
            "(optionally `failed:<stage>`)."
        ),
    )
    cost_usd: Decimal | None = Field(
        default=None, ge=0, lt=1_000_000, description="Total spent so far."
    )
    message: str | None = Field(default=None, max_length=2000)
    review: dict[str, Any] | None = Field(
        default=None,
        description="Required with `awaiting_review`: the frame to review, with an `items` map.",
    )
    review_sequence: int | None = Field(
        default=None,
        ge=1,
        le=2_147_483_647,
        description=(
            "Number of this review request within the run, from 1. Send it so that a "
            "repeated report of a request that was already answered is ignored."
        ),
    )


class RunStatusAck(BaseModel):
    run_id: str
    status: str


class ArtifactAck(BaseModel):
    id: str
    filename: str
    sha256: str
    size_bytes: int


class InboundEvent(BaseModel):
    source_seq: int = Field(
        ge=1, description="Strictly increasing source sequence number from Engine"
    )
    type: str = Field(max_length=48)
    stage_key: str | None = Field(default=None, max_length=32)
    actor: str | None = Field(default=None, max_length=32)
    payload: dict[str, Any] = Field(default_factory=dict)


class BatchEventsRequest(BaseModel):
    events: list[InboundEvent] = Field(min_length=1, max_length=200)


class BatchEventsAck(BaseModel):
    accepted: int
    skipped: int
    last_source_seq: int


async def _locked_run(db: AsyncSession, run_id: UUID) -> ResearchRun:
    project_id = await db.scalar(select(ResearchRun.project_id).where(ResearchRun.id == run_id))
    if project_id is None:
        raise APIError(404, "NOT_FOUND", "Run was not found")
    await lock_project_scope(db, project_id)
    run = await db.scalar(
        select(ResearchRun)
        .where(ResearchRun.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run is None:
        raise APIError(404, "NOT_FOUND", "Run was not found")
    return run


@router.post(
    "/runs/{run_id}/events",
    response_model=ApiResponse[BatchEventsAck],
    summary="Ingest batch of streaming events from Popper Engine",
    description="Ingests, validates, sequence-numbers, and stores events in run_events table.",
)
async def ingest_run_events(
    run_id: UUID,
    body: BatchEventsRequest,
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[BatchEventsAck]:
    run = await _locked_run(db, run_id)
    if run.status in FINISHED_RUN_STATUSES:
        raise APIError(409, "RUN_FINISHED", "Run is already finished")

    accepted_events: list[InboundEvent] = []
    skipped = 0
    expected_source_seq = run.last_source_seq + 1

    for evt in body.events:
        if evt.source_seq <= run.last_source_seq:
            skipped += 1
            continue
        if evt.source_seq != expected_source_seq:
            raise APIError(
                422,
                "EVENT_GAP",
                f"Expected source_seq {expected_source_seq}, got {evt.source_seq}",
            )
        accepted_events.append(evt)
        expected_source_seq += 1

    for evt in accepted_events:
        seq = run.last_seq + 1
        now = datetime.now(UTC)
        run_event = RunEvent(
            run_id=run.id,
            seq=seq,
            source_seq=evt.source_seq,
            type=evt.type,
            stage_key=evt.stage_key,
            actor=evt.actor,
            payload=evt.payload,
            created_at=now,
        )
        db.add(run_event)
        run.last_seq = seq
        run.last_source_seq = evt.source_seq

        if evt.type == "run.started":
            if run.status == "queued":
                run.status = "running"
                run.started_at = run.started_at or now
        elif evt.type == "gate.opened":
            gate_id = evt.payload.get("gate_id", "gate-1")
            kind = evt.payload.get("kind", "screen")
            run_gate = RunGate(
                id=uuid4(),
                run_id=run.id,
                gate_key=gate_id,
                kind=kind,
                spec=evt.payload,
                opened_seq=seq,
                created_at=now,
            )
            db.add(run_gate)
            run.status = "awaiting_review"
        elif evt.type == "run.status":
            new_status = evt.payload.get("status")
            if new_status in ("running", "paused", "awaiting_review"):
                run.status = new_status
            elif new_status == "failed":
                run.status = "failed"
                run.failure_message = evt.payload.get("reason", "Engine reported failure")
                run.finished_at = now
        elif evt.type == "run.completed":
            run.status = "completed"
            run.finished_at = now

        if "cost_usd" in evt.payload:
            try:
                run.cost_usd = Decimal(str(evt.payload["cost_usd"]))
            except Exception:
                pass

    await db.flush()
    if accepted_events:
        run_events_changed(db, run.id)
        notify_payload = f"{run.id}:{run.last_seq}"
        await db.execute(text(f"SELECT pg_notify('run_events', '{notify_payload}')"))
    await db.commit()

    return ok(
        BatchEventsAck(
            accepted=len(accepted_events),
            skipped=skipped,
            last_source_seq=run.last_source_seq,
        ),
        "Events ingested successfully",
    )


@router.post(
    "/runs/{run_id}/status",
    response_model=ApiResponse[RunStatusAck],
    summary="Report a run's status and cost",
    description=(
        "Safe to repeat: reporting the current status again changes nothing. "
        "A finished run refuses any other status with 409."
    ),
)
async def report_run_status(
    run_id: UUID,
    body: RunStatusReport,
    request: Request,
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> ApiResponse[RunStatusAck]:
    try:
        status, stage = normalize_popper_status(body.status)
    except ValueError as exc:
        raise APIError(422, "UNKNOWN_RUN_STATUS", "Popper may not report this status") from exc
    run = await _locked_run(db, run_id)
    await ingest_popper_state(
        db,
        store,
        run,
        status=status,
        cost_usd=body.cost_usd,
        message=body.message or stage,
        review=body.review,
        review_sequence=body.review_sequence,
        request_id=getattr(request.state, "request_id", None),
    )
    return ok(RunStatusAck(run_id=str(run.id), status=run.status))


@router.post(
    "/runs/{run_id}/artifacts",
    response_model=ApiResponse[ArtifactAck],
    summary="Deliver a result file of a run",
    description=(
        "Send every file before reporting the final status: a finished run accepts no "
        "more files. Sending the same file again changes nothing; a different file under "
        "an existing name is refused with 409."
    ),
)
async def deliver_run_artifact(
    run_id: UUID,
    kind: ArtifactKind = Form(),
    file: UploadFile = File(),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> ApiResponse[ArtifactAck]:
    filename = safe_filename(file.filename, "")
    if not filename:
        raise APIError(422, "INVALID_FILENAME", "The file needs a usable name")
    run = await db.scalar(select(ResearchRun).where(ResearchRun.id == run_id))
    if run is None:
        raise APIError(404, "NOT_FOUND", "Run was not found")
    if run.status == "queued":
        raise APIError(409, "RUN_NOT_STARTED", "A run that has not started has no result files")
    if run.status in FINISHED_RUN_STATUSES:
        raise APIError(409, "RUN_FINISHED", "A finished run accepts no more result files")

    artifact_id = uuid4()
    storage_key = f"projects/{run.project_id}/runs/{run.id}/artifacts/{artifact_id}/file"
    stored = await store.put(storage_key, iter_file(file.file))
    try:
        await _locked_run(db, run_id)
        existing = await db.scalar(
            select(RunArtifact).where(
                RunArtifact.run_id == run_id, RunArtifact.filename == filename
            )
        )
        if existing is not None:
            await store.delete(storage_key)
            if existing.sha256 != stored.sha256:
                raise APIError(
                    409, "ARTIFACT_CONFLICT", "A different file was already delivered by this name"
                )
            return ok(_ack(existing), "Artifact already delivered")
        extension = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        artifact = RunArtifact(
            id=artifact_id,
            run_id=run_id,
            kind=kind,
            filename=filename,
            content_type=_CONTENT_TYPES.get(extension, "application/octet-stream"),
            storage_key=storage_key,
            size_bytes=stored.size_bytes,
            sha256=stored.sha256,
        )
        db.add(artifact)
        await db.flush()
    except Exception:
        await store.delete(storage_key)
        raise
    return ok(_ack(artifact), "Artifact stored")


def _ack(artifact: RunArtifact) -> ArtifactAck:
    return ArtifactAck(
        id=str(artifact.id),
        filename=artifact.filename,
        sha256=artifact.sha256,
        size_bytes=artifact.size_bytes,
    )

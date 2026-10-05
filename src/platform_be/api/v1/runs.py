import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.api.v1.research_contexts import latest_research_context
from platform_be.auth.sessions import Principal, require_active_csrf, require_active_principal
from platform_be.core.config import Settings
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok, paginated
from platform_be.db.session import get_db
from platform_be.models.dataset import Dataset, DatasetVersion
from platform_be.models.research import ACTIVE_RUN_STATUSES, ResearchContext, ResearchRun
from platform_be.services.access import (
    ensure_writable_project,
    lock_project_scope,
    require_project_access,
)
from platform_be.services.audit import record_audit
from platform_be.services.file_store import FileStore, get_file_store, spool
from platform_be.services.popper_client import (
    PopperClient,
    PopperNotFound,
    PopperRejected,
    PopperUnavailable,
    PopperUncertain,
    get_popper_client,
)
from platform_be.services.project_status import refresh_project_status
from platform_be.services.research_markdown import render_research_markdown
from platform_be.services.runs import (
    apply_run_status,
    get_run,
    ingest_popper_state,
    normalize_popper_status,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/projects/{project_id}/runs", tags=["runs"])

RunStatus = Literal[
    "queued", "running", "awaiting_review", "completed", "budget_exceeded", "failed"
]

RUN_ERRORS = {
    403: {"model": ErrorResponse, "description": "Project Manager or Researcher role is required"},
    404: {"model": ErrorResponse, "description": "The project or run was not found"},
    409: {"model": ErrorResponse, "description": "The project is archived or already has a run"},
    502: {"model": ErrorResponse, "description": "Popper could not be reached"},
    503: {"model": ErrorResponse, "description": "Popper is not configured on this server"},
}


class RunCreate(BaseModel):
    dataset_version_id: UUID
    research_context_version: int | None = Field(
        default=None,
        ge=1,
        le=2_147_483_647,
        description="Defaults to the newest research context.",
    )
    budget_usd: Decimal | None = Field(
        default=None,
        max_digits=10,
        decimal_places=2,
        description="Spending cap for this run. Defaults to the server's standard budget.",
    )
    auto_review: bool = Field(
        default=False, description="Let Popper continue without a person reviewing the frame."
    )


class RunItem(BaseModel):
    id: str
    project_id: str
    dataset_id: str
    dataset_version_id: str
    dataset_version_number: int
    research_context_id: str
    research_context_version: int
    created_by_user_id: str
    status: RunStatus
    auto_review: bool
    budget_usd: Decimal
    cost_usd: Decimal
    failure_message: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    updated_at: datetime


def _run_query():
    return (
        select(ResearchRun, DatasetVersion, ResearchContext.version_number)
        .join(DatasetVersion, DatasetVersion.id == ResearchRun.dataset_version_id)
        .join(ResearchContext, ResearchContext.id == ResearchRun.research_context_id)
    )


def _run_item(run: ResearchRun, version: DatasetVersion, context_version: int) -> RunItem:
    return RunItem(
        id=str(run.id),
        project_id=str(run.project_id),
        dataset_id=str(version.dataset_id),
        dataset_version_id=str(version.id),
        dataset_version_number=version.version_number,
        research_context_id=str(run.research_context_id),
        research_context_version=context_version,
        created_by_user_id=str(run.created_by_user_id),
        status=run.status,
        auto_review=run.auto_review,
        budget_usd=run.budget_usd,
        cost_usd=run.cost_usd,
        failure_message=run.failure_message,
        created_at=run.created_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        updated_at=run.updated_at,
    )


async def _item(db: AsyncSession, run: ResearchRun) -> RunItem:
    await db.flush()
    row = (await db.execute(_run_query().where(ResearchRun.id == run.id))).one()
    return _run_item(run, row[1], row[2])


def _require_popper(client: PopperClient | None) -> PopperClient:
    if client is None:
        raise APIError(503, "POPPER_NOT_CONFIGURED", "Popper is not configured on this server")
    return client


def _ensure_not_dispatching(run: ResearchRun, settings: Settings) -> None:
    """Refuse to settle a run whose creating request may still be talking to Popper."""
    if run.popper_run_id is not None:
        return
    created_at = run.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    if datetime.now(UTC) - created_at < timedelta(seconds=2 * settings.popper_timeout_seconds):
        raise APIError(
            409, "RUN_DISPATCHING", "The run is still being sent to Popper; try again shortly"
        )


def _unknown_columns(context: ResearchContext, version: DatasetVersion) -> list[str]:
    """Variables the research context names that the dataset does not have.

    Checked only when `variables` is a mapping keyed by column name, the shape Popper
    documents; anything else is left for Popper to judge.
    """
    variables = (context.front_matter or {}).get("variables")
    if not isinstance(variables, dict):
        return []
    columns = set(version.column_names)
    return [name for name in variables if name not in columns]


@router.get(
    "",
    response_model=ApiResponse[list[RunItem]],
    summary="List the project's runs, newest first",
    responses={404: RUN_ERRORS[404]},
)
async def list_runs(
    project_id: UUID,
    status: RunStatus | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[RunItem]]:
    await require_project_access(db, principal, project_id)
    filters = [ResearchRun.project_id == project_id]
    if status is not None:
        filters.append(ResearchRun.status == status)
    total = int(await db.scalar(select(func.count()).select_from(ResearchRun).where(*filters)) or 0)
    rows = (
        await db.execute(
            _run_query()
            .where(*filters)
            .order_by(ResearchRun.created_at.desc(), ResearchRun.id)
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return paginated(
        [_run_item(run, version, context_version) for run, version, context_version in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post(
    "",
    response_model=ApiResponse[RunItem],
    status_code=201,
    summary="Start a run",
    description=(
        "Sends one dataset version and one research context version to Popper. "
        "A project works on one run at a time. Answers 202 with a `queued` run when Popper "
        "did not confirm in time; call `sync` on the run to find out what happened."
    ),
    responses={
        **RUN_ERRORS,
        202: {"model": ApiResponse[RunItem], "description": "Popper has not confirmed yet"},
    },
)
async def create_run(
    project_id: UUID,
    body: RunCreate,
    request: Request,
    response: Response,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
    popper: PopperClient | None = Depends(get_popper_client),
) -> ApiResponse[RunItem]:
    settings: Settings = request.app.state.settings
    request_id = getattr(request.state, "request_id", None)
    popper = _require_popper(popper)

    await lock_project_scope(db, project_id)
    project, _ = await require_project_access(db, principal, project_id, contribute=True, lock=True)
    ensure_writable_project(project)
    if project.status == "completed":
        raise APIError(409, "PROJECT_COMPLETED", "Reopen the project before starting a run")

    version = await db.scalar(
        select(DatasetVersion)
        .join(Dataset, Dataset.id == DatasetVersion.dataset_id)
        .where(DatasetVersion.id == body.dataset_version_id, Dataset.project_id == project_id)
    )
    if version is None:
        raise APIError(404, "NOT_FOUND", "Dataset version was not found")
    if body.research_context_version is None:
        context = await latest_research_context(db, project_id)
        if context is None:
            raise APIError(
                422, "RESEARCH_CONTEXT_REQUIRED", "Save a research context before starting a run"
            )
    else:
        context = await db.scalar(
            select(ResearchContext).where(
                ResearchContext.project_id == project_id,
                ResearchContext.version_number == body.research_context_version,
            )
        )
        if context is None:
            raise APIError(404, "NOT_FOUND", "Research context version was not found")

    unknown = _unknown_columns(context, version)
    if unknown:
        raise APIError(
            422,
            "UNKNOWN_COLUMNS",
            "The research context names variables the dataset does not have: "
            + ", ".join(unknown[:20]),
        )
    budget = body.budget_usd if body.budget_usd is not None else settings.run_default_budget_usd
    if not 0 < budget <= settings.run_max_budget_usd:
        raise APIError(
            422,
            "BUDGET_OUT_OF_RANGE",
            f"The budget must be above 0 and at most {settings.run_max_budget_usd} USD",
        )
    active = await db.scalar(
        select(ResearchRun.id).where(
            ResearchRun.project_id == project_id, ResearchRun.status.in_(ACTIVE_RUN_STATUSES)
        )
    )
    if active is not None:
        raise APIError(409, "RUN_ACTIVE", "This project already has a run in progress")

    run = ResearchRun(
        project_id=project_id,
        dataset_version_id=version.id,
        research_context_id=context.id,
        created_by_user_id=principal.user.id,
        status="queued",
        auto_review=body.auto_review,
        budget_usd=budget,
    )
    db.add(run)
    await db.flush()
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="run.created",
        resource_type="run",
        resource_id=run.id,
        project_id=project_id,
        request_id=request_id,
        details={
            "dataset_version_id": str(version.id),
            "research_context_version": context.version_number,
            "budget_usd": str(budget),
            "auto_review": body.auto_review,
        },
    )
    await refresh_project_status(db, project)
    # The run is saved before Popper is called, so a run Popper started is never lost here.
    await db.commit()

    failure: APIError | None = None
    popper_run_id: str | None = None
    dataset_file = None
    try:
        dataset_file = await spool(store, version.storage_key)
        popper_run_id = await popper.start_run(
            platform_run_id=run.id,
            research_markdown=render_research_markdown(context.body, context.front_matter),
            dataset=dataset_file,
            dataset_filename=version.original_filename,
            budget_usd=budget,
            auto_review=body.auto_review,
            callback_url=(
                f"{settings.public_base_url.rstrip('/')}{settings.api_prefix.rstrip('/')}"
                f"/internal/popper/runs/{run.id}"
            ),
        )
    except PopperUncertain:
        # Popper may have started the run; leave it queued until `sync` asks Popper.
        response.status_code = 202
        return ok(await _item(db, run), "Popper has not confirmed the run yet")
    except PopperUnavailable:
        failure = APIError(502, "POPPER_UNAVAILABLE", "Popper could not be reached")
    except PopperRejected as exc:
        failure = APIError(422, "POPPER_REJECTED", str(exc))
    except Exception:
        # Nothing reached Popper in a usable way; do not leave the project blocked by this run.
        logger.exception("could not send run %s to Popper", run.id)
        failure = APIError(500, "RUN_START_FAILED", "The run could not be sent to Popper")
    finally:
        if dataset_file is not None:
            dataset_file.close()

    await lock_project_scope(db, project_id)
    run = await get_run(db, project_id, run.id, lock=True)
    if failure is not None:
        await apply_run_status(db, run, "failed", message=failure.message, request_id=request_id)
        await db.commit()
        raise failure
    run.popper_run_id = popper_run_id
    if run.status == "queued":
        await apply_run_status(
            db, run, "running", actor_user_id=principal.user.id, request_id=request_id
        )
    return ok(await _item(db, run), "Run started")


@router.get(
    "/{run_id}",
    response_model=ApiResponse[RunItem],
    summary="Get a run",
    responses={404: RUN_ERRORS[404]},
)
async def get_run_route(
    project_id: UUID,
    run_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[RunItem]:
    await require_project_access(db, principal, project_id)
    return ok(await _item(db, await get_run(db, project_id, run_id)))


@router.post(
    "/{run_id}/sync",
    response_model=ApiResponse[RunItem],
    summary="Ask Popper for the run's current state",
    description=(
        "Use when a run looks stuck. A run Popper no longer knows, or never received, "
        "is marked failed so the project can start another. Answers 409 `RUN_DISPATCHING` "
        "while a run that was just created may still be on its way to Popper."
    ),
    responses=RUN_ERRORS,
)
async def sync_run(
    project_id: UUID,
    run_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
    popper: PopperClient | None = Depends(get_popper_client),
) -> ApiResponse[RunItem]:
    request_id = getattr(request.state, "request_id", None)
    popper = _require_popper(popper)
    await require_project_access(db, principal, project_id, contribute=True)
    # Popper is asked without holding the run's lock, so its callbacks are never kept waiting.
    run = await get_run(db, project_id, run_id)
    if run.status not in ACTIVE_RUN_STATUSES:
        return ok(await _item(db, run))
    _ensure_not_dispatching(run, request.app.state.settings)

    try:
        if run.popper_run_id is None:
            state = await popper.find_run(run.id)
        else:
            try:
                state = await popper.get_run(run.popper_run_id)
            except PopperNotFound:
                state = None
    except (PopperUnavailable, PopperUncertain) as exc:
        raise APIError(502, "POPPER_UNAVAILABLE", "Popper could not be reached") from exc

    await lock_project_scope(db, project_id)
    run = await get_run(db, project_id, run_id, lock=True)
    if run.status not in ACTIVE_RUN_STATUSES:
        return ok(await _item(db, run))
    if state is None:
        lost = (
            "Popper never received this run"
            if run.popper_run_id is None
            else ("Popper no longer has this run")
        )
        await apply_run_status(db, run, "failed", message=lost, request_id=request_id)
        return ok(await _item(db, run), "Run marked failed")

    try:
        status, stage = normalize_popper_status(state.status)
    except ValueError as exc:
        raise APIError(502, "POPPER_BAD_RESPONSE", "Popper reported an unknown status") from exc
    if run.popper_run_id is None:
        run.popper_run_id = state.popper_run_id
    try:
        await ingest_popper_state(
            db,
            store,
            run,
            status=status,
            cost_usd=state.cost_usd,
            message=state.message or stage,
            review=state.review,
            review_sequence=state.review_sequence,
            request_id=request_id,
        )
    except APIError as exc:
        if exc.code != "REVIEW_REQUIRED":
            raise
        raise APIError(
            502, "POPPER_BAD_RESPONSE", "Popper did not include the frame to review"
        ) from exc
    return ok(await _item(db, run), "Run synchronized")


@router.post(
    "/{run_id}/abandon",
    response_model=ApiResponse[RunItem],
    summary="Give up on a run that cannot finish",
    description=(
        "Project Manager only. Marks a run in progress as failed so the project can start "
        "another, for when Popper keeps failing to answer and `sync` cannot settle the run. "
        "Popper is not told: if it is still working on the run, its later reports are refused."
    ),
    responses={**RUN_ERRORS, 403: {"model": ErrorResponse, "description": "Project Manager only"}},
)
async def abandon_run(
    project_id: UUID,
    run_id: UUID,
    request: Request,
    principal: Principal = Depends(require_active_csrf),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[RunItem]:
    await lock_project_scope(db, project_id)
    project, _ = await require_project_access(db, principal, project_id, manage=True)
    ensure_writable_project(project)
    run = await get_run(db, project_id, run_id, lock=True)
    if run.status not in ACTIVE_RUN_STATUSES:
        raise APIError(409, "RUN_FINISHED", "This run has already finished")
    _ensure_not_dispatching(run, request.app.state.settings)
    request_id = getattr(request.state, "request_id", None)
    await apply_run_status(
        db,
        run,
        "failed",
        message="Abandoned by a project manager",
        actor_user_id=principal.user.id,
        request_id=request_id,
    )
    record_audit(
        db,
        actor_user_id=principal.user.id,
        action="run.abandoned",
        resource_type="run",
        resource_id=run.id,
        project_id=project_id,
        request_id=request_id,
        details={"popper_run_id": run.popper_run_id},
    )
    return ok(await _item(db, run), "Run abandoned")

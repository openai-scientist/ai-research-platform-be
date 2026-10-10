import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.core.errors import APIError
from platform_be.models.monitoring import MonitoringEvent
from platform_be.models.project import Project
from platform_be.models.research import FINISHED_RUN_STATUSES, FrameReview, ResearchRun
from platform_be.services.monitoring_queries import (
    MAX_EVENT_ROWS,
    SERVICE_CODE,
    aggregate_requests,
    check_capture_coverage,
    event_to_public,
    monitoring_service_id,
    request_bucket_points,
    request_events,
)

OverviewSection = Literal["overview", "operations", "projects", "governance"]
MAX_WINDOW = timedelta(days=366)
PROJECT_WINDOW = timedelta(days=28)
OPERATIONS_WINDOW = timedelta(hours=24)


def effective_window(
    *,
    section: OverviewSection,
    now: datetime,
    from_time: datetime | None,
    to_time: datetime | None,
    timezone: str,
) -> tuple[datetime, datetime, str]:
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise APIError(422, "INVALID_TIMEZONE", "Use a valid IANA timezone") from exc

    if (from_time is None) != (to_time is None):
        raise APIError(422, "TIME_RANGE_PAIR_REQUIRED", "Provide both 'from' and 'to' filters")
    if from_time is None or to_time is None:
        duration = OPERATIONS_WINDOW if section in ("operations", "governance") else PROJECT_WINDOW
        return now - duration, now, timezone
    if from_time.utcoffset() is None or to_time.utcoffset() is None:
        raise APIError(422, "TIMEZONE_REQUIRED", "Time filters must include a timezone offset")
    if from_time >= to_time:
        raise APIError(422, "INVALID_TIME_RANGE", "The 'from' filter must be before 'to'")
    if to_time - from_time > MAX_WINDOW:
        raise APIError(422, "TIME_RANGE_TOO_LARGE", "The requested time range exceeds 366 days")
    return from_time.astimezone(UTC), to_time.astimezone(UTC), timezone


def _count_metric(
    value: int,
    *,
    scope: Literal["snapshot", "window"],
    previous_value: int | None = None,
    desirable_direction: Literal["up", "down"] | None = None,
) -> dict[str, Any]:
    change = None
    if previous_value not in (None, 0):
        change = (value - previous_value) / previous_value * 100
    return {
        "value": value,
        "unit": "count",
        "scope": scope,
        "previous_value": previous_value,
        "change": change,
        "change_unit": "percent" if previous_value not in (None, 0) else None,
        "desirable_direction": desirable_direction,
        "available": True,
    }


def _rate_metric(
    value: float | None,
    *,
    previous_value: float | None,
    available: bool,
    desirable_direction: Literal["up", "down"] | None,
) -> dict[str, Any]:
    return {
        "value": value,
        "unit": "percent",
        "scope": "window",
        "previous_value": previous_value,
        "change": value - previous_value
        if value is not None and previous_value is not None
        else None,
        "change_unit": "percentage_points"
        if value is not None and previous_value is not None
        else None,
        "desirable_direction": desirable_direction,
        "available": available,
    }


def _unavailable(
    reason_code: str = "SOURCE_NOT_CONFIGURED", *, unit: str = "count"
) -> dict[str, Any]:
    return {
        "value": None,
        "unit": unit,
        "scope": "window",
        "previous_value": None,
        "change": None,
        "change_unit": None,
        "desirable_direction": None,
        "available": False,
        "reason_code": reason_code,
    }


def _unavailable_series(key: str, unit: str = "count") -> dict[str, Any]:
    return {
        "key": key,
        "unit": unit,
        "interval_seconds": 0,
        "points": [],
        "available": False,
    }


async def _run_status_counts(db: AsyncSession, start: datetime, end: datetime) -> dict[str, int]:
    rows = await db.execute(
        select(ResearchRun.status, func.count())
        .where(ResearchRun.created_at >= start, ResearchRun.created_at < end)
        .group_by(ResearchRun.status)
    )
    return {status: int(count) for status, count in rows}


async def _terminal_status_counts(
    db: AsyncSession, start: datetime, end: datetime
) -> dict[str, int]:
    rows = await db.execute(
        select(ResearchRun.status, func.count())
        .where(
            ResearchRun.status.in_(FINISHED_RUN_STATUSES),
            ResearchRun.finished_at >= start,
            ResearchRun.finished_at < end,
        )
        .group_by(ResearchRun.status)
    )
    return {status: int(count) for status, count in rows}


async def _project_snapshot(db: AsyncSession) -> tuple[dict[str, int], int]:
    rows = await db.execute(
        select(Project.status, func.count())
        .where(Project.archived_at.is_(None))
        .group_by(Project.status)
    )
    active_status_counts = {status: int(count) for status, count in rows}
    archived = int(
        await db.scalar(
            select(func.count()).select_from(Project).where(Project.archived_at.is_not(None))
        )
        or 0
    )
    return active_status_counts, archived


async def _pending_review_count(db: AsyncSession) -> int:
    return int(
        await db.scalar(
            select(func.count()).select_from(FrameReview).where(FrameReview.submitted_at.is_(None))
        )
        or 0
    )


async def _run_project_breakdown(
    db: AsyncSession, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    rows = await db.execute(
        select(Project.id, Project.name, func.count(ResearchRun.id).label("run_count"))
        .join(ResearchRun, ResearchRun.project_id == Project.id)
        .where(ResearchRun.created_at >= start, ResearchRun.created_at < end)
        .group_by(Project.id, Project.name)
        .order_by(func.count(ResearchRun.id).desc(), Project.id.asc())
        .limit(5)
    )
    return [
        {"id": str(project_id), "label": name, "value": int(count)}
        for project_id, name, count in rows
    ]


async def _recent_runs(db: AsyncSession, start: datetime, end: datetime) -> list[dict[str, Any]]:
    rows = await db.execute(
        select(ResearchRun, Project.name)
        .join(Project, Project.id == ResearchRun.project_id)
        .where(ResearchRun.created_at >= start, ResearchRun.created_at < end)
        .order_by(ResearchRun.created_at.desc(), ResearchRun.id.asc())
        .limit(5)
    )
    recent: list[dict[str, Any]] = []
    for run, project_name in rows:
        duration_ms = None
        if run.started_at is not None and run.finished_at is not None:
            duration_ms = max(0, int((run.finished_at - run.started_at).total_seconds() * 1000))
        recent.append(
            {
                "id": str(run.id),
                "project_id": str(run.project_id),
                "project_name": project_name,
                "status": run.status,
                "topic": run.topic,
                "created_at": _as_utc(run.created_at),
                "finished_at": _as_utc(run.finished_at),
                "duration_ms": duration_ms,
            }
        )
    return recent


async def _reported_run_cost(db: AsyncSession, start: datetime, end: datetime) -> Decimal:
    value = await db.scalar(
        select(func.sum(ResearchRun.cost_usd)).where(
            ResearchRun.created_at >= start, ResearchRun.created_at < end
        )
    )
    return Decimal(value or 0)


def _breakdown(
    key: str, unit: str, items: list[dict[str, Any]], total: int | float | None
) -> dict[str, Any]:
    return {"key": key, "unit": unit, "items": items, "total": total, "available": True}


def _unavailable_breakdown(key: str, unit: str = "count") -> dict[str, Any]:
    return {"key": key, "unit": unit, "items": [], "total": None, "available": False}


def _unavailable_money_metric() -> dict[str, Any]:
    return {
        "value_usd": None,
        "previous_value_usd": None,
        "change_percent": None,
        "available": False,
    }


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _series_for(section: OverviewSection) -> list[dict[str, Any]]:
    keys = {
        "overview": [("execution_reliability", "percent"), ("active_runs", "count")],
        "operations": [
            ("service_reliability", "percent"),
            ("request_rate", "req/s"),
            ("queue_depth", "count"),
        ],
        "projects": [("project_execution_health", "percent"), ("active_runs", "count")],
        "governance": [("decision_gate_quality", "percent")],
    }[section]
    return [_unavailable_series(key, unit) for key, unit in keys]


def _mark_unavailable(
    fields: list[dict[str, str]], key: str, reason_code: str = "SOURCE_NOT_CONFIGURED"
) -> None:
    fields.append({"key": key, "reason_code": reason_code})


def _observed_metric(
    value: float | int | None,
    *,
    unit: str,
    available: bool,
    reason: str | None = None,
    previous_value: float | int | None = None,
    change_unit: str = "percent",
    desirable_direction: Literal["up", "down"] | None = None,
) -> dict[str, Any]:
    if not available:
        return _unavailable(reason or "CAPTURE_INCOMPLETE", unit=unit)
    change = None
    if value is not None and previous_value is not None:
        if change_unit == "percentage_points":
            change = value - previous_value
        elif previous_value != 0:
            change = (value - previous_value) / previous_value * 100
    return {
        "value": value,
        "unit": unit,
        "scope": "window",
        "previous_value": previous_value,
        "change": change,
        "change_unit": change_unit if change is not None else None,
        "desirable_direction": desirable_direction,
        "available": value is not None,
        "reason_code": "NO_REQUESTS" if value is None else None,
    }


async def build_admin_overview(
    db: AsyncSession,
    *,
    section: OverviewSection,
    start: datetime,
    end: datetime,
    timezone: str,
    generated_at: datetime,
    environment: str = "local",
) -> dict[str, Any]:
    duration = end - start
    previous_start = start - duration
    previous_end = start
    comparison_window = {"from": previous_start, "to": previous_end}
    metrics: dict[str, Any] = {}
    money_metrics: dict[str, Any] = {}
    breakdowns: list[dict[str, Any]] = []
    unavailable_fields: list[dict[str, str]] = []
    recent_runs: list[dict[str, Any]] = []
    recent_events: list[dict[str, Any]] = []
    governance_events: list[dict[str, Any]] = []
    decision_gates: list[dict[str, Any]] = []
    series = _series_for(section)

    if section in ("overview", "projects"):
        project_status_counts, archived_projects = await _project_snapshot(db)
        project_total = sum(project_status_counts.values()) + archived_projects
        active_projects = sum(
            count for status, count in project_status_counts.items() if status != "completed"
        )
        active_runs = int(
            await db.scalar(
                select(func.count()).select_from(ResearchRun).where(ResearchRun.status == "running")
            )
            or 0
        )
        review_queue = await _pending_review_count(db)
        current_runs = await _run_status_counts(db, start, end)
        previous_runs = await _run_status_counts(db, previous_start, previous_end)
        current_terminal = await _terminal_status_counts(db, start, end)
        previous_terminal = await _terminal_status_counts(db, previous_start, previous_end)
        current_terminal_total = sum(current_terminal.values())
        previous_terminal_total = sum(previous_terminal.values())
        current_rate = (
            current_terminal.get("completed", 0) / current_terminal_total * 100
            if current_terminal_total
            else None
        )
        previous_rate = (
            previous_terminal.get("completed", 0) / previous_terminal_total * 100
            if previous_terminal_total
            else None
        )
        recent_runs = await _recent_runs(db, start, end)
        runs_by_project = await _run_project_breakdown(db, start, end)
        all_statuses = ("queued", "running", "paused", "awaiting_review", *FINISHED_RUN_STATUSES)
        status_items = [
            {
                "id": status,
                "label": status.replace("_", " ").title(),
                "value": current_runs.get(status, 0),
            }
            for status in all_statuses
        ]

        metrics.update(
            {
                "active_projects": _count_metric(active_projects, scope="snapshot"),
                "total_projects": _count_metric(project_total, scope="snapshot"),
                "archived_projects": _count_metric(archived_projects, scope="snapshot"),
                "projects_in_review": _count_metric(
                    project_status_counts.get("needs_review", 0), scope="snapshot"
                ),
                "experiment_runs": _count_metric(
                    sum(current_runs.values()),
                    scope="window",
                    previous_value=sum(previous_runs.values()),
                ),
                "successful_runs_rate": _rate_metric(
                    current_rate,
                    previous_value=previous_rate,
                    available=current_rate is not None,
                    desirable_direction="up",
                ),
                "active_runs": _count_metric(active_runs, scope="snapshot"),
                "review_queue": _count_metric(review_queue, scope="snapshot"),
                "queued_research_runs": _count_metric(
                    current_runs.get("queued", 0),
                    scope="window",
                    previous_value=previous_runs.get("queued", 0),
                ),
                "token_usage": _unavailable("SOURCE_NOT_CONFIGURED", unit="tokens"),
            }
        )
        if current_rate is None:
            _mark_unavailable(
                unavailable_fields, "metrics.successful_runs_rate", "NO_TERMINAL_RUNS"
            )
        if previous_rate is None:
            _mark_unavailable(
                unavailable_fields,
                "metrics.successful_runs_rate.previous_value",
                "NO_TERMINAL_RUNS",
            )
        _mark_unavailable(unavailable_fields, "metrics.token_usage")
        breakdowns.extend(
            [
                _breakdown("runs_by_status", "count", status_items, sum(current_runs.values())),
                _breakdown(
                    "runs_by_project",
                    "count",
                    runs_by_project,
                    sum(item["value"] for item in runs_by_project),
                ),
                _unavailable_breakdown("active_runs_by_service"),
                _unavailable_breakdown("activity_by_service"),
                _unavailable_breakdown("token_usage_by_model", "tokens"),
            ]
        )
        if section == "projects":
            project_items = [
                {"id": status, "label": status.replace("_", " ").title(), "value": count}
                for status, count in sorted(project_status_counts.items())
            ]
            if archived_projects:
                project_items.append(
                    {"id": "archived", "label": "Archived", "value": archived_projects}
                )
            breakdowns.append(
                _breakdown("projects_by_status", "count", project_items, project_total)
            )
            current_cost = await _reported_run_cost(db, start, end)
            previous_cost = await _reported_run_cost(db, previous_start, previous_end)
            money_metrics["reported_run_cost"] = {
                "value_usd": str(current_cost),
                "previous_value_usd": str(previous_cost),
                "change_percent": (
                    float((current_cost - previous_cost) / previous_cost * 100)
                    if previous_cost != 0
                    else None
                ),
                "available": True,
                "attribution": (
                    "ResearchRun.cost_usd summed for runs created in the selected window"
                ),
            }
            money_metrics["estimated_spend"] = _unavailable_money_metric()
            for key in (
                "validated_findings",
                "active_project_capacity",
                "review_completion",
                "monthly_budget_used",
            ):
                metrics[key] = _unavailable()
                _mark_unavailable(unavailable_fields, f"metrics.{key}")
            _mark_unavailable(unavailable_fields, "money_metrics.estimated_spend")
            _mark_unavailable(unavailable_fields, "series.project_execution_health")
            _mark_unavailable(unavailable_fields, "series.active_runs")
            _mark_unavailable(unavailable_fields, "breakdowns.runs_by_owner")
        else:
            metrics["runs_by_status_total"] = _count_metric(
                sum(current_runs.values()),
                scope="window",
                previous_value=sum(previous_runs.values()),
            )
            for key in ("execution_reliability", "active_runs"):
                _mark_unavailable(unavailable_fields, f"series.{key}")
            _mark_unavailable(unavailable_fields, "breakdowns.token_usage_by_model")
            _mark_unavailable(unavailable_fields, "breakdowns.activity_by_service")
            _mark_unavailable(unavailable_fields, "breakdowns.active_runs_by_service")
            _mark_unavailable(unavailable_fields, "recent_events")
    elif section == "operations":
        coverage = await check_capture_coverage(
            db,
            environment=environment,
            start=start,
            end=end,
            now=generated_at,
        )
        current_events, current_exceeded = await request_events(
            db, environment=environment, start=start, end=end, max_rows=MAX_EVENT_ROWS
        )
        previous_coverage = await check_capture_coverage(
            db,
            environment=environment,
            start=previous_start,
            end=previous_end,
            now=generated_at,
        )
        previous_events, previous_exceeded = await request_events(
            db,
            environment=environment,
            start=previous_start,
            end=previous_end,
            max_rows=MAX_EVENT_ROWS,
        )
        current = aggregate_requests(current_events, duration.total_seconds())
        previous = aggregate_requests(previous_events, duration.total_seconds())
        current_available = coverage.available and not current_exceeded
        previous_available = previous_coverage.available and not previous_exceeded
        capture_reason = "SAMPLE_LIMIT_EXCEEDED" if current_exceeded else coverage.reason_code
        metrics["request_rate"] = _observed_metric(
            current.request_rate,
            unit="req/s",
            available=current_available,
            reason=capture_reason,
            previous_value=previous.request_rate if previous_available else None,
            desirable_direction="up",
        )
        metrics["error_rate"] = _observed_metric(
            current.error_rate_percent,
            unit="percent",
            available=current_available,
            reason=capture_reason,
            previous_value=previous.error_rate_percent if previous_available else None,
            change_unit="percentage_points",
            desirable_direction="down",
        )
        metrics["p95_latency"] = _observed_metric(
            current.p95_latency_ms,
            unit="ms",
            available=current_available and current.p95_available,
            reason=capture_reason or "INSUFFICIENT_SAMPLES",
            previous_value=(
                previous.p95_latency_ms if previous_available and previous.p95_available else None
            ),
            desirable_direction="down",
        )
        metrics["queue_depth"] = _unavailable(unit="count")
        metrics["service_uptime"] = _unavailable(unit="percent")
        for key in ("queue_depth", "service_uptime"):
            _mark_unavailable(unavailable_fields, f"metrics.{key}")

        interval_seconds = next(
            interval
            for interval in (60, 300, 900, 3600, 21_600, 86_400)
            if math.ceil(duration.total_seconds() / interval) <= 500
        )
        point_rows = request_bucket_points(
            current_events,
            start=start,
            end=end,
            interval_seconds=interval_seconds,
        )
        rate_series = {
            "key": "request_rate",
            "unit": "req/s",
            "interval_seconds": interval_seconds,
            "points": [
                {
                    "timestamp": point["timestamp"],
                    "value": point["request_rate_per_second"] if current_available else None,
                    "target": None,
                }
                for point in point_rows
            ],
            "available": current_available,
        }
        series = [rate_series if item["key"] == "request_rate" else item for item in series]
        service_id = monitoring_service_id(environment)
        request_items = (
            [
                {
                    "id": service_id,
                    "label": "Platform API",
                    "value": current.request_count if current_available else 0,
                }
            ]
            if current_available
            else []
        )
        rate_items = (
            [
                {
                    "id": service_id,
                    "label": "Platform API",
                    "value": current.request_rate or 0,
                }
            ]
            if current_available
            else []
        )
        breakdowns.extend(
            [
                _breakdown(
                    "request_rate_by_service",
                    "req/s",
                    rate_items,
                    current.request_rate if current_available else None,
                )
                if current_available
                else _unavailable_breakdown("request_rate_by_service", "req/s"),
                _breakdown(
                    "requests_by_service",
                    "count",
                    request_items,
                    current.request_count,
                )
                if current_available
                else _unavailable_breakdown("requests_by_service"),
                _unavailable_breakdown("requests_by_region"),
                _unavailable_breakdown("pending_jobs_by_queue"),
            ]
        )
        recent_rows = list(
            (
                await db.scalars(
                    select(MonitoringEvent)
                    .where(
                        MonitoringEvent.service == SERVICE_CODE,
                        MonitoringEvent.environment == environment,
                        MonitoringEvent.created_at >= start,
                        MonitoringEvent.created_at < end,
                    )
                    .order_by(MonitoringEvent.created_at.desc(), MonitoringEvent.id.desc())
                    .limit(5)
                )
            ).all()
        )
        recent_events = [event_to_public(event, environment=environment) for event in recent_rows]
        if not current_available:
            for unavailable_key in (
                "series.request_rate",
                "breakdowns.request_rate_by_service",
                "breakdowns.requests_by_service",
            ):
                _mark_unavailable(
                    unavailable_fields, unavailable_key, capture_reason or "CAPTURE_INCOMPLETE"
                )
    else:
        for key in (
            "decisions_evaluated",
            "evidence_gate_pass_rate",
            "audit_coverage",
            "human_review_queue",
            "decision_overrides",
            "audit_trail_completeness",
            "provenance_linked_findings",
            "review_sla_within_target",
        ):
            unit = (
                "percent"
                if key
                in (
                    "evidence_gate_pass_rate",
                    "audit_coverage",
                    "audit_trail_completeness",
                    "review_sla_within_target",
                )
                else "count"
            )
            metrics[key] = _unavailable(unit=unit)
            _mark_unavailable(unavailable_fields, f"metrics.{key}")
        for key in ("decisions_by_outcome", "decisions_by_type", "decisions_by_routing"):
            breakdowns.append(_unavailable_breakdown(key))
            _mark_unavailable(unavailable_fields, f"breakdowns.{key}")
        _mark_unavailable(unavailable_fields, "governance_events")
        _mark_unavailable(unavailable_fields, "decision_gates")
        _mark_unavailable(unavailable_fields, "series.decision_gate_quality")

    return {
        "section": section,
        "generated_at": generated_at,
        "window": {"from": start, "to": end, "timezone": timezone},
        "comparison_window": comparison_window,
        "metrics": metrics,
        "money_metrics": money_metrics,
        "series": series,
        "breakdowns": breakdowns,
        "recent_runs": recent_runs,
        "recent_events": recent_events,
        "governance_events": governance_events,
        "decision_gates": decision_gates,
        "unavailable_fields": unavailable_fields,
    }

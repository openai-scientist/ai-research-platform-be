from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.auth.sessions import Principal, require_platform_admin
from platform_be.core.responses import ApiResponse, ErrorResponse, ok
from platform_be.db.session import get_db
from platform_be.services.admin_overview import (
    OverviewSection,
    build_admin_overview,
    effective_window,
)

router = APIRouter(prefix="/admin/overview", tags=["admin-overview"])


class Metric(BaseModel):
    value: int | float | None
    unit: str
    scope: Literal["snapshot", "window"]
    previous_value: int | float | None
    change: float | None
    change_unit: Literal["percent", "percentage_points"] | None
    desirable_direction: Literal["up", "down"] | None
    available: bool = Field(description="True when the metric has a measured source for its scope.")
    reason_code: str | None = Field(
        default=None,
        description="Stable reason code when unavailable; measured zero remains available.",
    )


class MoneyMetric(BaseModel):
    value_usd: str | None
    previous_value_usd: str | None
    change_percent: float | None
    available: bool
    attribution: str | None = None


class Window(BaseModel):
    from_time: datetime = Field(alias="from")
    to_time: datetime = Field(alias="to")
    timezone: str

    @classmethod
    def from_payload(cls, value: dict[str, Any]) -> "Window":
        return cls(**value)


class ComparisonWindow(BaseModel):
    from_time: datetime = Field(alias="from")
    to_time: datetime = Field(alias="to")

    @classmethod
    def from_payload(cls, value: dict[str, Any]) -> "ComparisonWindow":
        return cls(**value)


class SeriesPoint(BaseModel):
    timestamp: datetime
    value: float | None
    target: float | None = None


class ChartSeries(BaseModel):
    key: str
    unit: str
    interval_seconds: int
    points: list[SeriesPoint]
    available: bool


class BreakdownItem(BaseModel):
    id: str
    label: str
    value: int | float


class Breakdown(BaseModel):
    key: str
    unit: str
    items: list[BreakdownItem]
    total: int | float | None
    available: bool


class RecentRun(BaseModel):
    id: str
    project_id: str
    project_name: str
    status: str
    topic: str | None
    created_at: datetime
    finished_at: datetime | None
    duration_ms: int | None


class UnavailableField(BaseModel):
    key: str
    reason_code: str


class AdminOverviewData(BaseModel):
    section: OverviewSection
    generated_at: datetime
    window: Window
    comparison_window: ComparisonWindow
    metrics: dict[str, Metric]
    money_metrics: dict[str, MoneyMetric]
    series: list[ChartSeries]
    breakdowns: list[Breakdown]
    recent_runs: list[RecentRun]
    recent_events: list[dict[str, Any]]
    governance_events: list[dict[str, Any]]
    decision_gates: list[dict[str, Any]]
    unavailable_fields: list[UnavailableField]


@router.get(
    "",
    response_model=ApiResponse[AdminOverviewData],
    summary="Platform Admin overview aggregates",
    description=(
        "Returns metrics for one admin dashboard section. Time windows use [from, to), "
        "default to 28 days for overview/projects and 24 hours for operations/governance, "
        "and are limited to 366 days. Unsupported sources are explicitly unavailable. "
        "Each metric includes value, unit, snapshot/window scope, comparison data, availability, "
        "and a reason code when unavailable."
    ),
    responses={
        401: {
            "model": ErrorResponse,
            "description": "Session missing, expired, revoked, or suspended.",
        },
        403: {"model": ErrorResponse, "description": "Platform Admin role is required."},
        422: {
            "model": ErrorResponse,
            "description": "Section, timezone, or time range is invalid or exceeds 366 days.",
        },
    },
)
async def get_admin_overview(
    request: Request,
    section: OverviewSection = Query(
        default="overview", description="Admin tab section; one section is returned per request."
    ),
    from_time: datetime | None = Query(
        default=None, alias="from", description="Inclusive ISO-8601 start; provide with to."
    ),
    to_time: datetime | None = Query(
        default=None, alias="to", description="Exclusive ISO-8601 end; provide with from."
    ),
    timezone: str = Query(
        default="UTC",
        min_length=1,
        max_length=100,
        description="IANA timezone used to align returned series.",
    ),
    _: Principal = Depends(require_platform_admin),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[AdminOverviewData]:
    generated_at = datetime.now(UTC)
    start, end, timezone = effective_window(
        section=section,
        now=generated_at,
        from_time=from_time,
        to_time=to_time,
        timezone=timezone,
    )
    payload = await build_admin_overview(
        db,
        section=section,
        start=start,
        end=end,
        timezone=timezone,
        generated_at=generated_at,
        environment=request.app.state.settings.app_env,
    )
    data = AdminOverviewData(
        **{
            **payload,
            "window": Window.from_payload(payload["window"]),
            "comparison_window": ComparisonWindow.from_payload(payload["comparison_window"]),
        }
    )
    return ok(data)

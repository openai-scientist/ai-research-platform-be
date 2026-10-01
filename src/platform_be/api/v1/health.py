from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok

router = APIRouter(tags=["health"])


class HealthStatus(BaseModel):
    status: Literal["ok", "ready"]


@router.get("/health/live", response_model=ApiResponse[HealthStatus], summary="Liveness probe")
async def liveness() -> ApiResponse[HealthStatus]:
    return ok(HealthStatus(status="ok"))


@router.get(
    "/health/ready",
    response_model=ApiResponse[HealthStatus],
    summary="Readiness probe",
    responses={503: {"model": ErrorResponse, "description": "Database is not ready"}},
)
async def readiness(request: Request) -> ApiResponse[HealthStatus]:
    try:
        async with request.app.state.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise APIError(503, "DATABASE_UNAVAILABLE", "Database is not ready") from exc
    return ok(HealthStatus(status="ready"))

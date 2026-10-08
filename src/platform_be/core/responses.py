from collections.abc import Sequence
from contextvars import ContextVar
from typing import Any, Literal

from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Set once per request by the request-context middleware so handlers can build
# the envelope without threading the request through every call.
request_id_context: ContextVar[str | None] = ContextVar("request_id", default=None)


class Pagination(BaseModel):
    total: int
    limit: int
    offset: int


class ResponseMeta(BaseModel):
    request_id: str | None = None
    pagination: Pagination | None = None


class ApiResponse[T](BaseModel):
    """Envelope returned by every successful endpoint."""

    success: Literal[True] = True
    message: str = "OK"
    data: T
    meta: ResponseMeta


class ErrorDetail(BaseModel):
    field: str
    message: str


class ErrorBody(BaseModel):
    code: str
    details: list[ErrorDetail] = []
    reason: str | None = None


class ErrorMeta(BaseModel):
    request_id: str | None = None


class ErrorResponse(BaseModel):
    """Envelope returned by every failed request."""

    success: Literal[False] = False
    message: str
    error: ErrorBody
    meta: ErrorMeta


def ok[T](data: T, message: str = "OK") -> ApiResponse[T]:
    return ApiResponse(
        message=message, data=data, meta=ResponseMeta(request_id=request_id_context.get())
    )


def paginated[T](
    items: Sequence[T], *, total: int, limit: int, offset: int
) -> ApiResponse[list[T]]:
    return ApiResponse(
        data=list(items),
        meta=ResponseMeta(
            request_id=request_id_context.get(),
            pagination=Pagination(total=total, limit=limit, offset=offset),
        ),
    )


def error_content(
    code: str,
    message: str,
    request_id: str | None,
    details: Sequence[dict[str, str]] = (),
    reason: str | None = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "details": list(details)}
    if reason is not None:
        error["reason"] = reason
    return {
        "success": False,
        "message": message,
        "error": error,
        "meta": {"request_id": request_id},
    }


def error_response(
    status_code: int,
    code: str,
    message: str,
    request_id: str | None,
    *,
    details: Sequence[dict[str, str]] = (),
    headers: dict[str, str] | None = None,
    reason: str | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=error_content(code, message, request_id, details, reason),
        headers=headers,
    )

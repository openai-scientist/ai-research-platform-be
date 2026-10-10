import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from starlette.exceptions import HTTPException as StarletteHTTPException

from platform_be.api.v1.router import router as v1_router
from platform_be.auth.sessions import clear_session_cookie
from platform_be.core.config import Settings, get_settings
from platform_be.core.errors import APIError
from platform_be.core.logging import configure_logging
from platform_be.core.middleware import RequestProtectionMiddleware
from platform_be.core.responses import error_response, request_id_context
from platform_be.db.session import get_db
from platform_be.services.connectors import build_connector_executor, build_connector_factory
from platform_be.services.connectors.gate import ConnectionGate
from platform_be.services.default_admin import ensure_default_admin
from platform_be.services.email_sender import build_email_sender
from platform_be.services.file_store import build_file_store
from platform_be.services.google_drive_oauth import build_google_drive_oauth
from platform_be.services.google_oauth import build_google_oauth
from platform_be.services.invite_candidates_stream import InviteCandidatesHub
from platform_be.services.monitoring_alerts import MonitoringAlertEvaluator
from platform_be.services.monitoring_events import MonitoringTelemetry
from platform_be.services.monitoring_stream import MonitoringHub
from platform_be.services.notification_stream import NotificationHub
from platform_be.services.popper_client import build_popper_client
from platform_be.services.run_event_stream import RunEventHub
from platform_be.services.secret_box import SecretBox

logger = logging.getLogger("platform_be.http")


def create_app(
    settings: Settings | None = None,
    *,
    engine: AsyncEngine | None = None,
    session_factory: async_sessionmaker | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging("DEBUG" if settings.debug else "INFO")
    owned_engine = engine is None
    # hide_parameters keeps bound values (emails, IDs) out of logged database errors.
    app_engine = engine or create_async_engine(
        settings.database_url, pool_pre_ping=True, hide_parameters=True
    )
    app_session_factory = session_factory or async_sessionmaker(
        app_engine, expire_on_commit=False, autoflush=False
    )
    notification_hub = NotificationHub(app_engine)
    invite_candidates_hub = InviteCandidatesHub(app_engine)
    run_event_hub = RunEventHub(app_engine)
    monitoring_hub = MonitoringHub(app_engine)
    alert_evaluator = MonitoringAlertEvaluator(
        app_session_factory,
        environment=settings.app_env,
        threshold_percent=settings.monitoring_error_rate_alert_threshold_percent,
        recovery_percent=settings.monitoring_error_rate_alert_recovery_percent,
        minimum_samples=settings.monitoring_error_rate_alert_min_samples,
        lookback_seconds=settings.monitoring_error_rate_alert_lookback_seconds,
        stream_hub=monitoring_hub,
    )
    monitoring_telemetry = MonitoringTelemetry(
        app_session_factory,
        environment=settings.app_env,
        retention_days=settings.monitoring_event_retention_days,
        max_event_bytes=settings.monitoring_event_max_bytes,
        alert_evaluator=alert_evaluator.evaluate,
        stream_hub=monitoring_hub,
    )
    connector_executor = build_connector_executor(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await ensure_default_admin(app_session_factory, settings)
        await monitoring_telemetry.start()
        try:
            yield
        finally:
            try:
                await monitoring_telemetry.close()
            finally:
                try:
                    await notification_hub.close()
                finally:
                    try:
                        await invite_candidates_hub.close()
                    finally:
                        try:
                            await run_event_hub.close()
                        finally:
                            try:
                                await monitoring_hub.close()
                            finally:
                                # Do not wait on a server that may never answer.
                                connector_executor.shutdown(wait=False, cancel_futures=True)
                                if owned_engine:
                                    await app_engine.dispose()

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description=(
            "Identity, projects, datasets, runs, and review APIs for the AI Research Platform."
        ),
        debug=settings.debug,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.engine = app_engine
    app.state.session_factory = app_session_factory
    app.state.get_db = get_db
    app.state.file_store = build_file_store(settings)
    app.state.popper_client = build_popper_client(settings)
    app.state.email_sender = build_email_sender(settings)
    app.state.google_oauth = build_google_oauth(settings)
    app.state.google_drive_oauth = build_google_drive_oauth(settings)
    app.state.notification_hub = notification_hub
    app.state.invite_candidates_hub = invite_candidates_hub
    app.state.run_event_hub = run_event_hub
    app.state.monitoring_hub = monitoring_hub
    app.state.monitoring_telemetry = monitoring_telemetry
    app.state.secret_box = (
        SecretBox(settings.connection_secret_key.get_secret_value())
        if settings.connection_secret_key
        else None
    )
    app.state.connector_executor = connector_executor
    app.state.connector_factory = build_connector_factory(
        settings, executor=connector_executor, google_oauth=app.state.google_drive_oauth
    )
    app.state.connection_gate = ConnectionGate(
        max_concurrent=settings.connection_max_concurrent_queries,
        max_per_project=settings.connection_max_concurrent_per_owner,
        max_per_user=settings.connection_max_concurrent_per_owner,
        rate_limit=settings.connection_probe_rate_limit,
        query_rate_limit=settings.connection_query_rate_limit,
        rate_window_seconds=settings.auth_session_rate_window_seconds,
    )
    app.add_middleware(RequestProtectionMiddleware, settings=settings)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = getattr(request.state, "request_id", None)
        if request_id is None:
            try:
                request_id = str(uuid.UUID(request.headers.get("X-Request-ID", "")))
            except ValueError:
                request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        request_id_context.set(request_id)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            # Answer here rather than in Starlette's outermost error middleware so the
            # 500 still carries the request ID and CORS headers the frontend needs.
            logger.error(
                "unhandled application error",
                exc_info=exc,
                extra={"request_id": request_id},
            )
            response = error_response(
                500, "INTERNAL_ERROR", "An unexpected error occurred", request_id
            )
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "request completed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            },
        )
        content_type = response.headers.get("content-type", "")
        is_sse = "text/event-stream" in request.headers.get(
            "accept", ""
        ).lower() or content_type.startswith("text/event-stream")
        monitoring_path = f"{settings.api_prefix}/admin/log-monitoring"
        is_monitoring_request = request.url.path == monitoring_path or request.url.path.startswith(
            f"{monitoring_path}/"
        )
        if not is_sse and not is_monitoring_request:
            route = request.scope.get("route")
            route_template = getattr(route, "path", None)
            request.app.state.monitoring_telemetry.submit_request(
                request_id=request_id,
                method=request.method,
                route_template=route_template,
                status_code=response.status_code,
                duration_ms=(time.perf_counter() - started) * 1000,
            )
        return response

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-CSRF-Token", "X-Request-ID"],
        expose_headers=["X-Request-ID", "Retry-After"],
    )

    def request_id_of(request: Request) -> str | None:
        return getattr(request.state, "request_id", None)

    @app.exception_handler(APIError)
    async def api_error_handler(request: Request, exc: APIError) -> JSONResponse:
        response = error_response(
            exc.status_code,
            exc.code,
            exc.message,
            request_id_of(request),
            headers={"Retry-After": str(exc.retry_after)} if exc.retry_after else None,
            reason=exc.reason,
        )
        if exc.clear_session_cookie:
            clear_session_cookie(response, settings)
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        issues = [
            {"field": ".".join(str(part) for part in error["loc"]), "message": error["msg"]}
            for error in exc.errors()
        ]
        return error_response(
            422,
            "VALIDATION_ERROR",
            "Request validation failed",
            request_id_of(request),
            details=issues,
        )

    # Registered on the Starlette base class so router-level 404/405 use the envelope too.
    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED"}.get(exc.status_code, "HTTP_ERROR")
        message = (
            exc.detail if isinstance(exc.detail, str) else "The request could not be completed"
        )
        return error_response(
            exc.status_code, code, message, request_id_of(request), headers=exc.headers
        )

    @app.exception_handler(IntegrityError)
    async def integrity_error_handler(request: Request, _: IntegrityError) -> JSONResponse:
        return error_response(
            409, "CONFLICT", "The request conflicts with existing data", request_id_of(request)
        )

    app.include_router(v1_router, prefix=settings.api_prefix)
    return app


app = create_app()

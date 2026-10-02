from fastapi import APIRouter

from platform_be.api.v1.audit import router as audit_router
from platform_be.api.v1.auth import router as auth_router
from platform_be.api.v1.health import router as health_router
from platform_be.api.v1.projects import router as projects_router
from platform_be.api.v1.users import router as users_router
from platform_be.core.responses import ErrorResponse

# Every failure uses the same envelope; replace FastAPI's default 422 schema with it.
router = APIRouter(
    responses={422: {"model": ErrorResponse, "description": "Request validation failed"}}
)
router.include_router(health_router)
router.include_router(auth_router)
router.include_router(users_router)
router.include_router(audit_router)
router.include_router(projects_router)

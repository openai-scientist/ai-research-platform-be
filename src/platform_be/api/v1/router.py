from fastapi import APIRouter

from platform_be.api.v1.admin_log_monitoring import router as admin_log_monitoring_router
from platform_be.api.v1.admin_overview import router as admin_overview_router
from platform_be.api.v1.admin_usage import router as admin_usage_router
from platform_be.api.v1.audit import router as audit_router
from platform_be.api.v1.auth import router as auth_router
from platform_be.api.v1.auth_google import router as auth_google_router
from platform_be.api.v1.comments import router as comments_router
from platform_be.api.v1.connections import router as connections_router
from platform_be.api.v1.connections_google import router as connections_google_router
from platform_be.api.v1.datasets import router as datasets_router
from platform_be.api.v1.frame_reviews import router as frame_reviews_router
from platform_be.api.v1.health import router as health_router
from platform_be.api.v1.internal_popper import router as internal_popper_router
from platform_be.api.v1.invitations import router as invitations_router
from platform_be.api.v1.notifications import router as notifications_router
from platform_be.api.v1.project_files import router as project_files_router
from platform_be.api.v1.projects import router as projects_router
from platform_be.api.v1.run_artifacts import router as run_artifacts_router
from platform_be.api.v1.runs import router as runs_router
from platform_be.api.v1.users import router as users_router
from platform_be.core.responses import ErrorResponse

# Every failure uses the same envelope; replace FastAPI's default 422 schema with it.
router = APIRouter(
    responses={422: {"model": ErrorResponse, "description": "Request validation failed"}}
)
router.include_router(health_router)
router.include_router(auth_router)
router.include_router(auth_google_router)
router.include_router(users_router)
router.include_router(audit_router)
router.include_router(projects_router)
router.include_router(invitations_router)
router.include_router(connections_google_router)
router.include_router(connections_router)
router.include_router(datasets_router)
router.include_router(project_files_router)
router.include_router(runs_router)
router.include_router(frame_reviews_router)
router.include_router(run_artifacts_router)
router.include_router(comments_router)
router.include_router(notifications_router)
router.include_router(admin_usage_router)
router.include_router(admin_overview_router)
router.include_router(admin_log_monitoring_router)
router.include_router(internal_popper_router)

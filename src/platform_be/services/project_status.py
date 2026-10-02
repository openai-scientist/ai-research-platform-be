from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.models.dataset import Dataset, DatasetVersion
from platform_be.models.project import Project
from platform_be.models.research import ACTIVE_RUN_STATUSES, ResearchRun


async def derive_project_status(db: AsyncSession, project: Project) -> str:
    """Research progress as the project's datasets and runs show it."""
    await db.flush()
    active = await db.scalar(
        select(ResearchRun.status).where(
            ResearchRun.project_id == project.id, ResearchRun.status.in_(ACTIVE_RUN_STATUSES)
        )
    )
    if active == "awaiting_review":
        return "needs_review"
    if active is not None:
        return "researching"
    has_data = await db.scalar(
        select(DatasetVersion.id)
        .join(Dataset, Dataset.id == DatasetVersion.dataset_id)
        .where(Dataset.project_id == project.id)
        .limit(1)
    )
    return "data_ready" if has_data else "draft"


async def refresh_project_status(db: AsyncSession, project: Project) -> None:
    """Recompute the status after a dataset or run change.

    ``completed`` is set by a Project Manager and stays until they reopen the project.
    """
    if project.status == "completed":
        return
    status = await derive_project_status(db, project)
    if project.status != status:
        project.status = status

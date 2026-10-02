from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform_be.api.v1.internal_popper import ArtifactKind
from platform_be.auth.sessions import Principal, require_active_principal
from platform_be.core.errors import APIError
from platform_be.core.responses import ApiResponse, ErrorResponse, ok
from platform_be.db.session import get_db
from platform_be.models.research import RunArtifact
from platform_be.services.access import require_project_access
from platform_be.services.file_store import FileStore, attachment_headers, get_file_store
from platform_be.services.runs import get_run

router = APIRouter(prefix="/projects/{project_id}/runs/{run_id}/artifacts", tags=["runs"])

NOT_FOUND = {404: {"model": ErrorResponse, "description": "The run or file was not found"}}


class RunArtifactItem(BaseModel):
    id: str
    run_id: str
    kind: ArtifactKind
    filename: str
    content_type: str
    size_bytes: int
    sha256: str
    created_at: datetime


@router.get(
    "",
    response_model=ApiResponse[list[RunArtifactItem]],
    summary="List the result files of a run",
    responses=NOT_FOUND,
)
async def list_run_artifacts(
    project_id: UUID,
    run_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
) -> ApiResponse[list[RunArtifactItem]]:
    await require_project_access(db, principal, project_id)
    run = await get_run(db, project_id, run_id)
    artifacts = (
        await db.scalars(
            select(RunArtifact).where(RunArtifact.run_id == run.id).order_by(RunArtifact.filename)
        )
    ).all()
    return ok(
        [
            RunArtifactItem(
                id=str(artifact.id),
                run_id=str(artifact.run_id),
                kind=artifact.kind,
                filename=artifact.filename,
                content_type=artifact.content_type,
                size_bytes=artifact.size_bytes,
                sha256=artifact.sha256,
                created_at=artifact.created_at,
            )
            for artifact in artifacts
        ]
    )


@router.get(
    "/{artifact_id}/download",
    summary="Download a result file",
    description="Always served as a download, never rendered in the browser.",
    response_class=StreamingResponse,
    responses={200: {"content": {"application/octet-stream": {}}}, **NOT_FOUND},
)
async def download_run_artifact(
    project_id: UUID,
    run_id: UUID,
    artifact_id: UUID,
    principal: Principal = Depends(require_active_principal),
    db: AsyncSession = Depends(get_db),
    store: FileStore = Depends(get_file_store),
) -> StreamingResponse:
    await require_project_access(db, principal, project_id)
    run = await get_run(db, project_id, run_id)
    artifact = await db.scalar(
        select(RunArtifact).where(RunArtifact.id == artifact_id, RunArtifact.run_id == run.id)
    )
    if artifact is None or not await store.exists(artifact.storage_key):
        raise APIError(404, "NOT_FOUND", "Result file was not found")
    # End the transaction now so a slow download does not hold a database connection.
    await db.commit()
    return StreamingResponse(
        store.open(artifact.storage_key),
        media_type=artifact.content_type,
        headers={
            **attachment_headers(artifact.filename),
            "Content-Length": str(artifact.size_bytes),
        },
    )

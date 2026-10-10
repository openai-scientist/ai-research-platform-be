from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.identity import User
from platform_be.models.project import Project
from platform_be.models.research import FrameReview, ResearchRun
from tests.conftest import Harness, login

OVERVIEW = "/api/v1/admin/overview"
ADMIN_EMAIL = "overview-admin@example.com"


async def _login_admin(harness: Harness, client) -> None:
    await login(harness, client, uid="overview-admin", email=ADMIN_EMAIL)
    await bootstrap_admin(ADMIN_EMAIL, settings=harness.settings, session_factory=harness.factory)


@pytest.mark.asyncio
async def test_overview_uses_real_sources_and_half_open_windows(harness: Harness) -> None:
    async with harness.client() as admin_client, harness.client() as member_client:
        await _login_admin(harness, admin_client)
        await login(
            harness, member_client, uid="overview-member", email="overview-member@example.com"
        )
        now = datetime.now(UTC)
        start = datetime(2026, 10, 1, tzinfo=UTC)
        end = datetime(2026, 10, 2, tzinfo=UTC)
        previous_start = start - timedelta(days=1)

        async with harness.factory() as db:
            admin = await db.scalar(select(User).where(User.email == ADMIN_EMAIL))
            assert admin is not None
            active_project = Project(
                id=uuid4(),
                name="Active study",
                owner_user_id=admin.id,
                status="researching",
                created_at=previous_start,
            )
            completed_project = Project(
                id=uuid4(),
                name="Completed study",
                owner_user_id=admin.id,
                status="completed",
                created_at=previous_start,
            )
            archived_project = Project(
                id=uuid4(),
                name="Archived study",
                owner_user_id=admin.id,
                status="researching",
                archived_at=now,
                created_at=previous_start,
            )
            db.add_all([active_project, completed_project, archived_project])
            db.add_all(
                [
                    ResearchRun(
                        id=uuid4(),
                        project_id=active_project.id,
                        created_by_user_id=admin.id,
                        budget_usd=Decimal("10"),
                        cost_usd=Decimal("2.50"),
                        status="completed",
                        created_at=start,
                        started_at=start,
                        finished_at=start + timedelta(minutes=5),
                    ),
                    ResearchRun(
                        id=uuid4(),
                        project_id=completed_project.id,
                        created_by_user_id=admin.id,
                        budget_usd=Decimal("10"),
                        cost_usd=Decimal("0"),
                        status="completed",
                        created_at=previous_start,
                        started_at=start,
                        finished_at=start + timedelta(minutes=8),
                    ),
                    ResearchRun(
                        id=uuid4(),
                        project_id=completed_project.id,
                        created_by_user_id=admin.id,
                        budget_usd=Decimal("10"),
                        status="failed",
                        created_at=previous_start,
                        finished_at=previous_start + timedelta(hours=1),
                    ),
                    ResearchRun(
                        id=uuid4(),
                        project_id=completed_project.id,
                        created_by_user_id=admin.id,
                        budget_usd=Decimal("10"),
                        status="failed",
                        created_at=end,
                        finished_at=end,
                    ),
                    ResearchRun(
                        id=uuid4(),
                        project_id=active_project.id,
                        created_by_user_id=admin.id,
                        budget_usd=Decimal("10"),
                        status="running",
                        created_at=start + timedelta(hours=1),
                    ),
                ]
            )
            pending_run = ResearchRun(
                id=uuid4(),
                project_id=active_project.id,
                created_by_user_id=admin.id,
                budget_usd=Decimal("10"),
                status="failed",
                created_at=previous_start,
            )
            db.add(pending_run)
            await db.flush()
            db.add(
                FrameReview(
                    id=uuid4(),
                    run_id=pending_run.id,
                    sequence=1,
                    request_storage_key="reviews/pending.json",
                    requested_at=now,
                    submitted_at=None,
                )
            )
            await db.commit()

        denied = await member_client.get(
            OVERVIEW,
            params={"from": start.isoformat(), "to": end.isoformat()},
        )
        assert denied.status_code == 403

        response = await admin_client.get(
            OVERVIEW,
            params={
                "section": "projects",
                "from": start.isoformat(),
                "to": end.isoformat(),
                "timezone": "Asia/Ho_Chi_Minh",
            },
        )
        assert response.status_code == 200, response.text
        body = response.json()
        data = body["data"]
        assert body["meta"]["pagination"] is None
        assert data["window"] == {
            "from": start.isoformat().replace("+00:00", "Z"),
            "to": end.isoformat().replace("+00:00", "Z"),
            "timezone": "Asia/Ho_Chi_Minh",
        }
        assert data["metrics"]["active_projects"]["value"] == 1
        assert data["metrics"]["total_projects"]["value"] == 3
        assert data["metrics"]["archived_projects"]["value"] == 1
        assert data["metrics"]["experiment_runs"]["value"] == 2
        assert data["metrics"]["successful_runs_rate"]["value"] == 100
        assert data["metrics"]["successful_runs_rate"]["previous_value"] == 0
        assert data["metrics"]["successful_runs_rate"]["change"] == 100
        assert data["metrics"]["active_runs"]["value"] == 1
        assert data["metrics"]["review_queue"]["value"] == 1
        assert data["money_metrics"]["reported_run_cost"]["value_usd"] == "2.5000"
        assert data["money_metrics"]["estimated_spend"]["available"] is False
        assert [run["status"] for run in data["recent_runs"]] == ["running", "completed"]


@pytest.mark.asyncio
async def test_overview_defaults_and_marks_unmeasured_data_unavailable(harness: Harness) -> None:
    async with harness.client() as admin_client:
        await _login_admin(harness, admin_client)
        response = await admin_client.get(OVERVIEW, params={"section": "operations"})
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        start = datetime.fromisoformat(data["window"]["from"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(data["window"]["to"].replace("Z", "+00:00"))
        assert end - start == timedelta(hours=24)
        assert data["window"]["timezone"] == "UTC"
        for key in ("request_rate", "error_rate", "p95_latency", "queue_depth", "service_uptime"):
            assert data["metrics"][key]["value"] is None
            assert data["metrics"][key]["available"] is False


@pytest.mark.asyncio
async def test_overview_rejects_incomplete_invalid_and_oversized_ranges(harness: Harness) -> None:
    async with harness.client() as admin_client:
        await _login_admin(harness, admin_client)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        end = datetime(2026, 1, 2, tzinfo=UTC)
        incomplete = await admin_client.get(OVERVIEW, params={"from": start.isoformat()})
        assert incomplete.status_code == 422
        assert incomplete.json()["error"]["code"] == "TIME_RANGE_PAIR_REQUIRED"

        reversed_window = await admin_client.get(
            OVERVIEW,
            params={"from": end.isoformat(), "to": start.isoformat()},
        )
        assert reversed_window.status_code == 422
        assert reversed_window.json()["error"]["code"] == "INVALID_TIME_RANGE"

        oversized = await admin_client.get(
            OVERVIEW,
            params={
                "from": (start - timedelta(days=1)).isoformat(),
                "to": (start + timedelta(days=366)).isoformat(),
            },
        )
        assert oversized.status_code == 422
        assert oversized.json()["error"]["code"] == "TIME_RANGE_TOO_LARGE"

        invalid_timezone = await admin_client.get(OVERVIEW, params={"timezone": "Mars/Olympus"})
        assert invalid_timezone.status_code == 422
        assert invalid_timezone.json()["error"]["code"] == "INVALID_TIMEZONE"

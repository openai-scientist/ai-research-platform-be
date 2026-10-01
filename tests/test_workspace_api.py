from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import func, select, text

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.audit import AuditEvent
from platform_be.models.workspace import Organization, OrganizationMembership
from tests.conftest import Harness, login, mutation_headers


@pytest.mark.asyncio
async def test_org_project_role_scopes_archive_and_audit(harness: Harness) -> None:
    async with (
        harness.client() as platform_admin,
        harness.client() as org_admin,
        harness.client() as plain_org_member,
        harness.client() as researcher,
    ):
        pa = await login(harness, platform_admin, uid="pa", email="platform@example.com")
        oa = await login(harness, org_admin, uid="oa", email="org-admin@example.com")
        member = await login(harness, plain_org_member, uid="member", email="member@example.com")
        researcher_session = await login(
            harness, researcher, uid="researcher", email="researcher@example.com"
        )
        await bootstrap_admin(
            "platform@example.com",
            settings=harness.settings,
            session_factory=harness.factory,
        )

        created_org = await platform_admin.post(
            "/api/v1/organizations",
            json={
                "name": "Research Group",
                "slug": "research-group",
                "description": "Shared workspace",
                "initial_admin_email": "org-admin@example.com",
            },
            headers=mutation_headers(pa["csrf_token"]),
        )
        assert created_org.status_code == 201, created_org.text
        organization_id = created_org.json()["data"]["id"]

        cannot_suspend_last_org_admin = await platform_admin.patch(
            f"/api/v1/users/{oa['user']['id']}/status",
            json={"status": "suspended"},
            headers=mutation_headers(pa["csrf_token"]),
        )
        assert cannot_suspend_last_org_admin.status_code == 409
        assert cannot_suspend_last_org_admin.json()["error"]["code"] == "LAST_ORGANIZATION_ADMIN"

        add_member = await org_admin.post(
            f"/api/v1/organizations/{organization_id}/members",
            json={"email": "member@example.com", "role": "organization_member"},
            headers=mutation_headers(oa["csrf_token"]),
        )
        assert add_member.status_code == 201, add_member.text
        add_plain_member = await org_admin.post(
            f"/api/v1/organizations/{organization_id}/members",
            json={"email": "researcher@example.com", "role": "organization_member"},
            headers=mutation_headers(oa["csrf_token"]),
        )
        assert add_plain_member.status_code == 201, add_plain_member.text

        org_item = await plain_org_member.get(f"/api/v1/organizations/{organization_id}")
        assert org_item.status_code == 200
        created_project = await plain_org_member.post(
            f"/api/v1/organizations/{organization_id}/projects",
            json={"name": "Study A", "slug": "study-a", "domain": "biology"},
            headers=mutation_headers(member["csrf_token"]),
        )
        assert created_project.status_code == 201, created_project.text
        project_id = created_project.json()["data"]["id"]
        assert created_project.json()["data"]["created_by_user_id"] == member["user"]["id"]

        cannot_suspend_last_manager = await platform_admin.patch(
            f"/api/v1/users/{member['user']['id']}/status",
            json={"status": "suspended"},
            headers=mutation_headers(pa["csrf_token"]),
        )
        assert cannot_suspend_last_manager.status_code == 409
        assert cannot_suspend_last_manager.json()["error"]["code"] == "LAST_PROJECT_MANAGER"

        hidden_project = await researcher.get(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}"
        )
        assert hidden_project.status_code == 404
        add_researcher = await plain_org_member.post(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}/members",
            json={"email": "researcher@example.com", "role": "researcher"},
            headers=mutation_headers(member["csrf_token"]),
        )
        assert add_researcher.status_code == 201, add_researcher.text
        visible_project = await researcher.get(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}"
        )
        assert visible_project.status_code == 200
        read_only = await researcher.patch(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}",
            json={"description": "unauthorized edit"},
            headers=mutation_headers(researcher_session["csrf_token"]),
        )
        assert read_only.status_code == 403

        wrong_tenant = await plain_org_member.get(
            f"/api/v1/organizations/{UUID(int=99)}/projects/{project_id}"
        )
        assert wrong_tenant.status_code == 404
        members = await plain_org_member.get(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}/members"
        )
        assert members.status_code == 200
        manager_membership = next(
            item for item in members.json()["data"] if item["user_id"] == member["user"]["id"]
        )
        cannot_remove_final_manager = await plain_org_member.delete(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}/members/{manager_membership['id']}",
            headers=mutation_headers(member["csrf_token"]),
        )
        assert cannot_remove_final_manager.status_code == 409
        assert cannot_remove_final_manager.json()["error"]["code"] == "LAST_PROJECT_MANAGER"

        promote_second_manager = await plain_org_member.put(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}/members/{add_researcher.json()['data']['id']}",
            json={"role": "project_manager"},
            headers=mutation_headers(member["csrf_token"]),
        )
        assert promote_second_manager.status_code == 200, promote_second_manager.text

        suspend_second_manager = await platform_admin.patch(
            f"/api/v1/users/{researcher_session['user']['id']}/status",
            json={"status": "suspended"},
            headers=mutation_headers(pa["csrf_token"]),
        )
        assert suspend_second_manager.status_code == 200, suspend_second_manager.text
        cannot_demote_only_active_manager = await plain_org_member.put(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}/members/{manager_membership['id']}",
            json={"role": "researcher"},
            headers=mutation_headers(member["csrf_token"]),
        )
        assert cannot_demote_only_active_manager.status_code == 409
        assert cannot_demote_only_active_manager.json()["error"]["code"] == "LAST_PROJECT_MANAGER"

        archived_org = await org_admin.post(
            f"/api/v1/organizations/{organization_id}/archive",
            headers=mutation_headers(oa["csrf_token"]),
        )
        assert archived_org.status_code == 200
        blocked_write = await plain_org_member.patch(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}",
            json={"description": "blocked by parent archive"},
            headers=mutation_headers(member["csrf_token"]),
        )
        assert blocked_write.status_code == 409
        restore_org = await org_admin.post(
            f"/api/v1/organizations/{organization_id}/restore",
            headers=mutation_headers(oa["csrf_token"]),
        )
        assert restore_org.status_code == 200

        archive_project = await plain_org_member.post(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}/archive",
            headers=mutation_headers(member["csrf_token"]),
        )
        assert archive_project.status_code == 200
        await org_admin.post(
            f"/api/v1/organizations/{organization_id}/archive",
            headers=mutation_headers(oa["csrf_token"]),
        )
        await org_admin.post(
            f"/api/v1/organizations/{organization_id}/restore",
            headers=mutation_headers(oa["csrf_token"]),
        )
        still_archived = await plain_org_member.get(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}"
        )
        assert still_archived.status_code == 200
        blocked_by_child = await plain_org_member.patch(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}",
            json={"description": "still archived"},
            headers=mutation_headers(member["csrf_token"]),
        )
        assert blocked_by_child.status_code == 409
        restore_project = await plain_org_member.post(
            f"/api/v1/organizations/{organization_id}/projects/{project_id}/restore",
            headers=mutation_headers(member["csrf_token"]),
        )
        assert restore_project.status_code == 200

        remove_suspended_org_member = await org_admin.delete(
            f"/api/v1/organizations/{organization_id}/members/{add_plain_member.json()['data']['id']}",
            headers=mutation_headers(oa["csrf_token"]),
        )
        assert remove_suspended_org_member.status_code == 200
        org_members = await org_admin.get(f"/api/v1/organizations/{organization_id}/members")
        assert org_members.status_code == 200
        only_admin = next(
            item for item in org_members.json()["data"] if item["role"] == "organization_admin"
        )
        denied_last_admin = await org_admin.delete(
            f"/api/v1/organizations/{organization_id}/members/{only_admin['id']}",
            headers=mutation_headers(oa["csrf_token"]),
        )
        assert denied_last_admin.status_code == 409
        assert denied_last_admin.json()["error"]["code"] == "LAST_ORGANIZATION_ADMIN"

        scoped_audit = await org_admin.get(f"/api/v1/audit?organization_id={organization_id}")
        assert scoped_audit.status_code == 200
        assert scoped_audit.json()["meta"]["pagination"]["total"] >= 6
        organization_created_audit = await org_admin.get(
            "/api/v1/audit",
            params={
                "organization_id": organization_id,
                "action": "organization.created",
            },
        )
        assert organization_created_audit.status_code == 200
        assert organization_created_audit.json()["meta"]["pagination"]["total"] == 1
        created_at = datetime.fromisoformat(
            organization_created_audit.json()["data"][0]["created_at"]
        )
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        bounded_audit = await org_admin.get(
            "/api/v1/audit",
            params={
                "organization_id": organization_id,
                "action": "organization.created",
                "from": (created_at - timedelta(seconds=1)).isoformat(),
                "to": (created_at + timedelta(seconds=1)).isoformat(),
            },
        )
        assert bounded_audit.status_code == 200
        assert bounded_audit.json()["meta"]["pagination"]["total"] == 1
        out_of_range_audit = await org_admin.get(
            "/api/v1/audit",
            params={
                "organization_id": organization_id,
                "action": "organization.created",
                "from": (created_at + timedelta(days=1)).isoformat(),
            },
        )
        assert out_of_range_audit.status_code == 200
        assert out_of_range_audit.json()["meta"]["pagination"]["total"] == 0
        invalid_time_range = await org_admin.get(
            "/api/v1/audit",
            params={
                "organization_id": organization_id,
                "from": (created_at + timedelta(days=1)).isoformat(),
                "to": created_at.isoformat(),
            },
        )
        assert invalid_time_range.status_code == 422
        timezone_required = await org_admin.get(
            "/api/v1/audit",
            params={"organization_id": organization_id, "from": "2026-01-01T00:00:00"},
        )
        assert timezone_required.status_code == 422
        member_audit = await plain_org_member.get(
            f"/api/v1/audit?organization_id={organization_id}"
        )
        assert member_audit.status_code == 404
        global_audit = await platform_admin.get("/api/v1/audit")
        assert global_audit.status_code == 200

        async with harness.factory() as db:
            audit_count = int(await db.scalar(select(func.count()).select_from(AuditEvent)) or 0)
            assert audit_count > 0


@pytest.mark.asyncio
async def test_audit_insert_failure_rolls_back_organization_creation(harness: Harness) -> None:
    async with harness.client() as platform_admin:
        session = await login(
            harness, platform_admin, uid="audit-failure-pa", email="audit-failure-pa@example.com"
        )
        await bootstrap_admin(
            "audit-failure-pa@example.com",
            settings=harness.settings,
            session_factory=harness.factory,
        )
        async with harness.factory.begin() as db:
            await db.execute(
                text(
                    """CREATE TRIGGER fail_organization_audit
                    BEFORE INSERT ON audit_events
                    WHEN NEW.action = 'organization.created'
                    BEGIN
                        SELECT RAISE(ABORT, 'audit insert forced to fail');
                    END;"""
                )
            )

        response = await platform_admin.post(
            "/api/v1/organizations",
            json={
                "name": "Audit Rollback Organization",
                "slug": "audit-rollback-organization",
                "initial_admin_email": "audit-failure-pa@example.com",
            },
            headers=mutation_headers(session["csrf_token"]),
        )
        assert response.status_code == 409

    async with harness.factory() as db:
        organization_count = int(
            await db.scalar(select(func.count()).select_from(Organization)) or 0
        )
        orphaned_memberships = int(
            await db.scalar(select(func.count()).select_from(OrganizationMembership)) or 0
        )
        assert organization_count == 0
        assert orphaned_memberships == 0


@pytest.mark.asyncio
async def test_new_user_creates_organization_and_becomes_its_admin(harness: Harness) -> None:
    async with harness.client() as founder_client, harness.client() as colleague_client:
        founder = await login(harness, founder_client, uid="founder", email="founder@example.com")
        colleague = await login(
            harness, colleague_client, uid="colleague", email="colleague@example.com"
        )

        created = await founder_client.post(
            "/api/v1/organizations",
            json={"name": "Founder Lab", "slug": "founder-lab"},
            headers=mutation_headers(founder["csrf_token"]),
        )
        assert created.status_code == 201, created.text
        organization_id = created.json()["data"]["id"]

        members = await founder_client.get(f"/api/v1/organizations/{organization_id}/members")
        assert [(item["email"], item["role"]) for item in members.json()["data"]] == [
            ("founder@example.com", "organization_admin")
        ]
        added = await founder_client.post(
            f"/api/v1/organizations/{organization_id}/members",
            json={"email": "colleague@example.com", "role": "organization_member"},
            headers=mutation_headers(founder["csrf_token"]),
        )
        assert added.status_code == 201, added.text
        project = await colleague_client.post(
            f"/api/v1/organizations/{organization_id}/projects",
            json={"name": "First Study", "slug": "first-study"},
            headers=mutation_headers(colleague["csrf_token"]),
        )
        assert project.status_code == 201, project.text

        # An Organization Admin who names no manager runs the project themselves.
        own_project = await founder_client.post(
            f"/api/v1/organizations/{organization_id}/projects",
            json={"name": "Founder Study", "slug": "founder-study"},
            headers=mutation_headers(founder["csrf_token"]),
        )
        assert own_project.status_code == 201, own_project.text
        own_project_url = (
            f"/api/v1/organizations/{organization_id}/projects/{own_project.json()['data']['id']}"
        )
        reviewer_added = await founder_client.post(
            f"{own_project_url}/members",
            json={"email": "colleague@example.com", "role": "reviewer"},
            headers=mutation_headers(founder["csrf_token"]),
        )
        assert reviewer_added.status_code == 201, reviewer_added.text

        # Members can see who they work with, but cannot change the member lists.
        org_members = await colleague_client.get(f"/api/v1/organizations/{organization_id}/members")
        assert org_members.status_code == 200
        assert org_members.json()["meta"]["pagination"]["total"] == 2
        project_members = await colleague_client.get(f"{own_project_url}/members")
        assert project_members.status_code == 200
        assert {(item["email"], item["role"]) for item in project_members.json()["data"]} == {
            ("founder@example.com", "project_manager"),
            ("colleague@example.com", "reviewer"),
        }
        denied = await colleague_client.post(
            f"{own_project_url}/members",
            json={"email": "founder@example.com", "role": "reviewer"},
            headers=mutation_headers(colleague["csrf_token"]),
        )
        assert denied.status_code == 403

        # Only a Platform Admin may hand a new organization to someone else.
        for_other = await colleague_client.post(
            "/api/v1/organizations",
            json={
                "name": "Not Mine",
                "slug": "not-mine",
                "initial_admin_email": "founder@example.com",
            },
            headers=mutation_headers(colleague["csrf_token"]),
        )
        assert for_other.status_code == 403
        assert for_other.json()["error"]["code"] == "ROLE_REQUIRED"

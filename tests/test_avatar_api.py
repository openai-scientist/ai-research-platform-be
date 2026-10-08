import pytest
from httpx import AsyncClient
from sqlalchemy import select

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.audit import AuditEvent
from platform_be.services import avatars
from tests.conftest import ORIGIN, Harness, login, mutation_headers
from tests.test_comments_and_notifications_api import NOTIFICATIONS, team
from tests.test_projects_api import PROJECTS
from tests.test_runs_api import start_run

AUTH = "/api/v1/auth"
USERS = "/api/v1/users"

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 32
WEBP = b"RIFF\x24\x00\x00\x00WEBPVP8 " + b"\x00" * 32
GIF = b"GIF89a" + b"\x00" * 32
SVG = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'


async def upload(client: AsyncClient, session: dict, content: bytes, *, url: str | None = None):
    return await client.post(
        url or f"{AUTH}/me/avatar",
        # The name and content type lie on purpose: the server decides from the bytes.
        files={"file": ("picture.txt", content, "text/plain")},
        headers=mutation_headers(session["csrf_token"]),
    )


def stored_avatars(tmp_path) -> list[str]:
    root = tmp_path / "storage" / "users"
    return sorted(path.name for path in root.rglob("*") if path.is_file())


@pytest.mark.asyncio
async def test_upload_serve_replace_and_delete(harness: Harness, tmp_path) -> None:
    async with harness.client() as client, harness.client() as other:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        await login(harness, other, uid="other", email="other@example.com")
        assert session["user"]["avatar_url"] is None
        user_id = session["user"]["id"]

        urls = []
        for content, content_type, extension in (
            (PNG, "image/png", ".png"),
            (JPEG, "image/jpeg", ".jpg"),
            (WEBP, "image/webp", ".webp"),
        ):
            uploaded = await upload(client, session, content)
            assert uploaded.status_code == 200, uploaded.text
            url = uploaded.json()["data"]["avatar_url"]
            assert url.startswith(f"{USERS}/{user_id}/avatar?v=")
            assert (await client.get(f"{AUTH}/me")).json()["data"]["user"]["avatar_url"] == url
            # Any signed-in user may load it.
            served = await other.get(url)
            assert served.status_code == 200
            assert served.content == content
            assert served.headers["content-type"] == content_type
            assert served.headers["x-content-type-options"] == "nosniff"
            assert served.headers["content-disposition"] == "inline"
            assert served.headers["content-security-policy"] == "default-src 'none'; sandbox"
            assert served.headers["cache-control"] == "private, max-age=31536000, immutable"
            # Only the current versioned URL may be kept; any other is checked every time.
            for stale in (url.split("?")[0], *urls):
                again = await other.get(stale)
                assert again.content == content
                assert again.headers["cache-control"] == "private, no-cache"
            # Replacing leaves exactly the new image in the store.
            stored = stored_avatars(tmp_path)
            assert len(stored) == 1 and stored[0].endswith(extension)
            urls.append(url)
        assert len(set(urls)) == 3

        removed = await client.delete(
            f"{AUTH}/me/avatar", headers=mutation_headers(session["csrf_token"])
        )
        assert removed.status_code == 200, removed.text
        assert removed.json()["data"]["avatar_url"] is None
        assert stored_avatars(tmp_path) == []
        assert (await client.get(urls[-1])).status_code == 404
        again = await client.delete(
            f"{AUTH}/me/avatar", headers=mutation_headers(session["csrf_token"])
        )
        assert again.status_code == 200

    async with harness.factory() as db:
        actions = (
            await db.scalars(
                select(AuditEvent.action)
                .where(AuditEvent.action.like("user.avatar_%"))
                .order_by(AuditEvent.created_at, AuditEvent.id)
            )
        ).all()
    assert sorted(actions) == ["user.avatar_removed"] + ["user.avatar_updated"] * 3


@pytest.mark.asyncio
async def test_only_real_images_within_the_limit_are_stored(harness: Harness, tmp_path) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        for content in (b"just some text", SVG, GIF, b"", b"RIFF\x24\x00\x00\x00WAVEfmt "):
            refused = await upload(client, session, content)
            assert refused.status_code == 415, refused.text
            assert refused.json()["error"]["code"] == "UNSUPPORTED_IMAGE_TYPE"
        assert stored_avatars(tmp_path) == []

        # Above the general 1 MiB body limit, below the avatar limit.
        fits = await upload(client, session, PNG + b"\x00" * (3 * 1024 * 1024))
        assert fits.status_code == 200, fits.text
        too_big = await upload(client, session, PNG + b"\x00" * (5 * 1024 * 1024))
        assert too_big.status_code == 413
        assert too_big.json()["error"]["code"] == "REQUEST_BODY_TOO_LARGE"
        assert len(stored_avatars(tmp_path)) == 1


@pytest.mark.asyncio
async def test_who_can_set_and_see_an_avatar(harness: Harness, tmp_path) -> None:
    async with (
        harness.client() as admin_client,
        harness.client() as user_client,
        harness.client() as anonymous,
    ):
        admin = await login(harness, admin_client, uid="pa", email="pa@example.com")
        await bootstrap_admin(
            "pa@example.com", settings=harness.settings, session_factory=harness.factory
        )
        user = await login(harness, user_client, uid="user", email="user@example.com")
        admin_id, user_id = admin["user"]["id"], user["user"]["id"]

        # A user cannot touch someone else's picture, not even their own through the admin path.
        for target in (admin_id, user_id):
            denied = await upload(user_client, user, PNG, url=f"{USERS}/{target}/avatar")
            assert denied.status_code == 403
            assert denied.json()["error"]["code"] == "ROLE_REQUIRED"
            denied = await user_client.delete(
                f"{USERS}/{target}/avatar", headers=mutation_headers(user["csrf_token"])
            )
            assert denied.status_code == 403
        no_csrf = await user_client.post(
            f"{AUTH}/me/avatar", files={"file": ("a.png", PNG)}, headers={"Origin": ORIGIN}
        )
        assert no_csrf.status_code == 403
        assert stored_avatars(tmp_path) == []

        set_by_admin = await upload(admin_client, admin, JPEG, url=f"{USERS}/{user_id}/avatar")
        assert set_by_admin.status_code == 200, set_by_admin.text
        url = set_by_admin.json()["data"]["avatar_url"]
        assert url.startswith(f"{USERS}/{user_id}/avatar?v=")
        listed = (await admin_client.get(USERS, params={"email": "user@example.com"})).json()
        assert listed["data"][0]["avatar_url"] == url
        assert (await user_client.get(f"{AUTH}/me")).json()["data"]["user"]["avatar_url"] == url
        assert (await user_client.get(url)).content == JPEG
        assert (await anonymous.get(url)).status_code == 401

        missing = "00000000-0000-0000-0000-000000000000"
        unknown = await upload(admin_client, admin, PNG, url=f"{USERS}/{missing}/avatar")
        assert unknown.status_code == 404
        assert (await admin_client.get(f"{USERS}/{missing}/avatar")).status_code == 404
        assert (await admin_client.get(f"{USERS}/{admin_id}/avatar")).status_code == 404

        cleared = await admin_client.delete(
            f"{USERS}/{user_id}/avatar", headers=mutation_headers(admin["csrf_token"])
        )
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["data"]["avatar_url"] is None
        assert stored_avatars(tmp_path) == []

    async with harness.factory() as db:
        events = (
            await db.execute(
                select(AuditEvent.action, AuditEvent.actor_user_id, AuditEvent.resource_id).where(
                    AuditEvent.action.like("user.avatar_%")
                )
            )
        ).all()
    assert {(action, str(actor), resource) for action, actor, resource in events} == {
        ("user.avatar_updated", admin_id, user_id),
        ("user.avatar_removed", admin_id, user_id),
    }


@pytest.mark.asyncio
async def test_a_failed_save_leaves_no_image_behind(
    harness: Harness, tmp_path, monkeypatch
) -> None:
    async with harness.client() as client:
        session = await login(harness, client, uid="owner", email="owner@example.com")
        kept = await upload(client, session, PNG)
        assert kept.status_code == 200
        before = stored_avatars(tmp_path)

        def fail(*args, **kwargs):
            raise RuntimeError("audit store is down")

        monkeypatch.setattr(avatars, "record_audit", fail)
        failed = await upload(client, session, JPEG)
        assert failed.status_code == 500
        monkeypatch.undo()

        assert stored_avatars(tmp_path) == before
        me = (await client.get(f"{AUTH}/me")).json()["data"]["user"]
        assert me["avatar_url"] == kept.json()["data"]["avatar_url"]
        assert (await client.get(me["avatar_url"])).content == PNG


@pytest.mark.asyncio
async def test_avatar_url_follows_the_name_everywhere(harness: Harness) -> None:
    async with (
        harness.client() as manager_client,
        harness.client() as researcher_client,
        harness.client() as reviewer_client,
    ):
        manager, researcher, _, project, version, _ = await team(
            harness, manager_client, researcher_client, reviewer_client
        )
        url = (await upload(manager_client, manager, PNG)).json()["data"]["avatar_url"]

        members = (await researcher_client.get(f"{PROJECTS}/{project['id']}/members")).json()
        by_email = {member["email"]: member["avatar_url"] for member in members["data"]}
        assert by_email == {
            "manager@example.com": url,
            "researcher@example.com": None,
            "reviewer@example.com": None,
        }

        run = (await start_run(researcher_client, researcher, project["id"], version["id"])).json()[
            "data"
        ]
        comments = f"{PROJECTS}/{project['id']}/runs/{run['id']}/comments"
        posted = await manager_client.post(
            comments, json={"body": "A note"}, headers=mutation_headers(manager["csrf_token"])
        )
        assert posted.status_code == 201, posted.text
        assert posted.json()["data"]["author_avatar_url"] == url
        listed = (await researcher_client.get(comments)).json()["data"]
        assert [comment["author_avatar_url"] for comment in listed] == [url]
        deleted = await manager_client.delete(
            f"{comments}/{posted.json()['data']['id']}",
            headers=mutation_headers(manager["csrf_token"]),
        )
        assert deleted.json()["data"]["author_avatar_url"] == url

        notices = (await researcher_client.get(NOTIFICATIONS)).json()["data"]
        assert {notice["kind"] for notice in notices} == {"run_commented"}
        assert {notice["actor_avatar_url"] for notice in notices} == {url}
        read = await researcher_client.post(
            f"{NOTIFICATIONS}/{notices[0]['id']}/read",
            headers=mutation_headers(researcher["csrf_token"]),
        )
        assert read.json()["data"]["actor_avatar_url"] == url

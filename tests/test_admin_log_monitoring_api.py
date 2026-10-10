from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.models.audit import AuditEvent
from platform_be.models.monitoring import MonitoringAlert, MonitoringEvent, MonitoringWorkerState
from tests.conftest import Harness, login, mutation_headers

BASE = "/api/v1/admin/log-monitoring"
ADMIN_EMAIL = "log-monitoring-admin@example.com"


async def _login_admin(harness: Harness, client):
    session = await login(harness, client, uid="log-monitoring-admin", email=ADMIN_EMAIL)
    await bootstrap_admin(ADMIN_EMAIL, settings=harness.settings, session_factory=harness.factory)
    return session


def _request_event(
    *, created_at: datetime, status_code: int, duration_ms: float
) -> MonitoringEvent:
    return MonitoringEvent(
        id=uuid4(),
        event_type="http.request.completed",
        service="platform-api",
        environment="test",
        worker_id="api-worker-test",
        level="error" if status_code >= 500 else "warning" if status_code >= 400 else "info",
        message="HTTP request completed",
        request_id=f"request-{uuid4()}",
        method="GET",
        route="/api/v1/projects/{project_id}",
        status_code=status_code,
        duration_ms=duration_ms,
        attributes={"request_body": "must not escape", "password": "secret"},
        created_at=created_at,
    )


async def _seed_coverage(harness: Harness, *, start: datetime, end: datetime) -> None:
    async with harness.factory() as db:
        db.add(
            MonitoringWorkerState(
                worker_id="api-worker-test",
                service="platform-api",
                environment="test",
                status="ready",
                started_at=start - timedelta(hours=1),
                heartbeat_at=datetime.now(UTC),
                stopped_at=None,
                persisted_watermark=end + timedelta(seconds=1),
                dropped_count=0,
            )
        )
        await db.commit()


@pytest.mark.asyncio
async def test_monitoring_services_are_admin_only_and_unknown_without_measured_rule(
    harness: Harness,
) -> None:
    async with harness.client() as admin_client, harness.client() as member_client:
        await _login_admin(harness, admin_client)
        await login(harness, member_client, uid="member", email="monitoring-member@example.com")

        denied = await member_client.get(f"{BASE}/services")
        assert denied.status_code == 403

        response = await admin_client.get(f"{BASE}/services")
        assert response.status_code == 200, response.text
        assert response.json()["data"][0]["id"] == "platform-api:test"
        assert response.json()["data"][0]["status"] == "unknown"
        assert response.json()["data"][0]["metrics"]["available"] is False


def test_monitoring_openapi_documents_filters_errors_and_aggregate_semantics(
    harness: Harness,
) -> None:
    paths = harness.app.openapi()["paths"]

    services = paths["/api/v1/admin/log-monitoring/services"]["get"]
    assert services["summary"] == "List measured monitoring services"
    assert {"401", "403", "422", "429", "503"} <= services["responses"].keys()
    service_limit = next(
        parameter for parameter in services["parameters"] if parameter["name"] == "limit"
    )
    assert service_limit["schema"]["minimum"] == 1
    assert service_limit["schema"]["maximum"] == 100

    events = paths["/api/v1/admin/log-monitoring/events"]["get"]
    assert "stable event ID descending" in events["description"]
    assert "404" in events["responses"]
    event_params = {parameter["name"]: parameter for parameter in events["parameters"]}
    assert event_params["from"]["description"].startswith("Inclusive")
    assert event_params["to"]["description"].startswith("Exclusive")

    metrics = paths["/api/v1/admin/log-monitoring/services/{service_id}/metrics"]["get"]
    assert "nearest-rank" in metrics["description"]
    assert "100,000" in metrics["description"]

    acknowledge = paths["/api/v1/admin/log-monitoring/alerts/{alert_id}/acknowledge"]["post"]
    assert "409" in acknowledge["responses"]
    assert "CSRF token" in acknowledge["description"]

    overview = paths["/api/v1/admin/overview"]["get"]
    assert {"401", "403", "422"} <= overview["responses"].keys()

    stream = paths["/api/v1/admin/log-monitoring/stream"]["get"]
    assert {"200", "401", "403", "429", "503"} <= stream["responses"].keys()
    assert "15 seconds" in stream["description"]


@pytest.mark.asyncio
async def test_events_filter_stably_and_detail_does_not_return_raw_attributes(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        await _login_admin(harness, client)
        now = datetime.now(UTC)
        events = [
            _request_event(created_at=now - timedelta(seconds=20), status_code=200, duration_ms=12),
            _request_event(created_at=now - timedelta(seconds=10), status_code=500, duration_ms=15),
            _request_event(created_at=now - timedelta(seconds=5), status_code=404, duration_ms=18),
        ]
        async with harness.factory() as db:
            db.add_all(events)
            await db.commit()

        response = await client.get(
            f"{BASE}/events",
            params={"service_id": "platform-api:test", "level": "ERROR", "limit": 1},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["meta"]["pagination"]["total"] == 1
        assert len(body["data"]) == 1
        assert body["data"][0]["id"] == str(events[1].id)
        assert "request_body" not in body["data"][0]

        warning_response = await client.get(
            f"{BASE}/events",
            params={"service_id": "platform-api:test", "level": "WARN"},
        )
        assert warning_response.status_code == 200, warning_response.text
        assert warning_response.json()["data"][0]["id"] == str(events[2].id)
        assert warning_response.json()["data"][0]["level"] == "WARN"

        detail = await client.get(f"{BASE}/events/{events[1].id}")
        assert detail.status_code == 200, detail.text
        assert detail.json()["data"]["payload"] == {}
        assert "secret" not in detail.text


@pytest.mark.asyncio
async def test_service_metrics_use_persisted_events_and_enforce_capture_coverage(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        await _login_admin(harness, client)
        end = datetime.now(UTC) - timedelta(seconds=45)
        start = end - timedelta(minutes=5)
        await _seed_coverage(harness, start=start, end=end)
        async with harness.factory() as db:
            db.add_all(
                [
                    _request_event(
                        created_at=start + timedelta(seconds=index * 10),
                        status_code=500 if index < 2 else 200,
                        duration_ms=float(index + 1),
                    )
                    for index in range(20)
                ]
            )
            await db.commit()

        response = await client.get(
            f"{BASE}/services/platform-api:test/metrics",
            params={"from": start.isoformat(), "to": end.isoformat(), "interval_seconds": 300},
        )
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["available"] is True
        assert len(data["points"]) == 1
        point = data["points"][0]
        assert point["request_rate_per_second"] == pytest.approx(20 / 300)
        assert point["error_rate_percent"] == 10
        assert point["latency_ms"] == 19

        too_many = await client.get(
            f"{BASE}/services/platform-api:test/metrics",
            params={
                "from": (end - timedelta(days=1)).isoformat(),
                "to": end.isoformat(),
                "interval_seconds": 60,
            },
        )
        assert too_many.status_code == 422
        assert too_many.json()["error"]["code"] == "POINT_LIMIT_EXCEEDED"

        async with harness.factory() as db:
            state = await db.get(MonitoringWorkerState, "api-worker-test")
            assert state is not None
            state.persisted_watermark = start
            await db.commit()
        unavailable = await client.get(
            f"{BASE}/services/platform-api:test/metrics",
            params={"from": start.isoformat(), "to": end.isoformat(), "interval_seconds": 300},
        )
        assert unavailable.status_code == 200, unavailable.text
        unavailable_data = unavailable.json()["data"]
        assert unavailable_data["available"] is False
        assert unavailable_data["reason_code"] == "CAPTURE_INCOMPLETE"
        assert unavailable_data["points"][0]["request_rate_per_second"] is None


@pytest.mark.asyncio
async def test_alert_acknowledgement_is_csrf_protected_idempotent_and_audited(
    harness: Harness,
) -> None:
    async with harness.client() as client:
        session = await _login_admin(harness, client)
        alert_id = uuid4()
        async with harness.factory() as db:
            db.add(
                MonitoringAlert(
                    id=alert_id,
                    service_id="platform-api:test",
                    service="platform-api",
                    environment="test",
                    code="API_ERROR_RATE_HIGH",
                    severity="warning",
                    message="API error rate exceeded the configured threshold",
                    status="active",
                    active_key="platform-api:test:API_ERROR_RATE_HIGH",
                    started_at=datetime.now(UTC),
                    details={"sample_count": 25},
                )
            )
            await db.commit()

        url = f"{BASE}/alerts/{alert_id}/acknowledge"
        missing_csrf = await client.post(url, headers={"Origin": "http://localhost:3000"})
        assert missing_csrf.status_code == 403
        headers = mutation_headers(session["csrf_token"])
        first = await client.post(url, headers=headers)
        assert first.status_code == 200, first.text
        first_data = first.json()["data"]
        second = await client.post(url, headers=headers)
        assert second.status_code == 200, second.text
        assert second.json()["data"]["acknowledged_at"] == first_data["acknowledged_at"]
        assert (
            second.json()["data"]["acknowledged_by_user_id"]
            == first_data["acknowledged_by_user_id"]
        )
        async with harness.factory() as db:
            audit_count = await db.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(
                    AuditEvent.action == "monitoring.alert.acknowledged",
                    AuditEvent.resource_id == str(alert_id),
                )
            )
            persisted = await db.get(MonitoringAlert, alert_id)
            assert audit_count == 1
            assert persisted is not None and persisted.status == "active"

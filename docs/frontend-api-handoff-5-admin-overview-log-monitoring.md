# Admin Overview and Log Monitoring API Handoff

Backend contract for the Admin Overview tabs and `/admin/log-monitoring`.
All paths below are relative to `/api/v1`.

> **Delivery status:** the backend APIs are available. The FE route and UI wiring are
> deferred from this backend-only delivery; use this document as the implementation
> contract for the frontend work.

## Access and response envelope

- Every endpoint requires an authenticated Platform Admin session cookie.
- The frontend API client must send credentials. The acknowledge `POST` also uses the existing CSRF token and allowed-origin checks.
- Successful responses use `{ success: true, message, data, meta }`. Paginated responses include `meta.pagination = { total, limit, offset }`.
- Errors use `{ success: false, message, error: { code, details, reason? }, meta: { request_id } }`.
- `401` means the session is missing/expired; `403` means the user is not a Platform Admin. Do not map either to empty data.
- The Audit Log remains `/admin/logs` backed by `GET /audit`; technical events below are a separate store.

## Local frontend connection

The frontend API helper accepts either a backend origin or an origin already ending in
`/api/v1`. Its code default is `http://localhost:8000`, while the backend local stack
documented in its README listens on port `8080`. Use this frontend environment setting
when running the two repositories locally, then restart the Next.js dev server:

```dotenv
NEXT_PUBLIC_API_URL=http://localhost:8080
```

From the backend repository, start the backend and apply its migrations before testing the
frontend integration. The `/admin/log-monitoring` FE route must be implemented by the FE
team; it is not included in this backend delivery:

```bash
task up
task migrate
```

Sign in through the backend with a real Platform Admin session; a mocked UI session does
not grant access to these endpoints. Keep one hostname for the frontend and API session:
use `localhost` consistently, or use `127.0.0.1` consistently. The backend's
`CORS_ALLOWED_ORIGINS` must contain the exact frontend origin. Its local example already
contains `http://localhost:3000` and `http://127.0.0.1:3000`.

## Admin Overview

`GET /admin/overview`

Query parameters:

| Name | Values | Notes |
| --- | --- | --- |
| `section` | `overview`, `operations`, `projects`, `governance` | Defaults to `overview`; request one section per tab. |
| `from`, `to` | ISO 8601 timestamps with timezone | Provide both or neither. The interval is half-open `[from, to)`. Maximum 366 days. |
| `timezone` | IANA timezone | Defaults to `UTC`; returned with the effective window. |

Defaults are 28 days for `overview` and `projects`, and 24 hours for `operations` and `governance`, ending at `generated_at`.

`data` contains `section`, `generated_at`, `window`, `comparison_window`, `metrics`, `money_metrics`, `series`, `breakdowns`, `recent_runs`, `recent_events`, `governance_events`, `decision_gates`, and `unavailable_fields`.

Each metric carries `value`, `unit`, `scope` (`snapshot` or `window`), previous value/change metadata, `available`, and optional `reason_code`. A missing source is `value: null, available: false`; a measured zero remains zero. The UI should render unavailable values explicitly and should not synthesize charts from current snapshots.

### Response keys by section

The following keys are the current contract. Render a metric's `value` only when
`available` is true; unavailable keys may also be listed in `unavailable_fields` as
`{ key, reason_code }`.

| Section | `metrics` keys | `breakdowns` keys | `series` keys | Other section data |
| --- | --- | --- | --- | --- |
| `overview` | `active_projects`, `total_projects`, `archived_projects`, `projects_in_review`, `experiment_runs`, `successful_runs_rate`, `active_runs`, `review_queue`, `queued_research_runs`, `token_usage`, `runs_by_status_total` | `runs_by_status`, `runs_by_project`, `active_runs_by_service`, `activity_by_service`, `token_usage_by_model` | `execution_reliability`, `active_runs` | `recent_runs` (up to 5); `recent_events` unavailable |
| `projects` | Shared project/run keys above except `runs_by_status_total`, plus `validated_findings`, `active_project_capacity`, `review_completion`, `monthly_budget_used` | Shared run breakdowns above, plus `projects_by_status` | `project_execution_health`, `active_runs` | `money_metrics.reported_run_cost`, `money_metrics.estimated_spend`; `recent_runs` (up to 5) |
| `operations` | `request_rate`, `error_rate`, `p95_latency`, `queue_depth`, `service_uptime` | `request_rate_by_service`, `requests_by_service`, `requests_by_region`, `pending_jobs_by_queue` | `service_reliability`, `request_rate`, `queue_depth` | `recent_events` (up to 5) |
| `governance` | `decisions_evaluated`, `evidence_gate_pass_rate`, `audit_coverage`, `human_review_queue`, `decision_overrides`, `audit_trail_completeness`, `provenance_linked_findings`, `review_sla_within_target` | `decisions_by_outcome`, `decisions_by_type`, `decisions_by_routing` | `decision_gate_quality` | `governance_events` and `decision_gates` are unavailable |

Each breakdown has `key`, `unit`, `items` (`id`, `label`, `value`), `total`, and
`available`. Each series has `key`, `unit`, `interval_seconds`, `points` (`timestamp`,
`value`, `target`), and `available`. Each money metric has `value_usd`,
`previous_value_usd`, `change_percent`, `available`, and optional `attribution`.

The Operations request-rate, error-rate, and latency metrics are populated from persisted request telemetry once capture coverage is complete. CPU, memory, historical uptime, token usage, and other unmeasured sources remain unavailable.

Requests to `/admin/log-monitoring` itself, including its REST endpoints and SSE stream,
are excluded from the persisted request aggregates. Monitoring traffic therefore cannot
inflate the Operations metrics or produce recursive monitoring events.

## Log Monitoring

### Services

`GET /admin/log-monitoring/services?environment=<env>&limit=20&offset=0`

Returns configured/observed services and pagination metadata. The initial implementation exposes one `platform-api` service in the current `APP_ENV`; a different `environment` returns an empty page. A service has `id`, `code`, `name`, `category`, `environment`, `status`, `observed_at`, `stale_after_seconds`, `metrics_window_seconds`, `latency_statistic`, `metrics`, `alerts`, and `recent_events`. `alerts` contains up to five active alerts and `recent_events` contains up to five recent events.

`status` is `healthy | warning | critical | unknown`. It remains `unknown` when capture coverage is incomplete, the health rule is disabled, or the minimum sample count is not met. `metrics` contains request rate/sec, error-rate percent, p95 latency in ms, CPU/memory/queue fields, `available`, and `reason_code`. Unsupported infrastructure fields are `null`.

### Events

`GET /admin/log-monitoring/events`

Supported query parameters: `service_id`, `environment`, `level` (`DEBUG | INFO | WARN | ERROR`), `q`, `request_id`, `trace_id`, `project_id`, `run_id`, `from`, `to`, `limit` (default 20, max 100), and `offset` (default 0).

The default time window is the last 90 minutes. `from` and `to` must be supplied together, include timezones, and span no more than 14 days. Results are sorted newest first with stable ID tie-breaking; `meta.pagination.total` is counted after filters and before pagination.

Event list rows include `id`, `service_id`, `service`, `environment`, `level`, sanitized `message`, `created_at`, `duration_ms`, `status`, correlation IDs, `actor_user_id`, `provider_id`, `method`, and route template. Use the detail endpoint when the payload is needed:

`GET /admin/log-monitoring/events/{event_id}`

Detail adds `payload`. It is currently an empty allowlisted object. Do not expect request body, query-string values, headers, prompts, credentials, or raw exception traces.

### Service metrics

`GET /admin/log-monitoring/services/{service_id}/metrics`

Accepts `from`, `to`, and `interval_seconds` (`60`, `300`, or `900`; default `60`). The default window is 90 minutes and ends 45 seconds before request time to account for capture watermark. It returns at most 500 points; oversized ranges fail with `422 POINT_LIMIT_EXCEEDED` rather than silently truncating.

`data` contains `service_id`, `generated_at`, `window`, `interval_seconds`, `latency_statistic: "p95"`, `points`, `available`, and `reason_code`. Each point has timestamp, request rate/sec, error-rate percent, p95 latency ms, CPU percent, memory percent, and queue depth. Unavailable event-backed values are `null`; do not replace them with zero.

Request rate is request event count divided by the window duration; errors are HTTP statuses
`>= 500`. P95 uses the nearest-rank estimator and is unavailable below 20 latency samples.
Aggregates read at most 100,000 request observations; over that cap, event-backed values are
unavailable with `SAMPLE_LIMIT_EXCEEDED`. Events from `/admin/log-monitoring` REST endpoints
and SSE streams are excluded from telemetry so the monitor does not count its own polling.

### Alert acknowledgement

`POST /admin/log-monitoring/alerts/{alert_id}/acknowledge`

Send an empty JSON body through the shared API client so it attaches the session cookie and CSRF token. The action is idempotent for an active alert and records the first acknowledging user/time. It does not resolve the alert. A resolved alert returns `409 ALERT_RESOLVED`; a missing alert returns `404 NOT_FOUND`.

Alert evaluation is off by default. To enable it, backend deployment must set all three values together: `MONITORING_ERROR_RATE_ALERT_THRESHOLD_PERCENT`, `MONITORING_ERROR_RATE_ALERT_RECOVERY_PERCENT`, and `MONITORING_ERROR_RATE_ALERT_MIN_SAMPLES`. Recovery must be below the alert threshold. `MONITORING_ERROR_RATE_ALERT_LOOKBACK_SECONDS` defaults to 300 seconds.

## Live updates

`GET /admin/log-monitoring/stream?service_id=<id>&environment=<env>&limit=20&offset=0`

Credentialed Server-Sent Events. The browser `EventSource` must use `withCredentials: true`; use the same hostname as the signed-in frontend and include that origin in backend CORS configuration. Initial events are `service-snapshot` and `pipeline-status`. Changes can send `log-event`, `alert-updated`, `service-snapshot`, `pipeline-status`, and `refresh-required`. `alert-updated` is an object wrapper with an `alerts` list, for example `{ "alerts": [] }`; its full payload also includes `service_id` and `refresh_required`. `refresh-required` means refetch the REST endpoints. `session-ended` and `access-ended` are terminal; close the stream and stop retrying. The server sends keep-alives every 15 seconds and SSE retry advice of 3 seconds. Each heartbeat revalidates the session and Platform Admin role without extending the session idle deadline. Opening the stream is normal session activity and does extend that deadline. Per-process stream limits return `429` with `Retry-After`; temporary stream setup failures return `503` with `Retry-After`.

The stream is an invalidation channel, not a durable event log. A reconnect starts with a
fresh `service-snapshot` and establishes the newest-event baseline, so fetch the latest
events from REST after reconnect and after `refresh-required`; fetch services at the
same time when their state or embedded alerts must be current. Do not rely on browser
SSE replay for missed updates.

## Error handling

Handle the standard envelope and preserve the server `error.code`. Overview time filters can return `TIME_RANGE_PAIR_REQUIRED` or `INVALID_TIMEZONE`; monitoring time filters can return `INVALID_TIME_RANGE`, `TIMEZONE_REQUIRED`, or `TIME_RANGE_TOO_LARGE`. Other errors include `INVALID_INTERVAL`, `POINT_LIMIT_EXCEEDED`, `SERVICE_ENVIRONMENT_MISMATCH`, `NOT_FOUND`, `ALERT_RESOLVED`, `MONITORING_STREAM_LIMIT`, and `MONITORING_STREAM_UNAVAILABLE`. Availability reasons include `CAPTURE_NOT_STARTED`, `CAPTURE_INCOMPLETE`, and `SAMPLE_LIMIT_EXCEEDED`. Honor `Retry-After` on `429` and `503` responses. OpenAPI at `/docs` is the source for the live schema.

# AI Research Platform Backend

The Platform API of the AI Research Experimentation Platform. It handles sign-in, projects, who may do what, the inputs of a research run, the review step, the result files, comments, notifications, and the audit log.

The research itself is done by **Popper**, a separate service. The Platform sends Popper the inputs of a run, shows its progress, records who reviewed what, and keeps a copy of the results. It stores metadata and files only: it never reads or judges scientific content.

## Contents

- [Quick start](#quick-start)
- [How the Platform works](#how-the-platform-works)
- [Roles and permissions](#roles-and-permissions)
- [Research features](#research-features)
- [Authentication](#authentication)
- [API reference](#api-reference)
- [Configuration](#configuration)
- [Operations](#operations)

## Quick start

You need Python 3.12+, [`uv`](https://docs.astral.sh/uv/), Docker with the Compose plugin, and [Task](https://taskfile.dev/docs/installation).

```bash
cp .env.example .env.local
task up                        # PostgreSQL + API on 127.0.0.1
task migrate                   # in a second terminal, on a fresh database
```

| What | Where |
|---|---|
| API | `http://localhost:8000` |
| Swagger UI | `http://localhost:8000/docs` |
| OpenAPI schema | `http://localhost:8000/openapi.json` |
| Liveness / readiness | `/api/v1/health/live`, `/api/v1/health/ready` (checks PostgreSQL) |

Other shortcuts: `task down` (stops the stack, keeps the database volume), `task restart`, `task status`, `task logs`; `task --list` describes them all.

After the first account has registered (`POST /api/v1/auth/register`), make it the first Platform Admin:

```bash
docker compose --env-file .env.local -f docker/docker-compose.dev.yml run --rm api platform-be bootstrap-admin --email admin@example.com
```

The command refuses to run when a Platform Admin already exists, and does not accept an unregistered or suspended account.

Tests and static checks, on the host:

```bash
uv sync --all-groups
uv run ruff format --check src alembic tests
uv run ruff check src alembic tests
uv run pytest
```

Tests run on SQLite in memory. The tests that need real PostgreSQL behaviour (locks, concurrent requests) are skipped unless `PLATFORM_POSTGRES_TEST_URL` points at a scratch database.

## How the Platform works

The project is the top-level scope: there is no organization or workspace above it.

```text
Register with email + password (the account is active at once, no approval step)
   ↓
Create a project                     the creator is its Project Manager
   ↓
Add members by email                 registered users only; role: project_manager, researcher or reviewer
   ↓
Upload a dataset (CSV)               project: draft → data_ready
   ↓
Write the research context           every save is a new version
   ↓
Start a run                          project: researching    run: queued → running
   ↓
Popper asks for a frame review       project: needs_review   run: awaiting_review
   ↓
A Project Manager or Researcher decides: approve / edit / reject
   ↓                                 project: researching    run: running   (may repeat)
Popper delivers the result files and finishes
   ↓                                 project: data_ready     run: completed | budget_exceeded | failed
Members read the results, download files, comment
   ↓
Start another run, or the Project Manager marks the project completed
```

### Project status

Derived from what the project contains; it is never set by hand, except `completed`.

| Status | Meaning |
|---|---|
| `draft` | no dataset yet |
| `data_ready` | has a dataset, no run in progress |
| `researching` | a run is in progress |
| `needs_review` | the run waits for a frame review |
| `completed` | marked by a Project Manager while no run is in progress; can be reopened |
| `archived` | shown instead of the above while the project is archived; the real status comes back on restore |

### Run status

| Status | Meaning |
|---|---|
| `queued` | saved, Popper has not confirmed it yet |
| `running` | Popper is working |
| `awaiting_review` | Popper waits for a frame review |
| `completed`, `budget_exceeded`, `failed` | finished; never changes again |

### Rules that always hold

- A project works on **one run at a time** (`RUN_ACTIVE`).
- **Nothing is overwritten.** A new dataset file is a new version, saving the research context adds a version, and result files cannot be replaced. A run always points at the exact versions it used.
- **There is no hard delete.** A project is archived (read-only, restorable); removing a member revokes the membership; a deleted comment keeps its place with the text erased.
- A project always keeps one active Project Manager: the last one cannot be demoted or removed (`LAST_PROJECT_MANAGER`).
- Suspending a user is refused while they are the only Project Manager of a project other people work in.
- An archived project is read-only (`PROJECT_ARCHIVED`).
- Every change is written to the audit log.

## Roles and permissions

There are four fixed roles. A role alone decides access: there is no permission table, role editor, or custom role. A user holds at most one role in each project, and may hold different roles in different projects.

| Role | Scope | What it is for |
|---|---|---|
| `platform_admin` | Platform | System tasks: suspend users, grant Platform Admin, see and manage every project, read the global audit log and the cost report. |
| `project_manager` | Project | Given to whoever creates the project. Edits project details, manages members, archives, completes, reads the project audit log, and everything a Researcher does. |
| `researcher` | Project | Uploads datasets, writes the research context, starts runs, decides frame reviews, comments. |
| `reviewer` | Project | Reads everything in the project and comments. Changes nothing else. |

Someone who is not a member gets `404` for everything in a project, so its existence is not revealed. A member whose role is too low gets `403 ROLE_REQUIRED`.

**Projects**

| Action | Platform Admin | Project Manager | Researcher / Reviewer |
|---|:-:|:-:|:-:|
| Create a project | any signed-in user; the creator becomes Project Manager |||
| List projects | all | own | own |
| Read details and members | ✓ | ✓ | ✓ |
| Update details | ✓ | ✓ | 403 |
| Archive / restore | ✓ | ✓ | 403 |
| Add / change role / remove a member | ✓ | ✓ | 403 |
| Complete / reopen | ✓ | ✓ | 403 |
| Read the project audit log | ✓ | ✓ | 404 |

**Inside a project**

| Action | Platform Admin | Project Manager | Researcher | Reviewer |
|---|:-:|:-:|:-:|:-:|
| Read datasets, research context, runs, reviews, result files, comments | ✓ | ✓ | ✓ | ✓ |
| Download dataset and result files | ✓ | ✓ | ✓ | ✓ |
| Upload a dataset or a new version; rename a dataset | ✓ | ✓ | ✓ | 403 |
| Save the research context | ✓ | ✓ | ✓ | 403 |
| Start or sync a run; decide a frame review | ✓ | ✓ | ✓ | 403 |
| Abandon a run | ✓ | ✓ | 403 | 403 |
| Comment; edit own comment | ✓ | ✓ | ✓ | ✓ |
| Delete a comment | any | any | own | own |

## Research features

### Datasets

CSV only. A file must be UTF-8, have a header row of unique, non-empty column names, at least one data row, and the same number of values on every row. The size limit is 50 MiB by default (`DATASET_MAX_UPLOAD_BYTES`); a header may have at most 2000 columns with names of at most 200 characters.

The Platform records the row count, column names, size, and SHA-256. A file that fails a check is refused and nothing is stored.

### Research context

A Markdown body plus optional structured fields: `domain`, `objectives`, `variables`, `design`, `assumptions`, `constraints`, `concepts`, `notes`. Together they become the `research.md` file Popper reads, with the structured fields as its YAML front matter.

Send `base_version` when saving. If someone else saved first, the answer is `409 RESEARCH_CONTEXT_CONFLICT` instead of silently replacing their work.

### Runs

`POST /projects/{id}/runs` fixes one dataset version and one research context version, checks that every variable the context names is a column of the dataset, and sends both to Popper with a spending cap (`RUN_DEFAULT_BUDGET_USD`, at most `RUN_MAX_BUDGET_USD`).

When a run does not move:

| Situation | What to do |
|---|---|
| Popper did not answer in time when the run was started | The response is `202` and the run stays `queued`. Call `sync`. |
| A run looks stuck | `POST …/runs/{run}/sync` asks Popper for the current state. The Platform does not poll on its own. |
| `sync` answers `409 RUN_DISPATCHING` | The run was created moments ago and may still be on its way to Popper. Try again after twice `POPPER_TIMEOUT_SECONDS`. |
| Popper does not know the run | `sync` marks it `failed`, so the project is free again. |
| Popper keeps answering with errors | A Project Manager calls `POST …/runs/{run}/abandon`. The run becomes `failed` and the project can start another. |

Abandoning only changes the Platform's record. Popper is not told, so check on its side that the run is not still spending; a later report from Popper about an abandoned run is refused.

### Frame review

`GET …/runs/{run}/frame-review` returns Popper's request with its `items`. A Project Manager or Researcher answers with either:

- `{"approve_all": true}`, or
- a list of `signals`, one per item: `approve`, `edit` (with a `value`), `reject`, or `unknown`. Items left out are approved.

The decision is stored with who made it and when, then sent to Popper. If Popper does not confirm (`502`), the review stays pending with the decision kept: send it again, or it is confirmed when Popper reports the run moving on.

### Results

Popper delivers files (paper PDF/TeX, figures, results). They are listed per run and always served as downloads, never rendered by the API.

### Comments and notifications

Any member comments on a run, optionally about one result file. Only the author edits a comment.

Notifications are created for four events:

| `kind` | Sent to |
|---|---|
| `added_to_project` | the user who was added |
| `run_awaiting_review` | Project Managers and Researchers of the project |
| `run_finished` | all members |
| `run_commented` | the person who started the run |

A notification carries no text; the frontend words it from `kind`. There is no email.

### Realtime notifications for frontend clients

Open authenticated SSE `GET /api/v1/notifications/stream?limit=50` after sign-in. `limit`
defaults to `50` and accepts `1`–`100`; the server sends `notifications` immediately and
after each committed notification, read/read-all action, or membership removal:

```text
event: notifications
data: {"items":[{"id":"...","kind":"run_finished","project_id":"...","project_name":"...","run_id":null,"actor_user_id":null,"actor_display_name":null,"created_at":"...","read_at":null}],"unread_count":3}
```

`items` are newest first with the `NotificationItem` fields from `GET /notifications`. Each
event is a snapshot: replace the window and badge, do not append or poll. `unread_count` is
the total visible unread count. Reconnection gets another current snapshot; no cursor is used.

```ts
const stream = new EventSource("/api/v1/notifications/stream?limit=50", { withCredentials: true });
stream.addEventListener("notifications", (event) => {
  const { items, unread_count } = JSON.parse((event as MessageEvent<string>).data);
  replaceNotificationWindow(items); setUnreadBadge(unread_count);
});
stream.addEventListener("session-ended", (event) => {
  const { code } = JSON.parse((event as MessageEvent<string>).data);
  stream.close(); beginSignInFlow(code); // SESSION_EXPIRED, USER_SUSPENDED, UNAUTHENTICATED
});
const stopNotifications = () => stream.close(); // call on logout/unmount; network errors reconnect
```

Ignore the 15-second keep-alive comment: it rechecks the session but does not extend idle
expiry. Initial connection and notification snapshots use normal session activity. The proxy
must disable buffering and use read/idle timeouts over 15 seconds. A separate frontend origin
needs credential CORS for its exact origin and cross-site-capable cookies. `GET /notifications`
remains for paginated history; the CSRF-protected read `POST`s trigger a new snapshot.

Frontend integration instructions: [docs/notifications-handoff.md](./docs/notifications-handoff.md).

### Running without Popper

Leave `POPPER_BASE_URL` empty. Everything works except the three actions that talk to Popper (starting a run, syncing a run, deciding a frame review), which answer `503 POPPER_NOT_CONFIGURED`. The test suite uses an in-memory stand-in (`tests/fakes.py`).

The API Popper must offer, and the two endpoints it calls back, are specified in `docs/popper-integration-contract.md` (kept locally; `docs/` is not tracked in Git). Popper v2 does not implement that contract yet.

## Authentication

The Platform owns accounts, passwords, sessions, and roles; there is no external identity provider. The login name is the email address; there is no separate username.

### Sign-up and sign-in

| Action | Request | Result |
|---|---|---|
| Sign up | `POST /api/v1/auth/register` with `{ "email", "password", "display_name"? }` | `201`; the account is `active` and already signed in. `409 EMAIL_ALREADY_REGISTERED` when the email is taken. |
| Sign in | `POST /api/v1/auth/login` with `{ "email", "password" }` | `200`. `401 INVALID_CREDENTIALS` for a wrong email or password (the answer does not say which), `403 USER_SUSPENDED` for a suspended account. |
| Change password | `POST /api/v1/auth/change-password` with `{ "current_password", "new_password" }` | `200`; every other session of the user is signed out. `403 CURRENT_PASSWORD_INCORRECT`. |

Sign-up and sign-in both set the HttpOnly session cookie and return the user, session expiry, and CSRF token. Together they are limited to `AUTH_SESSION_RATE_LIMIT` requests per window for each client address (`429 RATE_LIMITED`).

Passwords are 8 to 128 characters. Only an scrypt hash with its own salt is stored (`PASSWORD_SCRYPT_LOG2_N` sets the cost); emails are compared without regard to letter case.

A session stays valid for 30 days from login (`SESSION_ABSOLUTE_DAYS`) as long as it is used at least once every 7 days (`SESSION_IDLE_MINUTES`, default 10080). There is no refresh token: each request extends the idle window on the server.

What the Platform does not do, because it sends no email:

- **No email verification.** Anyone can register with any address, so do not treat the email as proof of identity.
- **No "forgot password".** A user who forgets the password cannot recover the account through the API.
- Accounts created before passwords were kept on the Platform have no password and cannot sign in.

### Rules for the frontend

- Send every request with `credentials: "include"` and an `Origin` that exactly matches `CORS_ALLOWED_ORIGINS`.
- Send the CSRF token in `X-CSRF-Token` on every mutation (`POST`, `PATCH`, `PUT`, `DELETE`) except sign-up and sign-in. After a page reload, get it again from `GET /api/v1/auth/csrf-token`.
- Restore the signed-in user after a reload with `GET /api/v1/auth/me`. It returns the user, platform role, session expiry, and `memberships`: the user's projects with the role in each.
- On sign-out call `POST /api/v1/auth/logout`.
- On `401` with `SESSION_EXPIRED` or `USER_SUSPENDED` the session cookie is already cleared; send the user back to sign-in.

## API reference

All paths start with `/api/v1`. Full schemas and error cases are in Swagger UI at `/docs`.

**Session**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/auth/register` | Create an account with email and password; sets the session cookie |
| `POST` | `/auth/login` | Sign in with email and password; sets the session cookie |
| `POST` | `/auth/change-password` | Change the password; signs out the user's other sessions |
| `POST` | `/auth/logout` | End the current session |
| `POST` | `/auth/logout-all` | End every session of the signed-in user, on all devices |
| `GET` | `/auth/me` | Signed-in user, platform role, session expiry, and project memberships |
| `GET` | `/auth/csrf-token` | CSRF token for the current session |

**Administration**

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/users` | List users (Platform Admin) |
| `PATCH` | `/users/{id}/status` | Activate or suspend a user (Platform Admin) |
| `PUT` | `/users/{id}/platform-role` | Grant or remove Platform Admin |
| `GET` | `/audit` | Audit events: global for a Platform Admin, one project (`project_id`) for its Project Manager |
| `GET` | `/admin/usage/projects` | Runs and cost per project (Platform Admin; `from`, `to`) |

**Projects and members**

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects` | List my projects; create a project |
| `GET` `PATCH` | `/projects/{id}` | Read or update project details |
| `POST` | `/projects/{id}/archive`, `/restore` | Archive or restore a project |
| `POST` | `/projects/{id}/complete`, `/reopen` | Mark the project completed; reopen it |
| `GET` `POST` | `/projects/{id}/members` | List members; add a registered user by email |
| `PUT` `DELETE` | `/projects/{id}/members/{membership_id}` | Change a member's role; remove a member |

**Research inputs**

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects/{id}/datasets` | List datasets; upload a CSV as a new dataset (multipart: `name`, `description`, `file`) |
| `GET` `PATCH` | `/projects/{id}/datasets/{dataset_id}` | Read; rename or describe |
| `GET` `POST` | `/projects/{id}/datasets/{dataset_id}/versions` | List versions; upload a new version |
| `GET` | `/projects/{id}/datasets/{dataset_id}/versions/{version_id}/download` | Download a version's file |
| `GET` `PUT` | `/projects/{id}/research-context` | Newest research context; save a new version |
| `GET` | `/projects/{id}/research-context/versions`, `/versions/{n}` | Version history; one version |

**Runs, review, and results**

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects/{id}/runs` | List runs; start a run |
| `GET` | `/projects/{id}/runs/{run_id}` | One run |
| `POST` | `/projects/{id}/runs/{run_id}/sync` | Ask Popper for the run's current state |
| `POST` | `/projects/{id}/runs/{run_id}/abandon` | Mark a run that cannot finish as failed (Project Manager) |
| `GET` `POST` | `/projects/{id}/runs/{run_id}/frame-review` | Newest review request; decide the pending one |
| `GET` | `/projects/{id}/runs/{run_id}/frame-reviews` | All review requests of the run |
| `GET` | `/projects/{id}/runs/{run_id}/artifacts` | Result files |
| `GET` | `/projects/{id}/runs/{run_id}/artifacts/{artifact_id}/download` | Download a result file |

**Comments and notifications**

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects/{id}/runs/{run_id}/comments` | List comments (`artifact_id` filter); add one |
| `PATCH` `DELETE` | `/projects/{id}/runs/{run_id}/comments/{comment_id}` | Edit own comment; delete |
| `GET` | `/notifications`, `/notifications/unread-count` | My notifications (`unread_only`); unread count |
| `GET` | `/notifications/stream` | Real-time SSE snapshots of my latest notifications and unread count |
| `POST` | `/notifications/{id}/read`, `/notifications/read-all` | Mark as read |

**Called by Popper, not by the frontend**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/internal/popper/runs/{run_id}/status` | Report a run's status, cost, and review request |
| `POST` | `/internal/popper/runs/{run_id}/artifacts` | Deliver a result file |

Both require the `X-Service-Key` header (`POPPER_CALLBACK_KEY`); a request without a valid key is refused with `401 SERVICE_KEY_INVALID` before its body is read.

### Response format

REST/JSON endpoints return the same envelope. A successful `GET /notifications/stream` uses the
SSE frames documented above; startup errors still use this JSON envelope. `meta.request_id`
matches the `X-Request-ID` response header.

```json
{
  "success": true,
  "message": "OK",
  "data": { "id": "..." },
  "meta": { "request_id": "...", "pagination": null }
}
```

List endpoints return the array in `data` and fill `meta.pagination` with `{ "total", "limit", "offset" }`. Endpoints with nothing to return (logout, member removal) answer `200` with `"data": null`.

```json
{
  "success": false,
  "message": "Verify your email before accessing the platform",
  "error": { "code": "EMAIL_NOT_VERIFIED", "details": [] },
  "meta": { "request_id": "..." }
}
```

Branch on `error.code`, not on `message`. `error.details` lists `{ "field", "message" }` entries for `VALIDATION_ERROR` (422) and is empty otherwise.

## Configuration

Settings come from environment variables; `.env.example` lists them all with safe local defaults. The ones specific to research:

| Variable | Purpose |
|---|---|
| `STORAGE_LOCAL_ROOT` | Directory that holds uploaded and produced files (`var/storage` on the host). |
| `DATASET_MAX_UPLOAD_BYTES` | Largest dataset file (default 50 MiB). |
| `ARTIFACT_MAX_UPLOAD_BYTES` | Largest result file Popper may deliver (default 50 MiB). |
| `POPPER_BASE_URL` | Popper's address. Empty: runs cannot start. |
| `POPPER_SERVICE_KEY` | Sent to Popper in `X-Service-Key`. Required with the base URL. |
| `POPPER_CALLBACK_KEY` | Expected from Popper in `X-Service-Key`. Required with the base URL. |
| `POPPER_TIMEOUT_SECONDS` | Longest a whole call to Popper may take, dataset upload included (default 30, at most 300). |
| `PUBLIC_BASE_URL` | Address Popper uses to call this API back, without `/api/v1`. |
| `RUN_DEFAULT_BUDGET_USD` | Spending cap of a run when the user gives none (5). |
| `RUN_MAX_BUDGET_USD` | Highest cap a user may ask for (20). |

The two Popper keys are different secrets of at least 32 characters in production. The callback endpoints are reachable by anyone who can reach the API; the key is what protects them, so keep the API behind TLS.

## Operations

### File storage

Dataset files, review requests and decisions, and result files are kept in a file store, outside the database. The only store today is a directory (a named volume under Compose). Until a cloud store is added:

- Run a single API replica, or give every replica the same volume.
- Back up the storage volume together with the database: rows point at files by key.

### Migrations

Alembic owns the database schema. App startup never creates or migrates tables. Apply migrations once during local setup or deployment with `task migrate`. Do not point a developer command at a production database.

### Production stack

Production uses a separate Compose project, database volume, storage volume, and configuration file. Copy the template inside `docker/`, set strong unique database, session, and Popper secrets, then validate and start the stack:

```bash
cp docker/prod.env.example docker/prod.env.local
docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml config --quiet
docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml up -d --build
docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml run --rm api alembic upgrade head
```

The production API binds to `127.0.0.1` and expects a TLS-terminating reverse proxy in front of it. PostgreSQL has no published host port.

The supplied CLI and Docker commands give Uvicorn a 30-second graceful-shutdown timeout;
Compose reserves 40 seconds for the API to stop. When starting Uvicorn manually, also pass
`--timeout-graceful-shutdown 30`. This bounds a restart even with open SSE connections; their
EventSource clients reconnect to receive a fresh notification snapshot.

### Limits and deployment security

- Request bodies are limited to 1 MiB (`REQUEST_MAX_BODY_BYTES`); dataset uploads and result files have their own limits, above.
- `POST /api/v1/auth/login` allows 10 attempts per client IP per 60 seconds per API process (`AUTH_SESSION_RATE_LIMIT`, `AUTH_SESSION_RATE_WINDOW_SECONDS`).
- Production Compose does not configure Uvicorn's trusted proxy addresses, so behind a proxy all requests may share one IP and one login quota. Before public deployment, make Uvicorn trust only the real proxy addresses and add a shared rate limit at the ingress. Do not use caller-supplied forwarding headers as client identity.
- The hosting platform is not selected yet.

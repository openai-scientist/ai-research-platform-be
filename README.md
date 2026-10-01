# AI Research Platform Backend

Platform API for identity, users, organizations, projects, workspace access, and audit. The Platform BE is independent from Popper: the frontend uses Platform for accounts and workspaces, then connects to Popper for AI and research workflows.

## Current scope

- Firebase Authentication identity exchange and opaque Platform sessions.
- Fixed roles at platform, organization, and project scope.
- User administration, organization and project membership, archive/restore, and audit events.
- Dataset catalog/versioning, research lifecycle, paper writing, and Popper integration remain follow-up work.

The research flow remains a separate system contract: question → problem understanding → dataset → analysis → hypothesis → analysis → diagram → paper. This repository does not create placeholder routes or records for that workflow.

## Roles

Each user has at most one active role in each organization and project. Roles are fixed in the API schema; there is no permission editor or custom role system. Access is decided by role alone — there is no permission table. The role codes are defined once in `src/platform_be/core/roles.py` and published as enums in OpenAPI.

The PRD names four roles (Admin, Project Manager, Researcher, Reviewer). `platform_admin` is the PRD's Admin and the three project roles keep the PRD names; the two organization roles are added here because the ERD has organizations as tenants but the PRD names no role to manage them.

| Role | Scope | Platform BE capability |
|---|---|---|
| `platform_admin` | Platform | Manage users (suspend, grant Platform Admin) and administer any organization, project, and audit scope. |
| `organization_admin` | Organization | Held by whoever creates the organization. Manage the organization, its members, and all child project metadata/members. |
| `organization_member` | Organization | View the organization and create projects; creator becomes the first Project Manager. Existing projects require explicit project membership. |
| `project_manager` | Project | Read/update project metadata, manage project members, and archive/restore the project. |
| `researcher` | Project | Read project metadata. Research operations are not implemented here. |
| `reviewer` | Project | Read project metadata. Research review artifacts are not implemented here. |

Organization and project archive make the corresponding scope read-only. Archiving an organization blocks writes to all its projects without changing their individual archived status. Restore does not unarchive child projects.

### Workspace flow

1. A user signs up and is active immediately; no admin approval is needed.
2. Any user creates an organization (`POST /api/v1/organizations`) and becomes its Organization Admin.
3. The Organization Admin adds members by email; the person must already have an account.
4. Any member creates projects and becomes the Project Manager of each one they create.
5. The Project Manager adds organization members to the project as Researcher or Reviewer.

A Platform Admin is only needed for system tasks: suspending users, granting Platform Admin, global audit.

### Who can do what

There is no hard delete: "delete" for an organization or project is **archive** (read-only, restorable). Removing a member revokes the membership.

Organization:

| Action | Platform Admin | Organization Admin | Organization Member | Not a member |
|---|:-:|:-:|:-:|:-:|
| Create (`POST /organizations`) | ✓ | — | — | ✓ any user; creator becomes Organization Admin |
| Read details | ✓ | ✓ | ✓ | 404 |
| List | all | own | own | own |
| Update name/description | ✓ | ✓ | 403 | 404 |
| Archive / restore | ✓ | ✓ | 403 | 404 |
| List members | ✓ | ✓ | ✓ | 404 |
| Add / change role / remove members | ✓ | ✓ | 403 | 404 |
| View organization audit | ✓ | ✓ | 404 | 404 |

Project (the user must first belong to the organization):

| Action | Platform Admin / Organization Admin | Project Manager | Researcher | Reviewer | Organization Member outside the project |
|---|:-:|:-:|:-:|:-:|:-:|
| Create (`POST .../projects`) | ✓ may name another member as manager | — | — | — | ✓ creator becomes Project Manager |
| Read details | ✓ | ✓ | ✓ | ✓ | 404 |
| List | all in the organization | own | own | own | own |
| Update | ✓ | ✓ | 403 | 403 | 404 |
| Archive / restore | ✓ | ✓ | 403 | 403 | 404 |
| List members | ✓ | ✓ | ✓ | ✓ | 404 |
| Add / change role / remove members | ✓ | ✓ | 403 | 403 | 404 |

Project roles apply inside one project: a Researcher in one project is still free to create their own project in the same organization. A Platform Admin who is not an organization member must name the first Project Manager when creating a project there.

## Requirements

- Python 3.12+
- `uv`
- Docker with the Compose plugin for local PostgreSQL and the API
- [Task (go-task)](https://taskfile.dev/docs/installation) for the project shortcuts

## Local setup

Copy the sample configuration to the untracked local settings file, then start the services:

```bash
cp [.]env.example .env.local
task up
```

Compose starts PostgreSQL and the API on host loopback (`127.0.0.1`); the API waits for the database health check. On a fresh database, apply the schema in a second terminal:

```bash
task migrate
```

The API is at `http://localhost:8000`; interactive Swagger/OpenAPI docs are at `http://localhost:8000/docs` and the schema is at `http://localhost:8000/openapi.json`.

Use `task down` to stop the local stack without deleting its database volume. `task restart`, `task status`, `task logs`, and `task migrate` are also available; run `task --list` for descriptions. FE calls Platform APIs for user, organization, project, membership, and audit operations. Liveness is `/api/v1/health/live`; readiness is `/api/v1/health/ready` and checks PostgreSQL.

To run the test suite and static checks on the host:

```bash
uv sync --all-groups
uv run ruff format --check src alembic tests
uv run ruff check src alembic tests
uv run pytest
```

When an account has been registered and verified through Firebase, grant the first platform role once:

```bash
docker compose --env-file .env.local -f docker/docker-compose.dev.yml run --rm api platform-be bootstrap-admin --email admin@example.com
```

This CLI command refuses to run if a Platform Admin already exists. It does not accept an unregistered or suspended account.

## Authentication setup

The current Firebase project is `ai-research-platform-4ceb9`; the shown project settings have no registered Firebase apps yet. In this project, configure Authentication providers: enable Google and set its public-facing project name and support email, enable **Email/Password**, and add the frontend's local and deployed hostnames under Authentication → Settings → Authorized domains. The Platform requires the `email_verified` claim, so the frontend must complete Firebase's email-verification flow and refresh the Firebase ID token before exchanging it.

For local Platform API authentication, open **Project settings → Service accounts → Firebase Admin SDK**, click **Generate new private key**, and download the JSON file. Copy its `project_id`, `client_email`, and `private_key` fields into the ignored local settings file. The project ID is already set in the local template; add the other two values in this form, keeping the literal `\n` sequences inside the single quotes:

```dotenv
FIREBASE_CLIENT_EMAIL=firebase-adminsdk-xxxxx@ai-research-platform-4ceb9.iam.gserviceaccount.com
FIREBASE_PRIVATE_KEY='-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n'
```

The API expands those `\n` sequences before initializing Firebase Admin SDK. Keep the downloaded JSON and private key out of Git, frontend code, screenshots, and chat. If the key is exposed, disable/delete it and generate a replacement. On Google Cloud deployments, the API can use Application Default Credentials instead, so a long-lived service-account key does not need to be packaged with the deployment.

Separately, register a **Web App** from the Firebase project overview using the `</>` icon (or **Add app → Web**). Put that app's Firebase SDK configuration in the frontend's own local settings. The Web SDK `apiKey`, `authDomain`, `projectId`, and `appId` are for the frontend; they are different from the Admin SDK service-account key. Never expose the OAuth Web client secret or the service-account private key in frontend configuration.

### Sign-up and sign-in flow

Firebase owns credentials; Platform owns the session and roles. The login name is the email address — there is no separate username. Both providers end at the same Platform endpoint:

| Step | Google | Email + password |
|---|---|---|
| 1. Sign up / sign in (frontend, Firebase SDK) | `signInWithPopup(GoogleAuthProvider)` — first use creates the Firebase account | Sign up: `createUserWithEmailAndPassword`, then `sendEmailVerification`. Sign in: `signInWithEmailAndPassword` |
| 2. Email verified | Already verified by Google | User opens the verification link, then the frontend calls `getIdToken(true)` to refresh the token |
| 3. Platform session | `POST /api/v1/auth/login` with `{ "firebase_id_token": "<getIdToken()>" }` | Same |

`POST /api/v1/auth/login` verifies the token, registers the Platform user on first login (`is_new_user: true`, status `active` — usable right away), sets the HttpOnly session cookie, and returns the user, session expiry, and CSRF token. It rejects unverified emails (`EMAIL_NOT_VERIFIED`) and sign-ins older than five minutes (`RECENT_AUTH_REQUIRED` — sign in or reauthenticate again).

A session stays valid for 30 days from login (`SESSION_ABSOLUTE_DAYS`) as long as it is used at least once every 7 days (`SESSION_IDLE_MINUTES`, default 10080); after either limit the user signs in again. There is no refresh token: each request extends the idle window server-side.

Password reset (`sendPasswordResetEmail`), email verification, and provider linking stay in the Firebase SDK; Platform has no endpoints for them.

Frontend rules:

- Send every request with `credentials: "include"` and an `Origin` that exactly matches `CORS_ALLOWED_ORIGINS`.
- Send the CSRF token in `X-CSRF-Token` on every mutation (`POST`, `PATCH`, `PUT`, `DELETE`). After a page reload, get it again from `GET /api/v1/auth/csrf-token`.
- Restore the signed-in user after a reload with `GET /api/v1/auth/me` (user, platform role, session expiry).
- On sign-out call `POST /api/v1/auth/logout` and Firebase `signOut()`.
- On `401` with `SESSION_EXPIRED` or `USER_SUSPENDED` the session cookie is already cleared; send the user back to sign-in.

Swagger UI is available at `http://localhost:8000/docs`, with the OpenAPI schema at `http://localhost:8000/openapi.json`.

When a user wants both Google and email/password on one account, sign in to the existing Firebase account first, then link the second provider through Firebase SDK. Firebase does not automatically merge two accounts; if the credential already belongs to a different UID, resolve ownership and account data explicitly. Platform does not merge distinct Firebase UIDs.

Local settings are copied from the template to the ignored local settings file. This repository does not use the Firebase Auth Emulator; local authentication uses your Firebase project and requires the service-account values above (or valid Application Default Credentials).

## API outline

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/auth/login` | Sign in or sign up with a Firebase ID token; sets the session cookie |
| `POST` | `/api/v1/auth/logout` | End the current session (CSRF required) |
| `POST` | `/api/v1/auth/logout-all` | End every session of the signed-in user, on all devices (CSRF required) |
| `GET` | `/api/v1/auth/me` | Signed-in user, platform role, and session expiry |
| `GET` | `/api/v1/auth/csrf-token` | CSRF token for the current session |
| `GET` | `/api/v1/users` | List users (Platform Admin) |
| `PATCH` | `/api/v1/users/{id}/status` | Activate or suspend a user (Platform Admin) |
| `PUT` | `/api/v1/users/{id}/platform-role` | Grant or remove Platform Admin |
| `GET` | `/api/v1/audit` | Audit events (Platform Admin global; Organization Admin by organization) |

Organization CRUD, membership management, and archive/restore live under `/api/v1/organizations`; the same for projects under `/api/v1/organizations/{organization_id}/projects`. Detailed schemas, fixed role codes, and error cases are published by FastAPI OpenAPI at `/docs`.

### Response format

Every endpoint returns the same envelope. `meta.request_id` matches the `X-Request-ID` response header.

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

## Deployment security

The API limits request bodies to 1 MiB by default and allows 10 `POST /api/v1/auth/login` attempts per ASGI client IP per 60 seconds per API process. Tune these limits with `REQUEST_MAX_BODY_BYTES`, `AUTH_SESSION_RATE_LIMIT`, and `AUTH_SESSION_RATE_WINDOW_SECONDS`. Production Compose expects an external TLS proxy but does not configure Uvicorn's trusted proxy addresses. Until that proxy network is known and explicitly trusted, requests may share the proxy IP and therefore share the in-process login quota. Before public deployment, configure Uvicorn to trust only the actual proxy addresses and add a shared rate limit at the ingress/API gateway. Do not use caller-supplied forwarding headers as client identity. The hosting platform is not selected yet.

## Migrations

Alembic owns the database schema. App startup never creates or migrates tables automatically. Apply migrations once during local setup or deployment:

```bash
task migrate
```

Do not point a developer command at a production database.

## Docker production stack

Production uses a separate Compose project, database volume, and configuration file. Copy the template inside `docker/`, set strong unique database/session secrets and the production Firebase service-account values, then validate and start the stack:

```bash
cp docker/prod.env.example docker/prod.env.local
docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml config --quiet
docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml up -d --build
docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml run --rm api alembic upgrade head
```

The production API binds to `127.0.0.1` and expects a TLS-terminating reverse proxy in front of it. PostgreSQL has no published host port. For Google Cloud deployments, use Application Default Credentials through the runtime identity rather than storing a service-account private key in the production environment file.

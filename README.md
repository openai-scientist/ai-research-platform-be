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

The local stack starts with a Platform Admin: `admin@gmail.com`, with the password set in `DEFAULT_ADMIN_PASSWORD` (see `.env.example`). It is created once the migrations have been applied (`task migrate`, then `task restart`).

Without those two settings, register an account (`POST /api/v1/auth/register`), verify its email with the emailed code (`POST /api/v1/auth/verify-email`; without `RESEND_API_KEY` the code is in the API log) and make it the first Platform Admin:

```bash
docker compose --env-file .env.local -f docker/docker-compose.dev.yml run --rm api platform-be bootstrap-admin --email admin@example.com
```

The command refuses to run when a Platform Admin already exists, and does not accept an unregistered, unverified or suspended account.

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
Invite members by email              registered users only; role: project_manager, researcher or reviewer
   ↓
The invited user accepts             within 24 hours; no access to the project before that
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
- Suspending a user is refused while they are the only Project Manager of any project, also one they work in alone. Give the project another manager first.
- An archived project is read-only (`PROJECT_ARCHIVED`).
- **Adding a member is an invitation.** The user is a member only after accepting; until then the project answers `404` for them.
- Every change is written to the audit log.

## Roles and permissions

There are five fixed roles. A role alone decides access: there is no permission table, role editor, or custom role. A user has exactly one platform role, at most one role in each project, and may hold different roles in different projects.

| Role | Scope | What it is for |
|---|---|---|
| `user` | Platform | The role of every new account, self-registered or created by an admin. Signs in, creates projects, can be added to projects. It is not stored: an account without the Platform Admin role is a `user`. |
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
| Invite / change role / remove a member; cancel or resend an invitation | ✓ | ✓ | 403 |
| Complete / reopen | ✓ | ✓ | 403 |
| Read the project audit log | ✓ | ✓ | 404 |

**Inside a project**

| Action | Platform Admin | Project Manager | Researcher | Reviewer |
|---|:-:|:-:|:-:|:-:|
| Read datasets, research context, runs, reviews, result files, comments | ✓ | ✓ | ✓ | ✓ |
| Download dataset and result files | ✓ | ✓ | ✓ | ✓ |
| Upload a dataset or a new version; rename a dataset | ✓ | ✓ | ✓ | 403 |
| Upload or delete a project file | ✓ | ✓ | ✓ | 403 |
| Save the research context | ✓ | ✓ | ✓ | 403 |
| Start or sync a run; decide a frame review | ✓ | ✓ | ✓ | 403 |
| Abandon a run | ✓ | ✓ | 403 | 403 |
| Comment; edit own comment | ✓ | ✓ | ✓ | ✓ |
| Delete a comment | any | any | own | own |

## Research features

### Datasets

CSV only. A file must be UTF-8, have a header row of unique, non-empty column names, at least one data row, and the same number of values on every row. The size limit is 50 MiB by default (`DATASET_MAX_UPLOAD_BYTES`); a header may have at most 2000 columns with names of at most 200 characters.

The Platform records the row count, column names, size, and SHA-256. A file that fails a check is refused and nothing is stored.

### Project files

Documents attached to a project: PDF, CSV and Excel (`.xlsx`, `.xls`), up to 50 MiB each by default (`PROJECT_FILE_MAX_UPLOAD_BYTES`). They are reference material for the members and are never sent to Popper; data for a run is uploaded as a dataset.

The type comes from the file name, not from the content type the browser sends, and the first bytes must match it: a renamed file is refused with `INVALID_FILE`. Files are downloaded as attachments, never rendered by the API.

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

Notifications are created for these events:

| `kind` | Sent to |
|---|---|
| `run_awaiting_review` | Project Managers and Researchers of the project |
| `run_finished` | all members |
| `run_commented` | the person who started the run |
| `project_invited` | the invited user; shown while the invitation is open, removed once it is answered, cancelled or sent again |
| `invite_accepted`, `invite_declined` | the person who invited, while still a member of the project |
| `member_role_changed` | the member whose role changed |
| `removed_from_project` | the removed member; the only notice of that project they still see |

Membership notices go to the one person concerned and never to the person who acted. Archiving, restoring, completing, reopening and starting a run notify nobody. `added_to_project` is an older kind: it is no longer created, and existing ones are still listed.

A notification carries no text; the frontend words it from `kind`. Only invitations are also sent by email.

### Project invitations

`POST /projects/{id}/members` invites a registered user with a role. The member row is returned with `status: "invited"`, `invite_sent_at`, `invite_expires_at`, `invite_expired` and `invite_email_sent` (false when the email provider refused the message; the invitation still exists). The user gets a `project_invited` notification and an email.

- An invitation is valid for 24 hours (`PROJECT_INVITE_TTL_HOURS`).
- The invited user reads `GET /invitations` and answers with `POST /invitations/{membership_id}/accept` or `/decline`. Accepting makes them a member with the offered role; declining can be done at any time, also after expiry.
- `GET /projects/{id}/members` lists invited and active rows; filter with `status=invited` or `status=active`. An invited Project Manager does not count as a manager yet.
- The manager can change the role of an open invitation, cancel it (`DELETE` on the member row) and, **only after it has expired**, send it again with `POST /projects/{id}/members/{membership_id}/invite`, which starts a new 24 hours.
- A declined or cancelled invitation can be made again with a new `POST /projects/{id}/members`.
- The answer is reported to whoever sent the invitation last, as long as they are still a member of the project. A Platform Admin who invites without being a member gets no notice.

| Status | Code | When |
|---|---|---|
| 404 | `REGISTERED_USER_NOT_FOUND` | No account has this email |
| 409 | `MEMBERSHIP_EXISTS` | The user is already a member or already invited |
| 409 | `USER_SUSPENDED` | The user is suspended |
| 409 | `INVITE_STILL_VALID` | Resend asked before the invitation expired |
| 409 | `INVITE_NOT_PENDING` | Resend asked for someone who is already a member |
| 409 | `INVITE_EXPIRED` | Accept asked after 24 hours; the manager must send it again |
| 409 | `PROJECT_ARCHIVED` | Invite, resend or accept on an archived project |
| 404 | `NOT_FOUND` | Accept or decline of an invitation that is not the caller's or is already answered |

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
| Sign up | `POST /api/v1/auth/register` with `{ "email", "password", "display_name"? }` | `201` with `{ email, expires_in_seconds, resend_after_seconds }`. A 6-digit code is emailed; nobody is signed in. Without `display_name` the name is the part of the email before the `@`. `409 EMAIL_ALREADY_REGISTERED` when a verified account has the email. |
| Verify the email | `POST /api/v1/auth/verify-email` with `{ "email", "code", "password" }` | `200`; the email is verified and the user is signed in, as after `login`. `400 OTP_INVALID` otherwise. |
| Send the code again | `POST /api/v1/auth/resend-verification` with `{ "email" }` | Always `200` with the same body as `register`, whether or not the address has an account. |
| Sign in | `POST /api/v1/auth/login` with `{ "email", "password" }` | `200`. `401 INVALID_CREDENTIALS` for a wrong email or password (the answer does not say which), `403 USER_SUSPENDED` for a suspended account, `403 EMAIL_NOT_VERIFIED` for an account that has not entered its code yet (no email is sent; call `resend-verification`). |
| Forgot password | `POST /api/v1/auth/forgot-password` with `{ "email" }` | Always `200` with the same body as `register`. An active account is emailed a 6-digit code. |
| Verify the password reset OTP | `POST /api/v1/auth/verify-reset-password` with `{ "email", "code" }` | `200` with `{ "reset_token", "expires_in_seconds" }`; consumes the OTP without changing the password or sessions. `400 OTP_INVALID`. |
| Set a new password | `POST /api/v1/auth/reset-password` with `{ "email", "reset_token", "new_password" }` | `200`; the password is set, every session of the user ends and nobody is signed in. `400 RESET_TOKEN_INVALID` for a wrong, expired, used token or unavailable account; `400 PASSWORD_UNCHANGED` when the new password is the part of the email before the `@`. |
| Change password | `POST /api/v1/auth/change-password` with `{ "current_password", "new_password" }` | `200`; every other session of the user is signed out and the user is emailed a notice. `403 CURRENT_PASSWORD_INCORRECT`. `400 PASSWORD_UNCHANGED` when a user with a temporary password sends it again as the new one. |
| Edit own profile | `PATCH /api/v1/auth/me` with `{ "display_name" }` | `200`. The email cannot be changed. |
| Set own picture | `POST /api/v1/auth/me/avatar`, a multipart form with one `file` | `200` with the new `avatar_url`. `415 UNSUPPORTED_IMAGE_TYPE`, `413 REQUEST_BODY_TOO_LARGE`. `DELETE` on the same path removes it. |

`login` and `verify-email` set the HttpOnly session cookie and return the user, session expiry, and CSRF token. None of the six endpoints above the change-password row takes a CSRF token; all need an allowed `Origin`.

**One-time codes.** A code is 6 digits, emailed on its own labelled line, never in the subject. Only a keyed digest is stored.

| Rule | Value | Setting |
|---|---|---|
| A code works | once, for 10 minutes | `OTP_TTL_MINUTES` |
| Checks against one code | 5, right or wrong; then it is dead | `OTP_MAX_ATTEMPTS` |
| Wait between two codes | 60 seconds | `OTP_RESEND_COOLDOWN_SECONDS` |
| Codes per hour, per account and purpose | 5 | `OTP_MAX_SENDS_PER_HOUR` |
| Wrong checks before a lock | 10, counted across reissued codes | `OTP_LOCK_AFTER_FAILURES` |
| Length of the lock | 60 minutes: no check succeeds, no code is sent | `OTP_LOCK_MINUTES` |

Sign-up codes and reset codes are counted separately and cannot stand in for each other. A new code replaces the earlier one.

- `400 OTP_INVALID` is the single answer for a wrong, expired, used, spent or locked code, an unknown or suspended account, and (on `verify-email`) a wrong password. The API never says which.
- `verify-email` checks the password before the code, so only someone who knows the password can spend checks, and a mistyped password does not cost a code.
- Registering again for an address that is not verified yet replaces its password and name together with a new code. Inside the 60 second wait nothing changes and nothing is sent, but the answer is still `201`: the earlier password stays.
- `verify-reset-password` returns a random, one-use token bound to the account, valid for `OTP_TTL_MINUTES` (10 minutes by default). Only its hash is stored. Issuing a new reset OTP invalidates the earlier token. `reset-password` requires this token; it no longer accepts an OTP directly.
- Completing `reset-password` proves the inbox, so it also verifies an unverified email and ends a temporary password. An account from before the Platform kept passwords sets its first password this way.
- After `change-password` and `reset-password` the user is emailed a notice that the password changed.
- When email is down or the provider's quota is spent, no new account can be verified. There is no bypass.

**Rate limits**, per client address and API process, each over `AUTH_SESSION_RATE_WINDOW_SECONDS` (`429 RATE_LIMITED` with `Retry-After`): `login` and `register` share `AUTH_SESSION_RATE_LIMIT` (10); `verify-email`, `resend-verification`, `forgot-password`, `verify-reset-password` and `reset-password` share `AUTH_CODE_RATE_LIMIT` (20).

Passwords are 8 to 128 characters. Only an scrypt hash with its own salt is stored (`PASSWORD_SCRYPT_LOG2_N` sets the cost); emails are compared without regard to letter case.

A session stays valid for 30 days from login (`SESSION_ABSOLUTE_DAYS`) as long as it is used at least once every 7 days (`SESSION_IDLE_MINUTES`, default 10080). There is no refresh token: each request extends the idle window on the server.

### Accounts created by a Platform Admin

`POST /api/v1/users` needs only `{ "email" }`; `display_name` and `send_email` (default `true`) are optional. The account always gets the `user` role, and the admin types no password:

- The temporary password is the part of the email before the `@`, in lower case (`Dat.Ngo@gmail.com` gives `dat.ngo`). The response returns it once as `temporary_password` so the admin can pass it on. The same text is the default display name.
- The email is not verified (`email_verified: false` in the user list). The first `login` answers `403 EMAIL_NOT_VERIFIED`; the user asks for a code with `resend-verification` and enters it with the temporary password in `verify-email`, exactly like a self-registered user. Until then the account cannot be invited to a project (`404 REGISTERED_USER_NOT_FOUND`) or made Platform Admin (`409 EMAIL_NOT_VERIFIED`).
- If someone registered the address but never verified it, creating the account takes it over: it gets the temporary password and the pending code stops working. A verified address answers `409 EMAIL_ALREADY_REGISTERED`.
- The account has `must_change_password: true`. After signing in, every endpoint answers `403 PASSWORD_CHANGE_REQUIRED` except `GET /auth/me`, `GET /auth/csrf-token`, `POST /auth/change-password`, `POST /auth/logout` and `POST /auth/logout-all`. The frontend shows the change-password screen on that code or on the flag in `me`.
- The temporary password can be guessed by anyone who knows the email; the code sent to the inbox is what protects the first sign-in. The 8 character minimum applies to the password the user then chooses, not to the temporary one.

Platform Admin cannot be granted at creation, and `PUT /api/v1/users/{id}/platform-role` answers `409 PASSWORD_CHANGE_PENDING` while the user still has the password: an admin account must never sit behind a guessable password.

**Emailing the sign-in details.** With `send_email: true` the user is mailed their email address and the temporary password, with a link to `APP_URL`; with `false` nothing is sent and the admin passes the details on. The password is the same either way. The response carries `invite_email_sent`: `false` means either no email was asked for or sending failed, and the account exists in both cases. `invite_sent_at` in the user list is when the Platform last tried to mail them.

`POST /api/v1/users/{id}/invite` sends the same details again, unchanged: no password is reset and no session ends. It answers like the creation (`temporary_password`, `invite_email_sent`) and is the "Resend" button of the admin screen, enabled while `must_change_password` is `true`.

| Code | Meaning |
|---|---|
| `409 INVITE_NOT_PENDING` | The user already chose a password. The Platform cannot read it back and this endpoint does not reset it. |
| `409 USER_SUSPENDED` | The account is suspended. |
| `429 INVITE_COOLDOWN` | The details were sent less than a minute ago (`INVITE_RESEND_COOLDOWN_SECONDS`); `Retry-After` says how long to wait. |

The user list also shows `last_login_at` and `created_by_user_id` (the admin, or `null` for a self-registered account). `PATCH /api/v1/users/{id}` with `{ "display_name" }` lets an admin rename a user.

What the Platform does not do: a code at every sign-in (two-factor), changing the email of an account, and alerts for a new device. Accounts that existed before email verification was added count as verified.

### Profile pictures

A picture is a PNG, JPEG or WebP image of at most 4 MiB (`AVATAR_MAX_UPLOAD_BYTES`). The type is read from the first bytes of the file; its name and declared content type are ignored, so SVG, GIF and renamed files are refused.

- A user sets or removes their own picture at `/api/v1/auth/me/avatar`; a Platform Admin does it for anyone at `/api/v1/users/{id}/avatar`.
- `avatar_url` is returned wherever a user's name is: `me` and sign-in, the user list, project members (`avatar_url`), comments (`author_avatar_url`) and notifications (`actor_avatar_url`). It is `null` without a picture.
- The value is a path on the API origin, `/api/v1/users/{id}/avatar?v=...`, readable by any signed-in user. It needs the session cookie: when the frontend is on another site than the API, load it with credentials (`fetch` with `credentials: "include"`, then a blob URL) instead of a plain `<img src>`.
- The `v` part changes with every upload. With the current `v` the response may be cached for a year; a URL without it, or with an old one, still returns the current picture but is checked again on every use. Always use the URL the API returned last.
- Images are stored exactly as uploaded, in the same store as project files under `users/`. Cropping, resizing and removing photo metadata (a phone photo carries its location) are the frontend's job before upload.

### Rules for the frontend

- Send every request with `credentials: "include"` and an `Origin` that exactly matches `CORS_ALLOWED_ORIGINS`.
- Send the CSRF token in `X-CSRF-Token` on every mutation (`POST`, `PATCH`, `PUT`, `DELETE`) except the seven endpoints used before a session exists: `register`, `verify-email`, `resend-verification`, `login`, `forgot-password`, `verify-reset-password` and `reset-password`. After a page reload, get it again from `GET /api/v1/auth/csrf-token`.
- Restore the signed-in user after a reload with `GET /api/v1/auth/me`. It returns the user, platform role (`user` or `platform_admin`, never empty), `email_verified`, `must_change_password`, session expiry, and `memberships`: the user's projects with the role in each.
- On sign-out call `POST /api/v1/auth/logout`.
- On `401` with `SESSION_EXPIRED` or `USER_SUSPENDED` the session cookie is already cleared; send the user back to sign-in.

## API reference

All paths start with `/api/v1`. Full schemas and error cases are in Swagger UI at `/docs`.

**Session**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/auth/register` | Create an account with email and password; emails a 6-digit code |
| `POST` | `/auth/verify-email` | Enter the code and the password; verifies the email and sets the session cookie |
| `POST` | `/auth/resend-verification` | Email a new verification code |
| `POST` | `/auth/forgot-password` | Email a code to set a new password |
| `POST` | `/auth/verify-reset-password` | Verify the reset OTP and return a one-use reset token |
| `POST` | `/auth/reset-password` | Set a new password with the verified reset token; ends every session |
| `POST` | `/auth/login` | Sign in with email and password; sets the session cookie |
| `POST` | `/auth/change-password` | Change the password; signs out the user's other sessions |
| `POST` | `/auth/logout` | End the current session |
| `POST` | `/auth/logout-all` | End every session of the signed-in user, on all devices |
| `GET` | `/auth/me` | Signed-in user, platform role, session expiry, and project memberships |
| `PATCH` | `/auth/me` | Change your own display name |
| `POST` `DELETE` | `/auth/me/avatar` | Upload, replace or remove your own picture |
| `GET` | `/auth/csrf-token` | CSRF token for the current session |

**Administration**

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/users` | List users (Platform Admin) |
| `POST` | `/users` | Create a user from an email; returns the temporary password (Platform Admin) |
| `PATCH` | `/users/{id}` | Change a user's display name (Platform Admin) |
| `POST` | `/users/{id}/invite` | Email the user's sign-in details again (Platform Admin) |
| `POST` `DELETE` | `/users/{id}/avatar` | Upload, replace or remove a user's picture (Platform Admin) |
| `GET` | `/users/{id}/avatar` | The user's picture (any signed-in user) |
| `PATCH` | `/users/{id}/status` | Activate or suspend a user (Platform Admin) |
| `PUT` | `/users/{id}/platform-role` | `{ "role": "platform_admin" }` grants Platform Admin, `{ "role": "user" }` removes it |
| `GET` | `/audit` | Audit events: global for a Platform Admin, one project (`project_id`) for its Project Manager; filter by `action`, `resource_type`, `actor_user_id`, `from`/`to` |

**Search.** The `q` parameter matches a substring, case-insensitively, and needs at least 3 characters. It is backed by `pg_trgm` GIN indexes on `users.email`, `users.display_name`, `projects.name`, `projects.description` and `datasets.name` (migration `20261005_0011`). Full-text search (`tsvector`) is the next step only when searching document content is needed.
| `GET` | `/admin/usage/projects` | Runs and cost per project (Platform Admin; `q`, `from`, `to`) |

**Projects and members**

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects` | List my projects; create a project |
| `GET` `PATCH` | `/projects/{id}` | Read or update project details |
| `POST` | `/projects/{id}/archive`, `/restore` | Archive or restore a project |
| `POST` | `/projects/{id}/complete`, `/reopen` | Mark the project completed; reopen it |
| `GET` `POST` | `/projects/{id}/members` | List members and open invitations (`status`); invite a registered user by email |
| `PUT` `DELETE` | `/projects/{id}/members/{membership_id}` | Change a role; remove a member or cancel an invitation |
| `POST` | `/projects/{id}/members/{membership_id}/invite` | Send an expired invitation again |
| `GET` | `/invitations` | My open project invitations |
| `POST` | `/invitations/{membership_id}/accept`, `/invitations/{membership_id}/decline` | Answer an invitation |

**Research inputs**

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects/{id}/datasets` | List datasets; upload a CSV as a new dataset (multipart: `name`, `description`, `file`) |
| `GET` `PATCH` | `/projects/{id}/datasets/{dataset_id}` | Read; rename or describe |
| `GET` `POST` | `/projects/{id}/datasets/{dataset_id}/versions` | List versions; upload a new version |
| `GET` | `/projects/{id}/datasets/{dataset_id}/versions/{version_id}/download` | Download a version's file |
| `GET` `POST` | `/projects/{id}/files` | List files (`q`, `kind`); upload a PDF, CSV or Excel file (multipart: `file`) |
| `GET` | `/projects/{id}/files/{file_id}/download` | Download a file |
| `DELETE` | `/projects/{id}/files/{file_id}` | Delete a file |
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
  "message": "Verify this email with a code first",
  "error": { "code": "EMAIL_NOT_VERIFIED", "details": [] },
  "meta": { "request_id": "..." }
}
```

Branch on `error.code`, not on `message`. `error.details` lists `{ "field", "message" }` entries for `VALIDATION_ERROR` (422) and is empty otherwise.

## Configuration

Settings come from environment variables; `.env.example` lists them all with safe local defaults. The ones specific to research:

| Variable | Purpose |
|---|---|
| `STORAGE_BACKEND` | `local` (default) keeps files in a directory; `r2` keeps them in a Cloudflare R2 bucket. |
| `STORAGE_LOCAL_ROOT` | Directory used by the `local` store (`var/storage` on the host). |
| `R2_ACCOUNT_ID`, `R2_BUCKET` | The Cloudflare account and the bucket. Required with `r2`. |
| `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | An R2 API token with Object Read & Write on that bucket. Required with `r2`. |
| `R2_ENDPOINT_URL` | Only for a bucket with a jurisdiction; replaces the endpoint built from the account ID. |
| `PROJECT_FILE_MAX_UPLOAD_BYTES` | Largest project file (default 50 MiB). |
| `DATASET_MAX_UPLOAD_BYTES` | Largest dataset file (default 50 MiB). |
| `ARTIFACT_MAX_UPLOAD_BYTES` | Largest result file Popper may deliver (default 50 MiB). |
| `AVATAR_MAX_UPLOAD_BYTES` | Largest profile picture (default 4 MiB, at most 5 MiB). |
| `RESEND_API_KEY` | Resend key used to send email. Empty in `local`: each message is written to the API log instead. Required in `staging` and `production`. |
| `EMAIL_FROM` | Sender shown on emails; must be an address on the domain verified in Resend. |
| `AUTH_CODE_RATE_LIMIT` | Requests per window and client address to the four code endpoints (default 20). |
| `OTP_TTL_MINUTES`, `OTP_MAX_ATTEMPTS`, `OTP_RESEND_COOLDOWN_SECONDS`, `OTP_MAX_SENDS_PER_HOUR`, `OTP_LOCK_AFTER_FAILURES`, `OTP_LOCK_MINUTES` | Limits of the emailed one-time codes; defaults 10, 5, 60, 5, 10, 60. Not passed by the Compose files: add them there to change them. |
| `DEFAULT_ADMIN_EMAIL`, `DEFAULT_ADMIN_PASSWORD` | A Platform Admin created when the API starts if the account is missing (`admin@gmail.com` in the local stack). Local development only: staging and production refuse to start with them. An existing account keeps its password. |
| `PROJECT_INVITE_TTL_HOURS` | How long a project invitation can be accepted (default 24, at most 168). |
| `APP_URL` | Address of the frontend, put in emails as the sign-in link (`http://localhost:3000` locally). Must be `https://` in production. Empty: emails carry no link. |
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

Dataset files, project files, review requests and decisions, and result files are kept in a file store, outside the database. `STORAGE_BACKEND` chooses it.

**`local`** — a directory (a named volume under Compose):

- Run a single API replica, or give every replica the same volume.
- Back up the storage volume together with the database: rows point at files by key.

**`r2`** — a Cloudflare R2 bucket, reached through its S3-compatible API:

1. Create a bucket in the Cloudflare dashboard (R2 > Create bucket). Keep it private: no public access, no custom domain. Every download goes through this API, which checks project membership.
2. Create an API token (R2 > Manage API tokens) with **Object Read & Write**, limited to that bucket. Copy the Access Key ID and the Secret Access Key; the secret is shown once.
3. Set `STORAGE_BACKEND=r2`, `R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`, then restart the API. It refuses to start when one is missing.

Files pass through the API in both directions, so the browser never needs R2 credentials or a CORS policy on the bucket. A stored file is never overwritten: uploads use a conditional write that fails when the key exists.

Switching the store does not move files. Rows written under one store point at keys the other does not have, so copy the files across with the same keys (for example `rclone copy`) before switching a database that already has uploads.

### Migrations

Alembic owns the database schema. App startup never creates or migrates tables. Apply migrations once during local setup or deployment with `task migrate`. Do not point a developer command at a production database.

### Production stack

Production uses a separate Compose project, database volume, storage volume, and configuration file. Copy the template inside `docker/`, set strong unique database, session, and Popper secrets, then validate and start the stack:

```bash
cp docker/prod.env.example docker/prod.env.local
task prod:config               # checks the Compose file and the settings, starts nothing
task prod:up
task prod:migrate
```

Other shortcuts: `task prod:status`, `task prod:logs`, `task prod:down` (keeps the database and storage volumes). Each runs `docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml ...`.

The production API binds to `127.0.0.1` and expects a TLS-terminating reverse proxy in front of it. PostgreSQL has no published host port.

The supplied CLI and Docker commands give Uvicorn a 30-second graceful-shutdown timeout;
Compose reserves 40 seconds for the API to stop. When starting Uvicorn manually, also pass
`--timeout-graceful-shutdown 30`. This bounds a restart even with open SSE connections; their
EventSource clients reconnect to receive a fresh notification snapshot.

### Limits and deployment security

- Request bodies are limited to 1 MiB (`REQUEST_MAX_BODY_BYTES`); dataset uploads, project files, result files and profile pictures have their own limits, above.
- `login` and `register` together allow 10 requests per client IP per 60 seconds per API process (`AUTH_SESSION_RATE_LIMIT`, `AUTH_SESSION_RATE_WINDOW_SECONDS`); the four code endpoints together allow 20 (`AUTH_CODE_RATE_LIMIT`).
- Sign-up depends on email: `RESEND_API_KEY` is required in `staging` and `production`, and while the provider is down or over quota no account can be verified, admin-created ones included.
- The migration that adds email verification must run before the new code starts: the code reads a column the old schema does not have.
- Production Compose does not configure Uvicorn's trusted proxy addresses, so behind a proxy all requests may share one IP and one login quota. Before public deployment, make Uvicorn trust only the real proxy addresses and add a shared rate limit at the ingress. Do not use caller-supplied forwarding headers as client identity.
- The hosting platform is not selected yet.

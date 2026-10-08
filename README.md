# AI Research Platform Backend

The Platform API of the AI Research Experimentation Platform. It handles sign-in, projects, who may do what, the inputs of a research run, the review step, the result files, comments, notifications, and the audit log.

The research itself is done by **Popper**, a separate service. The Platform sends Popper the inputs of a run, shows its progress, records who reviewed what, and keeps a copy of the results. It stores metadata and files only: it never reads or judges scientific content.

## Contents

- [Features](#features)
- [Tech stack](#tech-stack)
- [Getting started](#getting-started)
- [Project structure](#project-structure)
- [Development](#development)
- [How the Platform works](#how-the-platform-works)
- [Roles and permissions](#roles-and-permissions)
- [Research features](#research-features)
- [Accounts and authentication](#accounts-and-authentication)
- [API reference](#api-reference)
- [Frontend integration](#frontend-integration)
- [Configuration](#configuration)
- [Deployment and operations](#deployment-and-operations)

## Features

- **Accounts and sessions**: email and password sign-up with an emailed 6-digit code, sign-in with Google, cookie sessions with CSRF protection, password reset, profile pictures, accounts created by an admin.
- **Projects and members**: five fixed roles, invitations by email that expire after 24 hours, archive and restore, no hard delete.
- **Research inputs**: versioned CSV datasets (uploaded, or imported through a data connection to PostgreSQL, MySQL, BigQuery, a Google spreadsheet, a Google Drive folder, Prometheus or InfluxDB), project files, a research topic and domains.
- **Runs**: topic-to-hypothesis runs, one at a time per project, sent to Popper with a spending cap; frame review; result files.
- **Collaboration**: comments on runs and result files, notifications over SSE, an audit log of every change.

## Tech stack

| Area | Choice |
|---|---|
| Language and API | Python 3.12+, FastAPI, Uvicorn |
| Database | PostgreSQL 16, SQLAlchemy (async, `asyncpg`), Alembic migrations |
| File storage | A local directory or a Cloudflare R2 bucket |
| Email | Resend |
| External data | `asyncpg` (PostgreSQL), PyMySQL (MySQL), `google-cloud-bigquery` |
| Tooling | [`uv`](https://docs.astral.sh/uv/), Ruff, pytest, Docker Compose, [Task](https://taskfile.dev/docs/installation) |

## Getting started

### Prerequisites

Python 3.12+, [`uv`](https://docs.astral.sh/uv/), Docker with the Compose plugin, and [Task](https://taskfile.dev/docs/installation).

### Run the local stack

```bash
cp .env.example .env.local
task up                        # PostgreSQL + API on 127.0.0.1
task migrate                   # in a second terminal, on a fresh database
```

| What | Where |
|---|---|
| API | `http://localhost:8080` |
| Swagger UI | `http://localhost:8080/docs` |
| OpenAPI schema | `http://localhost:8080/openapi.json` |
| Liveness / readiness | `/api/v1/health/live`, `/api/v1/health/ready` (checks PostgreSQL) |

### The first Platform Admin

The local stack starts with a Platform Admin: `admin@gmail.com`, with the password set in `DEFAULT_ADMIN_PASSWORD` (see `.env.example`). It is created once the migrations have been applied (`task migrate`, then `task restart`).

Without those two settings, register an account (`POST /api/v1/auth/register`), verify its email with the emailed code (`POST /api/v1/auth/verify-email`; without `RESEND_API_KEY` the code is in the API log) and make it the first Platform Admin:

```bash
docker compose --env-file .env.local -f docker/docker-compose.dev.yml run --rm api platform-be bootstrap-admin --email admin@example.com
```

The command refuses to run when a Platform Admin already exists, and does not accept an unregistered, unverified or suspended account.

### Task shortcuts

| Command | What it does |
|---|---|
| `task up` | Start or rebuild the local API and PostgreSQL |
| `task down` | Stop the stack, keep the database volume |
| `task restart` | Restart the containers without rebuilding |
| `task status`, `task logs` | Container status; follow the logs |
| `task migrate` | Apply pending database migrations |
| `task prod:*` | The same for the production stack; see [Production stack](#production-stack) |

`task --list` describes them all.

## Project structure

```text
src/platform_be/
  main.py          the FastAPI application
  api/v1/          routes, one module per resource
  auth/            sessions
  cli/             the `platform-be` command: `run`, `bootstrap-admin`
  core/            settings, errors, response envelope, middleware, roles, security
  db/              engine and session
  models/          SQLAlchemy models
  services/        business rules, file stores, email, the Popper client
    connectors/    PostgreSQL, MySQL and BigQuery connectors, with the network guard
alembic/           database migrations
docker/            Dockerfile, Compose files for dev and prod, the prod settings template
tests/             pytest suite
Taskfile.yml       shortcuts for the two stacks
```

`docs/` holds the frontend handoff documents and the Popper contract. It is kept locally and is not tracked in Git.

## Development

### Tests and static checks

On the host:

```bash
uv sync --all-groups
uv run ruff format --check src alembic tests
uv run ruff check src alembic tests
uv run pytest
```

Tests run on SQLite in memory. Some need a real service and are skipped unless a variable points at one:

| Variable | Turns on |
|---|---|
| `PLATFORM_POSTGRES_TEST_URL` | Tests of real PostgreSQL behaviour (locks, concurrent requests), against a scratch database |
| `PLATFORM_MYSQL_TEST_URL` | Tests of the MySQL connector against a real server |
| `PLATFORM_BIGQUERY_TEST_SERVICE_ACCOUNT` | The one test that talks to BigQuery itself |
| `PLATFORM_LIVE_CONNECTOR_TESTS=1` | The whole flow through public databases on the internet |
| `PLATFORM_LIVE_GOOGLE_REFRESH_TOKEN` and the four beside it | With the line above: the whole flow through Google's own Drive and Sheets APIs |
| `PLATFORM_LIVE_PROMETHEUS_URL`, `PLATFORM_LIVE_INFLUXDB_URL` and those beside them | With the same line: the whole flow through a Prometheus and an InfluxDB server |

**MySQL.** The URL must be of a user who can create databases and users. The Platform itself runs no MySQL: a throwaway container is enough, and it is gone once stopped.

```bash
docker run --rm -d --name connector-test-mysql -e MYSQL_ROOT_PASSWORD=scratch -e MYSQL_DATABASE=connector_test -p 127.0.0.1:3306:3306 --tmpfs /var/lib/mysql mysql:8
PLATFORM_MYSQL_TEST_URL=mysql://root:scratch@127.0.0.1:3306/connector_test uv run pytest
docker stop connector-test-mysql
```

**BigQuery.** The connector tests run against a stand-in for BigQuery's REST API, through Google's own client library. One test talks to BigQuery itself and needs the path of a service account key file. The account needs the roles BigQuery Job User and BigQuery Data Viewer on its project; the test reads a few rows of a public dataset, which is billed to that project (well inside the free monthly quota). CI never runs it.

```bash
PLATFORM_BIGQUERY_TEST_SERVICE_ACCOUNT=/path/to/key.json uv run pytest tests/test_bigquery_connector.py
```

**Public databases.** `tests/test_live_connectors.py` checks connections, browsing, preview and dataset import against Rfam (MySQL), RNAcentral (PostgreSQL, password on its [help page](https://rnacentral.org/help/public-database)) and, with the key file above, BigQuery. CI never runs it.

```bash
PLATFORM_LIVE_CONNECTOR_TESTS=1 PLATFORM_LIVE_RNACENTRAL_PASSWORD=... uv run pytest tests/test_live_connectors.py -s
```

**Google.** The same file reads a real spreadsheet and a real Drive folder as a real Google account. Nothing is written at Google. It needs five variables, and is skipped without them:

| Variable | Value |
|---|---|
| `PLATFORM_LIVE_GOOGLE_CLIENT_ID`, `PLATFORM_LIVE_GOOGLE_CLIENT_SECRET` | The OAuth client the refresh token was issued to |
| `PLATFORM_LIVE_GOOGLE_REFRESH_TOKEN` | A refresh token of a Google account, with the `drive.readonly` scope |
| `PLATFORM_LIVE_GOOGLE_SPREADSHEET` | Address or ID of a spreadsheet that account can open, with a header row and at least one row in the tab whose name sorts first |
| `PLATFORM_LIVE_GOOGLE_FOLDER` | Address or ID of a folder of that account that holds at least one CSV, Google Sheets or `.xlsx` file, each with a header row and at least one row in the tab whose name sorts first |

One way to get the refresh token: add `https://developers.google.com/oauthplayground` to the client's redirect URIs, open the [OAuth 2.0 Playground](https://developers.google.com/oauthplayground), tick **Use your own OAuth credentials** in its settings, authorize `https://www.googleapis.com/auth/drive.readonly` and exchange the code. Remove the redirect URI afterwards. The token is a credential for that account's whole Drive: keep it out of files that are committed and out of shell history.

```bash
PLATFORM_LIVE_CONNECTOR_TESTS=1 uv run pytest tests/test_live_connectors.py -k google -s
```

**Time series.** The same file takes a Prometheus and an InfluxDB server from a new connection through browsing, a preview, a live view and an import to a second version. Each is skipped without its address. Nothing is written to either server.

| Variable | Value |
|---|---|
| `PLATFORM_LIVE_PROMETHEUS_URL` | Address of a Prometheus server that keeps the metric `up`, as one that scrapes anything does. The project's public demo, `https://prometheus.demo.prometheus.io`, needs no credentials |
| `PLATFORM_LIVE_PROMETHEUS_USERNAME`, `PLATFORM_LIVE_PROMETHEUS_TOKEN` | Its credentials, when it asks for any: a token alone is sent as Bearer, with a user name as the password of Basic |
| `PLATFORM_LIVE_INFLUXDB_URL`, `PLATFORM_LIVE_INFLUXDB_DATABASE` | Address of an InfluxDB server (1.8, 2.x or 3 Core) and a database, or bucket, of it. The measurement whose name sorts first needs a field of numbers written to in the last hour |
| `PLATFORM_LIVE_INFLUXDB_TOKEN` | Its token, when it asks for one |
| `PLATFORM_LIVE_INFLUXDB_PRIVATE=1` | The InfluxDB server is a container on this machine: private hosts are allowed for that one test, so it says nothing about the host check |

```bash
PLATFORM_LIVE_CONNECTOR_TESTS=1 PLATFORM_LIVE_PROMETHEUS_URL=https://prometheus.demo.prometheus.io \
  uv run pytest tests/test_live_connectors.py -k "prometheus or inwards" -s
```

### Migrations

Alembic owns the database schema. App startup never creates or migrates tables. Apply migrations once during local setup or deployment with `task migrate`. Do not point a developer command at a production database.

### Running without Popper

Leave `POPPER_BASE_URL` empty. Everything works except the three actions that talk to Popper (starting a run, syncing a run, deciding a frame review), which answer `503 POPPER_NOT_CONFIGURED`. The test suite uses an in-memory stand-in (`tests/fakes.py`).

The API Popper must offer, and the two endpoints it calls back, are specified in `docs/popper-integration-contract.md`. Popper v2 does not implement that contract yet.

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
Add project data (optional): upload a CSV or import one through a data connection
   ↓
Choose a research topic and domains; a run does not require a dataset
   ↓
Start a run                          project: researching    run: queued → running
   ↓
Popper asks for a frame review       project: needs_review   run: awaiting_review
   ↓
A Project Manager or Researcher decides: approve / edit / reject
   ↓                                 project: researching    run: running   (may repeat)
Popper delivers the result files and finishes
   ↓                                 project: data_ready if it has datasets, otherwise draft
                                     run: completed | budget_exceeded | failed
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
- **Nothing is overwritten.** A new dataset file is a new version, and result files cannot be replaced. Historical runs retain the dataset and research-context references they were created with.
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
| `researcher` | Project | Uploads datasets, starts runs, decides frame reviews, comments. |
| `reviewer` | Project | Reads everything in the project and comments. Changes nothing else. |

Someone who is not a member gets `404` for everything in a project, so its existence is not revealed. A member whose role is too low gets `403 ROLE_REQUIRED`.

### Projects

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

### Inside a project

| Action | Platform Admin | Project Manager | Researcher | Reviewer |
|---|:-:|:-:|:-:|:-:|
| Read datasets, runs, reviews, result files, comments | ✓ | ✓ | ✓ | ✓ |
| Download dataset and result files | ✓ | ✓ | ✓ | ✓ |
| Upload a dataset or a new version; rename a dataset | ✓ | ✓ | ✓ | 403 |
| Upload or delete a project file | ✓ | ✓ | ✓ | 403 |
| List data connections (never their credentials) | ✓ | ✓ | ✓ | ✓ |
| Create, rename, delete or test a data connection; browse and preview through it; import from it | ✓ | ✓ | ✓ | 403 |
| Start or sync a run; decide a frame review | ✓ | ✓ | ✓ | 403 |
| Abandon a run | ✓ | ✓ | 403 | 403 |
| Comment; edit own comment | ✓ | ✓ | ✓ | ✓ |
| Delete a comment | any | any | own | own |

## Research features

### Datasets

A CSV file, or an Excel workbook (`.xlsx`; not `.xls`). The first sheet of a workbook is converted to CSV when it is uploaded, and that CSV is what the version stores, what a download returns and what Popper reads; the other sheets are ignored. A file must be UTF-8, have a header row of unique, non-empty column names, at least one data row, and the same number of values on every row. The size limit is 50 MiB by default (`DATASET_MAX_UPLOAD_BYTES`); a header may have at most 2000 columns with names of at most 200 characters.

The Platform records the row count, column names, size, and SHA-256. A file that fails a check is refused and nothing is stored.

### Data connections

The second way to bring data in: a project saves a connection to an external database, browses its tables, previews rows live, and imports one table or one `SELECT` statement as a dataset version. Seven kinds: `postgres` (Supabase included), `mysql` (MariaDB included) and `bigquery`; two that read files with a user's Google account, `google_sheets` and `google_drive` ([below](#google-sheets-and-google-drive)); and two that read time series, `prometheus` and `influxdb` ([below](#time-series-prometheus-and-influxdb)).

- **A run never queries the external database.** An import writes a CSV file checked the way an upload is, and the version records where it came from (`source_type`, `source`). Importing the same source again adds a version with what the source holds now.
- **Creating a connection tests it.** Nothing is saved when the test fails; the answer is 422 `CONNECTION_FAILED` and `error.reason` says why (`auth_failed`, `tls_unavailable`, `host_not_allowed`, ...).
- **Only the name can change.** Another host, database or user means deleting the connection and creating a new one, so stored credentials are never sent anywhere but where they were tested.
- **Credentials** are encrypted with `CONNECTION_SECRET_KEY` before they are stored and are never returned, logged or audited. Without the key the feature is off: saved connections can still be listed, renamed and deleted, and everything that needs the credentials (creating, testing, browsing, previewing, importing) answers 503 `CONNECTIONS_NOT_CONFIGURED`.
- **Reads are read-only as far as the connection can make them**: a read-only transaction, one statement per call, a time limit; on BigQuery only `SELECT`, within a scan limit. That stops writes to tables, not everything a database role may be allowed to do. The real protection is the role: connect as a user that can only read. Every member who may contribute to the project can run any `SELECT` that user can.
- A preview returns at most 100 rows and stores nothing. An import that would exceed `DATASET_MAX_UPLOAD_BYTES`, or holds a value longer than 100 000 characters, is refused, never cut.
- Previews and imports are audited with a hash of the SQL, not its text.

An import runs inside the request, for up to `CONNECTION_IMPORT_TIMEOUT_SECONDS` (300). A client whose request is cut off on the way should list the datasets before trying again: the import may have finished.

#### Google Sheets and Google Drive

Two kinds have no password to paste: a user lets the Platform read their Google Drive, and the connection keeps that access. Off until the settings in [Configuration](#data-connections-1) are complete; [Google Cloud setup](#google-cloud-setup-for-connections) is below. While they are off, `start` and the callback answer `404`, creating and reauthorizing answer 503 `CONNECTIONS_NOT_CONFIGURED`, and a connection saved earlier answers 422 `CONNECTION_FAILED`, reason `unreachable`, on every read and test.

| Kind | Points at | `schema` | `table` |
|---|---|---|---|
| `google_sheets` | One spreadsheet (`config.spreadsheet`: its address or ID) | The spreadsheet, by its title | Each tab |
| `google_drive` | One folder (`config.folder`: its address or ID) | Each CSV file (one Drive holds as `text/csv`), Google spreadsheet and Excel workbook (`.xlsx`) directly in the folder, by file name | Each tab; a CSV file has one table, named like the file |

Browsing, previewing and importing use the same endpoints as every other kind, with a `table` source. A `query` source answers 422 `SOURCE_INVALID`, reason `unsupported_source`.

**Giving access.** Creating a connection takes three steps:

| Step | Request | Result |
|---|---|---|
| Start | The browser navigates to `GET /api/v1/projects/{id}/connections/google/start` (a link, not a `fetch`). Needs a session and the Project Manager or Researcher role. | `302` to Google's consent page, which asks to see the account's Drive files. A refusal is a JSON error, not a redirect (401, 403, 404, or 409 for an archived project): offer the link only to those who may use it. |
| Return | Google sends the browser to `GET /api/v1/connections/google/callback` | `302` to `{APP_URL}/projects/{id}/connections?google_grant=ID`, or `?error=CODE` with nothing stored. When the browser carries nothing that names the project (another browser, or the 10 minutes are over), the redirect is `{APP_URL}/projects?error=GOOGLE_ACCESS_FAILED`. |
| Create | `POST /api/v1/projects/{id}/connections` with `kind`, `name`, `config` and `grant_id: ID` in place of `secret` | The connection, tested like any other. `config` now holds `spreadsheet_id` and `title`, or `folder_id` and `folder_name`, with `account_email` and `google_subject`. |

A grant works once, within 10 minutes of `start` (the time at Google's page counts), for the user who started and in that project. A connection that fails its test (422 `CONNECTION_FAILED`) leaves the grant usable, so another address can be tried without going back to Google. So does an address that is not a spreadsheet's or a folder's, such as a published `/d/e/...` link: that is refused with 422 `VALIDATION_ERROR` before Google is asked.

| Code | Where | Meaning |
|---|---|---|
| `GOOGLE_ACCESS_FAILED` | `?error=` | Cancelled at Google, expired, or not the browser that started. Start again. |
| `GOOGLE_ACCESS_NOT_GRANTED` | `?error=` | The user continued without ticking the Drive permission. Start again and tick it. |
| `GOOGLE_GRANT_INVALID` | 422 | The grant is expired, already used, or not this user's and project's. Start again. |
| `GOOGLE_ACCOUNT_MISMATCH` | 422 | `reauthorize` with another Google account than the connection's. |
| `CONNECTION_NOT_GOOGLE` | 422 | `reauthorize` on a connection of another kind. |

Reasons these kinds add to `error.reason`:

| `reason` | With | Meaning |
|---|---|---|
| `access_revoked` | `CONNECTION_FAILED` | Google no longer accepts the stored access: it expired or the user removed it. Reauthorize. |
| `permission_denied` | `CONNECTION_FAILED` | The Google account cannot open the spreadsheet or folder, or it does not exist. Also an Excel file opened in Google Sheets: its address looks like a spreadsheet's, but it is read only through a `google_drive` connection or after **File > Save as Google Sheets**. `message` says which. |
| `rate_limited` | `CONNECTION_FAILED` | Google is limiting requests for the account (HTTP 429). Try again shortly. A limit Google reports as 403 reads as `permission_denied`. |
| `source_not_found` | `SOURCE_INVALID` | No such file in the folder, or no such tab. |
| `source_malformed` | `SOURCE_INVALID` | The header row breaks the rules below, two files in the folder share the name, or the file is not what its type says. `message` says which. |
| `source_too_large` | `SOURCE_INVALID` | A CSV or Excel file is larger than a dataset may be. |
| `unsupported_source` | `SOURCE_INVALID` | A `query` source. |

**Reauthorizing.** When a read or a test says `access_revoked`, send the user through `start` again and pass the new grant to `POST /projects/{id}/connections/{connection_id}/reauthorize` (`{"grant_id": ID}`). The connection keeps its ID, so dataset versions imported through it still name it. Only the Google account the connection was created with is accepted, and the spreadsheet or folder must open with it; when either fails, the stored access stays as it was.

**How cells become a table.** One rule for a tab, a CSV file and an Excel sheet:

- The first row names the columns. Two columns with the same name are refused.
- A row shorter than the header is padded with empty values. Empty rows at the end are dropped; empty rows between rows of data are kept.
- A column without a name is skipped while it is empty. A value under an empty header, or to the right of the last header, is refused (`source_malformed`), never dropped.
- Google Sheets: numbers are read as stored (no thousands separators), dates and times as the text the cell shows.
- Excel: a date or a time is read as ISO 8601 (`2026-09-14T00:00:00`), whatever format the cell shows. A formula gives the value saved with it, and nothing when the file was written by a program that never computed it.
- CSV: UTF-8, with or without a byte-order mark; values are taken as they are written.

The result is then checked like any dataset file, so a tab with a header and no rows is refused there (422 `INVALID_DATASET`).

**Who can read what.**

- A connection reads with the Google account of the user who created it (`config.account_email`). Every member who may contribute to the project reads that one spreadsheet, or the files directly in that one folder, through it, whether or not their own Google account could open them. Reviewers see that the connection exists and nothing behind it.
- Nothing else of that account is reachable through the connection: what it points at cannot change, and a file outside the folder is never found, whatever name is asked for.
- The refresh token is encrypted with `CONNECTION_SECRET_KEY` like any other credential, and is never returned, logged or audited. Audit events name the spreadsheet or folder ID, not the Google account.
- Deleting a connection deletes its stored access and does not tell Google. To take the access back at Google, the account's owner opens [myaccount.google.com/connections](https://myaccount.google.com/connections), picks the app and removes its access. That ends every connection made with that account, in every project: each then answers `access_revoked` until it is reauthorized.

**Not supported.**

- Folders inside the folder: only files directly in it are listed, at most 1000.
- `.xls`, Google Docs, PDF and every other type: they are not listed.
- Shared drives: requests are sent in the form Drive needs for them, but this was not tested against one, so it is not promised.
- Syncing on a schedule and a token per member.

**Choosing a source with Google Picker.** After Google consent, the frontend calls
`POST /api/v1/projects/{id}/connections/google/picker` with `{"grant_id":"..."}` and its
CSRF header. The response's `data` contains `access_token`, `api_key`, and `app_id` for
Google's browser picker. The token belongs to the account that authorized the grant.
This endpoint requires contributor access to a writable project and a valid, unused
grant belonging to that user and project. It returns `Cache-Control: no-store`, never
returns a refresh token or client secret, and does not spend the grant. The frontend
must keep the response in memory only and dispose the picker on selection/cancellation.
Select a spreadsheet for Google Sheets or a folder for Google Drive, then create the
connection with the selected ID and the same grant. It uses the existing connection
request rate and concurrency limits. A missing picker configuration returns
`503 GOOGLE_PICKER_NOT_CONFIGURED`; revoked or invalid grants return
`422 GOOGLE_GRANT_INVALID`; a Google token failure returns `502 GOOGLE_PICKER_UNAVAILABLE`.

Enable **Google Picker API** in the OAuth client's Google Cloud project and set:

```dotenv
GOOGLE_PICKER_API_KEY=<browser API key>
GOOGLE_PICKER_APP_ID=<Cloud project number>
```

The app ID is the numeric **project number**, not the project ID. Restrict the browser
key to **Google Picker API**, with Website restrictions allowing the frontend origin
(for local use, `http://localhost:3000/*`) and `https://docs.google.com/*`.
Restart the backend after changing these settings. With the local Docker stack, run
`task up` to rebuild/recreate the API with the new environment; `task restart` alone
does not reload environment variables. See the
[Google Picker setup guide](https://developers.google.com/workspace/drive/picker/guides/web-picker).

<a id="google-cloud-setup-for-connections"></a>
**Google Cloud setup.** In the project that holds the OAuth client of [sign-in with Google](#sign-in-with-google) (the same client is used; sign-in itself does not have to be on):

1. **APIs & Services > Library**: enable **Google Drive API** and **Google Sheets API**.
2. **Google Auth Platform > Clients**, the web client: under **Authorised redirect URIs** add `http://localhost:8080/api/v1/connections/google/callback` and the production one, `https://<api-host>/api/v1/connections/google/callback`. `GOOGLE_OAUTH_CONNECTIONS_REDIRECT_URI` must match one of them character for character.
3. **Google Auth Platform > Data Access > Add or remove scopes**: add `https://www.googleapis.com/auth/drive.readonly`. It is listed under restricted scopes.
4. Set `GOOGLE_OAUTH_CONNECTIONS_REDIRECT_URI`, with `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `APP_URL` and `CONNECTION_SECRET_KEY`, and restart the API. Its log says `google drive access: on`.

`drive.readonly` is a restricted scope, and what users meet depends on the app's publishing status (**Google Auth Platform > Audience**):

| Status | Who can give access | What to expect |
|---|---|---|
| Testing | Only the accounts listed as test users, 100 at most | Access expires 7 days after it was given: the connection answers `access_revoked` and has to be reauthorized every week. |
| In production, not verified | Any Google account, 100 in total over the life of the project | No 7-day expiry. Google shows an "unverified app" warning that the user has to click through. |
| In production, verified | Any Google account | No warning. Verifying a restricted scope takes a security assessment by Google, which this project has not done. |

#### Time series: Prometheus and InfluxDB

Two kinds read the points of a metric over a span of time. They need no setting of their own: they are on wherever data connections are. What is read is chosen in a form, never written as a query, and a metric can be watched before it is imported.

| Kind | Points at | `config` | `secret` | `table` | Columns |
|---|---|---|---|---|---|
| `prometheus` | A Prometheus server, or one with the same HTTP API (VictoriaMetrics, Thanos, Mimir, Grafana Cloud) | `url` | `token` alone (Bearer), or `username` and `token` (Basic); neither for a server without credentials | Each metric | `time`, `value` and each label |
| `influxdb` | InfluxDB 1.8, 2.x or 3 Core, read with InfluxQL | `url`, and `database` (on 2.x and 3: the bucket) | 2.x/3: `token`; authenticated 1.8: `username` and `password` (Basic auth) | Each measurement | `time`, each field and each tag |

- **The address** is `http(s)://host[:port][/base-path]`, the part in front of `/api/v1` or `/query`. One with a user name, a password, a query or a fragment is refused with 422 `VALIDATION_ERROR` before anything is tried or recorded. The server is spoken to at the address the host check accepted, a redirect is never followed (`unreachable`), and over `https` the certificate must be one a public authority signed for that name. `http` is accepted; credentials then travel in the clear.
- **Browsing.** `default` is the only schema. `GET .../columns` says what each column is in `role`: `time`, `field` or `tag` (`null` for every other kind).
- **A source** is `{"type": "timeseries", "name", "fields", "tags", "start", "end", "bucket", "aggregate"}`, and the only type these kinds read; the other kinds refuse it (`unsupported_source`). `fields` is empty for Prometheus, whose one value is the column `value`, and names at least one field for InfluxDB. `bucket` is one of `1m`, `5m`, `15m`, `1h`, `6h`, `1d`, `1w` and `aggregate` one of `mean`, `sum`, `min`, `max`, `count`; both, or, for InfluxDB only, neither, which reads the points as they were written. `increase`, for Prometheus only, is how much a counter grew in each bucket: an estimate, so not a whole number.
- **The table** has `time` first, then the tags, then the fields. `time` is the start of the bucket, in UTC; buckets are counted from midnight UTC and weeks from Monday, so both kinds give the same rows for the same points. A bucket without a point has no row. NaN and the infinities are empty values.
- **Live.** `POST .../live` with `{"name", "fields", "tags", "aggregate", "last"}` returns the buckets of the last `15m`, `1h`, `6h` or `24h` that have ended (buckets of `15s`, `1m`, `5m`, `15m`), at most 2000 rows, and stores nothing. A client polls it; each call counts against the read budget and holds a slot. It is audited once in 10 minutes for each user, connection and metric.
- **A version** imported this way keeps the whole form in `source.source`, which is all there is to say about its span and its bucket.
- **Limits.** Prometheus refuses a span of more than 11 000 buckets (`too_many_points`). An answer of more than 16 MiB is refused (`source_too_large`), never cut. A span the server no longer keeps is an empty table, which cannot be imported (`INVALID_DATASET`).

Checked on 2026-10-08 against the Prometheus project's public demo (3.13.0) and against InfluxDB 1.8.10, 2.7.12 and 3 Core 3.12.0 in containers. Not checked against a real server: Prometheus credentials, a base path, the servers that share its API, and InfluxDB over HTTPS.

### Project files

Documents attached to a project: PDF, CSV and Excel (`.xlsx`, `.xls`), up to 50 MiB each by default (`PROJECT_FILE_MAX_UPLOAD_BYTES`). They are reference material for members and are never sent to Popper. Datasets are versioned project data; topic runs are started independently with a topic and domains.

The type comes from the file name, not from the content type the browser sends, and the first bytes must match it: a renamed file is refused with `INVALID_FILE`. Files are downloaded as attachments, never rendered by the API.

### Runs

`POST /projects/{id}/runs` starts a topic-to-hypothesis run. Supply a `topic`, at least one entry in `domains`, and an optional `review_mode` (`copilot` or `auto`). The server sends these inputs to Popper with a spending cap (`RUN_DEFAULT_BUDGET_USD`, at most `RUN_MAX_BUDGET_USD`).

Run responses keep `research_context_id` and `research_context_version` for historical runs. New topic runs leave those fields null; saved context records remain in the database for history and are no longer readable or writable through an API.
Requests using retired fields such as `dataset_version_id`, `research_context_version` or `auto_review` are rejected with `422 VALIDATION_ERROR`.

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

A notification carries no text; the frontend words it from `kind`. Only invitations are also sent by email. Clients receive them live over SSE: see [Realtime notifications](#realtime-notifications).

### Project invitations

`POST /projects/{id}/members` invites a registered user with a role. The member row is returned with `status: "invited"`, `invite_sent_at`, `invite_expires_at`, `invite_expired` and `invite_email_sent` (false when the email provider refused the message; the invitation still exists). The user gets a `project_invited` notification and an email.

- An invitation is valid for 24 hours (`PROJECT_INVITE_TTL_HOURS`).
- The invited user reads `GET /invitations` and answers with `POST /invitations/{membership_id}/accept` or `/decline`. Optional `q` (3–120 characters) searches project name and inviter display name/email, case-insensitively, before counting and pagination. Accepting makes them a member with the offered role; declining can be done at any time, also after expiry.
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

**Choosing whom to invite.** `GET /projects/{id}/invite-candidates` gives Project Managers and Platform Admins a paginated user picker (`q`, `limit`, `offset`). It returns only active, email-verified regular user accounts (excluding Platform Admins) with `id`, `email`, `display_name` and `avatar_url`. Active members and pending invitations, even expired ones, are excluded before counting and pagination. Revoked membership history does not exclude a user. Archived projects answer `409 PROJECT_ARCHIVED`. The same page can be followed live: see [Invite candidates stream](#invite-candidates-stream).

## Accounts and authentication

The Platform owns accounts, passwords, sessions, and roles. The login name is the email address; there is no separate username. A user signs in with a password, with [Google](#sign-in-with-google), or with either.

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

`login` and `verify-email` set the HttpOnly session cookie and return the user, session expiry, and CSRF token. None of the seven endpoints above the change-password row takes a CSRF token; all need an allowed `Origin`.

### Sign-in with Google

Off until `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `GOOGLE_OAUTH_REDIRECT_URI` and `APP_URL` are all set; until then both routes answer `404`.

| Step | Request | Result |
|---|---|---|
| Start | The browser navigates to `GET /api/v1/auth/google/start` (a link, not a `fetch`) | `302` to Google's sign-in page. |
| Return | Google sends the browser to `GET /api/v1/auth/google/callback` | `302` to `{APP_URL}/` with the session cookie set, or `302` to `{APP_URL}/auth/login?error=CODE` with nothing changed. |

| `CODE` | Meaning |
|---|---|
| `GOOGLE_SIGN_IN_FAILED` | The sign-in was cancelled, expired or could not be confirmed with Google, or the address belongs to an account that is linked to another Google account. Start again. |
| `GOOGLE_EMAIL_NOT_VERIFIED` | The address is not one Google manages. Only a verified `@gmail.com` address or a Google Workspace account is accepted; any other address signs in with a password. |
| `USER_SUSPENDED` | The account is suspended. |

What a sign-in does depends on the address Google reports:

| Case | Result |
|---|---|
| No account has the address | A new account, already verified, without a password. The name comes from Google. |
| A verified account has it | The Google account is linked to it. The password keeps working: there are now two ways in. |
| An account that never verified its email, or still has a temporary password, has it | Linked, and the email counts as verified. The password is removed and every session of the account ends: whoever set that password never proved the inbox. |
| The Google account signed in before | Signed in as the same user, found by Google's account ID. The email the Platform stored stays, even if the address at Google changed. |
| The account is linked to another Google account | Refused, `GOOGLE_SIGN_IN_FAILED`. |

- A link is made once and emails the account's owner. An owner who did not make it tells an administrator.
- `change-password` and `reset-password` never touch the link.
- A user without a password gets `401 INVALID_CREDENTIALS` on `login` like any wrong password, and sets one with `forgot-password`.
- A link cannot be removed yet.
- A user without an avatar gets the Google profile picture at sign-in, stored like an uploaded one. An avatar that is already there is never replaced; a user who removes theirs gets the Google picture again at the next sign-in with Google. A picture that cannot be fetched does not stop the sign-in.

**Google Cloud setup.** In the [Google Cloud console](https://console.cloud.google.com/), in one project:

1. **APIs & Services > OAuth consent screen**: user type External, with the scopes `openid`, `email` and `profile`. Publish the app; while it is in testing only the listed test users can sign in.
2. **APIs & Services > Credentials > Create credentials > OAuth client ID**, type Web application.
3. Under **Authorised redirect URIs** add `http://localhost:8080/api/v1/auth/google/callback` and the production one, `https://<api-host>/api/v1/auth/google/callback`. `GOOGLE_OAUTH_REDIRECT_URI` must match one of them character for character.
4. Copy the client ID and the client secret into the settings.

### One-time codes

A code is 6 digits, emailed on its own labelled line, never in the subject. Only a keyed digest is stored.

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

### Passwords, sessions and rate limits

Passwords are 8 to 128 characters. Only an scrypt hash with its own salt is stored (`PASSWORD_SCRYPT_LOG2_N` sets the cost); emails are compared without regard to letter case.

A session stays valid for 30 days from login (`SESSION_ABSOLUTE_DAYS`) as long as it is used at least once every 7 days (`SESSION_IDLE_MINUTES`, default 10080). There is no refresh token: each request extends the idle window on the server.

Rate limits are counted per client address and API process, each over `AUTH_SESSION_RATE_WINDOW_SECONDS` (60), and answer `429 RATE_LIMITED` with `Retry-After`:

| Endpoints | Limit | Setting |
|---|---|---|
| `login`, `register` | 10, shared | `AUTH_SESSION_RATE_LIMIT` |
| `verify-email`, `resend-verification`, `forgot-password`, `verify-reset-password`, `reset-password` | 20, shared | `AUTH_CODE_RATE_LIMIT` |

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

### Profile pictures

A picture is a PNG, JPEG or WebP image of at most 4 MiB (`AVATAR_MAX_UPLOAD_BYTES`). The type is read from the first bytes of the file; its name and declared content type are ignored, so SVG, GIF and renamed files are refused.

- A user sets or removes their own picture at `/api/v1/auth/me/avatar`; a Platform Admin does it for anyone at `/api/v1/users/{id}/avatar`.
- `avatar_url` is returned wherever a user's name is: `me` and sign-in, the user list, project members (`avatar_url`), comments (`author_avatar_url`) and notifications (`actor_avatar_url`). It is `null` without a picture.
- The value is a path on the API origin, `/api/v1/users/{id}/avatar?v=...`, readable by any signed-in user. It needs the session cookie: when the frontend is on another site than the API, load it with credentials (`fetch` with `credentials: "include"`, then a blob URL) instead of a plain `<img src>`.
- The `v` part changes with every upload. With the current `v` the response may be cached for a year; a URL without it, or with an old one, still returns the current picture but is checked again on every use. Always use the URL the API returned last.
- Images are stored exactly as uploaded, in the same store as project files under `users/`. Cropping, resizing and removing photo metadata (a phone photo carries its location) are the frontend's job before upload.

### What the Platform does not do

A code at every sign-in (two-factor), changing the email of an account, removing a Google link, and alerts for a new device. Accounts that existed before email verification was added count as verified.

## API reference

All paths start with `/api/v1`. Full schemas and error cases are in Swagger UI at `/docs`.

### Response format

REST/JSON endpoints return the same envelope. `meta.request_id` matches the `X-Request-ID` response header.

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

The two SSE endpoints answer with event frames once the stream is open (see [Frontend integration](#frontend-integration)); an error before that still uses the JSON envelope.

### Search

The `q` parameter matches a substring, case-insensitively, and needs at least 3 characters. It is backed by `pg_trgm` GIN indexes on `users.email`, `users.display_name`, `projects.name`, `projects.description` and `datasets.name` (migration `20261005_0011`). Full-text search (`tsvector`) is the next step only when searching document content is needed.

### Session

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/auth/register` | Create an account with email and password; emails a 6-digit code |
| `POST` | `/auth/verify-email` | Enter the code and the password; verifies the email and sets the session cookie |
| `POST` | `/auth/resend-verification` | Email a new verification code |
| `POST` | `/auth/forgot-password` | Email a code to set a new password |
| `POST` | `/auth/verify-reset-password` | Verify the reset OTP and return a one-use reset token |
| `POST` | `/auth/reset-password` | Set a new password with the verified reset token; ends every session |
| `POST` | `/auth/login` | Sign in with email and password; sets the session cookie |
| `GET` | `/auth/google/start` | Redirect the browser to Google to sign in |
| `GET` | `/auth/google/callback` | Where Google sends the browser back; sets the session cookie and redirects to the frontend |
| `POST` | `/auth/change-password` | Change the password; signs out the user's other sessions |
| `POST` | `/auth/logout` | End the current session |
| `POST` | `/auth/logout-all` | End every session of the signed-in user, on all devices |
| `GET` | `/auth/me` | Signed-in user, platform role, session expiry, and project memberships |
| `PATCH` | `/auth/me` | Change your own display name |
| `POST` `DELETE` | `/auth/me/avatar` | Upload, replace or remove your own picture |
| `GET` | `/auth/csrf-token` | CSRF token for the current session |

### Administration

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
| `GET` | `/admin/usage/projects` | Runs and cost per project (Platform Admin; `q`, `from`, `to`) |

### Projects and members

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects` | List my projects; create a project |
| `GET` `PATCH` | `/projects/{id}` | Read or update project details |
| `POST` | `/projects/{id}/archive`, `/restore` | Archive or restore a project |
| `POST` | `/projects/{id}/complete`, `/reopen` | Mark the project completed; reopen it |
| `GET` `POST` | `/projects/{id}/members` | List members and open invitations (`status`); invite a registered user by email |
| `GET` | `/projects/{id}/invite-candidates` | Search eligible users to invite; excludes existing members and open invitations (Project Manager or Platform Admin) |
| `GET` | `/projects/{id}/invite-candidates/stream` | SSE snapshots of the filtered candidate page after committed changes (Project Manager or Platform Admin) |
| `PUT` `DELETE` | `/projects/{id}/members/{membership_id}` | Change a role; remove a member or cancel an invitation |
| `POST` | `/projects/{id}/members/{membership_id}/invite` | Send an expired invitation again |
| `GET` | `/invitations` | My open project invitations |
| `POST` | `/invitations/{membership_id}/accept`, `/invitations/{membership_id}/decline` | Answer an invitation |

### Datasets and data connections

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects/{id}/datasets` | List datasets; upload a CSV or `.xlsx` file as a new dataset (multipart: `name`, `description`, `file`) |
| `GET` `PATCH` | `/projects/{id}/datasets/{dataset_id}` | Read; rename or describe |
| `GET` `POST` | `/projects/{id}/datasets/{dataset_id}/versions` | List versions; upload a new version |
| `GET` | `/projects/{id}/datasets/{dataset_id}/versions/{version_id}/download` | Download a version's file |
| `POST` | `/projects/{id}/datasets/from-connection` | Import a table, a `SELECT` or a time series through a data connection as a new dataset |
| `POST` | `/projects/{id}/datasets/{dataset_id}/versions/from-connection` | Import it again as the next version |
| `GET` `POST` | `/projects/{id}/connections` | List data connections (`q`); test and save a new one (`kind`: `postgres`, `mysql`, `bigquery`, `google_sheets`, `google_drive`, `prometheus`, `influxdb`) |
| `GET` | `/projects/{id}/connections/google/start` | Browser navigation: to Google, to give read access to Drive |
| `POST` | `/projects/{id}/connections/google/picker` | Short-lived Google Picker credentials for the user's project grant; requires CSRF |
| `GET` | `/connections/google/callback` | Where Google sends the browser back; redirects to the frontend with `google_grant` or `error` |
| `POST` | `/projects/{id}/connections/{connection_id}/reauthorize` | Give a Google connection a fresh access to the same Google account |
| `GET` `PATCH` `DELETE` | `/projects/{id}/connections/{connection_id}` | Read; rename; delete with its stored credentials |
| `POST` | `/projects/{id}/connections/{connection_id}/test` | Test again; the outcome is in `last_tested_at` and `last_error_code` |
| `GET` | `/projects/{id}/connections/{connection_id}/schemas` | Schemas the database user can read (BigQuery: datasets; Google: the spreadsheet, or the files of the folder) |
| `GET` | `/projects/{id}/connections/{connection_id}/tables` | Tables and views of one `schema`, at most 500 (`search`); Google: the tabs of one file |
| `GET` | `/projects/{id}/connections/{connection_id}/columns` | Column names and types of one `table` |
| `POST` | `/projects/{id}/connections/{connection_id}/preview` | First rows of a table, a `SELECT` or a time series, as text |
| `POST` | `/projects/{id}/connections/{connection_id}/live` | `prometheus` and `influxdb`: the latest points of a metric, for a chart that polls |

### Project files

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects/{id}/files` | List files (`q`, `kind`); upload a PDF, CSV or Excel file (multipart: `file`) |
| `GET` | `/projects/{id}/files/{file_id}/download` | Download a file |
| `DELETE` | `/projects/{id}/files/{file_id}` | Delete a file |

### Runs, review, and results

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

### Comments and notifications

| Method | Path | Purpose |
|---|---|---|
| `GET` `POST` | `/projects/{id}/runs/{run_id}/comments` | List comments (`artifact_id` filter); add one |
| `PATCH` `DELETE` | `/projects/{id}/runs/{run_id}/comments/{comment_id}` | Edit own comment; delete |
| `GET` | `/notifications`, `/notifications/unread-count` | My notifications (`unread_only`); unread count |
| `GET` | `/notifications/stream` | Real-time SSE snapshots of my latest notifications and unread count |
| `POST` | `/notifications/{id}/read`, `/notifications/read-all` | Mark as read |

### Called by Popper, not by the frontend

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/internal/popper/runs/{run_id}/status` | Report a run's status, cost, and review request |
| `POST` | `/internal/popper/runs/{run_id}/artifacts` | Deliver a result file |

Both require the `X-Service-Key` header (`POPPER_CALLBACK_KEY`); a request without a valid key is refused with `401 SERVICE_KEY_INVALID` before its body is read.

## Frontend integration

### Requests and sessions

- Send every request with `credentials: "include"` and an `Origin` that exactly matches `CORS_ALLOWED_ORIGINS`.
- Send the CSRF token in `X-CSRF-Token` on every mutation (`POST`, `PATCH`, `PUT`, `DELETE`) except the seven endpoints used before a session exists: `register`, `verify-email`, `resend-verification`, `login`, `forgot-password`, `verify-reset-password` and `reset-password`. After a page reload, get it again from `GET /api/v1/auth/csrf-token`.
- Restore the signed-in user after a reload with `GET /api/v1/auth/me`. It returns the user, platform role (`user` or `platform_admin`, never empty), `email_verified`, `must_change_password`, session expiry, and `memberships`: the user's projects with the role in each.
- "Continue with Google" is a plain navigation to `GET /api/v1/auth/google/start`. The user comes back on `{APP_URL}/` signed in: call `me` and `csrf-token` as after a reload. A failure comes back on `{APP_URL}/auth/login?error=CODE`; the codes are in [Sign-in with Google](#sign-in-with-google).
- On sign-out call `POST /api/v1/auth/logout`.
- On `401` with `SESSION_EXPIRED` or `USER_SUSPENDED` the session cookie is already cleared; send the user back to sign-in.

### Realtime notifications

Open authenticated SSE `GET /api/v1/notifications/stream?limit=50` after sign-in. `limit` defaults to `50` and accepts `1`–`100`; the server sends `notifications` immediately and after each committed notification, read/read-all action, or membership removal:

```text
event: notifications
data: {"items":[{"id":"...","kind":"run_finished","project_id":"...","project_name":"...","run_id":null,"actor_user_id":null,"actor_display_name":null,"created_at":"...","read_at":null}],"unread_count":3}
```

`items` are newest first with the `NotificationItem` fields from `GET /notifications`. Each event is a snapshot: replace the window and badge, do not append or poll. `unread_count` is the total visible unread count. Reconnection gets another current snapshot; no cursor is used.

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

Ignore the 15-second keep-alive comment: it rechecks the session but does not extend idle expiry. Initial connection and notification snapshots use normal session activity. The proxy must disable buffering and use read/idle timeouts over 15 seconds. A separate frontend origin needs credential CORS for its exact origin and cross-site-capable cookies. `GET /notifications` remains for paginated history; the CSRF-protected read `POST`s trigger a new snapshot.

### Invite candidates stream

`GET /projects/{id}/invite-candidates/stream` accepts the same query parameters as `GET /projects/{id}/invite-candidates` and follows that page with SSE. Each `invite-candidates` event contains the full GET envelope: replace the page and pagination. Snapshots arrive immediately and after committed membership/account changes, across API workers through PostgreSQL LISTEN/NOTIFY. Rollbacks publish nothing. `session-ended` or `access-ended` tells the frontend to close the stream; reconnect gets current state. Heartbeats revalidate access without polling candidates.

### Handoff documents

Step-by-step integration notes with real responses are kept in `docs/` (local, not tracked in Git): `frontend-api-handoff.md`, `notifications-handoff.md`, `frontend-api-handoff-3-data-connections.md` and `frontend-api-handoff-5-time-series-connections.md`.

## Configuration

Settings come from environment variables; `.env.example` lists them all with safe local defaults. The main ones:

### Storage and upload limits

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

### Email, accounts and invitations

| Variable | Purpose |
|---|---|
| `RESEND_API_KEY` | Resend key used to send email. Empty in `local`: each message is written to the API log instead. Required in `staging` and `production`. |
| `EMAIL_FROM` | Sender shown on emails; must be an address on the domain verified in Resend. |
| `APP_URL` | Address of the frontend, put in emails as the sign-in link (`http://localhost:3000` locally). Must be `https://` in production. Empty: emails carry no link. Sign-in with Google redirects the browser here. |
| `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET` | The OAuth client from the Google Cloud console. Empty: sign-in with Google is off. |
| `GOOGLE_OAUTH_REDIRECT_URI` | This API's `/api/v1/auth/google/callback`, exactly as listed on that client (`http://localhost:8080/api/v1/auth/google/callback` locally). |
| `AUTH_CODE_RATE_LIMIT` | Requests per window and client address to the five code endpoints (default 20). |
| `OTP_TTL_MINUTES`, `OTP_MAX_ATTEMPTS`, `OTP_RESEND_COOLDOWN_SECONDS`, `OTP_MAX_SENDS_PER_HOUR`, `OTP_LOCK_AFTER_FAILURES`, `OTP_LOCK_MINUTES` | Limits of the emailed one-time codes; defaults 10, 5, 60, 5, 10, 60. Not passed by the Compose files: add them there to change them. |
| `DEFAULT_ADMIN_EMAIL`, `DEFAULT_ADMIN_PASSWORD` | A Platform Admin created when the API starts if the account is missing (`admin@gmail.com` in the local stack). Local development only: staging and production refuse to start with them. An existing account keeps its password. |
| `PROJECT_INVITE_TTL_HOURS` | How long a project invitation can be accepted (default 24, at most 168). |

### Popper and runs

| Variable | Purpose |
|---|---|
| `POPPER_BASE_URL` | Popper's address. Empty: runs cannot start. |
| `POPPER_SERVICE_KEY` | Sent to Popper in `X-Service-Key`. Required with the base URL. |
| `POPPER_CALLBACK_KEY` | Expected from Popper in `X-Service-Key`. Required with the base URL. |
| `POPPER_TIMEOUT_SECONDS` | Longest a whole call to Popper may take, dataset upload included (default 30, at most 300). |
| `PUBLIC_BASE_URL` | Address Popper uses to call this API back, without `/api/v1`. |
| `RUN_DEFAULT_BUDGET_USD` | Spending cap of a run when the user gives none (5). |
| `RUN_MAX_BUDGET_USD` | Highest cap a user may ask for (20). |

The two Popper keys are different secrets of at least 32 characters in production. The callback endpoints are reachable by anyone who can reach the API; the key is what protects them, so keep the API behind TLS.

### Data connections

| Variable | Purpose |
|---|---|
| `CONNECTION_SECRET_KEY` | Fernet key that encrypts the credentials of data connections. Empty: the feature is off. |
| `GOOGLE_OAUTH_CONNECTIONS_REDIRECT_URI` | This API's `/api/v1/connections/google/callback`, exactly as listed on the Google OAuth client (`http://localhost:8080/api/v1/connections/google/callback` locally). Empty: the `google_sheets` and `google_drive` kinds are off, as they are without `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `APP_URL` or `CONNECTION_SECRET_KEY`; the other kinds are not affected. |
| `CONNECTION_ALLOW_PRIVATE_HOSTS` | Lets a connection point at a loopback or private address (default `false`). Local development only: staging and production refuse to start with it, and the production Compose file does not pass it. |
| `CONNECTION_CONNECT_TIMEOUT_SECONDS`, `CONNECTION_QUERY_TIMEOUT_SECONDS`, `CONNECTION_IMPORT_TIMEOUT_SECONDS` | Longest a connection attempt, a browse or preview, and a whole import may take (10, 60, 300). |
| `CONNECTION_MAX_CONCURRENT_QUERIES`, `CONNECTION_MAX_CONCURRENT_PER_OWNER` | Calls to external databases running at once in one API process, and how many of them one project or one user may hold (4, 2). A call over the limit answers 429 `CONNECTION_BUSY` at once. |
| `CONNECTION_PROBE_RATE_LIMIT`, `CONNECTION_QUERY_RATE_LIMIT` | Per user and window (`AUTH_SESSION_RATE_WINDOW_SECONDS`): connections created or tested (30), and reads through a saved one (120). Over it: 429 `RATE_LIMITED`. |
| `CONNECTION_PREVIEW_MAX_ROWS` | Rows a preview returns (100). |
| `CONNECTION_BIGQUERY_MAX_BYTES_BILLED` | The most one BigQuery query may scan, in bytes (1 GiB). |

Generate the connection key once and keep a copy somewhere safe:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

A lost or replaced key leaves every saved connection unreadable (409 `CONNECTION_SECRET_UNREADABLE`): each one has to be deleted and created again. Datasets already imported are not affected.

## Deployment and operations

### Production stack

Production uses a separate Compose project, database volume, storage volume, and configuration file. Copy the template inside `docker/`, set strong unique database, session, and Popper secrets, then validate and start the stack:

```bash
cp docker/prod.env.example docker/prod.env.local
task prod:config               # checks the Compose file and the settings, starts nothing
task prod:up
task prod:migrate
```

Other shortcuts: `task prod:status`, `task prod:logs`, `task prod:down` (keeps the database and storage volumes). Each runs `docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml ...`.

The production API binds to `127.0.0.1` (port `API_PORT`, default 8080) and expects a TLS-terminating reverse proxy in front of it. PostgreSQL has no published host port.

The supplied CLI and Docker commands give Uvicorn a 30-second graceful-shutdown timeout; Compose reserves 40 seconds for the API to stop. When starting Uvicorn manually, also pass `--timeout-graceful-shutdown 30`. This bounds a restart even with open SSE connections; their EventSource clients reconnect to receive a fresh notification snapshot.

### Releasing a migration

The order above is for the first start only. **When a release brings a migration, migrate before the new code serves requests**: the new code reads columns the old schema does not have, and the endpoints that use them would answer 500 in between.

```bash
docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml build api
task prod:migrate              # runs in a one-off container of the new image
task prod:up
```

**Rolling back the data connection migrations.** `20261006_0017` adds the connections table and `20261006_0018` adds where a dataset version came from. They only add a table and columns with defaults, so existing rows need nothing. To roll back, start the previous code first, then downgrade with the new image, the only one that holds these migration scripts:

```bash
docker compose --env-file docker/prod.env.local -f docker/docker-compose.prod.yml run --rm api alembic downgrade 20261006_0017
```

Run it before the image is replaced, or from a checkout of the new code. Downgrading to `20261006_0017` drops the two source columns, and with them the record of which versions were imported; downgrading to `20261006_0016` also deletes every saved connection and its credentials.

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

### Limits and deployment security

**Requests and sign-in**

- Request bodies are limited to 1 MiB (`REQUEST_MAX_BODY_BYTES`); dataset uploads, project files, result files and profile pictures have their own limits, in [Configuration](#storage-and-upload-limits).
- The sign-in and code [rate limits](#passwords-sessions-and-rate-limits) are counted per client IP and per API process.
- The Google callbacks (`/auth/google/callback`, `/connections/google/callback`) and `/auth/google/start` have no rate limit yet: each call to a callback with a valid state makes the API ask Google once. Add a limit at the ingress before public deployment.
- Production Compose does not configure Uvicorn's trusted proxy addresses, so behind a proxy all requests may share one IP and one login quota. Before public deployment, make Uvicorn trust only the real proxy addresses and add a shared rate limit at the ingress. Do not use caller-supplied forwarding headers as client identity.
- Sign-up depends on email: `RESEND_API_KEY` is required in `staging` and `production`, and while the provider is down or over quota no account can be verified, admin-created ones included.

**Data connections**

- Data connections reach hosts that users choose. Names are resolved once and the address checked: loopback, private, link-local (cloud metadata included) and IPv4-in-IPv6 transition ranges are refused, in staging as in production, and the connection is made to the checked address. The `prometheus` and `influxdb` kinds speak HTTP to that address and never follow a redirect, which could lead back inside.
- The limits on data connections (slots, per-user rates) are counted per API process, like the sign-in limit. Run one replica, or expect them to multiply by the number of workers.
- TLS to an external database defaults to `require`: encrypted, certificate not checked, which also works with self-signed and private-CA servers. `verify-full` checks the certificate against public authorities and the host name; `disable` is for servers without TLS and sends the password and the rows in the clear.
- An import keeps its request open for up to 300 seconds. A reverse proxy with a shorter read timeout cuts the response while the import still completes.
- The server has no fixed outbound address yet, so a database behind an allowlist firewall cannot be connected.

**Not decided yet**

- The hosting platform is not selected yet.

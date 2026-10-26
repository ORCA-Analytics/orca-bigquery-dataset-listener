---
title: "BigQuery Dataset Listener — ORCA Context"
scope: codebase
last_reviewed: 2026-02-13
owner: nate
generated_by: "cursor"
reviewed: false
---

# BigQuery Dataset Listener

## Quick Mental Model

This is an **event-driven microservice** that:
1. Listens for BigQuery dataset creation events (via Pub/Sub push)
2. Parses the dataset name using a strict `parent__client` naming convention
3. Looks up templates in an internal catalog based on the `parent` (e.g., `shopify`, `facebook_ads`)
4. Waits until the raw tables those templates read exist in BigQuery (the connector's first sync creates them after the dataset)
5. Dispatches a GitHub repository event to `ORCA-Analytics/orca-dbt` with file creation instructions
6. The dbt repo workflow then generates client-specific SQL files from templates

**Think of it as:** _Infrastructure-as-code event handler that automates dbt model scaffolding when new client datasets appear in BigQuery._

## What This Repo Does

This Cloud Run service receives BigQuery audit log events (specifically dataset creation events) via Pub/Sub, extracts the dataset ID, matches it against a hardcoded catalog of known data source patterns (Shopify, Facebook Ads, Google Analytics, etc.), and triggers a GitHub repository dispatch to the `orca-dbt` repo. The dbt repo's GitHub Actions workflow then creates SQL model files from templates for the new client dataset. This eliminates manual file creation when onboarding new clients.

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Node.js 22 (CommonJS) |
| Framework | Express 4.x |
| Key dependencies | `express`, `@google-cloud/bigquery` (table checks) |
| Database(s) | None (stateless service) |
| Deploys to | Google Cloud Run (`us-central1`) |
| CI/CD | Google Cloud Build (docker build → push → deploy) |

## How to Run Locally

```bash
# Prerequisites
# - Node.js 22+ (matches Dockerfile base)
# - GITHUB_TOKEN env var with repo:dispatch scope

# Install
npm install

# Run
GITHUB_TOKEN=ghp_your_token_here node server.js

# Test health endpoint
curl http://localhost:8080/healthz
```

### Required Environment Variables

| Variable | Purpose | Where to find the value |
|---|---|---|
| `GITHUB_TOKEN` | GitHub API token for repository dispatch to `orca-dbt` | Google Cloud Secret Manager (prod) or generate personal token with `repo` scope (local dev) |
| `PORT` | HTTP server port (optional, defaults to `8080`) | Auto-set by Cloud Run, can override locally |
| `MAX_WAIT_HOURS` | How long to wait for a new dataset's tables before dispatching anyway (optional, defaults to `72`) | Cloud Run service env var |
| `SUPABASE_URL` | ORCA app Supabase project URL, for turning on company settings (optional) | Supabase project `orca_analytics` > Settings > API |
| `SUPABASE_SERVICE_ROLE_KEY` | Service-role key for that project (optional) | Google Cloud Secret Manager (prod); Supabase > Settings > API |
| `SUPABASE_SOURCE_DATASET_URL` | URL of the ORCA app's `source-dataset-created` edge function, pinged after each dispatch (optional) | `https://sscfogzzbwlgpqcnnfjx.supabase.co/functions/v1/source-dataset-created` |
| `SOURCE_DATASET_SECRET` | Shared secret sent as `x-orca-source-secret` to that function (optional) | Google Cloud Secret Manager (prod); the same value the edge function checks |

The Cloud Run runtime service account needs `roles/bigquery.metadataViewer` on `orcaanalytics` to list tables (see Deployment). Without it the listener logs `TABLE_CHECK_FAILED` and dispatches immediately, as it did before table checks existed.

Without both Supabase variables the listener logs `COMPANY_FLAGS_DISABLED` at startup and only dispatches scaffolds, as before.

Without both source dataset variables the listener logs `SOURCE_DATASET_PING_SKIPPED` after each dispatch instead of pinging.

## Folder Structure

```
orca-bigquery-dataset-listener/
├── server.js              # All application logic (routes, catalog, table checks, dispatch)
├── test/server.test.js    # Handler tests (node --test), fake BigQuery client
├── package.json           # Dependencies: express, @google-cloud/bigquery
├── Dockerfile             # Node 20 Alpine container
├── cloudbuild.yaml        # GCP deployment pipeline config
├── .dockerignore          # Build exclusions
└── .gitignore             # Git exclusions
```

### Key Entry Points

- **Main entry:** `server.js` (lines 1-217) - entire application in one file
- **Core logic:**
  - `splitDataset()` - parses dataset ID into `parent` and `client` parts
  - `buildPlan()` - matches dataset against catalog and generates file plan, plus the tables to wait for
  - `findMissingTables()` - lists which of those tables don't exist yet
  - `dispatchToGitHub()` - sends repository dispatch event
  - `createSourceDatasetNotifier()` - tells the ORCA app's `source-dataset-created` edge function a dataset landed
- **API routes:**
  - `GET /healthz` - health check (returns `200 ok`)
  - `POST /` - Pub/Sub push endpoint (receives base64-encoded audit log events)

## How It Relates to Other ORCA Services

```mermaid
flowchart LR
    BQDataset[BigQuery Dataset Created] --> AuditLog[GCP Audit Logs]
    AuditLog --> PubSub[Pub/Sub Topic]
    PubSub --> Listener[orca-bigquery-dataset-listener Cloud Run]
    Listener --> GitHubAPI[GitHub API]
    GitHubAPI --> DBTRepo[orca-dbt repo workflow]
    DBTRepo --> NewFiles[Client-specific SQL models]
    Listener --> EdgeFn[Supabase edge function source-dataset-created]
    EdgeFn --> Checklist[ORCA app onboarding checklist]
```

- **Reads from:** Pub/Sub push messages containing BigQuery audit log entries (`protoPayload.resourceName`, `timestamp`), and BigQuery table listings for the datasets a scaffold reads
- **Writes to:** GitHub API (`/repos/ORCA-Analytics/orca-dbt/dispatches` endpoint), the ORCA app's Supabase `companies` table (one setting per new source, see Company settings), and the ORCA app's `source-dataset-created` edge function (see Source dataset ping)
- **Triggered by:** GCP Pub/Sub push subscription (configured external to this repo)
- **Depended on by:** `orca-dbt` repo GitHub Actions workflow expects `bq_dataset_created` event type with specific payload structure

**Assumptions (not verified in code):**
- Pub/Sub subscription and BigQuery audit log sink are configured in GCP console
- `orca-dbt` repo has a `.github/workflows/*.yml` listening for `repository_dispatch` events with type `bq_dataset_created`

## Conventions

### Naming

- **Files:** All lowercase, underscore-separated (e.g., `server.js`)
- **Variables/Functions:** camelCase (e.g., `buildPlan`, `datasetId`)
- **Constants:** SCREAMING_SNAKE_CASE (e.g., `GITHUB_TOKEN`, `GH_OWNER`)
- **Dataset IDs:** `parent__client` format (e.g., `shopify__acmecorp`, `facebook_ads__clientxyz`)
  - Pattern: `/^([a-z0-9_]+)__([a-z0-9_]+)$/`
  - Double underscore is required separator
- **Branches:** Git log shows `update/description`, `fix/description`, `bug/description` patterns

### Code Patterns

- **Single-file service:** All logic in `server.js` (no modules/imports beyond Express)
- **Catalog-driven logic:** `CATALOG` object maps parent dataset names to template entries. Each entry's `tables` lists the raw tables its orca-dbt template reads (`dataset.table`, with `{client}` filled in), which the listener waits for. `dataset.a|b` is satisfied by either table, for loaders that name a table differently (Seller Central: Airbyte `GET_SALES_AND_TRAFFIC_REPORT`/`Orders`, Fivetran `sales_and_traffic_business_report_daily`/`orders`)
- **Optional entries:** an entry with `optional: true` reads a table most connections don't sync (Google Ads `custom_searchterms`: 14 of 211 Google Ads datasets as of 2026-10-17). Its file is scaffolded only if all of its tables exist when the dataset dispatches, otherwise the listener logs `SKIPPING_OPTIONAL_MODEL`. Without `waitHours` its tables don't hold up the dispatch. With `waitHours` (searchterms: 24) a missing table holds the dispatch until the dataset is that old: new clients get `custom_searchterms` and `custom_pmax_searchterms` (Performance Max terms from `campaign_search_term_view`) from a separate `_searchterms` connection that can land after the main one, while older connections never sync it and a `_searchterms` source with expired OAuth never lands it. The check is on the table existing, so empty tables still get the model (a Performance Max-only account returns no rows from `search_term_view`; one without PMax returns none in `custom_pmax_searchterms`). A connection that adds the stream later gets no new dataset event, so that model is added by hand
- **Amazon:** `amazon_ads__<client>` scaffolds the Amazon Ads model and `amazon_sellercentral__<client>` the three Seller Central models (sales, cohorts, new/return), both under orca-dbt's `templates/amazon/`. The client part often carries a loader suffix (`acme_airbyte`, `acme_us_fivetran`); the scaffold keeps it, as for every platform, so the models read the right dataset. Both entries set `requireSomeTables`: a dataset holding none of the template's tables after `MAX_WAIT_HOURS` (fulfillments-only `..._airbyte_fulfillments`, Fivetran dbt-package outputs like `amazon_ads__acme_amazon_ads_source`) is logged as `NOT_DISPATCHING` instead of scaffolded. The dispatch sends `vars.parent` = `amazon_ads` / `amazon_sellercentral`, so orca-dbt needs `templates/sources/amazon_ads.yml` and `templates/sources/amazon_sellercentral.yml`
- **Waiting via Pub/Sub redelivery:** while tables are missing (and the dataset is younger than `MAX_WAIT_HOURS`), the handler returns 429, so Pub/Sub redelivers the message later; no state is stored. After the max wait it dispatches anyway and includes `missingTables` in the payload
- **Company settings:** after a dispatch whose tables were checked and all exist (not after `TABLE_CHECK_FAILED`), `COMPANY_FLAGS` maps the parent to the `companies` column the ORCA app reads (e.g. `fairing` → `hdyhau = 'Fairing'`, `google_ads` → `google_ads = true`) and `createCompanyFlagSetter()` writes it through Supabase's REST API. It matches the company on `table_identifier = client` (for Amazon: any `amazon*` dataset, including ones without a template such as `amazong_ads__acme`, turns on `amazon` for the longest `table_identifier` the client starts with, so `acme_us_airbyte` finds `acme_us`; it waits only for the dataset to hold any table, not for a template's tables), skips unless exactly one company matches, and only replaces the unset value (`'none'`/`false`/null), with that filter repeated in the update itself. It logs `COMPANY_FLAG_SET`, `COMPANY_FLAG_SKIPPED` (with the reason) or `COMPANY_FLAG_FAILED`; a failure never fails the message, because a redelivery would dispatch the scaffold again. Each Supabase call times out after 5 seconds for the same reason: a hang past the Pub/Sub ack deadline would also redeliver. New datasets only: brands connected before this shipped aren't backfilled
- **Source dataset ping (ENG-263):** after every successful GitHub dispatch, `createSourceDatasetNotifier()` POSTs `{"dataset_id": "<parent>__<client>"}` to `SUPABASE_SOURCE_DATASET_URL` with header `x-orca-source-secret: $SOURCE_DATASET_SECRET`. The ORCA app's `source-dataset-created` edge function (ENG-262) uses it to tick the source on the brand's onboarding checklist. It runs under the same condition as the dispatch: no ping for a dataset without a template, one still waiting for tables, one logged `NOT_DISPATCHING`, an `amazon*` dataset that only turns on the company setting, or a failed dispatch. It does ping a dataset dispatched with `missingTables` after `MAX_WAIT_HOURS` or after `TABLE_CHECK_FAILED`. It authenticates with the shared secret only and sends no `Authorization` header, so the edge function must be deployed with JWT verification off (`--no-verify-jwt`), or every ping fails with a 401 from Supabase's gateway. It logs `SOURCE_DATASET_PINGED`, `SOURCE_DATASET_PING_FAILED` (non-2xx, network error, or the 5-second timeout) or `SOURCE_DATASET_PING_SKIPPED` (variables not set). A failure never fails the message, for the same reason as the company setting: a redelivery would dispatch the scaffold again. The secret is never logged
- **Custom output functions:** Some catalog entries use `out: (tpl, client) => filename` for non-standard naming
- **Graceful skips:** Returns HTTP 204 (no content) when dataset doesn't match catalog (not an error)
- **Error handling:** Try/catch on main handler, logs to stdout, returns 500 on dispatch failures

### Testing

- **Framework:** Node's built-in test runner (`node:test`), no dev dependencies
- **Where tests live:** `test/server.test.js` (drives the HTTP handler with Pub/Sub-shaped messages and a fake BigQuery client)
- **How to run:** `npm test`

## Gotchas

> **If you're an AI agent and you're about to change something in this repo, read this section first.**

### Things That Look Wrong but Are Intentional

- **Hardcoded constants in code:** `GH_OWNER`, `GH_REPO`, `PROJECT` are intentionally hardcoded (not env vars) because they're ORCA infrastructure constants
- **Global fetch:** Uses Node 18+ native `fetch` (no `node-fetch` import needed) - see commit `58092e6`
- **204 responses for non-matches:** Returning 204 instead of 404/400 when dataset doesn't match catalog is intentional (Pub/Sub retries on errors)
- **429 responses while waiting:** Expected. A 429 means "tables not there yet"; Pub/Sub redelivers on the subscription's retry policy. Look for `WAITING_FOR_TABLES` log lines
- **Service name:** the Cloud Run service is `orca-bigquery-dataset-listener`. `_SERVICE` in `cloudbuild.yaml` must match it, or a manual `gcloud builds submit` deploys a second, separate service
- **Dispatching when the table check fails:** Intentional fail-open, so a missing permission or BigQuery outage never blocks scaffolding (logged as `TABLE_CHECK_FAILED`)

### Fragile Areas

- **CATALOG object accuracy:** If template paths or folder structures change in `orca-dbt`, this catalog becomes stale. No automated validation.
- **`tables` lists:** If an orca-dbt template starts reading a new raw table, add it to that entry's `tables`. A table that's listed but never synced (e.g. a store without `customer_journey_summary`) delays that platform's scaffold until `MAX_WAIT_HOURS`, then it dispatches with `missingTables` set
- **Pub/Sub subscription settings (outside this repo):** the wait relies on the subscription's retry policy and retention. A dead-letter policy caps deliveries (max 100), which can end the wait early; see Deployment
- **Dataset naming regex:** `splitDataset()` regex is strict - any variation (single underscore, triple underscore, uppercase) silently fails matching
- **Directory aliases:** Some parents use `dir` override (e.g., `fairing` uses `hdyhau_fairing` directory) - easy to miss when adding new entries
- **Custom output functions:** Entries with `out: (tpl, client) => ...` override standard naming - must be maintained manually
- **GitHub token scope:** Must have `repo` scope for private repo dispatch, will fail silently with 404 if token lacks permissions
- **Dead GitHub token:** a revoked or expired token makes every dispatch fail with `GitHub dispatch failed: 401 Bad credentials`. The handler returns 500 and Pub/Sub keeps retrying until the message's 7-day retention runs out; nothing alerts. This went unnoticed from 2026-10-14 to 2026-10-17 (ENG-80). To recover: add a new version to the `orca-bigquery-dataset-listener-github-token` secret, then start a new revision so instances re-read `latest` (env-var secrets are resolved at instance start). Messages still inside retention dispatch on their next redelivery

### Known Tech Debt

- **Catalog is embedded in code:** Template catalog should ideally live in `orca-dbt` repo as source of truth (single source)
- **No validation on dispatch success:** GitHub dispatch returns 204 regardless of whether workflow exists/runs
- **No structured logging:** Uses `console.log` instead of structured logging (Cloud Logging compatibility could be better)
- **No retry logic:** If GitHub API is temporarily down, message is lost (Pub/Sub will retry the POST, but within deadline)

### Environment-Specific Quirks

- **Local vs Cloud Run:** `PORT` defaults to 8080 locally, but Cloud Run injects its own port via env var
- **Token authentication:** Production uses Secret Manager injection, local dev requires manual export
- **No Pub/Sub emulator pattern:** Testing requires actual Pub/Sub or manual curl with base64-encoded payloads

## Deployment

### One-time GCP setup for table checks

Do these before merging the table-check change (merging deploys). If the code deploys first, it fails open and dispatches immediately, as before.

```bash
# 1. Let the Cloud Run service list BigQuery tables
SA=$(gcloud run services describe orca-bigquery-dataset-listener --region us-central1 \
  --format='value(spec.template.spec.serviceAccountName)')
# (empty = the default compute service account, <PROJECT_NUMBER>-compute@developer.gserviceaccount.com)
gcloud projects add-iam-policy-binding orcaanalytics \
  --member="serviceAccount:$SA" --role="roles/bigquery.metadataViewer"

# 2. Find the push subscription that targets this service
gcloud pubsub subscriptions list --format='table(name, pushConfig.pushEndpoint)'

# 3. Redeliver waiting messages every 1-10 minutes instead of as fast as possible
gcloud pubsub subscriptions update SUBSCRIPTION --min-retry-delay=60s --max-retry-delay=600s

# 4. Check retention and dead-lettering
gcloud pubsub subscriptions describe SUBSCRIPTION \
  --format='yaml(messageRetentionDuration, deadLetterPolicy)'
```

Retention must exceed `MAX_WAIT_HOURS` (the 7-day default does). A dead-letter policy stops redelivery after `maxDeliveryAttempts` (at most 100, roughly 16 hours at a 10-minute retry delay): either remove it or set `MAX_WAIT_HOURS` below that.

### One-time setup for company settings

Do these before merging (merging deploys). Until both variables are set, the listener skips company settings and logs `COMPANY_FLAGS_DISABLED` at startup.

```bash
# 1. Store the service-role key (Supabase > Project Settings > API > service_role)
printf '%s' "$SUPABASE_SERVICE_ROLE_KEY" | gcloud secrets create supabase-service-role-key --data-file=-

# 2. Let the Cloud Run service account read it ($SA as in step 1 above)
gcloud secrets add-iam-policy-binding supabase-service-role-key \
  --member="serviceAccount:$SA" --role="roles/secretmanager.secretAccessor"

# 3. Add both variables to the service (kept across Cloud Build deploys)
gcloud run services update orca-bigquery-dataset-listener --region us-central1 \
  --update-env-vars SUPABASE_URL=https://sscfogzzbwlgpqcnnfjx.supabase.co \
  --update-secrets SUPABASE_SERVICE_ROLE_KEY=supabase-service-role-key:latest
```

After the next new source lands, check the logs for `COMPANY_FLAG_SET` / `COMPANY_FLAG_SKIPPED`.

### One-time setup for the source dataset ping

Do this once the `source-dataset-created` edge function (ENG-262) is live, deployed with `--no-verify-jwt`. Merging first is safe: until both variables are set, the listener logs `SOURCE_DATASET_PING_SKIPPED` after each dispatch.

```bash
# 1. Store the shared secret (the same value the edge function checks)
printf '%s' "$SOURCE_DATASET_SECRET" | gcloud secrets create source-dataset-secret --data-file=-

# 2. Let the Cloud Run service account read it ($SA as in the table-check setup above)
gcloud secrets add-iam-policy-binding source-dataset-secret \
  --member="serviceAccount:$SA" --role="roles/secretmanager.secretAccessor"

# 3. Add both variables to the service (kept across Cloud Build deploys; starts a new revision)
gcloud run services update orca-bigquery-dataset-listener --region us-central1 \
  --update-env-vars SUPABASE_SOURCE_DATASET_URL=https://sscfogzzbwlgpqcnnfjx.supabase.co/functions/v1/source-dataset-created \
  --update-secrets SOURCE_DATASET_SECRET=source-dataset-secret:latest
```

After the next new source lands, check the logs for `SOURCE_DATASET_PINGED`. Datasets that landed before then aren't backfilled.

### How to Deploy

**Automated (recommended):**
1. Push to `main` branch or merge PR
2. Cloud Build trigger automatically runs `cloudbuild.yaml`
3. Steps:
   - Builds Docker image tagged with `$SHORT_SHA`
   - Pushes to Artifact Registry (`us-central1-docker.pkg.dev/.../listeners/orca-bigquery-dataset-listener`)
   - Deploys to Cloud Run service `orca-bigquery-dataset-listener` in `us-central1`
   - Service requires authentication (`--no-allow-unauthenticated`)

**Manual:**
```bash
gcloud builds submit --config cloudbuild.yaml
```

### How to Roll Back

Cloud Run keeps previous revisions:

```bash
# List revisions
gcloud run revisions list --service=orca-bigquery-dataset-listener --region=us-central1

# Route 100% traffic to previous revision
gcloud run services update-traffic orca-bigquery-dataset-listener \
  --region=us-central1 \
  --to-revisions=PREVIOUS_REVISION_NAME=100
```

## Key Code References

**Dataset parsing logic:**
```javascript
// server.js lines 13-16
function splitDataset(datasetId) {
  const m = datasetId.match(/^([a-z0-9_]+)__([a-z0-9_]+)$/);
  return m ? { parent: m[1], client: m[2] } : null;
}
```

**GitHub dispatch payload structure:**
```javascript
// server.js lines 201-205
await dispatchToGitHub({
  datasetId,           // e.g., "shopify__acmecorp"
  files: plan.files,   // Array of {template: "path", path: "output/path"}
  vars: plan.vars,     // {datasetId, parent, client, project}
});
```

<!-- STATUS: needs-review -->
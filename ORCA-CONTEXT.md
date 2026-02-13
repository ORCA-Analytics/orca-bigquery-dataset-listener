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
4. Dispatches a GitHub repository event to `ORCA-Analytics/orca-dbt` with file creation instructions
5. The dbt repo workflow then generates client-specific SQL files from templates

**Think of it as:** _Infrastructure-as-code event handler that automates dbt model scaffolding when new client datasets appear in BigQuery._

## What This Repo Does

This Cloud Run service receives BigQuery audit log events (specifically dataset creation events) via Pub/Sub, extracts the dataset ID, matches it against a hardcoded catalog of known data source patterns (Shopify, Facebook Ads, Google Analytics, etc.), and triggers a GitHub repository dispatch to the `orca-dbt` repo. The dbt repo's GitHub Actions workflow then creates SQL model files from templates for the new client dataset. This eliminates manual file creation when onboarding new clients.

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Node.js 20 (CommonJS) |
| Framework | Express 4.x |
| Key dependencies | `express` (only runtime dependency) |
| Database(s) | None (stateless service) |
| Deploys to | Google Cloud Run (`us-central1`) |
| CI/CD | Google Cloud Build (docker build → push → deploy) |

## How to Run Locally

```bash
# Prerequisites
# - Node.js 20+ (matches Dockerfile base)
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

## Folder Structure

```
orca-bigquery-dataset-listener/
├── server.js              # All application logic (routes, catalog, dispatch)
├── package.json           # Minimal dependencies (express only)
├── Dockerfile             # Node 20 Alpine container
├── cloudbuild.yaml        # GCP deployment pipeline config
├── .dockerignore          # Build exclusions
└── .gitignore             # Git exclusions
```

### Key Entry Points

- **Main entry:** `server.js` (lines 1-217) - entire application in one file
- **Core logic:**
  - `splitDataset()` - parses dataset ID into `parent` and `client` parts
  - `buildPlan()` - matches dataset against catalog and generates file plan
  - `dispatchToGitHub()` - sends repository dispatch event
- **API routes:**
  - `GET /healthz` - health check (returns `200 ok`)
  - `POST /` - Pub/Sub push endpoint (receives base64-encoded audit log events)

## How It Relates to Other ORCA Services

```mermaid
flowchart LR
    BQDataset[BigQuery Dataset Created] --> AuditLog[GCP Audit Logs]
    AuditLog --> PubSub[Pub/Sub Topic]
    PubSub --> Listener[bq-dataset-listener Cloud Run]
    Listener --> GitHubAPI[GitHub API]
    GitHubAPI --> DBTRepo[orca-dbt repo workflow]
    DBTRepo --> NewFiles[Client-specific SQL models]
```

- **Reads from:** Pub/Sub push messages containing BigQuery audit log entries (`protoPayload.resourceName`)
- **Writes to:** GitHub API (`/repos/ORCA-Analytics/orca-dbt/dispatches` endpoint)
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
- **Catalog-driven logic:** `CATALOG` object maps parent dataset names to template entries
- **Custom output functions:** Some catalog entries use `out: (tpl, client) => filename` for non-standard naming
- **Graceful skips:** Returns HTTP 204 (no content) when dataset doesn't match catalog (not an error)
- **Error handling:** Try/catch on main handler, logs to stdout, returns 500 on dispatch failures

### Testing

- **Framework:** None (no tests currently)
- **Where tests live:** N/A
- **How to run:** N/A

## Gotchas

> **If you're an AI agent and you're about to change something in this repo, read this section first.**

### Things That Look Wrong but Are Intentional

- **Hardcoded constants in code:** `GH_OWNER`, `GH_REPO`, `PROJECT` are intentionally hardcoded (not env vars) because they're ORCA infrastructure constants
- **Global fetch:** Uses Node 18+ native `fetch` (no `node-fetch` import needed) - see commit `58092e6`
- **204 responses for non-matches:** Returning 204 instead of 404/400 when dataset doesn't match catalog is intentional (Pub/Sub retries on errors)

### Fragile Areas

- **CATALOG object accuracy:** If template paths or folder structures change in `orca-dbt`, this catalog becomes stale. No automated validation.
- **Dataset naming regex:** `splitDataset()` regex is strict - any variation (single underscore, triple underscore, uppercase) silently fails matching
- **Directory aliases:** Some parents use `dir` override (e.g., `fairing` uses `hdyhau_fairing` directory) - easy to miss when adding new entries
- **Custom output functions:** Entries with `out: (tpl, client) => ...` override standard naming - must be maintained manually
- **GitHub token scope:** Must have `repo` scope for private repo dispatch, will fail silently with 404 if token lacks permissions

### Known Tech Debt

- **Catalog is embedded in code:** Template catalog should ideally live in `orca-dbt` repo as source of truth (single source)
- **No validation on dispatch success:** GitHub dispatch returns 204 regardless of whether workflow exists/runs
- **No structured logging:** Uses `console.log` instead of structured logging (Cloud Logging compatibility could be better)
- **No retry logic:** If GitHub API is temporarily down, message is lost (Pub/Sub will retry the POST, but within deadline)
- **Missing syntax in catalog:** Line 98 missing comma after `amazon_sellercentral_cohorts` entry (see server.js L98)

### Environment-Specific Quirks

- **Local vs Cloud Run:** `PORT` defaults to 8080 locally, but Cloud Run injects its own port via env var
- **Token authentication:** Production uses Secret Manager injection, local dev requires manual export
- **No Pub/Sub emulator pattern:** Testing requires actual Pub/Sub or manual curl with base64-encoded payloads

## Deployment

### How to Deploy

**Automated (recommended):**
1. Push to `main` branch or merge PR
2. Cloud Build trigger automatically runs `cloudbuild.yaml`
3. Steps:
   - Builds Docker image tagged with `$SHORT_SHA`
   - Pushes to Artifact Registry (`us-central1-docker.pkg.dev/.../listeners/bq-dataset-listener`)
   - Deploys to Cloud Run service `bq-dataset-listener` in `us-central1`
   - Service requires authentication (`--no-allow-unauthenticated`)

**Manual:**
```bash
gcloud builds submit --config cloudbuild.yaml
```

### How to Roll Back

Cloud Run keeps previous revisions:

```bash
# List revisions
gcloud run revisions list --service=bq-dataset-listener --region=us-central1

# Route 100% traffic to previous revision
gcloud run services update-traffic bq-dataset-listener \
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
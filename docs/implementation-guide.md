# Implementation Guide

This guide is for engineers who need to run the ROAS Radar MVP locally, understand how the backend and dashboard fit together, and validate the ingestion, attribution, and reporting flow end to end.

## Short Onboarding Path

Use this sequence when you are new to the repository and want the shortest path to productive local work:

1. Read `docs/README.md` to see the current doc map and the operator-facing runbooks.
2. Skim this guide through `Local Setup` so you know which services to start and which env vars matter.
3. Read `docs/attribution-schema-v1.md` before changing capture fields, Shopify attribute keys, or normalization logic.
4. Read `docs/operational-attribution-contracts.md` before changing resolver precedence, Shopify writeback, retention, or recovery flows.
5. Read `docs/analytics-playbook.md` and `docs/reporting-metrics.md` before changing dashboard-facing metrics or attribution interpretation.

If you only need a starting point for one area:

- capture and ingestion: `src/modules/tracking/index.ts` plus `docs/attribution-schema-v1.md`
- Shopify ingestion and writeback: `src/modules/shopify/index.ts`, `src/modules/shopify/writeback.ts`, and `docs/operational-attribution-contracts.md`
- attribution resolution: `src/modules/attribution/index.ts`, `src/modules/attribution/resolver.ts`, and `docs/last-non-direct-touch-approval-matrix.md`
- dashboard and reporting: `src/modules/reporting/index.ts`, `dashboard/`, `docs/analytics-playbook.md`, and `docs/reporting-metrics.md`

## What Runs In The MVP

The repository currently contains one Node.js backend, one React dashboard, and several worker entrypoints:

- API service: Express app in `src/app.ts`, started by `src/server.ts`
- Attribution worker: batch processor in `src/worker.ts`
- Attribution worker service: long-running worker with health endpoints in `src/worker-service.ts`
- Meta Ads sync worker: `src/meta-ads-worker.ts`
- Google Ads sync worker: `src/google-ads-worker.ts`
- Data quality worker: `src/data-quality-worker.ts`
- React dashboard: Vite app under `dashboard/`
- PostgreSQL schema and migrations: `db/migrations/*.sql`

For the core Shopify attribution MVP, you need PostgreSQL, the API service, and the attribution worker. The ads sync and data quality workers are optional for local engineering work unless you are validating those specific integrations.

## Service Topology

### Backend API

`src/app.ts` wires the API into these route groups:

- `GET /healthz`: liveness check
- `GET /readyz`: database readiness check
- `POST /track`: public tracking ingestion
- `POST /webhooks/shopify/orders-create`: Shopify order-created webhook
- `POST /webhooks/shopify/orders-paid`: Shopify order-paid webhook
- `POST /webhooks/shopify/app-uninstalled`: Shopify uninstall webhook
- `GET /api/reporting/summary`: authenticated summary metrics
- `GET /api/reporting/campaigns`: authenticated campaign table
- `GET /api/reporting/timeseries`: authenticated chart data
- `GET /api/reporting/orders`: authenticated order-level attribution view
- `GET /api/reporting/reconciliation`: authenticated data quality report
- `GET /shopify/install`: Shopify app install entrypoint
- `GET /shopify/oauth/callback`: Shopify OAuth callback
- `GET /api/admin/shopify/connection`: installation status
- `POST /api/admin/shopify/webhooks/sync`: webhook re-provisioning
- `GET /meta-ads/oauth/callback`: Meta OAuth callback
- `GET /api/admin/meta-ads/oauth/start`: Meta OAuth start
- `GET /api/admin/meta-ads/status`: Meta connection status
- `POST /api/admin/meta-ads/sync`: Meta sync queue enqueue
- `POST /api/admin/meta-ads/refresh-token`: Meta token refresh
- `POST /api/admin/google-ads/connections`: Google Ads connection creation
- `GET /api/admin/google-ads/status`: Google Ads connection status
- `POST /api/admin/google-ads/sync`: Google Ads sync queue enqueue
- `POST /api/admin/google-ads/reconcile`: Google Ads reconciliation enqueue

### Attribution worker

The attribution logic lives in:

- `src/modules/attribution/index.ts`: queueing, journey resolution, persistence, aggregate refresh
- `src/modules/attribution/engine.ts`: multi-model credit allocation

The worker claims jobs from `attribution_jobs`, resolves the winning journey, persists `attribution_results` plus `attribution_order_credits`, and refreshes reporting aggregates. The matching order is:

1. `landing_session_id`
2. `checkout_token`
3. `cart_token`
4. stitched customer identity / email fallback
5. unattributed fallback

The operational contract for resolver precedence, Shopify writeback, reconciliation, and retention is documented in `docs/operational-attribution-contracts.md`.

### React dashboard

The dashboard in `dashboard/src/App.tsx` calls the reporting API only. It does not write directly to the database and does not own attribution logic. API configuration is read from:

- `VITE_API_BASE_URL`
- `VITE_REPORTING_API_TOKEN`
- `VITE_REPORTING_TENANT_ID`

`dashboard/src/lib/api.ts` defaults the API base URL to `http://localhost:3000`, so set `VITE_API_BASE_URL` if your local API runs on a different port.

### Cloud Run shape

The current deployment docs in `infra/cloud-run/README.md` assume:

- `roas-radar-api`: public service
- `roas-radar-attribution-worker`: internal worker service
- `roas-radar-migrate`: migration job
- `roas-radar-meta-ads-sync`: scheduled spend sync job
- `roas-radar-google-ads-sync`: scheduled spend sync job

That topology matches the current Node.js codebase and should remain the reference when preparing Cloud Run manifests or deployment scripts.

## Raw Sync Audit Storage

Meta Ads and Google Ads sync workers now persist per-call audit rows in `ad_sync_api_transactions`.

- `request_payload` stores the outbound sync request payload when the platform call has a body, or the structured request query payload for Meta sync reads
- `response_payload` stores the decoded platform response before any normalization or row projection
- `transaction_source` and `source_metadata` identify which sync step produced the record
- `request_started_at` and `response_received_at` provide the transport timing for each audited platform call

This table is an audit surface for sync transactions. The existing raw spend tables remain the canonical row-level source-payload store for spend records.

## Raw Payload Persistence Rule

For ingestion surfaces that expose `raw_payload` or equivalent raw-source JSONB columns, ROAS Radar persists the decoded-and-parsed upstream payload exactly as received before any trimming, lowercasing, schema projection, or derived-field injection.

- tracking request bodies are cloned before normalization so `tracking_events.raw_payload` and `session_attribution_touch_events.raw_payload` retain the exact inbound browser or capture payload
- Shopify order rows persist the original order object, and Shopify line-item rows persist the original line-item node rather than a schema-reduced subset
- Meta Ads and Google Ads raw spend tables persist the decoded API row exactly; normalized daily spend tables remain derived projections

Normalization, URL cleanup, consent coercion, idempotency fingerprints, and log redaction still apply to typed columns, hashes, and logs. They must not mutate the JSON written to raw-source storage.

When you need to look raw rows up later, use the source-specific metadata columns first instead of querying inside JSONB:

- Shopify raw tables: `payload_source`, `payload_external_id`, and `payload_received_at` or `received_at`
- Meta Ads raw tables: `raw_account_source` or `payload_source`, paired with `raw_account_external_id` or `payload_external_id`
- Google Ads raw tables: `raw_customer_source` or `payload_source`, paired with `raw_customer_external_id` or `payload_external_id`

Those lookup columns are intentionally narrow so staging and production do not need broad JSONB GIN indexes on high-write ingestion tables.

## Environment Configuration

All runtime configuration is validated in `src/config/env.ts`. `DATABASE_URL` is the only hard requirement for process startup. Most other values have defaults, but several are functionally required if you want non-local integrations to work.

### Required For Local Backend Startup

- `DATABASE_URL`: PostgreSQL connection string

### Required For Authenticated Reporting

- `REPORTING_API_TOKEN`

Default is `dev-reporting-token`, but set it explicitly in local shells so your API, tests, and dashboard all agree.

### Tracking Ingestion Controls

- `PORT`
- `TRACKING_ALLOWED_ORIGINS`
- `TRACKING_MAX_EVENT_AGE_HOURS`
- `TRACKING_MAX_FUTURE_SKEW_SECONDS`
- `TRACKING_RATE_LIMIT_MAX`
- `TRACKING_RATE_LIMIT_WINDOW_MS`

For local testing, set `TRACKING_ALLOWED_ORIGINS=http://localhost:5173,https://store.example.com` so browser-based dashboard or synthetic requests are not rejected on origin checks.

### Tracking Consent Policy

Tracking capture is opt-out-compatible for attribution storage. UTM parameters, click IDs, landing URL, referrer URL, page URL, and `roas_radar_session_id` are persisted even when the client reports consent opt-out.

The governing rule is:

- capture storage is not suppressed by consent state
- every tracking event and mirrored attribution touch stores a `consent_state` of `granted`, `denied`, or `unknown`
- downstream reporting, governance, or activation logic must filter on `consent_state` instead of assuming missing attribution data means opt-out
- request-context fallback captures default `consent_state` to `unknown` when no explicit browser consent signal is available

### Database Pooling

- `DATABASE_POOL_MAX`
- `DATABASE_POOL_MIN`
- `DATABASE_IDLE_TIMEOUT_MS`
- `DATABASE_CONNECTION_TIMEOUT_MS`
- `DATABASE_STATEMENT_TIMEOUT_MS`
- `DATABASE_QUERY_TIMEOUT_MS`
- `DATABASE_MAX_USES`
- `DATABASE_SSL`

### Attribution Worker

- `ATTRIBUTION_JOB_BATCH_SIZE`
- `ATTRIBUTION_STALE_SCAN_BATCH_SIZE`
- `ATTRIBUTION_WORKER_LOOP`
- `ATTRIBUTION_WORKER_POLL_INTERVAL_MS`

Defaults are tuned for MVP local development: 25 jobs per batch, 10 second polling, and a 15 minute stale-processing threshold in `src/modules/attribution/index.ts`.

### Shopify Integration

- `SHOPIFY_WEBHOOK_SECRET`
- `SHOPIFY_APP_API_KEY`
- `SHOPIFY_APP_API_SECRET`
- `SHOPIFY_APP_API_VERSION`
- `SHOPIFY_APP_BASE_URL`
- `SHOPIFY_APP_ENCRYPTION_KEY`
- `SHOPIFY_APP_POST_INSTALL_REDIRECT_URL`
- `SHOPIFY_APP_SCOPES`

These are required only if you are running the Shopify OAuth install flow or validating real Shopify webhooks. For local synthetic webhook testing, the integration tests use `SHOPIFY_WEBHOOK_SECRET=test-webhook-secret`.

### Meta Ads Integration

- `META_ADS_APP_ID`
- `META_ADS_APP_SECRET`
- `META_ADS_APP_BASE_URL`
- `META_ADS_APP_SCOPES`
- `META_ADS_AD_ACCOUNT_ID`
- `META_ADS_API_VERSION`
- `META_ADS_ENCRYPTION_KEY`
- `META_ADS_SYNC_BATCH_SIZE`
- `META_ADS_SYNC_INITIAL_LOOKBACK_DAYS`
- `META_ADS_SYNC_LOOKBACK_DAYS`
- `META_ADS_SYNC_MAX_RETRIES`
- `META_ADS_TOKEN_REFRESH_LEEWAY_HOURS`
- `META_ADS_WORKER_LOOP`
- `META_ADS_WORKER_POLL_INTERVAL_MS`

### Google Ads Integration

- `GOOGLE_ADS_API_VERSION`
- `GOOGLE_ADS_ENCRYPTION_KEY`
- `GOOGLE_ADS_SYNC_BATCH_SIZE`
- `GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS`
- `GOOGLE_ADS_SYNC_LOOKBACK_DAYS`
- `GOOGLE_ADS_SYNC_MAX_RETRIES`
- `GOOGLE_ADS_WORKER_LOOP`
- `GOOGLE_ADS_WORKER_POLL_INTERVAL_MS`

### Data Quality Checks

- `DATA_QUALITY_TARGET_LAG_DAYS`
- `DATA_QUALITY_ANOMALY_LOOKBACK_DAYS`
- `DATA_QUALITY_ANOMALY_THRESHOLD_RATIO`
- `DATA_QUALITY_ANOMALY_MIN_BASELINE`
- `DATA_QUALITY_CHECK_LOOP`
- `DATA_QUALITY_CHECK_INTERVAL_MS`

## Local Setup

### Prerequisites

- Node.js 22 or newer
- npm
- PostgreSQL

The backend `package.json` declares `node >=22`. The dashboard is a separate npm project in `dashboard/`.

### 1. Install dependencies

From the repository root:

```bash
npm install
npm --prefix dashboard install
```

### 2. Create a local database

Create a PostgreSQL database, for example `roas_radar`, and point `DATABASE_URL` at it.

Example:

```bash
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/roas_radar
export REPORTING_API_TOKEN=dev-reporting-token
export TRACKING_ALLOWED_ORIGINS=http://localhost:5173,https://store.example.com
```

If you want Shopify webhook tests to behave like the integration harness, also set:

```bash
export SHOPIFY_WEBHOOK_SECRET=test-webhook-secret
export SHOPIFY_APP_API_SECRET=test-app-secret
```

### 3. Run migrations

```bash
npm run db:migrate
```

The migration runner in `src/db/migrate.ts` acquires a PostgreSQL advisory lock, creates `schema_migrations` if needed, and applies pending SQL files from `db/migrations/` in lexical order.

### 4. Start the API

```bash
npm run dev
```

This starts `tsx watch src/server.ts`. By default the API listens on port `8080` unless `PORT` is set.

### 5. Start the attribution worker

In a second shell:

```bash
npm run build
npm run start:worker
```

For a local always-on worker with health endpoints instead of the bare daemon, use:

```bash
npm run build
PORT=8081 npm run start:worker-service
```

Use the worker service variant when you want `/healthz` and `/readyz` on the worker process itself.

### 6. Start the dashboard

In a third shell:

```bash
export VITE_API_BASE_URL=http://localhost:8080
export VITE_REPORTING_API_TOKEN=dev-reporting-token
npm --prefix dashboard run dev
```

Vite will print the local dashboard URL, typically `http://localhost:5173`.

## Where Core Logic Lives

Use these files as the source of truth when debugging or extending the MVP:

- Tracking ingestion: `src/modules/tracking/index.ts`
- Shopify ingestion and OAuth: `src/modules/shopify/index.ts`
- Attribution queue and journey resolution: `src/modules/attribution/index.ts`
- Attribution model math: `src/modules/attribution/engine.ts`
- Reporting API: `src/modules/reporting/index.ts`
- Reporting aggregate refresh: `src/modules/reporting/aggregates.ts`
- Identity stitching: `src/modules/identity/index.ts`
- Data quality checks: `src/modules/data-quality/index.ts`
- API wiring: `src/app.ts`
- Process entrypoints: `src/server.ts`, `src/worker.ts`, `src/worker-service.ts`

## Route Overview By Module

### Tracking

`src/modules/tracking/index.ts`

- `POST /track`

Behavior:

- validates event type, timestamp, UUID session id, and URL fields
- enforces origin restrictions and rate limiting
- deduplicates by `clientEventId` and by a computed ingestion fingerprint
- upserts `tracking_sessions`
- inserts into `tracking_events`
- enqueues attribution refreshes when new checkout/cart evidence appears
- refreshes affected reporting aggregates

Accepted MVP event types:

- `page_view`
- `product_view`
- `add_to_cart`
- `checkout_started`

### Shopify

`src/modules/shopify/index.ts`

- `GET /shopify/install`
- `GET /shopify/oauth/callback`
- `POST /webhooks/shopify/orders-create`
- `POST /webhooks/shopify/orders-paid`
- `POST /webhooks/shopify/app-uninstalled`
- `GET /api/admin/shopify/connection`
- `POST /api/admin/shopify/webhooks/sync`

Behavior:

- verifies Shopify HMAC
- records webhook receipts for idempotency
- normalizes orders and line items
- upserts customers
- stitches customer identity when possible
- enqueues attribution work instead of resolving attribution inline

### Reporting

`src/modules/reporting/index.ts`

- `GET /api/reporting/summary`
- `GET /api/reporting/campaigns`
- `GET /api/reporting/timeseries`
- `GET /api/reporting/orders`
- `GET /api/reporting/reconciliation`

These routes require `Authorization: Bearer <REPORTING_API_TOKEN>`.

The reporting layer reads from `daily_reporting_metrics` and attribution tables rather than rebuilding aggregates in-request. It supports the attribution models exported by `src/modules/attribution/engine.ts`:

- `first_touch`
- `last_touch`
- `linear`
- `time_decay`
- `position_based`
- `rule_based_weighted`

### Optional admin integrations

- Meta Ads: `src/modules/meta-ads/index.ts`
- Google Ads: `src/modules/google-ads/index.ts`

These routes are useful for integration work but are not required to validate the Shopify attribution MVP.

## Worker Responsibilities

### Attribution worker

Start with `npm run start:worker` or `npm run start:worker-service`.

Responsibilities:

- claim pending or retryable jobs from `attribution_jobs`
- requeue stale `processing` jobs older than 15 minutes
- resolve journey evidence from sessions, events, and identity links
- compute credit outputs across all attribution models
- persist `attribution_results` and `attribution_order_credits`
- refresh `daily_reporting_metrics`
- emit backlog and outcome logs for monitoring

### Meta Ads worker

Start with `npm run meta-ads:sync`.

Responsibilities:

- process Meta spend sync jobs
- refresh near-expiry tokens
- backfill and maintain rolling spend windows

### Google Ads worker

Start with `npm run google-ads:sync`.

Responsibilities:

- process Google Ads spend sync jobs
- fetch spend and write normalized daily spend data
- requeue missing dates through reconciliation

### Data quality worker

Start with `npm run data-quality:check`.

Responsibilities:

- evaluate lagged reporting dates
- compare current metric totals against trailing baselines
- persist anomaly results into `data_quality_check_runs`

## Engineer Validation Flow

The fastest reliable way to validate the MVP is to use the existing integration harness and tests before doing any manual probing.

### 1. Run the full automated test suite

```bash
npm test
```

`scripts/run-tests.mjs` will:

- run migrations before integration tests
- execute unit tests under `test/*.test.ts`
- execute integration tests:
  - `test/attribution-e2e.integration.test.ts`
  - `test/reporting-api.integration.test.ts`

Use these narrower commands when iterating:

```bash
npm run test:unit
npm run test:integration
```

### 2. Understand what the synthetic harness validates

`test/e2e-harness.ts` seeds two deterministic journeys:

- a multi-touch order with Google, direct, and Meta touchpoints
- a checkout-token-matched order

The harness:

- resets the relevant tables
- sends synthetic `/track` events
- sends a signed Shopify `orders/create` webhook
- links sessions to the stitched customer identity when needed
- runs `processAttributionQueue`
- fetches reporting and persisted attribution outputs for assertions

This makes it the best reference for how the system is expected to behave from ingestion through reporting.

### 3. Validate attribution behavior directly

Run:

```bash
npm run test:integration
```

`test/attribution-e2e.integration.test.ts` proves that:

- jobs are created and processed end to end
- attribution credits are persisted deterministically
- every supported attribution model returns the expected revenue split
- first-touch, last-touch, and rule-based weighted outputs differ in the expected way

Expected evidence:

- the multi-touch synthetic order has three credited touchpoints
- `first_touch` assigns all revenue to the first Google touch
- `last_touch` assigns all revenue to the last Meta touch
- `rule_based_weighted` splits revenue across Google, direct, and Meta, with Meta marked primary
- the checkout-token order attributes to the checkout-started session

### 4. Validate reporting responses against seeded data

`test/reporting-api.integration.test.ts` validates the reporting endpoints directly against persisted aggregates.

Expected evidence:

- `GET /api/reporting/summary` returns visits, orders, revenue, and conversion rate from `daily_reporting_metrics`
- filtering by `source`, `campaign`, and `attributionModel` works
- reporting output changes correctly when the attribution model changes

The seeded journey assertions in `test/attribution-e2e.integration.test.ts` also validate:

- `/api/reporting/campaigns`
- `/api/reporting/timeseries`
- `/api/reporting/orders`

### 5. Optional manual API smoke test

After starting the API and worker locally, post a synthetic event:

```bash
curl -X POST http://localhost:8080/track \
  -H 'content-type: application/json' \
  -H 'origin: https://store.example.com' \
  --data '{
    "eventType":"page_view",
    "occurredAt":"2026-04-10T12:00:00.000Z",
    "sessionId":"8f5c0b53-c812-4a59-a7e6-8df0b0c7a1f1",
    "pageUrl":"https://store.example.com/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gclid=abc123",
    "referrerUrl":"https://www.google.com/",
    "shopifyCartToken":null,
    "shopifyCheckoutToken":null,
    "clientEventId":"evt_manual_smoke_1",
    "context":{
      "userAgent":"manual-smoke-test",
      "screen":"1440x900",
      "language":"en-US"
    }
  }'
```

Expected result:

- `200 OK`
- a JSON response with `ok: true`, the echoed `sessionId`, an `eventId`, and `ingestedAt`

Then confirm readiness:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

And fetch reporting data:

```bash
curl 'http://localhost:8080/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-12&attributionModel=last_touch' \
  -H 'Authorization: Bearer dev-reporting-token'
```

### 6. Optional dashboard validation

With the API and dashboard running:

1. Open the dashboard in the browser.
2. Confirm summary cards load without auth errors.
3. Change attribution models and date ranges.
4. Confirm campaigns, chart, and order list refresh against the same API base URL.

If the dashboard fails immediately, check `dashboard/src/lib/api.ts` first for missing `VITE_API_BASE_URL` or `VITE_REPORTING_API_TOKEN`.

## Troubleshooting

Use this section as the first stop for local setup problems. If the symptom looks like a production contract issue rather than a local shell issue, jump directly to the linked runbook or contract doc instead of debugging from source first.

### Common local issues

- `401 Unauthorized` on reporting routes:
  `REPORTING_API_TOKEN` in the API process does not match the dashboard or curl token.
- `/track` rejected with origin errors:
  `TRACKING_ALLOWED_ORIGINS` does not include the request origin.
- Shopify webhook rejected:
  `SHOPIFY_WEBHOOK_SECRET` does not match the signature used by the sender.
- Integration tests failing before assertions:
  local PostgreSQL is unavailable or `DATABASE_URL` points at the wrong database.
- Dashboard loads but shows fetch errors:
  `VITE_API_BASE_URL` still points at the wrong local port.

### Runbooks and supporting docs

Use these docs when local symptoms match production behavior:

- Attribution completeness and missing session capture: `docs/runbooks/attribution-completeness.md`
- Ingestion failures: `docs/runbooks/ingestion-failures.md`
- Attribution backlog: `docs/runbooks/attribution-worker-backlog.md`
- API latency: `docs/runbooks/api-latency.md`
- Database operations: `docs/database-operations.md`
- Operational attribution contracts: `docs/operational-attribution-contracts.md`
- Attribution Schema V1 reference: `docs/attribution-schema-v1.md`
- Shopify app setup: `docs/shopify-app-setup.md`
- Visitor identity stitching: `docs/visitor-identity-stitching.md`
- Reporting metrics definitions: `docs/reporting-metrics.md`
- Marketing dimensions: `docs/marketing-dimensions.md`

### Dashboard interpretation quick links

When the local dashboard is loading but the numbers look wrong, use this map before tracing requests by hand:

- card and KPI formulas: `docs/reporting-metrics.md`
- order-to-session matching semantics: `docs/analytics-playbook.md`
- field naming, null handling, and canonical Shopify keys: `docs/attribution-schema-v1.md`
- writeback, reconciliation, retention, and dead-letter behavior: `docs/operational-attribution-contracts.md`

## Recommended Engineer Workflow

When making changes to tracking, Shopify ingestion, attribution, or reporting:

1. Run `npm run test:integration`.
2. Run any directly related unit tests or `npm test`.
3. Smoke-test `/healthz`, `/readyz`, and the affected API route locally.
4. If the change affects dashboard behavior, run `npm --prefix dashboard run dev` and verify the corresponding view.
5. If the change affects Cloud Run deployment shape or secrets, re-read `infra/cloud-run/README.md` before merging.

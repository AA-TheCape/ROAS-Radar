# ROAS Radar

ROAS Radar is a Shopify attribution and reporting product built around a Node.js API, PostgreSQL, and a React dashboard.

## Quick Start

```bash
npm install
npm --prefix dashboard install

export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/roas_radar
export REPORTING_API_TOKEN=dev-reporting-token
export TRACKING_ALLOWED_ORIGINS=http://localhost:5173,https://store.example.com

npm run db:migrate
npm run dev
```

Run the attribution worker in a second shell:

```bash
npm run build
npm run start:worker
```

Run the React dashboard in a third shell:

```bash
export VITE_API_BASE_URL=http://localhost:8080
export VITE_REPORTING_API_TOKEN=dev-reporting-token
npm --prefix dashboard run dev
```

The authoritative environment contract lives in `src/config/env.ts`.

## Documentation

- Engineer setup and validation: [docs/implementation-guide.md](docs/implementation-guide.md)
- Identity stitching: [docs/visitor-identity-stitching.md](docs/visitor-identity-stitching.md)
- Shopify app setup: [docs/shopify-app-setup.md](docs/shopify-app-setup.md)
- Reporting metrics: [docs/reporting-metrics.md](docs/reporting-metrics.md)
- Marketing dimensions: [docs/marketing-dimensions.md](docs/marketing-dimensions.md)
- Database operations: [docs/database-operations.md](docs/database-operations.md)
- Runbook: ingestion failures: [docs/runbooks/ingestion-failures.md](docs/runbooks/ingestion-failures.md)
- Runbook: attribution backlog: [docs/runbooks/attribution-worker-backlog.md](docs/runbooks/attribution-worker-backlog.md)
- Runbook: API latency: [docs/runbooks/api-latency.md](docs/runbooks/api-latency.md)

## Existing Shopify storefront assets

The Shopify theme snippet and assets under `shopify/theme/` are the expected frontend mechanism for:

- creating and persisting the first-party `_hba_id` tracking cookie for 365 days,
- sending a `page_view` payload to `/track` on page load with `sendBeacon` plus fetch/XMLHttpRequest fallback and retry queueing,
- propagating the same UUID into `roas_radar_session_id` cart and checkout attributes for order attribution.

## Identity Stitching

Visitor identity stitching is documented in [docs/visitor-identity-stitching.md](docs/visitor-identity-stitching.md).

The backend now creates canonical customer identities from hashed email plus Shopify customer id, links historical tracked sessions when checkout evidence is present, and refuses automatic merges when those identifiers disagree with already-linked records.

## Shopify OAuth Setup

Shopify OAuth installation, encrypted Admin API credential storage, and automatic webhook provisioning are documented in [docs/shopify-app-setup.md](docs/shopify-app-setup.md).

## Meta Ads Spend Sync

The backend now includes a Meta Ads OAuth connection flow plus a retryable daily spend sync worker:

- `GET /api/admin/meta-ads/oauth/start` creates a short-lived OAuth state and returns the Meta authorization URL.
- `GET /meta-ads/oauth/callback` exchanges the authorization code for a long-lived token, stores it encrypted in PostgreSQL, and records account metadata.
- `POST /api/admin/meta-ads/sync` can enqueue a manual backfill date range.
- `npm run meta-ads:sync` processes the queue, refreshes tokens when nearing expiry, fetches daily spend at account/campaign/adset/ad level, enriches ad rows with creative metadata, and writes both raw and normalized spend tables.

In production, run `npm run meta-ads:sync` from a Cloud Run Job or scheduled worker once per day. The worker will automatically enqueue the configured rolling lookback window and retry API failures with exponential backoff.

## Google Ads Spend Sync

The backend now includes a Google Ads spend ingestion worker with encrypted credential storage and reconciliation:

- `POST /api/admin/google-ads/connections` validates a customer against the Google Ads API and stores the developer token, client secret, and refresh token encrypted in PostgreSQL.
- `POST /api/admin/google-ads/sync` can enqueue a manual backfill date range.
- `POST /api/admin/google-ads/reconcile` checks the rolling sync window for missing completed dates and re-enqueues gaps.
- `npm run google-ads:sync` processes the queue, exchanges the refresh token for a short-lived access token, fetches daily spend from Google Ads at campaign and ad level, maps Google ad groups into the Meta-aligned `adset_*` fields, and writes both raw and normalized daily spend tables.

In production, run `npm run google-ads:sync` from a Cloud Run Job or scheduled worker once per day. The worker keeps a rolling lookback window warm and writes reconciliation records so missing dates are visible and automatically requeued.

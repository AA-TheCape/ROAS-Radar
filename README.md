# ROAS Radar

ROAS Radar is a Node.js backend plus React dashboard for Shopify attribution, reporting, GA4 fallback ingestion, and ad-platform spend sync.

## Runtime baseline

- Backend Node version: `22.13.0+` (`.nvmrc` and `.node-version` pin the local baseline)
- Backend install command: `npm ci --include=dev`
- Dashboard install command: `npm ci --include=dev --prefix dashboard`
- Production backend image: root [`Dockerfile`](Dockerfile)
- Cloud Run runtime entrypoints:
  - API service: `npm run start:api`
  - Attribution worker service: `npm run start:worker-service`
  - Migration job: `npm run db:migrate`
  - GA4 ingestion job: `npm run ga4:ingest:start`

## Required backend verification order

Run the supported full verification path from a clean checkout before merge or deploy. This path requires Node `22.13.0+` and a PostgreSQL database reachable through `DATABASE_URL`.

```bash
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/roas_radar
export REPORTING_API_TOKEN=dev-reporting-token
npm run verify:full
```

`npm run verify:full` runs backend install, backend build/lint, dashboard install/build/lint, `db:migrate:check`, DB-backed integration tests, and the attribution critical suite. The attribution suite runs migrations before exercising persistence paths, including GA4 fallback campaign metadata handling through the attribution candidate and integration coverage.

Optional release smoke checks after `npm run build`:

```bash
npm run start:api
npm run ga4:ingest:start
docker build -t roas-radar .
```

## Docs map

- Engineer setup and local validation: [docs/implementation-guide.md](docs/implementation-guide.md)
- Cloud Run deployment contract: [infra/cloud-run/README.md](infra/cloud-run/README.md)
- Cloud Run deploy and rollback runbook: [docs/runbooks/cloud-run-pipelines.md](docs/runbooks/cloud-run-pipelines.md)
- Production manual backfill and recovery runbook: [docs/runbooks/production-manual-backfill-recovery.md](docs/runbooks/production-manual-backfill-recovery.md)
- GA4 hourly ingestion operations: [docs/runbooks/ga4-hourly-ingestion.md](docs/runbooks/ga4-hourly-ingestion.md)
- Full docs index: [docs/README.md](docs/README.md)

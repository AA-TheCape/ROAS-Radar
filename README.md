# ROAS Radar

ROAS Radar is a Node.js backend plus React dashboard for Shopify attribution, reporting, GA4 fallback ingestion, and ad-platform spend sync.

## Runtime baseline

- Backend Node version: `22.x`
- Backend install command: `npm ci --include=dev`
- Dashboard install command: `npm ci --include=dev --prefix dashboard`
- Production backend image: root [`Dockerfile`](Dockerfile)
- Cloud Run runtime entrypoints:
  - API service: `npm run start:api`
  - Attribution worker service: `npm run start:worker-service`
  - Migration job: `npm run db:migrate`
  - GA4 ingestion job: `npm run ga4:ingest:start`

## Required backend verification order

Run these from a clean checkout before merge or deploy:

```bash
npm ci --include=dev
npm run build
npm run start:api
npm run ga4:ingest:start
npm run db:migrate:check
npm run test:unit
npm run test:integration
npm run test:attribution
docker build -t roas-radar .
```

`npm run start:api` and `npm run ga4:ingest:start` are runtime smoke steps against the compiled `dist/` output, so run them after `npm run build`.

## Docs map

- Engineer setup and local validation: [docs/implementation-guide.md](docs/implementation-guide.md)
- Cloud Run deployment contract: [infra/cloud-run/README.md](infra/cloud-run/README.md)
- Cloud Run deploy and rollback runbook: [docs/runbooks/cloud-run-pipelines.md](docs/runbooks/cloud-run-pipelines.md)
- GA4 hourly ingestion operations: [docs/runbooks/ga4-hourly-ingestion.md](docs/runbooks/ga4-hourly-ingestion.md)
- Full docs index: [docs/README.md](docs/README.md)

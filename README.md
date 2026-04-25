## Session Attribution Retention

- `npm run session-attribution:retention` deletes expired session capture rows in batches
- default cutoff is 30 days via `retained_until`
- rows tied to `order_attribution_links` are preserved
- production deploys this as a scheduled Cloud Run Job

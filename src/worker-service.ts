New Cloud Run worker entrypoint that:
- exposes `/healthz` and `/readyz`
- continuously drains the attribution queue
- tracks last run/error state
- supports graceful shutdown

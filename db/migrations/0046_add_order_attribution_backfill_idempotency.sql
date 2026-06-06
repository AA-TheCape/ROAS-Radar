BEGIN;

ALTER TABLE order_attribution_backfill_runs
  ADD COLUMN IF NOT EXISTS idempotency_key text;

CREATE UNIQUE INDEX IF NOT EXISTS order_attribution_backfill_runs_idempotency_key_uidx
  ON order_attribution_backfill_runs (idempotency_key)
  WHERE idempotency_key IS NOT NULL;

COMMIT;

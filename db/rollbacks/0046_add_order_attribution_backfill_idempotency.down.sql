BEGIN;

DROP INDEX IF EXISTS order_attribution_backfill_runs_idempotency_key_uidx;

ALTER TABLE order_attribution_backfill_runs
  DROP COLUMN IF EXISTS idempotency_key;

COMMIT;

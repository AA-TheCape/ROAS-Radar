BEGIN;

ALTER TABLE attribution_results
  ADD COLUMN IF NOT EXISTS match_source text,
  ADD COLUMN IF NOT EXISTS confidence_label text;

ALTER TABLE attribution_order_credits
  ADD COLUMN IF NOT EXISTS match_source text,
  ADD COLUMN IF NOT EXISTS confidence_label text;

COMMIT;

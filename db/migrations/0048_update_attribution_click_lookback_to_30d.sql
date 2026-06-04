DO $$
DECLARE
  constraint_name text;
BEGIN
  SELECT c.conname
  INTO constraint_name
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  WHERE t.relname = 'attribution_model_summaries'
    AND c.contype = 'c'
    AND pg_get_constraintdef(c.oid) LIKE '%lookback_rule_applied%'
  LIMIT 1;

  IF constraint_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE attribution_model_summaries DROP CONSTRAINT %I', constraint_name);
  END IF;
END $$;

UPDATE attribution_model_summaries
SET lookback_rule_applied = '30d_click'
WHERE lookback_rule_applied = '28d_click';

ALTER TABLE attribution_model_summaries
  ADD CONSTRAINT attribution_model_summaries_lookback_rule_chk
  CHECK (lookback_rule_applied IN ('30d_click', '7d_view', 'mixed'));

BEGIN;

ALTER TABLE deterministic_model_outputs
  DROP CONSTRAINT IF EXISTS deterministic_model_outputs_model_event_type_chk,
  DROP CONSTRAINT IF EXISTS deterministic_model_outputs_model_key_chk,
  ADD CONSTRAINT deterministic_model_outputs_model_key_chk
  CHECK (model_key IN (
    'first_touch',
    'last_touch',
    'last_non_direct',
    'linear',
    'clicks_only',
    'hinted_fallback_only',
    'deterministic_views',
    'deterministic_impressions'
  ));

COMMIT;

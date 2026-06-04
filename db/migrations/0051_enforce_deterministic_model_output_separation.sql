BEGIN;

DELETE FROM deterministic_model_outputs
WHERE model_key NOT IN ('deterministic_views', 'deterministic_impressions')
  OR (model_key = 'deterministic_views' AND event_type <> 'view')
  OR (model_key = 'deterministic_impressions' AND event_type <> 'impression');

ALTER TABLE deterministic_model_outputs
  DROP CONSTRAINT IF EXISTS deterministic_model_outputs_model_event_type_chk,
  DROP CONSTRAINT IF EXISTS deterministic_model_outputs_model_key_chk,
  ADD CONSTRAINT deterministic_model_outputs_model_key_chk
  CHECK (model_key IN ('deterministic_views', 'deterministic_impressions')),
  ADD CONSTRAINT deterministic_model_outputs_model_event_type_chk
  CHECK (
    (model_key = 'deterministic_views' AND event_type = 'view')
    OR (model_key = 'deterministic_impressions' AND event_type = 'impression')
  );

COMMIT;

BEGIN;

ALTER TABLE attribution_raw_evidence
  ALTER COLUMN retained_until SET DEFAULT (now() + interval '180 days');

COMMIT;

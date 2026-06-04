BEGIN;

ALTER TABLE attribution_raw_evidence
  ALTER COLUMN retained_until SET DEFAULT (now() + interval '30 days');

COMMIT;

BEGIN;

DROP TABLE IF EXISTS recovery_job_status_events;
DROP TABLE IF EXISTS recovery_job_errors;
DROP TABLE IF EXISTS recovery_job_checkpoints;
DROP TABLE IF EXISTS recovery_job_records;
DROP TABLE IF EXISTS recovery_job_runs;

COMMIT;

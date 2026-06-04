# MMM Pipelines Runbook

Use this runbook for scheduled MMM mart refresh, baseline training, calibration drift, and measurement-readiness incidents.

## Triggers

- `ROAS Radar * MMM Baseline Failures`
- `ROAS Radar * MMM Baseline Drift`
- `ROAS Radar * Data Quality Alerts` for `mmm_readiness_*` checks
- dashboard shows missing weekly MMM runs, stale readiness checks, or high deterministic-vs-MMM divergence

## Immediate Checks

1. Confirm the scheduler state:
   `sh infra/cloud-run/scheduler.sh <environment> mmm-baseline status`
2. Inspect the latest Cloud Run execution for `roas-radar-mmm-baseline-<environment>`.
3. Filter Cloud Logging on `jsonPayload.event="mmm_baseline_job_lifecycle"` and compare `stage`, `modelRunId`, `governanceStatus`, `divergenceAlertCount`, `maxDivergenceRate`, and `error.message`.
4. Check the latest data-quality run for `mmm_readiness_*` rows in `data_quality_check_runs`.
5. Confirm the approved calibration freeze used by training:
   `SELECT id, freeze_status, calibration_start_date, calibration_end_date, attribution_model, row_counts, data_quality_checks, evidence_hash, approved_by, approved_at FROM mmm_baseline_calibration_freezes ORDER BY generation_timestamp DESC LIMIT 5;`
6. Query the latest model run:
   `SELECT id, run_status, training_start_date, training_end_date, input_summary, calibration_report, validation_report, completed_at FROM mmm_model_runs ORDER BY completed_at DESC NULLS LAST LIMIT 5;`

## Failure Modes

- `MMM weekly channel mart failed leakage checks`: weekly mart input has failed quality rows. Check `mmm_weekly_channel_input_mart_v1.quality_flags` and recent reporting aggregate freshness.
- `baseline_linear_mmm_v1 training requires --freeze-id or MMM_BASELINE_FREEZE_ID`: create and approve an immutable baseline calibration freeze, then rerun training with that freeze id.
- `MMM approved freeze contains failed quality rows`: the approved freeze captured failed DQ evidence. Create a new freeze after upstream remediation; freeze rows are immutable.
- `MMM baseline requires at least ... observations`: the training window is too short, spend/revenue inputs are missing, or the scheduler ran before upstream daily mart refresh completed.
- Cloud Run timeout or memory failure: reduce segment count with `MMM_BASELINE_MAX_SEGMENTS`, widen lag with `MMM_BASELINE_LAG_DAYS`, or temporarily run a narrower date window manually.
- `governanceStatus` not `passed` or `divergenceAlertCount > 0`: MMM output diverged from deterministic attribution anchors and should not be promoted until triaged.

## Recovery

1. If upstream data is stale, keep the MMM scheduler paused:
   `sh infra/cloud-run/scheduler.sh <environment> mmm-baseline pause`
2. Resolve upstream ingestion first:
   - spend freshness: check Meta and Google Ads schedulers and logs
   - attribution freshness: check `attribution_backlog_snapshot` and order materialization
   - campaign metadata freshness: check metadata refresh jobs and unresolved campaign IDs
3. Re-run data quality:
   `gcloud run jobs execute roas-radar-data-quality-<environment> --region <region> --wait`
4. Create an approved immutable calibration freeze after readiness checks pass:
   `npm run mmm:freeze-baseline -- --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD> --attribution-model last_touch --status approved --approved-by <owner>`
5. Re-run MMM baseline with the approved freeze id:
   `gcloud run jobs execute roas-radar-mmm-baseline-<environment> --region <region> --wait`
6. Confirm a completed `mmm_baseline_job_lifecycle` log has `alertable=false`, `governanceStatus="passed"`, and a current `modelRunId`.
7. Resume the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> mmm-baseline resume`

## Drift Triage

1. Compare `maxDivergenceRate` and `calibration_report.divergenceAlerts` against the previous healthy `mmm_model_runs` row.
2. If only one paid segment diverged, inspect spend rows, campaign metadata, and attribution credit revenue for that segment in the training window.
3. If every segment diverged, check attribution aggregate freshness and whether the run used a different `MMM_BASELINE_ATTRIBUTION_MODEL`.
4. Treat large drift as a measurement incident, not a model-tuning task, until deterministic ingestion and attribution freshness are proven healthy.

## Escalation

- Page on-call when production MMM baseline fails twice in a row or when drift breaches while deterministic attribution is also alerting.
- Open a follow-up incident when MMM succeeds after manual recovery but the scheduler had been paused for more than one production cycle.

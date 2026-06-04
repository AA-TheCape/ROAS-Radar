# GCP Pipeline Terraform

This stack provisions one deterministic ROAS Radar environment on GCP:

- required project APIs
- Artifact Registry Docker repository
- private VPC peering for Cloud SQL
- Cloud SQL for PostgreSQL 16 with PITR backups and deletion protection
- `roas_app`, `roas_migrator`, and `roas_readonly` database users
- Secret Manager placeholders and least-privilege secret access
- service accounts and IAM for API, dashboard, worker, migration, ingestion, processing, scheduler, and deployer identities
- Cloud Run services for API, attribution worker, and dashboard
- Cloud Run Jobs for migrations, ad ingestion, retention, data quality, identity reconciliation, attribution materialization, baseline MMM training, Bayesian hierarchical MMM training, and metadata refreshes
- Cloud Scheduler HTTP triggers that invoke the Cloud Run Jobs through a dedicated scheduler service account

## Apply

Create a private tfvars file from the examples and provide the three database passwords through tfvars or `TF_VAR_*` environment variables:

```sh
cd infra/terraform/gcp-pipeline
terraform init
terraform plan -var-file=environments/staging.tfvars
terraform apply -var-file=environments/staging.tfvars
```

Do the same for production with `environments/production.tfvars`.

Terraform creates Secret Manager secret containers, not secret versions. Populate each secret after apply:

```sh
printf '%s' "$DATABASE_URL" | gcloud secrets versions add DATABASE_URL --data-file=-
printf '%s' "$MIGRATOR_DATABASE_URL" | gcloud secrets versions add MIGRATOR_DATABASE_URL --data-file=-
```

Repeat for the remaining secret names shown by `terraform output secret_names`.

## Baseline MMM Release Gate

Terraform defines `roas-radar-mmm-baseline-<environment>` separately from `roas-radar-mmm-bayesian-<environment>`. Set `mmm_baseline_freeze_id` to the approved `mmm_baseline_calibration_freezes.id` for the target calibration window and attribution model before applying the baseline job. Terraform refuses to deploy the baseline MMM Cloud Run Job with an empty baseline freeze id.

Manual validation and execution:

```sh
DATABASE_URL=postgres://placeholder \
MMM_BASELINE_FREEZE_ID=<approved-freeze-id> \
MMM_BASELINE_ATTRIBUTION_MODEL=last_touch \
MMM_BASELINE_LOOKBACK_DAYS=90 \
npm run mmm:train-baseline -- --validate-config

gcloud run jobs execute roas-radar-mmm-baseline-<environment> --region <region> --wait
```

For production promotion, choose the freeze id from an immutable approved baseline freeze after staging validation. Record the freeze id, calibration window, attribution model, approver, and evidence hash in the release notes before deploying or unpausing the production scheduler.

## Bayesian MMM Release Gate

Terraform defines `roas-radar-mmm-bayesian-<environment>` separately from `roas-radar-mmm-baseline-<environment>`. Set `mmm_bayesian_freeze_id` only after an immutable approved calibration freeze exists for the target window and attribution model. Keep the `mmm_bayesian` scheduler in `paused_schedulers` until staging has a successful `bayesian_hierarchical_mmm_v1` run with persisted artifacts and passing posterior diagnostics.

Manual validation and execution:

```sh
DATABASE_URL=postgres://placeholder \
MMM_BAYESIAN_FREEZE_ID=<approved-freeze-id> \
MMM_BAYESIAN_ATTRIBUTION_MODEL=last_touch \
MMM_BAYESIAN_LOOKBACK_DAYS=90 \
npm run mmm:train-bayesian -- --validate-config

gcloud run jobs execute roas-radar-mmm-bayesian-<environment> --region <region> --wait
```

## Migration And Promotion Contract

Terraform establishes the infrastructure shape. Runtime promotion remains image-tag based:

1. Build and push images with `cloudbuild.staging.yaml` or `cloudbuild.release.yaml`.
2. Run `infra/cloud-run/promote.sh staging` or `infra/cloud-run/promote.sh production`.
3. The promotion script deploys the migration job first and executes `npm run db:migrate:start` before shifting API, worker, and dashboard traffic.
4. Smoke tests run after each environment deployment.
5. Production promotion can require a staging rollback drill with `RUN_STAGING_ROLLBACK_DRILL=true`.

Rollback has two layers:

- service rollback: `infra/cloud-run/rollback.sh <environment> <metadata-file> previous`
- schema rollback: manually apply the matching SQL in `db/rollbacks/` when an additive migration must be reversed

Do not store `.tfvars` files with passwords or project-private URLs in git.

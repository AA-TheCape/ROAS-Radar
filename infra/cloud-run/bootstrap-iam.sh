#!/bin/sh

set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <environment>" >&2
  exit 1
fi

ENVIRONMENT="$1"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
ENV_FILE="$SCRIPT_DIR/environments/$ENVIRONMENT.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "missing environment file: $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

require_var() {
  eval "value=\${$1:-}"
  if [ -z "$value" ]; then
    echo "missing required variable $1 in $ENV_FILE" >&2
    exit 1
  fi
}

service_account_email() {
  printf '%s@%s.iam.gserviceaccount.com' "$1" "$GCP_PROJECT_ID"
}

ensure_service_account() {
  account_name="$1"
  display_name="$2"

  if ! gcloud iam service-accounts describe "$(service_account_email "$account_name")" \
    --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    gcloud iam service-accounts create "$account_name" \
      --project="$GCP_PROJECT_ID" \
      --display-name="$display_name"
  fi
}

ensure_secret() {
  secret_name="$1"

  if ! gcloud secrets describe "$secret_name" --project "$GCP_PROJECT_ID" >/dev/null 2>&1; then
    gcloud secrets create "$secret_name" \
      --project "$GCP_PROJECT_ID" \
      --replication-policy=automatic \
      --labels="app=roas-radar,environment=$ENVIRONMENT"
  fi
}

grant_project_role() {
  member="$1"
  role="$2"

  gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
    --member="$member" \
    --role="$role" \
    >/dev/null
}

grant_roles_csv() {
  service_account="$1"
  roles_csv="$2"
  old_ifs=$IFS
  IFS=','
  set -- $roles_csv
  IFS=$old_ifs

  for role in "$@"; do
    trimmed=$(printf '%s' "$role" | awk '{$1=$1; print}')
    if [ -n "$trimmed" ]; then
      grant_project_role "serviceAccount:$(service_account_email "$service_account")" "$trimmed"
    fi
  done
}

grant_secret_access() {
  service_account="$1"
  secret_name="$2"

  if [ -z "$secret_name" ]; then
    return
  fi

  ensure_secret "$secret_name"
  gcloud secrets add-iam-policy-binding "$secret_name" \
    --project="$GCP_PROJECT_ID" \
    --member="serviceAccount:$(service_account_email "$service_account")" \
    --role="roles/secretmanager.secretAccessor" \
    >/dev/null
}

for var in \
  GCP_PROJECT_ID \
  API_SERVICE_ACCOUNT_NAME \
  DASHBOARD_SERVICE_ACCOUNT_NAME \
  WORKER_SERVICE_ACCOUNT_NAME \
  MIGRATOR_JOB_SERVICE_ACCOUNT_NAME \
  META_ADS_JOB_SERVICE_ACCOUNT_NAME \
  META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME \
  GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME \
  GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME \
  CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME \
  SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_NAME \
  ORDER_ATTRIBUTION_BACKFILL_JOB_SERVICE_ACCOUNT_NAME \
  SHOPIFY_ATTRIBUTION_RECOVERY_JOB_SERVICE_ACCOUNT_NAME \
  GA4_FALLBACK_RECOVERY_JOB_SERVICE_ACCOUNT_NAME \
  DEAD_LETTER_REPLAY_JOB_SERVICE_ACCOUNT_NAME \
  RETENTION_JOB_SERVICE_ACCOUNT_NAME \
  DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME \
  IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME \
  ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME \
  SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME \
  DEPLOYER_SERVICE_ACCOUNT_NAME
do
  require_var "$var"
done

ensure_service_account "$API_SERVICE_ACCOUNT_NAME" "ROAS Radar API $ENVIRONMENT"
ensure_service_account "$DASHBOARD_SERVICE_ACCOUNT_NAME" "ROAS Radar dashboard $ENVIRONMENT"
ensure_service_account "$WORKER_SERVICE_ACCOUNT_NAME" "ROAS Radar attribution worker $ENVIRONMENT"
ensure_service_account "$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar migrator $ENVIRONMENT"
ensure_service_account "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar Meta Ads jobs $ENVIRONMENT"
ensure_service_account "$META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar Meta deterministic sync $ENVIRONMENT"
ensure_service_account "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar Google Ads jobs $ENVIRONMENT"
ensure_service_account "$GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar GA4 ingestion job $ENVIRONMENT"
ensure_service_account "$CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar campaign metadata backfill $ENVIRONMENT"
ensure_service_account "$SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar Shopify order reimport $ENVIRONMENT"
ensure_service_account "$ORDER_ATTRIBUTION_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar order attribution backfill $ENVIRONMENT"
ensure_service_account "$SHOPIFY_ATTRIBUTION_RECOVERY_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar Shopify attribution recovery $ENVIRONMENT"
ensure_service_account "$GA4_FALLBACK_RECOVERY_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar GA4 fallback recovery $ENVIRONMENT"
ensure_service_account "$DEAD_LETTER_REPLAY_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar dead-letter replay $ENVIRONMENT"
ensure_service_account "$RETENTION_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar retention job $ENVIRONMENT"
ensure_service_account "$DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar data quality job $ENVIRONMENT"
ensure_service_account "$IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar identity graph backfill $ENVIRONMENT"
ensure_service_account "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar order materialization $ENVIRONMENT"
ensure_service_account "$SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME" "ROAS Radar scheduler invoker $ENVIRONMENT"
ensure_service_account "$DEPLOYER_SERVICE_ACCOUNT_NAME" "ROAS Radar deployer $ENVIRONMENT"

grant_roles_csv "$API_SERVICE_ACCOUNT_NAME" "${API_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$DASHBOARD_SERVICE_ACCOUNT_NAME" "${DASHBOARD_SERVICE_ACCOUNT_ROLES:-roles/logging.logWriter}"
grant_roles_csv "$WORKER_SERVICE_ACCOUNT_NAME" "${WORKER_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter,roles/run.invoker}"
grant_roles_csv "$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME" "${MIGRATOR_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter}"
grant_roles_csv "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "${META_ADS_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME" "${META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME" "${GOOGLE_ADS_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME" "${GA4_INGESTION_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter,roles/bigquery.jobUser,roles/bigquery.dataViewer}"
grant_roles_csv "$CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "${CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_NAME" "${SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$ORDER_ATTRIBUTION_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "${ORDER_ATTRIBUTION_BACKFILL_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$SHOPIFY_ATTRIBUTION_RECOVERY_JOB_SERVICE_ACCOUNT_NAME" "${SHOPIFY_ATTRIBUTION_RECOVERY_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$GA4_FALLBACK_RECOVERY_JOB_SERVICE_ACCOUNT_NAME" "${GA4_FALLBACK_RECOVERY_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$DEAD_LETTER_REPLAY_JOB_SERVICE_ACCOUNT_NAME" "${DEAD_LETTER_REPLAY_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$RETENTION_JOB_SERVICE_ACCOUNT_NAME" "${RETENTION_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter}"
grant_roles_csv "$DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME" "${DATA_QUALITY_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "${IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME" "${ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME" "${SCHEDULER_INVOKER_SERVICE_ACCOUNT_ROLES:-roles/logging.logWriter}"
grant_roles_csv "$DEPLOYER_SERVICE_ACCOUNT_NAME" "${DEPLOYER_SERVICE_ACCOUNT_ROLES:-roles/run.developer,roles/artifactregistry.writer,roles/cloudbuild.builds.editor,roles/iam.serviceAccountUser,roles/cloudscheduler.admin,roles/monitoring.editor}"

grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "REPORTING_API_TOKEN"
grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "SHOPIFY_WEBHOOK_SECRET"
grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_API_KEY"
grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_API_SECRET"
grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_ENCRYPTION_KEY"
grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "META_ADS_APP_SECRET"
grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "META_ADS_ENCRYPTION_KEY"
grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "${META_ADS_METADATA_ACCESS_TOKEN_SECRET_NAME:-}"
grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "GOOGLE_ADS_ENCRYPTION_KEY"

grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "REPORTING_API_TOKEN"
grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "SHOPIFY_WEBHOOK_SECRET"
grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_API_KEY"
grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_API_SECRET"
grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_ENCRYPTION_KEY"
grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "META_ADS_APP_SECRET"
grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "META_ADS_ENCRYPTION_KEY"
grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "${META_ADS_METADATA_ACCESS_TOKEN_SECRET_NAME:-}"
grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "GOOGLE_ADS_ENCRYPTION_KEY"

grant_secret_access "$DASHBOARD_SERVICE_ACCOUNT_NAME" "REPORTING_API_TOKEN"
grant_secret_access "$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME" "MIGRATOR_DATABASE_URL"
grant_secret_access "$DEPLOYER_SERVICE_ACCOUNT_NAME" "REPORTING_API_TOKEN"

grant_secret_access "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "META_ADS_APP_SECRET"
grant_secret_access "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "META_ADS_ENCRYPTION_KEY"
grant_secret_access "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "${META_ADS_METADATA_ACCESS_TOKEN_SECRET_NAME:-}"
grant_secret_access "$META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME" "META_ADS_APP_SECRET"
grant_secret_access "$META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME" "META_ADS_ENCRYPTION_KEY"
grant_secret_access "$META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME" "${META_ADS_METADATA_ACCESS_TOKEN_SECRET_NAME:-}"

grant_secret_access "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME" "GOOGLE_ADS_ENCRYPTION_KEY"

grant_secret_access "$GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"

grant_secret_access "$CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "META_ADS_APP_SECRET"
grant_secret_access "$CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "META_ADS_ENCRYPTION_KEY"
grant_secret_access "$CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "${META_ADS_METADATA_ACCESS_TOKEN_SECRET_NAME:-}"
grant_secret_access "$CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "GOOGLE_ADS_ENCRYPTION_KEY"

grant_secret_access "$SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_API_KEY"
grant_secret_access "$SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_API_SECRET"
grant_secret_access "$SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_ENCRYPTION_KEY"

grant_secret_access "$ORDER_ATTRIBUTION_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$ORDER_ATTRIBUTION_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_ENCRYPTION_KEY"

grant_secret_access "$SHOPIFY_ATTRIBUTION_RECOVERY_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$SHOPIFY_ATTRIBUTION_RECOVERY_JOB_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_ENCRYPTION_KEY"

grant_secret_access "$GA4_FALLBACK_RECOVERY_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$DEAD_LETTER_REPLAY_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"

grant_secret_access "$RETENTION_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_ENCRYPTION_KEY"

echo "Bootstrap complete for $ENVIRONMENT"
echo "API service account: $(service_account_email "$API_SERVICE_ACCOUNT_NAME")"
echo "Dashboard service account: $(service_account_email "$DASHBOARD_SERVICE_ACCOUNT_NAME")"
echo "Worker service account: $(service_account_email "$WORKER_SERVICE_ACCOUNT_NAME")"
echo "Migrator service account: $(service_account_email "$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME")"
echo "Meta Ads job service account: $(service_account_email "$META_ADS_JOB_SERVICE_ACCOUNT_NAME")"
echo "Meta deterministic job service account: $(service_account_email "$META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME")"
echo "Google Ads job service account: $(service_account_email "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME")"
echo "GA4 ingestion job service account: $(service_account_email "$GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME")"
echo "Campaign metadata backfill service account: $(service_account_email "$CAMPAIGN_METADATA_BACKFILL_JOB_SERVICE_ACCOUNT_NAME")"
echo "Shopify order reimport service account: $(service_account_email "$SHOPIFY_ORDER_REIMPORT_JOB_SERVICE_ACCOUNT_NAME")"
echo "Order attribution backfill service account: $(service_account_email "$ORDER_ATTRIBUTION_BACKFILL_JOB_SERVICE_ACCOUNT_NAME")"
echo "Shopify attribution recovery service account: $(service_account_email "$SHOPIFY_ATTRIBUTION_RECOVERY_JOB_SERVICE_ACCOUNT_NAME")"
echo "GA4 fallback recovery service account: $(service_account_email "$GA4_FALLBACK_RECOVERY_JOB_SERVICE_ACCOUNT_NAME")"
echo "Dead-letter replay service account: $(service_account_email "$DEAD_LETTER_REPLAY_JOB_SERVICE_ACCOUNT_NAME")"
echo "Retention service account: $(service_account_email "$RETENTION_JOB_SERVICE_ACCOUNT_NAME")"
echo "Data quality service account: $(service_account_email "$DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME")"
echo "Identity graph backfill service account: $(service_account_email "$IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME")"
echo "Order attribution materialization service account: $(service_account_email "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME")"
echo "Scheduler invoker service account: $(service_account_email "$SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME")"
echo "Deployer service account: $(service_account_email "$DEPLOYER_SERVICE_ACCOUNT_NAME")"

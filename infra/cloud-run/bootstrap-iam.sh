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

grant_project_role() {
  account_name="$1"
  role="$2"

  gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
    --member="serviceAccount:$(service_account_email "$account_name")" \
    --role="$role" \
    >/dev/null
}

grant_secret_access() {
  account_name="$1"
  secret_name="$2"

  if [ -z "$secret_name" ]; then
    return
  fi

  gcloud secrets add-iam-policy-binding "$secret_name" \
    --project="$GCP_PROJECT_ID" \
    --member="serviceAccount:$(service_account_email "$account_name")" \
    --role="roles/secretmanager.secretAccessor" \
    >/dev/null
}

grant_roles_csv() {
  account_name="$1"
  roles_csv="$2"
  old_ifs=$IFS
  IFS=','
  set -- $roles_csv
  IFS=$old_ifs

  for role in "$@"; do
    trimmed=$(printf '%s' "$role" | awk '{$1=$1; print}')
    if [ -n "$trimmed" ]; then
      grant_project_role "$account_name" "$trimmed"
    fi
  done
}

for var in \
  GCP_PROJECT_ID \
  API_SERVICE_ACCOUNT_NAME \
  DASHBOARD_SERVICE_ACCOUNT_NAME \
  WORKER_SERVICE_ACCOUNT_NAME \
  MIGRATOR_JOB_SERVICE_ACCOUNT_NAME \
  META_ADS_JOB_SERVICE_ACCOUNT_NAME \
  GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME \
  RETENTION_JOB_SERVICE_ACCOUNT_NAME \
  DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME \
  IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME \
  ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME \
  MMM_BASELINE_JOB_SERVICE_ACCOUNT_NAME \
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
ensure_service_account "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar Google Ads jobs $ENVIRONMENT"
ensure_service_account "$RETENTION_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar retention job $ENVIRONMENT"
ensure_service_account "$DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar data quality job $ENVIRONMENT"
ensure_service_account "$IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar identity graph backfill $ENVIRONMENT"
ensure_service_account "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar attribution materialization $ENVIRONMENT"
ensure_service_account "$MMM_BASELINE_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar MMM baseline $ENVIRONMENT"
ensure_service_account "$SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME" "ROAS Radar scheduler invoker $ENVIRONMENT"
ensure_service_account "$DEPLOYER_SERVICE_ACCOUNT_NAME" "ROAS Radar deployer $ENVIRONMENT"

grant_roles_csv "$API_SERVICE_ACCOUNT_NAME" "${API_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$DASHBOARD_SERVICE_ACCOUNT_NAME" "${DASHBOARD_SERVICE_ACCOUNT_ROLES:-roles/logging.logWriter}"
grant_roles_csv "$WORKER_SERVICE_ACCOUNT_NAME" "${WORKER_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME" "${MIGRATOR_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter}"
grant_roles_csv "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "${META_ADS_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME" "${GOOGLE_ADS_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$RETENTION_JOB_SERVICE_ACCOUNT_NAME" "${RETENTION_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter}"
grant_roles_csv "$DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME" "${DATA_QUALITY_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "${IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME" "${ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$MMM_BASELINE_JOB_SERVICE_ACCOUNT_NAME" "${MMM_BASELINE_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME" "${SCHEDULER_INVOKER_SERVICE_ACCOUNT_ROLES:-roles/logging.logWriter}"
grant_roles_csv "$DEPLOYER_SERVICE_ACCOUNT_NAME" "${DEPLOYER_SERVICE_ACCOUNT_ROLES:-roles/run.admin,roles/cloudscheduler.admin,roles/artifactregistry.reader,roles/iam.serviceAccountUser}"

for secret in \
  DATABASE_URL \
  REPORTING_API_TOKEN \
  SHOPIFY_WEBHOOK_SECRET \
  SHOPIFY_APP_API_KEY \
  SHOPIFY_APP_API_SECRET \
  SHOPIFY_APP_ENCRYPTION_KEY \
  META_ADS_APP_SECRET \
  META_ADS_ENCRYPTION_KEY \
  GOOGLE_ADS_ENCRYPTION_KEY
do
  grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "$secret"
  grant_secret_access "$WORKER_SERVICE_ACCOUNT_NAME" "$secret"
done

grant_secret_access "$DASHBOARD_SERVICE_ACCOUNT_NAME" "REPORTING_API_TOKEN"
grant_secret_access "$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME" "MIGRATOR_DATABASE_URL"
grant_secret_access "$DEPLOYER_SERVICE_ACCOUNT_NAME" "REPORTING_API_TOKEN"

grant_secret_access "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "META_ADS_APP_SECRET"
grant_secret_access "$META_ADS_JOB_SERVICE_ACCOUNT_NAME" "META_ADS_ENCRYPTION_KEY"

grant_secret_access "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME" "GOOGLE_ADS_ENCRYPTION_KEY"

grant_secret_access "$RETENTION_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"
grant_secret_access "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME" "SHOPIFY_APP_ENCRYPTION_KEY"
grant_secret_access "$MMM_BASELINE_JOB_SERVICE_ACCOUNT_NAME" "DATABASE_URL"

echo "Bootstrap complete for $ENVIRONMENT"
echo "API service account: $(service_account_email "$API_SERVICE_ACCOUNT_NAME")"
echo "Dashboard service account: $(service_account_email "$DASHBOARD_SERVICE_ACCOUNT_NAME")"
echo "Worker service account: $(service_account_email "$WORKER_SERVICE_ACCOUNT_NAME")"
echo "Migrator service account: $(service_account_email "$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME")"
echo "Meta Ads job service account: $(service_account_email "$META_ADS_JOB_SERVICE_ACCOUNT_NAME")"
echo "Google Ads job service account: $(service_account_email "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME")"
echo "Retention service account: $(service_account_email "$RETENTION_JOB_SERVICE_ACCOUNT_NAME")"
echo "Data quality service account: $(service_account_email "$DATA_QUALITY_JOB_SERVICE_ACCOUNT_NAME")"
echo "Identity graph backfill service account: $(service_account_email "$IDENTITY_GRAPH_BACKFILL_JOB_SERVICE_ACCOUNT_NAME")"
echo "Order attribution materialization service account: $(service_account_email "$ORDER_ATTRIBUTION_MATERIALIZATION_JOB_SERVICE_ACCOUNT_NAME")"
echo "MMM baseline service account: $(service_account_email "$MMM_BASELINE_JOB_SERVICE_ACCOUNT_NAME")"
echo "Scheduler invoker service account: $(service_account_email "$SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME")"
echo "Deployer service account: $(service_account_email "$DEPLOYER_SERVICE_ACCOUNT_NAME")"

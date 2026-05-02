#!/bin/sh

set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <environment>" >&2
  exit 1
}

if [ "$#" -ne 1 ]; then
  usage
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

  if ! gcloud iam service-accounts describe "$(service_account_email "$account_name")" --project "$GCP_PROJECT_ID" >/dev/null 2>&1; then
    gcloud iam service-accounts create "$account_name" \
      --project "$GCP_PROJECT_ID" \
      --display-name "$display_name"
  fi
}

grant_project_role() {
  member="$1"
  role="$2"

  gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
    --member "$member" \
    --role "$role" \
    >/dev/null
}

grant_secret_access() {
  service_account="$1"
  secret_name="$2"

  if [ -z "$secret_name" ]; then
    return
  fi

  gcloud secrets add-iam-policy-binding "$secret_name" \
    --project "$GCP_PROJECT_ID" \
    --member "serviceAccount:$(service_account_email "$service_account")" \
    --role roles/secretmanager.secretAccessor \
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

for var in \
  GCP_PROJECT_ID \
  API_SERVICE_ACCOUNT_NAME \
  ATTRIBUTION_WORKER_SERVICE_ACCOUNT_NAME \
  MIGRATION_JOB_SERVICE_ACCOUNT_NAME \
  META_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME \
  GOOGLE_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME \
  GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME \
  GA4_INGESTION_SCHEDULER_SERVICE_ACCOUNT_NAME
do
  require_var "$var"
done

grant_project_role "serviceAccount:$DASHBOARD_SA" "roles/logging.logWriter"
grant_project_role "serviceAccount:$SCHEDULER_INVOKER_SA" "roles/logging.logWriter"

grant_roles_csv "$API_SERVICE_ACCOUNT_NAME" "${API_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$ATTRIBUTION_WORKER_SERVICE_ACCOUNT_NAME" "${ATTRIBUTION_WORKER_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$MIGRATION_JOB_SERVICE_ACCOUNT_NAME" "${MIGRATION_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter}"
grant_roles_csv "$META_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME" "${META_ADS_SYNC_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$GOOGLE_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME" "${GOOGLE_ADS_SYNC_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter}"
grant_roles_csv "$GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME" "${GA4_INGESTION_JOB_SERVICE_ACCOUNT_ROLES:-roles/cloudsql.client,roles/logging.logWriter,roles/monitoring.metricWriter,roles/bigquery.jobUser,roles/bigquery.dataViewer}"
grant_roles_csv "$GA4_INGESTION_SCHEDULER_SERVICE_ACCOUNT_NAME" "${GA4_INGESTION_SCHEDULER_SERVICE_ACCOUNT_ROLES:-roles/run.developer}"

grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "${DATABASE_URL_SECRET_NAME:-}"
grant_secret_access "$ATTRIBUTION_WORKER_SERVICE_ACCOUNT_NAME" "${DATABASE_URL_SECRET_NAME:-}"
grant_secret_access "$MIGRATION_JOB_SERVICE_ACCOUNT_NAME" "${DATABASE_URL_SECRET_NAME:-}"
grant_secret_access "$META_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME" "${DATABASE_URL_SECRET_NAME:-}"
grant_secret_access "$GOOGLE_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME" "${DATABASE_URL_SECRET_NAME:-}"
grant_secret_access "$GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME" "${DATABASE_URL_SECRET_NAME:-}"

grant_secret_access "$API_SERVICE_ACCOUNT_NAME" "${REPORTING_API_TOKEN_SECRET_NAME:-}"
grant_secret_access "$ATTRIBUTION_WORKER_SERVICE_ACCOUNT_NAME" "${REPORTING_API_TOKEN_SECRET_NAME:-}"
grant_secret_access "$GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME" "${REPORTING_API_TOKEN_SECRET_NAME:-}"

for SECRET in \
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
  grant_secret_accessor "$WORKER_SA" "$SECRET"
done

grant_secret_accessor "$DASHBOARD_SA" "REPORTING_API_TOKEN"
grant_secret_accessor "$MIGRATOR_SA" "MIGRATOR_DATABASE_URL"
grant_secret_accessor "$DEPLOYER_SA" "REPORTING_API_TOKEN"
grant_secret_accessor "$META_ADS_SA" "DATABASE_URL"
grant_secret_accessor "$META_ADS_SA" "META_ADS_APP_SECRET"
grant_secret_accessor "$META_ADS_SA" "META_ADS_ENCRYPTION_KEY"
grant_secret_accessor "$GOOGLE_ADS_SA" "DATABASE_URL"
grant_secret_accessor "$GOOGLE_ADS_SA" "GOOGLE_ADS_ENCRYPTION_KEY"
grant_secret_accessor "$RETENTION_SA" "DATABASE_URL"
grant_secret_accessor "$DATA_QUALITY_SA" "DATABASE_URL"
grant_secret_accessor "$IDENTITY_GRAPH_BACKFILL_SA" "DATABASE_URL"
grant_secret_accessor "$ORDER_ATTRIBUTION_MATERIALIZATION_SA" "DATABASE_URL"
grant_secret_accessor "$ORDER_ATTRIBUTION_MATERIALIZATION_SA" "SHOPIFY_APP_ENCRYPTION_KEY"

echo "Bootstrap complete for $ENVIRONMENT"
echo "API service account: $API_SA"
echo "Dashboard service account: $DASHBOARD_SA"
echo "Worker service account: $WORKER_SA"
echo "Migrator service account: $MIGRATOR_SA"
echo "Meta Ads job service account: $META_ADS_SA"
echo "Google Ads job service account: $GOOGLE_ADS_SA"
echo "Retention service account: $RETENTION_SA"
echo "Data quality service account: $DATA_QUALITY_SA"
echo "Identity graph backfill service account: $IDENTITY_GRAPH_BACKFILL_SA"
echo "Order attribution materialization service account: $ORDER_ATTRIBUTION_MATERIALIZATION_SA"
echo "Scheduler invoker service account: $SCHEDULER_INVOKER_SA"
echo "Deployer service account: $DEPLOYER_SA"

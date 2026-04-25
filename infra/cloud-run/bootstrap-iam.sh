#!/bin/sh

set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <staging|production>" >&2
  exit 1
fi

ENVIRONMENT="$1"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
ENV_FILE="$SCRIPT_DIR/environments/$ENVIRONMENT.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "missing environment file: $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
. "$ENV_FILE"

require_var() {
  eval "value=\${$1:-}"
  if [ -z "$value" ]; then
    echo "missing required variable $1 in $ENV_FILE" >&2
    exit 1
  fi
}

require_var GCP_PROJECT_ID
require_var GCP_REGION
require_var ARTIFACT_REGISTRY_REPOSITORY
require_var API_SERVICE_ACCOUNT_NAME
require_var DASHBOARD_SERVICE_ACCOUNT_NAME
require_var WORKER_SERVICE_ACCOUNT_NAME
require_var MIGRATOR_JOB_SERVICE_ACCOUNT_NAME
require_var RETENTION_JOB_SERVICE_ACCOUNT_NAME
require_var DEPLOYER_SERVICE_ACCOUNT_NAME

PROJECT_NUMBER=$(gcloud projects describe "$GCP_PROJECT_ID" --format='value(projectNumber)')

create_service_account() {
  ACCOUNT_NAME="$1"
  DISPLAY_NAME="$2"
  EMAIL="$ACCOUNT_NAME@$GCP_PROJECT_ID.iam.gserviceaccount.com"

  if ! gcloud iam service-accounts describe "$EMAIL" --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    gcloud iam service-accounts create "$ACCOUNT_NAME" \
      --project="$GCP_PROJECT_ID" \
      --display-name="$DISPLAY_NAME"
  fi

  printf '%s' "$EMAIL"
}

grant_project_role() {
  MEMBER="$1"
  ROLE="$2"

  gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
    --member="$MEMBER" \
    --role="$ROLE" \
    --quiet >/dev/null
}

grant_secret_accessor() {
  SERVICE_ACCOUNT_EMAIL="$1"
  SECRET_NAME="$2"

  gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
    --project="$GCP_PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet >/dev/null
}

ensure_repo() {
  if ! gcloud artifacts repositories describe "$ARTIFACT_REGISTRY_REPOSITORY" \
    --project="$GCP_PROJECT_ID" \
    --location="$GCP_REGION" >/dev/null 2>&1; then
    gcloud artifacts repositories create "$ARTIFACT_REGISTRY_REPOSITORY" \
      --project="$GCP_PROJECT_ID" \
      --location="$GCP_REGION" \
      --repository-format=docker \
      --description="ROAS Radar application images"
  fi
}

API_SA=$(create_service_account "$API_SERVICE_ACCOUNT_NAME" "ROAS Radar API ($ENVIRONMENT)")
DASHBOARD_SA=$(create_service_account "$DASHBOARD_SERVICE_ACCOUNT_NAME" "ROAS Radar dashboard ($ENVIRONMENT)")
WORKER_SA=$(create_service_account "$WORKER_SERVICE_ACCOUNT_NAME" "ROAS Radar worker ($ENVIRONMENT)")
MIGRATOR_SA=$(create_service_account "$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar migrator ($ENVIRONMENT)")
RETENTION_SA=$(create_service_account "$RETENTION_JOB_SERVICE_ACCOUNT_NAME" "ROAS Radar retention ($ENVIRONMENT)")
DEPLOYER_SA=$(create_service_account "$DEPLOYER_SERVICE_ACCOUNT_NAME" "ROAS Radar deployer ($ENVIRONMENT)")

ensure_repo

for SA in "$API_SA" "$DASHBOARD_SA" "$WORKER_SA" "$MIGRATOR_SA" "$RETENTION_SA"; do
  grant_project_role "serviceAccount:$SA" "roles/cloudsql.client"
  grant_project_role "serviceAccount:$SA" "roles/logging.logWriter"
  grant_project_role "serviceAccount:$SA" "roles/monitoring.metricWriter"
done

grant_project_role "serviceAccount:$WORKER_SA" "roles/run.invoker"

grant_project_role "serviceAccount:$DEPLOYER_SA" "roles/artifactregistry.writer"
grant_project_role "serviceAccount:$DEPLOYER_SA" "roles/cloudbuild.builds.editor"
grant_project_role "serviceAccount:$DEPLOYER_SA" "roles/logging.logWriter"
grant_project_role "serviceAccount:$DEPLOYER_SA" "roles/logging.configWriter"
grant_project_role "serviceAccount:$DEPLOYER_SA" "roles/monitoring.editor"
grant_project_role "serviceAccount:$DEPLOYER_SA" "roles/run.admin"
grant_project_role "serviceAccount:$DEPLOYER_SA" "roles/iam.serviceAccountUser"
grant_project_role "serviceAccount:$DEPLOYER_SA" "roles/cloudscheduler.admin"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:$PROJECT_NUMBER@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer" \
  --quiet >/dev/null || true

for SECRET in \
  DATABASE_URL \
  MIGRATOR_DATABASE_URL \
  REPORTING_API_TOKEN \
  SHOPIFY_WEBHOOK_SECRET \
  SHOPIFY_APP_API_KEY \
  SHOPIFY_APP_API_SECRET \
  SHOPIFY_APP_ENCRYPTION_KEY \
  META_ADS_APP_SECRET \
  META_ADS_ENCRYPTION_KEY \
  GOOGLE_ADS_ENCRYPTION_KEY
do
  grant_secret_accessor "$API_SA" "$SECRET"
  grant_secret_accessor "$WORKER_SA" "$SECRET"
done

grant_secret_accessor "$DASHBOARD_SA" "REPORTING_API_TOKEN"
grant_secret_accessor "$MIGRATOR_SA" "MIGRATOR_DATABASE_URL"
grant_secret_accessor "$RETENTION_SA" "DATABASE_URL"

echo "Bootstrap complete for $ENVIRONMENT"
echo "API service account: $API_SA"
echo "Dashboard service account: $DASHBOARD_SA"
echo "Worker service account: $WORKER_SA"
echo "Migrator service account: $MIGRATOR_SA"
echo "Retention service account: $RETENTION_SA"
echo "Deployer service account: $DEPLOYER_SA"

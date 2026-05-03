#!/bin/sh

set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <environment>" >&2
  exit 1
fi

ENVIRONMENT="$1"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
ENV_FILE="$SCRIPT_DIR/environments/$ENVIRONMENT.env"
TMP_DIR=$(mktemp -d)

cleanup() {
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT INT TERM

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

escape_yaml() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

write_yaml_var() {
  FILE_PATH="$1"
  KEY="$2"
  VALUE="${3:-}"

  if [ -z "$VALUE" ]; then
    return
  fi

  printf '%s: "%s"\n' "$KEY" "$(escape_yaml "$VALUE")" >>"$FILE_PATH"
}

capture_current_revision() {
  SERVICE_NAME="$1"

  gcloud run services describe "$SERVICE_NAME" \
    --project="$GCP_PROJECT_ID" \
    --region="$GCP_REGION" \
    --format='value(status.latestReadyRevisionName)' 2>/dev/null || true
}

service_account_email() {
  printf '%s@%s.iam.gserviceaccount.com' "$1" "$GCP_PROJECT_ID"
}

is_true() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

append_secret_mapping() {
  CURRENT="$1"
  ENTRY="$2"

  if [ -z "$CURRENT" ]; then
    printf '^#^%s' "$ENTRY"
    return
  fi

  printf '%s#%s' "$CURRENT" "$ENTRY"
}

deploy_service() {
  SERVICE_NAME="$1"
  IMAGE_URI="$2"
  SERVICE_ACCOUNT="$3"
  ENV_VARS_FILE="$4"
  SECRETS_SPEC="$5"
  COMMAND="$6"
  ARGS="$7"
  CPU="$8"
  MEMORY="$9"
  MIN_INSTANCES="${10}"
  MAX_INSTANCES="${11}"
  AUTH_MODE="${12}"

  PREVIOUS_REVISION=$(capture_current_revision "$SERVICE_NAME")

  if [ -n "$SECRETS_SPEC" ]; then
    gcloud run deploy "$SERVICE_NAME" \
      --project="$GCP_PROJECT_ID" \
      --region="$GCP_REGION" \
      --platform=managed \
      --image="$IMAGE_URI" \
      --service-account="$SERVICE_ACCOUNT" \
      --port=8080 \
      --cpu="$CPU" \
      --memory="$MEMORY" \
      --min-instances="$MIN_INSTANCES" \
      --max-instances="$MAX_INSTANCES" \
      --command="$COMMAND" \
      --args="$ARGS" \
      --env-vars-file="$ENV_VARS_FILE" \
      --set-secrets="$SECRETS_SPEC" \
      "$AUTH_MODE"
  else
    gcloud run deploy "$SERVICE_NAME" \
      --project="$GCP_PROJECT_ID" \
      --region="$GCP_REGION" \
      --platform=managed \
      --image="$IMAGE_URI" \
      --service-account="$SERVICE_ACCOUNT" \
      --port=8080 \
      --cpu="$CPU" \
      --memory="$MEMORY" \
      --min-instances="$MIN_INSTANCES" \
      --max-instances="$MAX_INSTANCES" \
      --command="$COMMAND" \
      --args="$ARGS" \
      --env-vars-file="$ENV_VARS_FILE" \
      "$AUTH_MODE"
  fi

  LATEST_REVISION=$(gcloud run services describe "$SERVICE_NAME" \
    --project="$GCP_PROJECT_ID" \
    --region="$GCP_REGION" \
    --format='value(status.latestReadyRevisionName)')

  case "$SERVICE_NAME" in
    "$API_SERVICE_NAME")
      API_PREVIOUS_REVISION="$PREVIOUS_REVISION"
      API_LATEST_REVISION="$LATEST_REVISION"
      ;;
    "$WORKER_SERVICE_NAME")
      WORKER_PREVIOUS_REVISION="$PREVIOUS_REVISION"
      WORKER_LATEST_REVISION="$LATEST_REVISION"
      ;;
    "$DASHBOARD_SERVICE_NAME")
      DASHBOARD_PREVIOUS_REVISION="$PREVIOUS_REVISION"
      DASHBOARD_LATEST_REVISION="$LATEST_REVISION"
      ;;
  esac
}

deploy_job() {
  JOB_NAME="$1"
  IMAGE_URI="$2"
  SERVICE_ACCOUNT="$3"
  ENV_VARS_FILE="$4"
  SECRETS_SPEC="$5"
  COMMAND="$6"
  ARGS="$7"
  CPU="$8"
  MEMORY="$9"
  TASK_TIMEOUT="${10}"
  MAX_RETRIES="${11}"

  if [ -n "$SECRETS_SPEC" ]; then
    gcloud run jobs deploy "$JOB_NAME" \
      --project="$GCP_PROJECT_ID" \
      --region="$GCP_REGION" \
      --image="$IMAGE_URI" \
      --service-account="$SERVICE_ACCOUNT" \
      --cpu="$CPU" \
      --memory="$MEMORY" \
      --task-timeout="$TASK_TIMEOUT" \
      --max-retries="$MAX_RETRIES" \
      --command="$COMMAND" \
      --args="$ARGS" \
      --env-vars-file="$ENV_VARS_FILE" \
      --set-secrets="$SECRETS_SPEC"
  else
    gcloud run jobs deploy "$JOB_NAME" \
      --project="$GCP_PROJECT_ID" \
      --region="$GCP_REGION" \
      --image="$IMAGE_URI" \
      --service-account="$SERVICE_ACCOUNT" \
      --cpu="$CPU" \
      --memory="$MEMORY" \
      --task-timeout="$TASK_TIMEOUT" \
      --max-retries="$MAX_RETRIES" \
      --command="$COMMAND" \
      --args="$ARGS" \
      --env-vars-file="$ENV_VARS_FILE"
  fi
}

run_job() {
  JOB_NAME="$1"
  echo "Executing $JOB_NAME"
  gcloud run jobs execute "$JOB_NAME" \
    --project="$GCP_PROJECT_ID" \
    --region="$GCP_REGION" \
    --wait
}

configure_metadata_scheduler() {
  PLATFORM_KEY="$1"
  JOB_NAME="$2"
  SCHEDULER_NAME="$3"
  SCHEDULE="$4"
  TIME_ZONE="$5"
  ENABLED_FLAG="$6"
  REQUESTED_BY="$7"

  JOB_URI="https://run.googleapis.com/v2/projects/$GCP_PROJECT_ID/locations/$GCP_REGION/jobs/$JOB_NAME:run"
  DESCRIPTION="ROAS Radar $ENVIRONMENT $PLATFORM_KEY metadata refresh requested-by=$REQUESTED_BY"
  SCHEDULER_SERVICE_ACCOUNT=$(service_account_email "$SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME")

  if gcloud scheduler jobs describe "$SCHEDULER_NAME" \
    --project="$GCP_PROJECT_ID" \
    --location="$GCP_SCHEDULER_REGION" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "$SCHEDULER_NAME" \
      --project="$GCP_PROJECT_ID" \
      --location="$GCP_SCHEDULER_REGION" \
      --schedule="$SCHEDULE" \
      --time-zone="$TIME_ZONE" \
      --uri="$JOB_URI" \
      --http-method=POST \
      --headers=Content-Type=application/json \
      --message-body='{}' \
      --oauth-service-account-email="$SCHEDULER_SERVICE_ACCOUNT" \
      --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform \
      --description="$DESCRIPTION"
  else
    gcloud scheduler jobs create http "$SCHEDULER_NAME" \
      --project="$GCP_PROJECT_ID" \
      --location="$GCP_SCHEDULER_REGION" \
      --schedule="$SCHEDULE" \
      --time-zone="$TIME_ZONE" \
      --uri="$JOB_URI" \
      --http-method=POST \
      --headers=Content-Type=application/json \
      --message-body='{}' \
      --oauth-service-account-email="$SCHEDULER_SERVICE_ACCOUNT" \
      --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform \
      --description="$DESCRIPTION"
  fi

  if is_true "$ENABLED_FLAG"; then
    gcloud scheduler jobs resume "$SCHEDULER_NAME" \
      --project="$GCP_PROJECT_ID" \
      --location="$GCP_SCHEDULER_REGION" >/dev/null
    SCHEDULER_STATE="ENABLED"
  else
    gcloud scheduler jobs pause "$SCHEDULER_NAME" \
      --project="$GCP_PROJECT_ID" \
      --location="$GCP_SCHEDULER_REGION" >/dev/null
    SCHEDULER_STATE="PAUSED"
  fi

  echo "Configured scheduler $SCHEDULER_NAME for $PLATFORM_KEY ($SCHEDULER_STATE)"
}

write_deploy_metadata() {
  if [ -z "${DEPLOY_METADATA_FILE:-}" ]; then
    return
  fi

  mkdir -p "$(dirname "$DEPLOY_METADATA_FILE")"

  cat >"$DEPLOY_METADATA_FILE" <<EOF
ENVIRONMENT=$ENVIRONMENT
DEPLOYED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
API_IMAGE=$API_IMAGE
DASHBOARD_IMAGE=$DASHBOARD_IMAGE
APP_IMAGE=$APP_IMAGE
API_PREVIOUS_REVISION=${API_PREVIOUS_REVISION:-}
API_LATEST_REVISION=${API_LATEST_REVISION:-}
WORKER_PREVIOUS_REVISION=${WORKER_PREVIOUS_REVISION:-}
WORKER_LATEST_REVISION=${WORKER_LATEST_REVISION:-}
DASHBOARD_PREVIOUS_REVISION=${DASHBOARD_PREVIOUS_REVISION:-}
DASHBOARD_LATEST_REVISION=${DASHBOARD_LATEST_REVISION:-}
META_ADS_METADATA_SCHEDULER_NAME=$META_ADS_METADATA_SCHEDULER_NAME
META_ADS_METADATA_SCHEDULER_ENABLED=$META_ADS_METADATA_SCHEDULER_ENABLED
GOOGLE_ADS_METADATA_SCHEDULER_NAME=$GOOGLE_ADS_METADATA_SCHEDULER_NAME
GOOGLE_ADS_METADATA_SCHEDULER_ENABLED=$GOOGLE_ADS_METADATA_SCHEDULER_ENABLED
EOF
}

require_var GCP_PROJECT_ID
require_var GCP_REGION
require_var GCP_SCHEDULER_REGION
require_var ARTIFACT_REGISTRY_REPOSITORY
require_var API_SERVICE_NAME
require_var DASHBOARD_SERVICE_NAME
require_var WORKER_SERVICE_NAME
require_var MIGRATOR_JOB_NAME
require_var META_ADS_METADATA_JOB_NAME
require_var GOOGLE_ADS_METADATA_JOB_NAME
require_var API_SERVICE_ACCOUNT_NAME
require_var DASHBOARD_SERVICE_ACCOUNT_NAME
require_var WORKER_SERVICE_ACCOUNT_NAME
require_var MIGRATOR_JOB_SERVICE_ACCOUNT_NAME
require_var META_ADS_JOB_SERVICE_ACCOUNT_NAME
require_var GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME
require_var SCHEDULER_INVOKER_SERVICE_ACCOUNT_NAME
require_var META_ADS_METADATA_SCHEDULER_NAME
require_var META_ADS_METADATA_SCHEDULE
require_var META_ADS_METADATA_TIME_ZONE
require_var META_ADS_METADATA_SCHEDULER_ENABLED
require_var META_ADS_METADATA_REFRESH_REQUESTED_BY
require_var GOOGLE_ADS_METADATA_SCHEDULER_NAME
require_var GOOGLE_ADS_METADATA_SCHEDULE
require_var GOOGLE_ADS_METADATA_TIME_ZONE
require_var GOOGLE_ADS_METADATA_SCHEDULER_ENABLED
require_var GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY

DEPLOY_SUFFIX=${SHORT_SHA:-$(date -u +%Y%m%d%H%M%S)}
APP_IMAGE="$GCP_REGION-docker.pkg.dev/$GCP_PROJECT_ID/$ARTIFACT_REGISTRY_REPOSITORY/roas-radar-app:$DEPLOY_SUFFIX"
DASHBOARD_IMAGE="$GCP_REGION-docker.pkg.dev/$GCP_PROJECT_ID/$ARTIFACT_REGISTRY_REPOSITORY/roas-radar-dashboard:$DEPLOY_SUFFIX"
API_IMAGE="$APP_IMAGE"

if ! is_true "${SKIP_BUILDS:-false}"; then
  echo "Building application image $APP_IMAGE"
  gcloud builds submit "$REPO_ROOT" \
    --project="$GCP_PROJECT_ID" \
    --tag="$APP_IMAGE" \
    --file="$REPO_ROOT/Dockerfile"

  echo "Building dashboard image $DASHBOARD_IMAGE"
  gcloud builds submit "$REPO_ROOT" \
    --project="$GCP_PROJECT_ID" \
    --tag="$DASHBOARD_IMAGE" \
    --file="$REPO_ROOT/dashboard/Dockerfile"
fi

API_ENV_FILE="$TMP_DIR/api-env.yaml"
WORKER_ENV_FILE="$TMP_DIR/worker-env.yaml"
DASHBOARD_ENV_FILE="$TMP_DIR/dashboard-env.yaml"
MIGRATOR_ENV_FILE="$TMP_DIR/migrator-env.yaml"
META_METADATA_ENV_FILE="$TMP_DIR/meta-metadata-env.yaml"
GOOGLE_METADATA_ENV_FILE="$TMP_DIR/google-metadata-env.yaml"

: >"$API_ENV_FILE"
: >"$WORKER_ENV_FILE"
: >"$DASHBOARD_ENV_FILE"
: >"$MIGRATOR_ENV_FILE"
: >"$META_METADATA_ENV_FILE"
: >"$GOOGLE_METADATA_ENV_FILE"

for FILE_PATH in "$API_ENV_FILE" "$WORKER_ENV_FILE" "$DASHBOARD_ENV_FILE" "$MIGRATOR_ENV_FILE" "$META_METADATA_ENV_FILE" "$GOOGLE_METADATA_ENV_FILE"
do
  write_yaml_var "$FILE_PATH" NODE_ENV production
done

write_yaml_var "$API_ENV_FILE" PORT 8080
write_yaml_var "$API_ENV_FILE" API_ALLOWED_ORIGINS "${API_ALLOWED_ORIGINS:-}"
write_yaml_var "$API_ENV_FILE" TRACKING_ALLOWED_ORIGINS "${TRACKING_ALLOWED_ORIGINS:-}"
write_yaml_var "$API_ENV_FILE" SHOPIFY_APP_BASE_URL "${SHOPIFY_APP_BASE_URL:-}"
write_yaml_var "$API_ENV_FILE" SHOPIFY_APP_POST_INSTALL_REDIRECT_URL "${SHOPIFY_APP_POST_INSTALL_REDIRECT_URL:-}"
write_yaml_var "$API_ENV_FILE" META_ADS_APP_ID "${META_ADS_APP_ID:-}"
write_yaml_var "$API_ENV_FILE" META_ADS_APP_BASE_URL "${META_ADS_APP_BASE_URL:-}"
write_yaml_var "$API_ENV_FILE" GOOGLE_ADS_APP_BASE_URL "${GOOGLE_ADS_APP_BASE_URL:-}"
write_yaml_var "$API_ENV_FILE" GOOGLE_ADS_CLIENT_ID "${GOOGLE_ADS_CLIENT_ID:-}"
write_yaml_var "$API_ENV_FILE" GOOGLE_ADS_CLIENT_SECRET "${GOOGLE_ADS_CLIENT_SECRET:-}"
write_yaml_var "$API_ENV_FILE" GOOGLE_ADS_DEVELOPER_TOKEN "${GOOGLE_ADS_DEVELOPER_TOKEN:-}"

write_yaml_var "$WORKER_ENV_FILE" PORT 8080
write_yaml_var "$WORKER_ENV_FILE" ATTRIBUTION_WORKER_LOOP true
write_yaml_var "$WORKER_ENV_FILE" ATTRIBUTION_WORKER_POLL_INTERVAL_MS "${ATTRIBUTION_WORKER_POLL_INTERVAL_MS:-5000}"
write_yaml_var "$WORKER_ENV_FILE" ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY "${ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY:-cloud-run-worker}"

write_yaml_var "$MIGRATOR_ENV_FILE" DATABASE_URL_SECRET_NAME "${MIGRATOR_DATABASE_SECRET_NAME:-MIGRATOR_DATABASE_URL}"

write_yaml_var "$META_METADATA_ENV_FILE" META_ADS_METADATA_REFRESH_REQUESTED_BY "$META_ADS_METADATA_REFRESH_REQUESTED_BY"
write_yaml_var "$GOOGLE_METADATA_ENV_FILE" GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY "$GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY"

API_SECRETS=''
API_SECRETS=$(append_secret_mapping "$API_SECRETS" "DATABASE_URL=DATABASE_URL:latest")
API_SECRETS=$(append_secret_mapping "$API_SECRETS" "REPORTING_API_TOKEN=REPORTING_API_TOKEN:latest")
API_SECRETS=$(append_secret_mapping "$API_SECRETS" "SHOPIFY_WEBHOOK_SECRET=SHOPIFY_WEBHOOK_SECRET:latest")
API_SECRETS=$(append_secret_mapping "$API_SECRETS" "SHOPIFY_APP_API_KEY=SHOPIFY_APP_API_KEY:latest")
API_SECRETS=$(append_secret_mapping "$API_SECRETS" "SHOPIFY_APP_API_SECRET=SHOPIFY_APP_API_SECRET:latest")
API_SECRETS=$(append_secret_mapping "$API_SECRETS" "SHOPIFY_APP_ENCRYPTION_KEY=SHOPIFY_APP_ENCRYPTION_KEY:latest")
API_SECRETS=$(append_secret_mapping "$API_SECRETS" "META_ADS_APP_SECRET=META_ADS_APP_SECRET:latest")
API_SECRETS=$(append_secret_mapping "$API_SECRETS" "META_ADS_ENCRYPTION_KEY=META_ADS_ENCRYPTION_KEY:latest")
API_SECRETS=$(append_secret_mapping "$API_SECRETS" "GOOGLE_ADS_ENCRYPTION_KEY=GOOGLE_ADS_ENCRYPTION_KEY:latest")

WORKER_SECRETS=''
WORKER_SECRETS=$(append_secret_mapping "$WORKER_SECRETS" "DATABASE_URL=DATABASE_URL:latest")
WORKER_SECRETS=$(append_secret_mapping "$WORKER_SECRETS" "SHOPIFY_APP_ENCRYPTION_KEY=SHOPIFY_APP_ENCRYPTION_KEY:latest")
WORKER_SECRETS=$(append_secret_mapping "$WORKER_SECRETS" "REPORTING_API_TOKEN=REPORTING_API_TOKEN:latest")

DASHBOARD_SECRETS=''
DASHBOARD_SECRETS=$(append_secret_mapping "$DASHBOARD_SECRETS" "DASHBOARD_REPORTING_API_TOKEN=REPORTING_API_TOKEN:latest")

MIGRATOR_SECRETS=''
MIGRATOR_SECRETS=$(append_secret_mapping "$MIGRATOR_SECRETS" "DATABASE_URL=${MIGRATOR_DATABASE_SECRET_NAME:-MIGRATOR_DATABASE_URL}:latest")

META_METADATA_SECRETS=''
META_METADATA_SECRETS=$(append_secret_mapping "$META_METADATA_SECRETS" "DATABASE_URL=DATABASE_URL:latest")
META_METADATA_SECRETS=$(append_secret_mapping "$META_METADATA_SECRETS" "META_ADS_APP_SECRET=META_ADS_APP_SECRET:latest")
META_METADATA_SECRETS=$(append_secret_mapping "$META_METADATA_SECRETS" "META_ADS_ENCRYPTION_KEY=META_ADS_ENCRYPTION_KEY:latest")

GOOGLE_METADATA_SECRETS=''
GOOGLE_METADATA_SECRETS=$(append_secret_mapping "$GOOGLE_METADATA_SECRETS" "DATABASE_URL=DATABASE_URL:latest")
GOOGLE_METADATA_SECRETS=$(append_secret_mapping "$GOOGLE_METADATA_SECRETS" "GOOGLE_ADS_ENCRYPTION_KEY=GOOGLE_ADS_ENCRYPTION_KEY:latest")

API_SERVICE_ACCOUNT=$(service_account_email "$API_SERVICE_ACCOUNT_NAME")
WORKER_SERVICE_ACCOUNT=$(service_account_email "$WORKER_SERVICE_ACCOUNT_NAME")
DASHBOARD_SERVICE_ACCOUNT=$(service_account_email "$DASHBOARD_SERVICE_ACCOUNT_NAME")
MIGRATOR_SERVICE_ACCOUNT=$(service_account_email "$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME")
META_METADATA_SERVICE_ACCOUNT=$(service_account_email "$META_ADS_JOB_SERVICE_ACCOUNT_NAME")
GOOGLE_METADATA_SERVICE_ACCOUNT=$(service_account_email "$GOOGLE_ADS_JOB_SERVICE_ACCOUNT_NAME")

deploy_service \
  "$API_SERVICE_NAME" \
  "$API_IMAGE" \
  "$API_SERVICE_ACCOUNT" \
  "$API_ENV_FILE" \
  "$API_SECRETS" \
  node \
  dist/src/server.js \
  "${API_SERVICE_CPU:-1}" \
  "${API_SERVICE_MEMORY:-512Mi}" \
  "${API_SERVICE_MIN_INSTANCES:-0}" \
  "${API_SERVICE_MAX_INSTANCES:-4}" \
  --allow-unauthenticated

API_URL=$(gcloud run services describe "$API_SERVICE_NAME" \
  --project="$GCP_PROJECT_ID" \
  --region="$GCP_REGION" \
  --format='value(status.url)')

write_yaml_var "$DASHBOARD_ENV_FILE" PORT 8080
write_yaml_var "$DASHBOARD_ENV_FILE" DASHBOARD_API_BASE_URL "$API_URL"
write_yaml_var "$DASHBOARD_ENV_FILE" DASHBOARD_REPORTING_TENANT_ID "${DASHBOARD_REPORTING_TENANT_ID:-roas-radar}"

deploy_service \
  "$WORKER_SERVICE_NAME" \
  "$APP_IMAGE" \
  "$WORKER_SERVICE_ACCOUNT" \
  "$WORKER_ENV_FILE" \
  "$WORKER_SECRETS" \
  node \
  dist/src/worker-service.js \
  "${WORKER_SERVICE_CPU:-1}" \
  "${WORKER_SERVICE_MEMORY:-512Mi}" \
  "${WORKER_SERVICE_MIN_INSTANCES:-0}" \
  "${WORKER_SERVICE_MAX_INSTANCES:-2}" \
  --no-allow-unauthenticated

deploy_service \
  "$DASHBOARD_SERVICE_NAME" \
  "$DASHBOARD_IMAGE" \
  "$DASHBOARD_SERVICE_ACCOUNT" \
  "$DASHBOARD_ENV_FILE" \
  "$DASHBOARD_SECRETS" \
  node \
  server.mjs \
  "${DASHBOARD_SERVICE_CPU:-1}" \
  "${DASHBOARD_SERVICE_MEMORY:-512Mi}" \
  "${DASHBOARD_SERVICE_MIN_INSTANCES:-0}" \
  "${DASHBOARD_SERVICE_MAX_INSTANCES:-2}" \
  --allow-unauthenticated

deploy_job \
  "$MIGRATOR_JOB_NAME" \
  "$APP_IMAGE" \
  "$MIGRATOR_SERVICE_ACCOUNT" \
  "$MIGRATOR_ENV_FILE" \
  "$MIGRATOR_SECRETS" \
  node \
  dist/src/db/migrate.js \
  "${MIGRATOR_JOB_CPU:-1}" \
  "${MIGRATOR_JOB_MEMORY:-512Mi}" \
  "${MIGRATOR_JOB_TASK_TIMEOUT:-900s}" \
  "${MIGRATOR_JOB_MAX_RETRIES:-0}"

deploy_job \
  "$META_ADS_METADATA_JOB_NAME" \
  "$APP_IMAGE" \
  "$META_METADATA_SERVICE_ACCOUNT" \
  "$META_METADATA_ENV_FILE" \
  "$META_METADATA_SECRETS" \
  node \
  dist/src/meta-ads-metadata-refresh-worker.js \
  "${METADATA_REFRESH_JOB_CPU:-1}" \
  "${METADATA_REFRESH_JOB_MEMORY:-512Mi}" \
  "${METADATA_REFRESH_JOB_TASK_TIMEOUT:-3600s}" \
  "${METADATA_REFRESH_JOB_MAX_RETRIES:-0}"

deploy_job \
  "$GOOGLE_ADS_METADATA_JOB_NAME" \
  "$APP_IMAGE" \
  "$GOOGLE_METADATA_SERVICE_ACCOUNT" \
  "$GOOGLE_METADATA_ENV_FILE" \
  "$GOOGLE_METADATA_SECRETS" \
  node \
  dist/src/google-ads-metadata-refresh-worker.js \
  "${METADATA_REFRESH_JOB_CPU:-1}" \
  "${METADATA_REFRESH_JOB_MEMORY:-512Mi}" \
  "${METADATA_REFRESH_JOB_TASK_TIMEOUT:-3600s}" \
  "${METADATA_REFRESH_JOB_MAX_RETRIES:-0}"

if is_true "${RUN_MIGRATIONS_ON_DEPLOY:-true}"; then
  run_job "$MIGRATOR_JOB_NAME"
fi

configure_metadata_scheduler \
  meta-ads \
  "$META_ADS_METADATA_JOB_NAME" \
  "$META_ADS_METADATA_SCHEDULER_NAME" \
  "$META_ADS_METADATA_SCHEDULE" \
  "$META_ADS_METADATA_TIME_ZONE" \
  "$META_ADS_METADATA_SCHEDULER_ENABLED" \
  "$META_ADS_METADATA_REFRESH_REQUESTED_BY"

configure_metadata_scheduler \
  google-ads \
  "$GOOGLE_ADS_METADATA_JOB_NAME" \
  "$GOOGLE_ADS_METADATA_SCHEDULER_NAME" \
  "$GOOGLE_ADS_METADATA_SCHEDULE" \
  "$GOOGLE_ADS_METADATA_TIME_ZONE" \
  "$GOOGLE_ADS_METADATA_SCHEDULER_ENABLED" \
  "$GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY"

write_deploy_metadata

echo "Deployment complete for $ENVIRONMENT"

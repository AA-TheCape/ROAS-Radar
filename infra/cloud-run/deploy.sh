#!/bin/sh

set -eu

usage() {
  echo "usage: $0 <staging|production>" >&2
  exit 1
}

if [ "$#" -ne 1 ]; then
  usage
fi

ENVIRONMENT="$1"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
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

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

append_common_runtime_flags() {
  if [ -n "${CLOUDSQL_INSTANCE_CONNECTION_NAME:-}" ]; then
    set -- "$@" --set-cloudsql-instances "$CLOUDSQL_INSTANCE_CONNECTION_NAME"
  fi

  if [ -n "${VPC_CONNECTOR_NAME:-}" ]; then
    set -- "$@" --vpc-connector "$VPC_CONNECTOR_NAME"
  fi

  if [ -n "${RUNTIME_CPU:-}" ]; then
    set -- "$@" --cpu "$RUNTIME_CPU"
  fi

  if [ -n "${RUNTIME_MEMORY:-}" ]; then
    set -- "$@" --memory "$RUNTIME_MEMORY"
  fi

  if [ -n "${RUNTIME_TIMEOUT_SECONDS:-}" ]; then
    set -- "$@" --timeout "${RUNTIME_TIMEOUT_SECONDS}s"
  fi

  printf '%s\n' "$*"
}

append_optional_env_flags() {
  current="$1"
  env_vars="$2"
  secrets="$3"

  set -- $current

  if [ -n "$env_vars" ]; then
    set -- "$@" --update-env-vars "$env_vars"
  fi

  if [ -n "$secrets" ]; then
    set -- "$@" --set-secrets "$secrets"
  fi

  printf '%s\n' "$*"
}

build_and_push_image() {
  image_name="$1"
  dockerfile_path="$2"
  image_ref="$3"

  docker build -f "$dockerfile_path" -t "$image_ref" "$REPO_ROOT"
  docker push "$image_ref"

  echo "Published $image_name image: $image_ref"
}

deploy_service() {
  service_name="$1"
  image_ref="$2"
  service_account_name="$3"
  ingress="$4"
  allow_unauthenticated="$5"
  env_vars="$6"
  secrets="$7"
  command_value="$8"
  args_value="$9"

  args="run deploy $service_name --project $GCP_PROJECT_ID --region $GCP_REGION --image $image_ref --service-account $(service_account_email "$service_account_name") --quiet"
  args=$(append_common_runtime_flags "$args")
  args="$args --ingress $ingress"

  if [ "$allow_unauthenticated" = "true" ]; then
    args="$args --allow-unauthenticated"
  else
    args="$args --no-allow-unauthenticated"
  fi

  if [ -n "$command_value" ]; then
    args="$args --command $command_value"
  fi

  if [ -n "$args_value" ]; then
    args="$args --args $args_value"
  fi

  args=$(append_optional_env_flags "$args" "$env_vars" "$secrets")

  # shellcheck disable=SC2086
  gcloud $args
}

deploy_job() {
  job_name="$1"
  image_ref="$2"
  service_account_name="$3"
  env_vars="$4"
  secrets="$5"
  args_value="$6"

  args="run jobs deploy $job_name --project $GCP_PROJECT_ID --region $GCP_REGION --image $image_ref --service-account $(service_account_email "$service_account_name") --max-retries 0 --quiet --command npm --args $args_value"
  args=$(append_common_runtime_flags "$args")
  args=$(append_optional_env_flags "$args" "$env_vars" "$secrets")

  # shellcheck disable=SC2086
  gcloud $args
}

upsert_scheduler_job() {
  require_var GA4_INGESTION_SCHEDULER_SERVICE_ACCOUNT_NAME
  scheduler_account_email=$(service_account_email "$GA4_INGESTION_SCHEDULER_SERVICE_ACCOUNT_NAME")
  scheduler_uri="https://run.googleapis.com/v2/projects/$GCP_PROJECT_ID/locations/$GCP_REGION/jobs/$GA4_INGESTION_JOB_NAME:run"
  scheduler_body='{}'

  create_or_update="create"
  if gcloud scheduler jobs describe "$GA4_INGESTION_SCHEDULER_JOB_NAME" --location "$GCP_REGION" --project "$GCP_PROJECT_ID" >/dev/null 2>&1; then
    create_or_update="update"
  fi

  if [ "$create_or_update" = "create" ]; then
    gcloud scheduler jobs create http "$GA4_INGESTION_SCHEDULER_JOB_NAME" \
      --location "$GCP_REGION" \
      --project "$GCP_PROJECT_ID" \
      --schedule "$GA4_INGESTION_SCHEDULE" \
      --time-zone "${GA4_INGESTION_SCHEDULER_TIME_ZONE:-Etc/UTC}" \
      --uri "$scheduler_uri" \
      --http-method POST \
      --headers "Content-Type=application/json" \
      --message-body "$scheduler_body" \
      --oauth-service-account-email "$scheduler_account_email" \
      --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
  else
    gcloud scheduler jobs update http "$GA4_INGESTION_SCHEDULER_JOB_NAME" \
      --location "$GCP_REGION" \
      --project "$GCP_PROJECT_ID" \
      --schedule "$GA4_INGESTION_SCHEDULE" \
      --time-zone "${GA4_INGESTION_SCHEDULER_TIME_ZONE:-Etc/UTC}" \
      --uri "$scheduler_uri" \
      --http-method POST \
      --headers "Content-Type=application/json" \
      --message-body "$scheduler_body" \
      --oauth-service-account-email "$scheduler_account_email" \
      --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
  fi
}

for var in \
  GCP_PROJECT_ID \
  GCP_REGION \
  ARTIFACT_REGISTRY_REPOSITORY \
  API_SERVICE_NAME \
  ATTRIBUTION_WORKER_SERVICE_NAME \
  MIGRATION_JOB_NAME \
  META_ADS_SYNC_JOB_NAME \
  GOOGLE_ADS_SYNC_JOB_NAME \
  GA4_INGESTION_JOB_NAME \
  GA4_INGESTION_SCHEDULER_JOB_NAME \
  API_SERVICE_ACCOUNT_NAME \
  ATTRIBUTION_WORKER_SERVICE_ACCOUNT_NAME \
  MIGRATION_JOB_SERVICE_ACCOUNT_NAME \
  META_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME \
  GOOGLE_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME \
  GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME \
  GA4_INGESTION_SCHEDULE
do
  require_var "$var"
done

if [ "${SKIP_BUILDS:-false}" != "true" ]; then
  if ! command_exists docker; then
    echo "docker is required unless SKIP_BUILDS=true" >&2
    exit 1
  fi
fi

IMAGE_TAG="${IMAGE_TAG:-${SHORT_SHA:-$(git -C "$REPO_ROOT" rev-parse --short HEAD)}}"
API_IMAGE_NAME="${API_IMAGE_NAME:-roas-radar-app}"
DASHBOARD_IMAGE_NAME="${DASHBOARD_IMAGE_NAME:-roas-radar-dashboard}"
IMAGE_REGISTRY="$GCP_REGION-docker.pkg.dev/$GCP_PROJECT_ID/$ARTIFACT_REGISTRY_REPOSITORY"
API_IMAGE="$IMAGE_REGISTRY/$API_IMAGE_NAME:$IMAGE_TAG"
DASHBOARD_IMAGE="$IMAGE_REGISTRY/$DASHBOARD_IMAGE_NAME:$IMAGE_TAG"

if [ "${SKIP_BUILDS:-false}" != "true" ]; then
  build_and_push_image "$API_IMAGE_NAME" "$REPO_ROOT/Dockerfile" "$API_IMAGE"

  if [ -n "${DASHBOARD_SERVICE_NAME:-}" ] || [ "${BUILD_DASHBOARD_IMAGE:-true}" = "true" ]; then
    build_and_push_image "$DASHBOARD_IMAGE_NAME" "$REPO_ROOT/dashboard/Dockerfile" "$DASHBOARD_IMAGE"
  fi
fi

deploy_service \
  "$API_SERVICE_NAME" \
  "$API_IMAGE" \
  "$API_SERVICE_ACCOUNT_NAME" \
  "${API_INGRESS:-all}" \
  "${API_ALLOW_UNAUTHENTICATED:-true}" \
  "${API_ENV_VARS:-NODE_ENV=production}" \
  "${API_SECRET_BINDINGS:-}" \
  "" \
  ""

deploy_service \
  "$ATTRIBUTION_WORKER_SERVICE_NAME" \
  "$API_IMAGE" \
  "$ATTRIBUTION_WORKER_SERVICE_ACCOUNT_NAME" \
  "${ATTRIBUTION_WORKER_INGRESS:-internal}" \
  "${ATTRIBUTION_WORKER_ALLOW_UNAUTHENTICATED:-false}" \
  "${ATTRIBUTION_WORKER_ENV_VARS:-NODE_ENV=production,ATTRIBUTION_WORKER_LOOP=true}" \
  "${ATTRIBUTION_WORKER_SECRET_BINDINGS:-${API_SECRET_BINDINGS:-}}" \
  "npm" \
  "run,start:worker-service"

deploy_job \
  "$MIGRATION_JOB_NAME" \
  "$API_IMAGE" \
  "$MIGRATION_JOB_SERVICE_ACCOUNT_NAME" \
  "${MIGRATION_JOB_ENV_VARS:-NODE_ENV=production}" \
  "${MIGRATION_JOB_SECRET_BINDINGS:-${API_SECRET_BINDINGS:-}}" \
  "run,db:migrate"

deploy_job \
  "$META_ADS_SYNC_JOB_NAME" \
  "$API_IMAGE" \
  "$META_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME" \
  "${META_ADS_SYNC_ENV_VARS:-NODE_ENV=production}" \
  "${META_ADS_SYNC_SECRET_BINDINGS:-${API_SECRET_BINDINGS:-}}" \
  "run,meta-ads:sync"

deploy_job \
  "$GOOGLE_ADS_SYNC_JOB_NAME" \
  "$API_IMAGE" \
  "$GOOGLE_ADS_SYNC_JOB_SERVICE_ACCOUNT_NAME" \
  "${GOOGLE_ADS_SYNC_ENV_VARS:-NODE_ENV=production}" \
  "${GOOGLE_ADS_SYNC_SECRET_BINDINGS:-${API_SECRET_BINDINGS:-}}" \
  "run,google-ads:sync"

deploy_job \
  "$GA4_INGESTION_JOB_NAME" \
  "$API_IMAGE" \
  "$GA4_INGESTION_JOB_SERVICE_ACCOUNT_NAME" \
  "${GA4_INGESTION_ENV_VARS:-NODE_ENV=production,GA4_BIGQUERY_ENABLED=true}" \
  "${GA4_INGESTION_SECRET_BINDINGS:-${API_SECRET_BINDINGS:-}}" \
  "run,ga4:ingest:start"

upsert_scheduler_job

if [ -n "${DASHBOARD_SERVICE_NAME:-}" ]; then
  deploy_service \
    "$DASHBOARD_SERVICE_NAME" \
    "$DASHBOARD_IMAGE" \
    "${DASHBOARD_SERVICE_ACCOUNT_NAME:-$API_SERVICE_ACCOUNT_NAME}" \
    "${DASHBOARD_INGRESS:-all}" \
    "${DASHBOARD_ALLOW_UNAUTHENTICATED:-true}" \
    "${DASHBOARD_ENV_VARS:-NODE_ENV=production}" \
    "${DASHBOARD_SECRET_BINDINGS:-}" \
    "" \
    ""
fi

if [ "${RUN_MIGRATIONS_ON_DEPLOY:-false}" = "true" ]; then
  gcloud run jobs execute "$MIGRATION_JOB_NAME" \
    --project "$GCP_PROJECT_ID" \
    --region "$GCP_REGION" \
    --wait
fi

if [ "${APPLY_MONITORING_ON_DEPLOY:-false}" = "true" ]; then
  "$REPO_ROOT/infra/monitoring/apply.sh" "$ENVIRONMENT"
fi

echo "Deployed Cloud Run workloads for $ENVIRONMENT using image tag $IMAGE_TAG"

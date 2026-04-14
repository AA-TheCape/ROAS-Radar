#!/bin/sh

set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <staging|production>" >&2
  exit 1
fi

ENVIRONMENT="$1"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
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

for var in \
  GCP_PROJECT_ID \
  GCP_REGION \
  CLOUD_SQL_CONNECTION_NAME \
  ARTIFACT_REGISTRY_REPOSITORY \
  IMAGE_NAME \
  IMAGE_NAME_DASHBOARD \
  API_SERVICE_NAME \
  DASHBOARD_SERVICE_NAME \
  WORKER_SERVICE_NAME \
  MIGRATOR_JOB_NAME \
  API_SERVICE_ACCOUNT_NAME \
  DASHBOARD_SERVICE_ACCOUNT_NAME \
  WORKER_SERVICE_ACCOUNT_NAME \
  MIGRATOR_JOB_SERVICE_ACCOUNT_NAME \
  API_INGRESS \
  DASHBOARD_INGRESS \
  API_MIN_INSTANCES \
  API_MAX_INSTANCES \
  DASHBOARD_MIN_INSTANCES \
  DASHBOARD_MAX_INSTANCES \
  WORKER_MIN_INSTANCES \
  WORKER_MAX_INSTANCES \
  DATABASE_POOL_MAX \
  WORKER_DATABASE_POOL_MAX \
  DASHBOARD_API_BASE_URL \
  TRACKING_ALLOWED_ORIGINS \
  SHOPIFY_APP_BASE_URL
do
  require_var "$var"
done

SHORT_SHA=${SHORT_SHA:-$(git -C "$REPO_ROOT" rev-parse --short HEAD)}
IMAGE_URI="$GCP_REGION-docker.pkg.dev/$GCP_PROJECT_ID/$ARTIFACT_REGISTRY_REPOSITORY/$IMAGE_NAME:$SHORT_SHA"
DASHBOARD_IMAGE_URI="$GCP_REGION-docker.pkg.dev/$GCP_PROJECT_ID/$ARTIFACT_REGISTRY_REPOSITORY/$IMAGE_NAME_DASHBOARD:$SHORT_SHA"

API_SA="$API_SERVICE_ACCOUNT_NAME@$GCP_PROJECT_ID.iam.gserviceaccount.com"
DASHBOARD_SA="$DASHBOARD_SERVICE_ACCOUNT_NAME@$GCP_PROJECT_ID.iam.gserviceaccount.com"
WORKER_SA="$WORKER_SERVICE_ACCOUNT_NAME@$GCP_PROJECT_ID.iam.gserviceaccount.com"
MIGRATOR_SA="$MIGRATOR_JOB_SERVICE_ACCOUNT_NAME@$GCP_PROJECT_ID.iam.gserviceaccount.com"

COMMON_SECRET_FLAGS="DATABASE_URL=DATABASE_URL:latest,REPORTING_API_TOKEN=REPORTING_API_TOKEN:latest,SHOPIFY_WEBHOOK_SECRET=SHOPIFY_WEBHOOK_SECRET:latest,SHOPIFY_APP_API_KEY=SHOPIFY_APP_API_KEY:latest,SHOPIFY_APP_API_SECRET=SHOPIFY_APP_API_SECRET:latest,SHOPIFY_APP_ENCRYPTION_KEY=SHOPIFY_APP_ENCRYPTION_KEY:latest,META_ADS_APP_SECRET=META_ADS_APP_SECRET:latest,META_ADS_ENCRYPTION_KEY=META_ADS_ENCRYPTION_KEY:latest,GOOGLE_ADS_ENCRYPTION_KEY=GOOGLE_ADS_ENCRYPTION_KEY:latest"
COMMON_ENV_VARS="^@^NODE_ENV=production@DATABASE_POOL_MIN=0@DATABASE_SSL=false@TRACKING_ALLOWED_ORIGINS=$TRACKING_ALLOWED_ORIGINS@SHOPIFY_APP_BASE_URL=$SHOPIFY_APP_BASE_URL@SHOPIFY_APP_API_VERSION=${SHOPIFY_APP_API_VERSION:-2026-01}@SHOPIFY_APP_SCOPES=${SHOPIFY_APP_SCOPES:-read_orders}@SHOPIFY_APP_POST_INSTALL_REDIRECT_URL=${SHOPIFY_APP_POST_INSTALL_REDIRECT_URL:-}@META_ADS_APP_ID=${META_ADS_APP_ID:-}@META_ADS_APP_BASE_URL=${META_ADS_APP_BASE_URL:-$SHOPIFY_APP_BASE_URL}@META_ADS_APP_SCOPES=${META_ADS_APP_SCOPES:-ads_read}@META_ADS_AD_ACCOUNT_ID=${META_ADS_AD_ACCOUNT_ID:-}@GOOGLE_ADS_API_VERSION=${GOOGLE_ADS_API_VERSION:-v19}"

if [ "${SKIP_BUILDS:-false}" != "true" ]; then
  echo "Building image $IMAGE_URI"
  gcloud builds submit "$REPO_ROOT" \
    --project="$GCP_PROJECT_ID" \
    --tag="$IMAGE_URI"

  echo "Building dashboard image $DASHBOARD_IMAGE_URI"
  gcloud builds submit "$REPO_ROOT/dashboard" \
    --project="$GCP_PROJECT_ID" \
    --tag="$DASHBOARD_IMAGE_URI"
fi

echo "Deploying API service $API_SERVICE_NAME"
gcloud run deploy "$API_SERVICE_NAME" \
  --project="$GCP_PROJECT_ID" \
  --region="$GCP_REGION" \
  --image="$IMAGE_URI" \
  --service-account="$API_SA" \
  --allow-unauthenticated \
  --ingress="$API_INGRESS" \
  --min-instances="$API_MIN_INSTANCES" \
  --max-instances="$API_MAX_INSTANCES" \
  --port=8080 \
  --cpu=1 \
  --memory=512Mi \
  --concurrency=80 \
  --timeout=300 \
  --set-env-vars="${COMMON_ENV_VARS}@DATABASE_POOL_MAX=$DATABASE_POOL_MAX@ATTRIBUTION_WORKER_LOOP=false" \
  --set-secrets="$COMMON_SECRET_FLAGS" \
  --add-cloudsql-instances="$CLOUD_SQL_CONNECTION_NAME"

echo "Deploying worker service $WORKER_SERVICE_NAME"
gcloud run deploy "$WORKER_SERVICE_NAME" \
  --project="$GCP_PROJECT_ID" \
  --region="$GCP_REGION" \
  --image="$IMAGE_URI" \
  --service-account="$WORKER_SA" \
  --no-allow-unauthenticated \
  --ingress=internal \
  --min-instances="$WORKER_MIN_INSTANCES" \
  --max-instances="$WORKER_MAX_INSTANCES" \
  --port=8080 \
  --cpu=1 \
  --memory=512Mi \
  --concurrency=1 \
  --timeout=900 \
  --command=npm \
  --args=run,start:worker-service \
  --set-env-vars="${COMMON_ENV_VARS}@DATABASE_POOL_MAX=$WORKER_DATABASE_POOL_MAX@ATTRIBUTION_WORKER_LOOP=true" \
  --set-secrets="$COMMON_SECRET_FLAGS" \
  --add-cloudsql-instances="$CLOUD_SQL_CONNECTION_NAME"

echo "Deploying dashboard service $DASHBOARD_SERVICE_NAME"
gcloud run deploy "$DASHBOARD_SERVICE_NAME" \
  --project="$GCP_PROJECT_ID" \
  --region="$GCP_REGION" \
  --image="$DASHBOARD_IMAGE_URI" \
  --service-account="$DASHBOARD_SA" \
  --allow-unauthenticated \
  --ingress="$DASHBOARD_INGRESS" \
  --min-instances="$DASHBOARD_MIN_INSTANCES" \
  --max-instances="$DASHBOARD_MAX_INSTANCES" \
  --port=8080 \
  --cpu=1 \
  --memory=256Mi \
  --concurrency=80 \
  --timeout=300 \
  --set-env-vars="NODE_ENV=production,DASHBOARD_API_BASE_URL=$DASHBOARD_API_BASE_URL,DASHBOARD_REPORTING_TENANT_ID=${DASHBOARD_REPORTING_TENANT_ID:-roas-radar}" \
  --set-secrets="DASHBOARD_REPORTING_API_TOKEN=REPORTING_API_TOKEN:latest"

echo "Deploying migration job $MIGRATOR_JOB_NAME"
gcloud run jobs deploy "$MIGRATOR_JOB_NAME" \
  --project="$GCP_PROJECT_ID" \
  --region="$GCP_REGION" \
  --image="$IMAGE_URI" \
  --service-account="$MIGRATOR_SA" \
  --cpu=1 \
  --memory=512Mi \
  --max-retries=1 \
  --task-timeout=1800 \
  --parallelism=1 \
  --tasks=1 \
  --command=npm \
  --args=run,db:migrate:start \
  --set-env-vars="NODE_ENV=production,DATABASE_POOL_MAX=1,DATABASE_POOL_MIN=0,DATABASE_SSL=false" \
  --set-secrets="DATABASE_URL=MIGRATOR_DATABASE_URL:latest" \
  --add-cloudsql-instances="$CLOUD_SQL_CONNECTION_NAME"

if [ "${RUN_MIGRATIONS_ON_DEPLOY:-true}" = "true" ]; then
  echo "Executing migration job $MIGRATOR_JOB_NAME"
  gcloud run jobs execute "$MIGRATOR_JOB_NAME" \
    --project="$GCP_PROJECT_ID" \
    --region="$GCP_REGION" \
    --wait
fi

if [ "${APPLY_MONITORING_ON_DEPLOY:-true}" = "true" ]; then
  echo "Applying monitoring configuration for $ENVIRONMENT"
  "$REPO_ROOT/infra/monitoring/apply.sh" "$ENVIRONMENT"
fi

echo "Deployment complete for $ENVIRONMENT"

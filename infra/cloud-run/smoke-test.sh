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
require_var API_SERVICE_NAME
require_var DASHBOARD_SERVICE_NAME
require_var WORKER_SERVICE_NAME

SMOKE_TEST_DATE=${SMOKE_TEST_DATE:-$(date -u +%F)}
REPORTING_PATH=${SMOKE_TEST_REPORTING_PATH:-/api/reporting/summary}
REPORTING_QUERY="startDate=$SMOKE_TEST_DATE&endDate=$SMOKE_TEST_DATE"

API_URL=$(gcloud run services describe "$API_SERVICE_NAME" \
  --project="$GCP_PROJECT_ID" \
  --region="$GCP_REGION" \
  --format='value(status.url)')

DASHBOARD_URL=$(gcloud run services describe "$DASHBOARD_SERVICE_NAME" \
  --project="$GCP_PROJECT_ID" \
  --region="$GCP_REGION" \
  --format='value(status.url)')

WORKER_READY_REVISION=$(gcloud run services describe "$WORKER_SERVICE_NAME" \
  --project="$GCP_PROJECT_ID" \
  --region="$GCP_REGION" \
  --format='value(status.latestReadyRevisionName)')

if [ -z "$API_URL" ] || [ -z "$DASHBOARD_URL" ] || [ -z "$WORKER_READY_REVISION" ]; then
  echo "missing Cloud Run service URL or worker ready revision for $ENVIRONMENT" >&2
  exit 1
fi

REPORTING_API_TOKEN=$(gcloud secrets versions access latest \
  --project="$GCP_PROJECT_ID" \
  --secret=REPORTING_API_TOKEN)

echo "Smoke testing API health for $ENVIRONMENT"
curl --fail --silent --show-error "$API_URL/healthz/" >/dev/null
curl --fail --silent --show-error "$API_URL/readyz" >/dev/null

echo "Smoke testing authenticated reporting route for $ENVIRONMENT"
curl --fail --silent --show-error \
  -H "Authorization: Bearer $REPORTING_API_TOKEN" \
  "$API_URL$REPORTING_PATH?$REPORTING_QUERY" >/dev/null

echo "Smoke testing dashboard entrypoint for $ENVIRONMENT"
curl --fail --silent --show-error "$DASHBOARD_URL/" >/dev/null

echo "Smoke test complete for $ENVIRONMENT"

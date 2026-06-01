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

SMOKE_TEST_END_DATE=${SMOKE_TEST_END_DATE:-${SMOKE_TEST_DATE:-$(date -u +%F)}}
SMOKE_TEST_START_DATE=${SMOKE_TEST_START_DATE:-$SMOKE_TEST_END_DATE}
REPORTING_PATH=${SMOKE_TEST_REPORTING_PATH:-/api/reporting/meta-order-value}
REPORTING_QUERY="startDate=$SMOKE_TEST_START_DATE&endDate=$SMOKE_TEST_END_DATE&limit=${SMOKE_TEST_REPORTING_LIMIT:-5}"
CONFIDENCE_SMOKE_PATH=${SMOKE_TEST_CONFIDENCE_PATH:-/api/reporting/orders}
CONFIDENCE_SMOKE_QUERY="startDate=$SMOKE_TEST_START_DATE&endDate=$SMOKE_TEST_END_DATE&limit=${SMOKE_TEST_CONFIDENCE_LIMIT:-5}"

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

validate_meta_order_value_response() {
  RESPONSE_FILE="$1"
  RESPONSE_START_DATE="$2"
  RESPONSE_END_DATE="$3"

  node - "$RESPONSE_FILE" "$RESPONSE_START_DATE" "$RESPONSE_END_DATE" <<'JS'
const [responseFile, expectedStartDate, expectedEndDate] = process.argv.slice(2);
const fs = await import('node:fs/promises');
const payload = JSON.parse(await fs.readFile(responseFile, 'utf8'));

if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
  throw new Error('response body must be a JSON object');
}

const scope = payload.scope;
if (!scope || typeof scope !== 'object' || !Number.isInteger(scope.organizationId) || scope.organizationId <= 0) {
  throw new Error('response scope.organizationId must be a positive integer');
}

const dateRange = payload.range;
if (
  !dateRange ||
  typeof dateRange !== 'object' ||
  dateRange.startDate !== expectedStartDate ||
  dateRange.endDate !== expectedEndDate
) {
  throw new Error('response range does not match smoke-test query');
}

const pagination = payload.pagination;
if (
  !pagination ||
  typeof pagination !== 'object' ||
  !Number.isInteger(pagination.limit) ||
  !Number.isInteger(pagination.offset)
) {
  throw new Error('response pagination is missing required integers');
}

const totals = payload.totals;
if (!totals || typeof totals !== 'object' || Array.isArray(totals)) {
  throw new Error('response totals object is required');
}

if (!Array.isArray(payload.rows)) {
  throw new Error('response rows must be an array');
}

for (const key of ['attributedRevenue', 'purchaseCount', 'spend', 'roas']) {
  const value = totals[key];
  if (value !== null && typeof value !== 'number') {
    throw new Error(`response totals.${key} must be numeric or null`);
  }
}
JS
}

validate_confidence_orders_response() {
  RESPONSE_FILE="$1"

  node - "$RESPONSE_FILE" <<'JS'
const [responseFile] = process.argv.slice(2);
const fs = await import('node:fs/promises');
const payload = JSON.parse(await fs.readFile(responseFile, 'utf8'));

if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
  throw new Error('confidence smoke response body must be a JSON object');
}

if (!Array.isArray(payload.rows)) {
  throw new Error('confidence smoke response rows must be an array');
}

payload.rows.forEach((row, index) => {
  if (!row || typeof row !== 'object' || Array.isArray(row)) {
    throw new Error(`confidence smoke row ${index} must be an object`);
  }

  if (!Object.hasOwn(row, 'confidenceScore')) {
    throw new Error(`confidence smoke row ${index} is missing confidenceScore`);
  }

  const confidenceScore = row.confidenceScore;
  if (
    confidenceScore !== null &&
    (typeof confidenceScore !== 'number' || confidenceScore < 0 || confidenceScore > 1)
  ) {
    throw new Error(`confidence smoke row ${index} has an invalid confidenceScore`);
  }

  for (const key of ['attributionSource', 'matchingMethod', 'lastAttributionRunAt']) {
    if (!Object.hasOwn(row, key)) {
      throw new Error(`confidence smoke row ${index} is missing ${key}`);
    }
  }
});
JS
}

echo "Smoke testing API health for $ENVIRONMENT"
curl --fail --silent --show-error "$API_URL/readyz" >/dev/null

echo "Smoke testing reporting auth for $ENVIRONMENT"
UNAUTH_STATUS=$(curl --silent --show-error \
  --output /dev/null \
  --write-out '%{http_code}' \
  "$API_URL$REPORTING_PATH?$REPORTING_QUERY")

if [ "$UNAUTH_STATUS" != "401" ]; then
  echo "expected unauthenticated $REPORTING_PATH smoke request to return 401, got $UNAUTH_STATUS" >&2
  exit 1
fi

echo "Smoke testing authenticated Meta order value route for $ENVIRONMENT"
RESPONSE_FILE=$(mktemp)
trap 'rm -f "$RESPONSE_FILE"' EXIT INT TERM

curl --fail --silent --show-error \
  -H "Authorization: Bearer $REPORTING_API_TOKEN" \
  "$API_URL$REPORTING_PATH?$REPORTING_QUERY" >"$RESPONSE_FILE"

validate_meta_order_value_response "$RESPONSE_FILE" "$SMOKE_TEST_START_DATE" "$SMOKE_TEST_END_DATE"

if [ "${SMOKE_TEST_VALIDATE_CONFIDENCE:-true}" = "true" ]; then
  echo "Smoke testing attribution confidence reporting route for $ENVIRONMENT"
  curl --fail --silent --show-error \
    -H "Authorization: Bearer $REPORTING_API_TOKEN" \
    "$API_URL$CONFIDENCE_SMOKE_PATH?$CONFIDENCE_SMOKE_QUERY" >"$RESPONSE_FILE"

  validate_confidence_orders_response "$RESPONSE_FILE"
fi

echo "Smoke testing dashboard entrypoint for $ENVIRONMENT"
curl --fail --silent --show-error "$DASHBOARD_URL/" >/dev/null

echo "Smoke test complete for $ENVIRONMENT"

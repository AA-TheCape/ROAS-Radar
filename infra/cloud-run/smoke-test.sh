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

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command $1 in PATH for smoke tests" >&2
    exit 127
  fi
}

require_var GCP_PROJECT_ID
require_var GCP_REGION
require_var API_SERVICE_NAME
require_var DASHBOARD_SERVICE_NAME
require_var WORKER_SERVICE_NAME

require_command gcloud
require_command curl
require_command date
require_command mktemp
require_command python3

if [ "${SMOKE_TEST_DEPENDENCY_CHECK_ONLY:-false}" = "true" ]; then
  echo "Smoke test dependency check complete for $ENVIRONMENT"
  exit 0
fi

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

  python3 - "$RESPONSE_FILE" "$RESPONSE_START_DATE" "$RESPONSE_END_DATE" <<'PY'
import json
import sys

response_file, expected_start_date, expected_end_date = sys.argv[1:4]

with open(response_file, encoding="utf-8") as handle:
    payload = json.load(handle)

if not isinstance(payload, dict):
    raise SystemExit("response body must be a JSON object")

scope = payload.get("scope")
if (
    not isinstance(scope, dict)
    or not isinstance(scope.get("organizationId"), int)
    or isinstance(scope.get("organizationId"), bool)
    or scope["organizationId"] <= 0
):
    raise SystemExit("response scope.organizationId must be a positive integer")

date_range = payload.get("range")
if (
    not isinstance(date_range, dict)
    or date_range.get("startDate") != expected_start_date
    or date_range.get("endDate") != expected_end_date
):
    raise SystemExit("response range does not match smoke-test query")

pagination = payload.get("pagination")
if (
    not isinstance(pagination, dict)
    or not isinstance(pagination.get("limit"), int)
    or isinstance(pagination.get("limit"), bool)
    or not isinstance(pagination.get("offset"), int)
    or isinstance(pagination.get("offset"), bool)
):
    raise SystemExit("response pagination is missing required integers")

totals = payload.get("totals")
if not isinstance(totals, dict):
    raise SystemExit("response totals object is required")

if not isinstance(payload.get("rows"), list):
    raise SystemExit("response rows must be an array")

for key in ("attributedRevenue", "purchaseCount", "spend", "roas"):
    value = totals.get(key)
    if value is not None and (not isinstance(value, (int, float)) or isinstance(value, bool)):
        raise SystemExit(f"response totals.{key} must be numeric or null")
PY
}

validate_confidence_orders_response() {
  RESPONSE_FILE="$1"

  python3 - "$RESPONSE_FILE" <<'PY'
import json
import sys

response_file = sys.argv[1]

with open(response_file, encoding="utf-8") as handle:
    payload = json.load(handle)

if not isinstance(payload, dict):
    raise SystemExit("confidence smoke response body must be a JSON object")

rows = payload.get("rows")
if not isinstance(rows, list):
    raise SystemExit("confidence smoke response rows must be an array")

for index, row in enumerate(rows):
    if not isinstance(row, dict):
        raise SystemExit(f"confidence smoke row {index} must be an object")

    if "confidenceScore" not in row:
        raise SystemExit(f"confidence smoke row {index} is missing confidenceScore")

    confidence_score = row["confidenceScore"]
    if confidence_score is not None and (
        not isinstance(confidence_score, (int, float))
        or isinstance(confidence_score, bool)
        or confidence_score < 0
        or confidence_score > 1
    ):
        raise SystemExit(f"confidence smoke row {index} has an invalid confidenceScore")

    for key in ("attributionSource", "matchingMethod", "lastAttributionRunAt"):
        if key not in row:
            raise SystemExit(f"confidence smoke row {index} is missing {key}")
PY
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

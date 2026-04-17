#!/bin/sh

set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <staging|production>" >&2
  exit 1
fi

ENVIRONMENT="$1"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
ENV_FILE="$ROOT_DIR/infra/cloud-run/environments/$ENVIRONMENT.env"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM

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

ALERT_NOTIFICATION_CHANNELS_JSON="[]"
if [ -n "${ALERT_NOTIFICATION_CHANNELS:-}" ]; then
  ALERT_NOTIFICATION_CHANNELS_JSON=$(python3 -c "
import json
import sys

raw = sys.argv[1] if len(sys.argv) > 1 else ''
items = [value.strip() for value in raw.split(',') if value.strip()]
sys.stdout.write(json.dumps(items))
" "$ALERT_NOTIFICATION_CHANNELS")
fi

RUNBOOK_BASE_URL=${RUNBOOK_BASE_URL:-"https://github.com/AA-TheCape/ROAS-Radar/blob/$(git -C "$ROOT_DIR" rev-parse --abbrev-ref HEAD)/docs/runbooks"}
DASHBOARD_DISPLAY_NAME=${OBSERVABILITY_DASHBOARD_DISPLAY_NAME:-"ROAS Radar ${ENVIRONMENT} Pipeline Health"}

escape_for_sed() {
  printf '%s' "$1" | sed 's/[|&]/\\&/g'
}

render_template() {
  TEMPLATE_PATH="$1"
  OUTPUT_PATH="$2"

  sed \
    -e "s|__PROJECT_ID__|$(escape_for_sed "$GCP_PROJECT_ID")|g" \
    -e "s|__ENVIRONMENT__|$(escape_for_sed "$ENVIRONMENT")|g" \
    -e "s|__ALERT_NOTIFICATION_CHANNELS__|$(escape_for_sed "$ALERT_NOTIFICATION_CHANNELS_JSON")|g" \
    -e "s|__RUNBOOK_URL_INGESTION__|$(escape_for_sed "$RUNBOOK_BASE_URL/ingestion-failures.md")|g" \
    -e "s|__RUNBOOK_URL_ATTRIBUTION__|$(escape_for_sed "$RUNBOOK_BASE_URL/attribution-worker-backlog.md")|g" \
    -e "s|__RUNBOOK_URL_API_LATENCY__|$(escape_for_sed "$RUNBOOK_BASE_URL/api-latency.md")|g" \
    -e "s|__DASHBOARD_DISPLAY_NAME__|$(escape_for_sed "$DASHBOARD_DISPLAY_NAME")|g" \
    "$TEMPLATE_PATH" >"$OUTPUT_PATH"
}

apply_log_metric() {
  TEMPLATE_PATH="$1"
  OUTPUT_PATH="$TMP_DIR/$(basename "$TEMPLATE_PATH")"
  render_template "$TEMPLATE_PATH" "$OUTPUT_PATH"
  METRIC_NAME=$(python3 -c "
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as fh:
    payload = json.load(fh)
sys.stdout.write(payload['name'])
" "$OUTPUT_PATH")

  if gcloud logging metrics describe "$METRIC_NAME" --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    gcloud logging metrics update "$METRIC_NAME" \
      --project="$GCP_PROJECT_ID" \
      --config-from-file="$OUTPUT_PATH" >/dev/null
  else
    gcloud logging metrics create "$METRIC_NAME" \
      --project="$GCP_PROJECT_ID" \
      --config-from-file="$OUTPUT_PATH" >/dev/null
  fi

  echo "Applied log metric $METRIC_NAME"
}

delete_by_display_name() {
  RESOURCE_KIND="$1"
  DISPLAY_NAME="$2"
  LIST_URL="$3"

  ACCESS_TOKEN=$(gcloud auth print-access-token)
  RESPONSE_FILE="$TMP_DIR/${RESOURCE_KIND}.json"

  curl -sS -H "Authorization: Bearer $ACCESS_TOKEN" "$LIST_URL" >"$RESPONSE_FILE"

  MATCHING_NAME=$(python3 -c "
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as fh:
    payload = json.load(fh)
display_name = sys.argv[2]
resource_kind = sys.argv[3]
collection = payload.get(resource_kind, [])
match = next((entry for entry in collection if entry.get('displayName') == display_name), None)
sys.stdout.write(match.get('name', '') if match else '')
" "$RESPONSE_FILE" "$DISPLAY_NAME" "$RESOURCE_KIND")

  if [ -n "$MATCHING_NAME" ]; then
    curl -sS -X DELETE -H "Authorization: Bearer $ACCESS_TOKEN" "https://monitoring.googleapis.com/v1/$MATCHING_NAME" >/dev/null
  fi
}

create_monitoring_resource() {
  RESOURCE_KIND="$1"
  TEMPLATE_PATH="$2"
  COLLECTION_URL="$3"

  OUTPUT_PATH="$TMP_DIR/${RESOURCE_KIND}-$(basename "$TEMPLATE_PATH")"
  render_template "$TEMPLATE_PATH" "$OUTPUT_PATH"
  DISPLAY_NAME=$(python3 -c "
import json
import sys

with open(sys.argv[1], 'r', encoding='utf-8') as fh:
    payload = json.load(fh)
sys.stdout.write(payload['displayName'])
" "$OUTPUT_PATH")

  delete_by_display_name "$RESOURCE_KIND" "$DISPLAY_NAME" "$COLLECTION_URL"

  ACCESS_TOKEN=$(gcloud auth print-access-token)
  curl -sS -X POST \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H "Content-Type: application/json" \
    -d @"$OUTPUT_PATH" \
    "$COLLECTION_URL" >/dev/null

  echo "Applied $RESOURCE_KIND $DISPLAY_NAME"
}

for metric_template in "$SCRIPT_DIR"/log-metrics/*.json; do
  apply_log_metric "$metric_template"
done

create_monitoring_resource \
  "dashboards" \
  "$SCRIPT_DIR/dashboard.json" \
  "https://monitoring.googleapis.com/v1/projects/$GCP_PROJECT_ID/dashboards"

for policy_template in "$SCRIPT_DIR"/alert-policies/*.json; do
  create_monitoring_resource \
    "alertPolicies" \
    "$policy_template" \
    "https://monitoring.googleapis.com/v3/projects/$GCP_PROJECT_ID/alertPolicies"
done

echo "Monitoring configuration applied for $ENVIRONMENT"

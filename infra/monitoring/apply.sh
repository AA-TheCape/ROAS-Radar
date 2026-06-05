#!/bin/sh

set -eu

usage() {
  echo "usage: $0 <environment>" >&2
}

if [ "$#" -ne 1 ]; then
  usage
  exit 1
fi

ENVIRONMENT="$1"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
ENV_FILE="$REPO_ROOT/infra/cloud-run/environments/$ENVIRONMENT.env"

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

escape_for_sed() {
  printf '%s' "$1" | sed 's/[\/&]/\\&/g'
}

json_array_from_csv() {
  CSV_VALUE="$1"
  if [ -z "$CSV_VALUE" ]; then
    printf '[]'
    return
  fi

  node -e 'console.log(JSON.stringify(process.argv[1].split(",").map((part) => part.trim()).filter(Boolean)))' "$CSV_VALUE"
}

build_runbook_url() {
  RUNBOOK_FILE="$1"
  if [ -n "${RUNBOOK_BASE_URL:-}" ]; then
    printf '%s/%s' "${RUNBOOK_BASE_URL%/}" "$RUNBOOK_FILE"
  else
    printf 'docs/runbooks/%s' "$RUNBOOK_FILE"
  fi
}

render_template() {
  sed \
    -e "s|__ENVIRONMENT__|$(escape_for_sed "$ENVIRONMENT")|g" \
    -e "s|__RUNBOOK_URL_INGESTION__|$(escape_for_sed "$(build_runbook_url "ingestion-failures.md")")|g" \
    -e "s|__RUNBOOK_URL_META_ORDER_VALUE__|$(escape_for_sed "$(build_runbook_url "meta-order-value-ingestion.md")")|g" \
    -e "s|__RUNBOOK_URL_ATTRIBUTION__|$(escape_for_sed "$(build_runbook_url "attribution-worker-backlog.md")")|g" \
    -e "s|__RUNBOOK_URL_ATTRIBUTION_QA__|$(escape_for_sed "$(build_runbook_url "attribution-qa-tooling.md")")|g" \
    -e "s|__RUNBOOK_URL_ATTRIBUTION_COMPLETENESS__|$(escape_for_sed "$(build_runbook_url "attribution-completeness.md")")|g" \
    -e "s|__RUNBOOK_URL_API_LATENCY__|$(escape_for_sed "$(build_runbook_url "api-latency.md")")|g" \
    -e "s|__RUNBOOK_URL_DATA_QUALITY__|$(escape_for_sed "$(build_runbook_url "identity-data-quality.md")")|g" \
    -e "s|__RUNBOOK_URL_MMM__|$(escape_for_sed "$(build_runbook_url "mmm-pipelines.md")")|g" \
    -e "s|\"__ALERT_NOTIFICATION_CHANNELS__\"|$notification_channels_sed|g" \
    -e "s|__ALERT_NOTIFICATION_CHANNELS__|$notification_channels_sed|g" \
    -e "s|__DASHBOARD_DISPLAY_NAME__|$(escape_for_sed "$OBSERVABILITY_DASHBOARD_DISPLAY_NAME")|g"
}

normalize_json_file() {
  node -e '
    const fs = require("fs");
    const file = process.argv[1];
    const config = JSON.parse(fs.readFileSync(file, "utf8"));
    if (typeof config.notificationChannels === "string") {
      config.notificationChannels = JSON.parse(config.notificationChannels);
    }
    if (Array.isArray(config.notificationChannels) && config.notificationChannels.length === 0) {
      delete config.notificationChannels;
    }
    fs.writeFileSync(file, `${JSON.stringify(config, null, 2)}\n`);
  ' "$1"
}

require_var GCP_PROJECT_ID
require_var OBSERVABILITY_DASHBOARD_DISPLAY_NAME

if [ "$ENVIRONMENT" = "production" ] && [ -z "${ALERT_NOTIFICATION_CHANNELS:-}" ]; then
  echo "production ALERT_NOTIFICATION_CHANNELS must list on-call notification channel resource names" >&2
  exit 1
fi

notification_channels_json=$(json_array_from_csv "${ALERT_NOTIFICATION_CHANNELS:-}")
notification_channels_sed=$(escape_for_sed "$notification_channels_json")
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

node "$SCRIPT_DIR/validate.mjs"

for metric_file in "$SCRIPT_DIR"/log-metrics/*.json; do
  metric_name=$(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1], "utf8")).name)' "$metric_file")
  rendered="$tmp_dir/$(basename "$metric_file")"
  render_template <"$metric_file" >"$rendered"
  normalize_json_file "$rendered"

  if gcloud logging metrics describe "$metric_name" --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    gcloud logging metrics update "$metric_name" --project="$GCP_PROJECT_ID" --config-from-file="$rendered"
  else
    gcloud logging metrics create "$metric_name" --project="$GCP_PROJECT_ID" --config-from-file="$rendered"
  fi
done

for policy_file in "$SCRIPT_DIR"/alert-policies/*.json; do
  rendered="$tmp_dir/$(basename "$policy_file")"
  render_template <"$policy_file" >"$rendered"
  normalize_json_file "$rendered"
  display_name=$(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1], "utf8")).displayName)' "$rendered")
  existing_name=$(
    gcloud monitoring policies list \
      --project="$GCP_PROJECT_ID" \
      --filter="displayName=\"$display_name\"" \
      --format="value(name)" \
      --limit=1 2>/dev/null || true
  )

  if [ -n "$existing_name" ]; then
    gcloud monitoring policies update "$existing_name" --project="$GCP_PROJECT_ID" --policy-from-file="$rendered"
  else
    gcloud monitoring policies create --project="$GCP_PROJECT_ID" --policy-from-file="$rendered"
  fi
done

dashboard_file="$tmp_dir/dashboard.json"
render_template <"$SCRIPT_DIR/dashboard.json" >"$dashboard_file"
normalize_json_file "$dashboard_file"
dashboard_name=$(
  gcloud monitoring dashboards list \
    --project="$GCP_PROJECT_ID" \
    --filter="displayName=\"$OBSERVABILITY_DASHBOARD_DISPLAY_NAME\"" \
    --format="value(name)" \
    --limit=1 2>/dev/null || true
)

if [ -n "$dashboard_name" ]; then
  gcloud monitoring dashboards update "$dashboard_name" --project="$GCP_PROJECT_ID" --config-from-file="$dashboard_file"
else
  gcloud monitoring dashboards create --project="$GCP_PROJECT_ID" --config-from-file="$dashboard_file"
fi

echo "Monitoring configuration applied for $ENVIRONMENT."

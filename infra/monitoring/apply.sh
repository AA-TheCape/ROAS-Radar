#!/bin/sh

set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <staging|production>" >&2
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

for var in GCP_PROJECT_ID GCP_REGION OBSERVABILITY_DASHBOARD_DISPLAY_NAME; do
  require_var "$var"
done

escape_for_sed() {
  printf '%s' "$1" | sed 's/[\/&]/\\&/g'
}

build_runbook_url() {
  path="$1"

  if [ -z "${RUNBOOK_BASE_URL:-}" ]; then
    printf '%s' ""
    return
  fi

  printf '%s' "${RUNBOOK_BASE_URL%/}/$path"
}

notification_channels_json='[]'
if [ -n "${ALERT_NOTIFICATION_CHANNELS:-}" ]; then
  notification_channels_json=$(printf '%s' "$ALERT_NOTIFICATION_CHANNELS" | awk '
    BEGIN { FS=","; printf "[" }
    {
      first = 1
      for (i = 1; i <= NF; i++) {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
        if ($i != "") {
          if (!first) {
            printf ", "
          }
          printf "\"%s\"", $i
          first = 0
        }
      }
    }
    END { printf "]" }
  ')
fi

render_template() {
  sed \
    -e "s|__ENVIRONMENT__|$(escape_for_sed "$ENVIRONMENT")|g" \
    -e "s|__RUNBOOK_URL_INGESTION__|$(escape_for_sed "$(build_runbook_url "ingestion-failures.md")")|g" \
    -e "s|__RUNBOOK_URL_ATTRIBUTION__|$(escape_for_sed "$(build_runbook_url "attribution-worker-backlog.md")")|g" \
    -e "s|__RUNBOOK_URL_ATTRIBUTION_COMPLETENESS__|$(escape_for_sed "$(build_runbook_url "attribution-completeness.md")")|g" \
    -e "s|__RUNBOOK_URL_API_LATENCY__|$(escape_for_sed "$(build_runbook_url "api-latency.md")")|g" \
    -e "s|__RUNBOOK_URL_DATA_QUALITY__|$(escape_for_sed "$(build_runbook_url "identity-data-quality.md")")|g" \
    -e "s|__ALERT_NOTIFICATION_CHANNELS__|$notification_channels_json|g" \
    -e "s|__DASHBOARD_DISPLAY_NAME__|$(escape_for_sed "$OBSERVABILITY_DASHBOARD_DISPLAY_NAME")|g"
}

python3 "$SCRIPT_DIR/validate.py"

upsert_log_metric() {
  template_path="$1"
  metric_name=$(sed -n 's/.*"name": "\([^"]*\)".*/\1/p' "$template_path" | head -n 1)

  if [ -z "$metric_name" ]; then
    echo "unable to determine metric name from $template_path" >&2
    exit 1
  fi

  rendered=$(mktemp)
  render_template <"$template_path" >"$rendered"

  if gcloud logging metrics describe "$metric_name" --project="$GCP_PROJECT_ID" >/dev/null 2>&1; then
    gcloud logging metrics update "$metric_name" \
      --project="$GCP_PROJECT_ID" \
      --config-from-file="$rendered"
  else
    gcloud logging metrics create "$metric_name" \
      --project="$GCP_PROJECT_ID" \
      --config-from-file="$rendered"
  fi

  rm -f "$rendered"
}

upsert_alert_policy() {
  template_path="$1"
  rendered=$(mktemp)
  render_template <"$template_path" >"$rendered"

  display_name=$(sed -n 's/.*"displayName": "\([^"]*\)".*/\1/p' "$rendered" | head -n 1)

  if [ -z "$display_name" ]; then
    echo "unable to determine alert display name from $template_path" >&2
    exit 1
  fi

  existing_name=$(gcloud alpha monitoring policies list \
    --project="$GCP_PROJECT_ID" \
    --format='value(name)' \
    --filter="displayName=\"$display_name\"" | head -n 1 || true)

  if [ -n "$existing_name" ]; then
    gcloud alpha monitoring policies delete "$existing_name" \
      --project="$GCP_PROJECT_ID" \
      --quiet
  fi

  gcloud alpha monitoring policies create \
    --project="$GCP_PROJECT_ID" \
    --policy-from-file="$rendered"

  rm -f "$rendered"
}

upsert_dashboard() {
  template_path="$1"
  rendered=$(mktemp)
  render_template <"$template_path" >"$rendered"

  display_name=$(sed -n 's/.*"displayName": "\([^"]*\)".*/\1/p' "$rendered" | head -n 1)

  if [ -z "$display_name" ]; then
    echo "unable to determine dashboard display name from $template_path" >&2
    exit 1
  fi

  existing_name=$(gcloud monitoring dashboards list \
    --project="$GCP_PROJECT_ID" \
    --format='value(name)' \
    --filter="displayName=\"$display_name\"" | head -n 1 || true)

  if [ -n "$existing_name" ]; then
    gcloud monitoring dashboards delete "$existing_name" \
      --project="$GCP_PROJECT_ID" \
      --quiet
  fi

  gcloud monitoring dashboards create \
    --project="$GCP_PROJECT_ID" \
    --config-from-file="$rendered"

  rm -f "$rendered"
}

for metric_template in "$SCRIPT_DIR"/log-metrics/*.json; do
  upsert_log_metric "$metric_template"
done

for alert_template in "$SCRIPT_DIR"/alert-policies/*.json; do
  upsert_alert_policy "$alert_template"
done

upsert_dashboard "$SCRIPT_DIR/dashboard.json"

echo "Applied monitoring configuration for $ENVIRONMENT"

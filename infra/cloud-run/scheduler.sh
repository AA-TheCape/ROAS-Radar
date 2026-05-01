#!/bin/sh

set -eu

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <environment> <meta-ads|meta-order-value|google-ads|retention|data-quality|identity-graph-backfill|order-attribution-materialization> <status|pause|resume>" >&2
  exit 1
fi

ENVIRONMENT="$1"
PIPELINE="$2"
ACTION="$3"
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

case "$PIPELINE" in
  meta-ads)
    JOB_NAME="$META_ADS_SCHEDULER_JOB_NAME"
    ;;
  meta-order-value)
    JOB_NAME="$META_ADS_ORDER_VALUE_SCHEDULER_JOB_NAME"
    ;;
  google-ads)
    JOB_NAME="$GOOGLE_ADS_SCHEDULER_JOB_NAME"
    ;;
  retention)
    JOB_NAME="$RETENTION_SCHEDULER_JOB_NAME"
    ;;
  data-quality)
    JOB_NAME="$DATA_QUALITY_SCHEDULER_JOB_NAME"
    ;;
  identity-graph-backfill)
    JOB_NAME="$IDENTITY_GRAPH_BACKFILL_SCHEDULER_JOB_NAME"
    ;;
  order-attribution-materialization)
    JOB_NAME="$ORDER_ATTRIBUTION_MATERIALIZATION_SCHEDULER_JOB_NAME"
    ;;
  *)
    echo "unsupported pipeline: $PIPELINE" >&2
    exit 1
    ;;
esac

case "$ACTION" in
  status)
    gcloud scheduler jobs describe "$JOB_NAME" \
      --project="$GCP_PROJECT_ID" \
      --location="$GCP_REGION"
    ;;
  pause)
    gcloud scheduler jobs pause "$JOB_NAME" \
      --project="$GCP_PROJECT_ID" \
      --location="$GCP_REGION"
    ;;
  resume)
    gcloud scheduler jobs resume "$JOB_NAME" \
      --project="$GCP_PROJECT_ID" \
      --location="$GCP_REGION"
    ;;
  *)
    echo "unsupported action: $ACTION" >&2
    exit 1
    ;;
esac

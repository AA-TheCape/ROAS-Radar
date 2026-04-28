#!/bin/sh

set -eu

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <environment> <deploy-metadata-file> [previous|latest]" >&2
  exit 1
fi

ENVIRONMENT="$1"
DEPLOY_METADATA_FILE="$2"
ROLLBACK_TARGET="${3:-previous}"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
ENV_FILE="$SCRIPT_DIR/environments/$ENVIRONMENT.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "missing environment file: $ENV_FILE" >&2
  exit 1
fi

if [ ! -f "$DEPLOY_METADATA_FILE" ]; then
  echo "missing deploy metadata file: $DEPLOY_METADATA_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
. "$ENV_FILE"
# shellcheck disable=SC1090
. "$DEPLOY_METADATA_FILE"

require_var() {
  eval "value=\${$1:-}"
  if [ -z "$value" ]; then
    echo "missing required variable $1" >&2
    exit 1
  fi
}

require_var GCP_PROJECT_ID
require_var GCP_REGION
require_var API_SERVICE_NAME
require_var DASHBOARD_SERVICE_NAME
require_var WORKER_SERVICE_NAME

case "$ROLLBACK_TARGET" in
  previous)
    API_TARGET_REVISION=${API_PREVIOUS_REVISION:-}
    WORKER_TARGET_REVISION=${WORKER_PREVIOUS_REVISION:-}
    DASHBOARD_TARGET_REVISION=${DASHBOARD_PREVIOUS_REVISION:-}
    ;;
  latest)
    API_TARGET_REVISION=${API_LATEST_REVISION:-}
    WORKER_TARGET_REVISION=${WORKER_LATEST_REVISION:-}
    DASHBOARD_TARGET_REVISION=${DASHBOARD_LATEST_REVISION:-}
    ;;
  *)
    echo "invalid rollback target: $ROLLBACK_TARGET" >&2
    exit 1
    ;;
esac

update_service_traffic() {
  SERVICE_NAME="$1"
  REVISION_NAME="$2"

  if [ -z "$REVISION_NAME" ]; then
    echo "missing target revision for $SERVICE_NAME" >&2
    exit 1
  fi

  echo "Routing $SERVICE_NAME to revision $REVISION_NAME"
  gcloud run services update-traffic "$SERVICE_NAME" \
    --project="$GCP_PROJECT_ID" \
    --region="$GCP_REGION" \
    --to-revisions="$REVISION_NAME=100"
}

update_service_traffic "$API_SERVICE_NAME" "$API_TARGET_REVISION"
update_service_traffic "$WORKER_SERVICE_NAME" "$WORKER_TARGET_REVISION"
update_service_traffic "$DASHBOARD_SERVICE_NAME" "$DASHBOARD_TARGET_REVISION"

echo "Rollback target '$ROLLBACK_TARGET' applied for $ENVIRONMENT"

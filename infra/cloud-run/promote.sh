#!/bin/sh

set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <dev|staging|production>" >&2
  exit 1
fi

TARGET_ENVIRONMENT="$1"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
STATE_DIR=${DEPLOY_STATE_DIR:-"$SCRIPT_DIR/.deploy-state"}

case "$TARGET_ENVIRONMENT" in
  dev|staging|production)
    ;;
  *)
    echo "invalid promotion target: $TARGET_ENVIRONMENT" >&2
    exit 1
    ;;
esac

mkdir -p "$STATE_DIR"

promote_environment() {
  ENVIRONMENT="$1"
  METADATA_FILE="$STATE_DIR/$ENVIRONMENT.env"

  echo "Deploying $ENVIRONMENT"
  DEPLOY_METADATA_FILE="$METADATA_FILE" \
  SHORT_SHA="${SHORT_SHA:-}" \
  SKIP_BUILDS="${SKIP_BUILDS:-false}" \
  RUN_MIGRATIONS_ON_DEPLOY="${RUN_MIGRATIONS_ON_DEPLOY:-true}" \
  APPLY_MONITORING_ON_DEPLOY="${APPLY_MONITORING_ON_DEPLOY:-true}" \
  sh "$SCRIPT_DIR/deploy.sh" "$ENVIRONMENT"

  echo "Running smoke tests for $ENVIRONMENT"
  SMOKE_TEST_DATE="${SMOKE_TEST_DATE:-}" \
  SMOKE_TEST_REPORTING_PATH="${SMOKE_TEST_REPORTING_PATH:-}" \
  sh "$SCRIPT_DIR/smoke-test.sh" "$ENVIRONMENT"

  if [ "$ENVIRONMENT" = "staging" ] && [ "${RUN_STAGING_ROLLBACK_DRILL:-false}" = "true" ]; then
    echo "Running staging rollback drill"
    sh "$SCRIPT_DIR/rollback.sh" "$ENVIRONMENT" "$METADATA_FILE" previous
    sh "$SCRIPT_DIR/smoke-test.sh" "$ENVIRONMENT"
    sh "$SCRIPT_DIR/rollback.sh" "$ENVIRONMENT" "$METADATA_FILE" latest
    sh "$SCRIPT_DIR/smoke-test.sh" "$ENVIRONMENT"
  fi
}

promote_environment dev

if [ "$TARGET_ENVIRONMENT" = "dev" ]; then
  exit 0
fi

promote_environment staging

if [ "$TARGET_ENVIRONMENT" = "staging" ]; then
  exit 0
fi

promote_environment production

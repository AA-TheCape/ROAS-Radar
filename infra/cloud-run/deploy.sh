# added required vars:
RETENTION_JOB_NAME
RETENTION_SCHEDULER_JOB_NAME
RETENTION_JOB_SERVICE_ACCOUNT_NAME
RETENTION_SCHEDULE

# deploys:
gcloud run jobs deploy "$RETENTION_JOB_NAME" \
  --command=npm \
  --args=run,session-attribution:retention:start \
  --set-env-vars="...SESSION_ATTRIBUTION_RETENTION_*..."

# configures scheduler:
gcloud scheduler jobs create|update http "$RETENTION_SCHEDULER_JOB_NAME" \
  --schedule="$RETENTION_SCHEDULE" \
  --uri="https://run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$GCP_PROJECT_ID/jobs/$RETENTION_JOB_NAME:run" \
  --http-method=POST \
  --oauth-service-account-email="$RETENTION_SA"

# added:
require_var RETENTION_JOB_SERVICE_ACCOUNT_NAME
RETENTION_SA=$(create_service_account ...)
grant_project_role "serviceAccount:$RETENTION_SA" "roles/cloudsql.client"
grant_project_role "serviceAccount:$RETENTION_SA" "roles/logging.logWriter"
grant_project_role "serviceAccount:$RETENTION_SA" "roles/monitoring.metricWriter"
grant_project_role "serviceAccount:$RETENTION_SA" "roles/run.invoker"
grant_secret_accessor "$RETENTION_SA" "$SECRET"

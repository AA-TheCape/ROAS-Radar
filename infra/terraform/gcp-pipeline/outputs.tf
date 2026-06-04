output "cloud_sql_connection_name" {
  description = "Cloud SQL connection name for Cloud Run --add-cloudsql-instances."
  value       = google_sql_database_instance.postgres.connection_name
}

output "artifact_registry_repository" {
  description = "Docker repository used by Cloud Build and Cloud Run."
  value       = google_artifact_registry_repository.docker.name
}

output "service_accounts" {
  description = "Provisioned runtime service account emails."
  value       = { for key, account in google_service_account.accounts : key => account.email }
}

output "secret_names" {
  description = "Secret Manager secret ids that must receive per-environment payloads before deploy."
  value       = sort(tolist(local.secret_names))
}

output "cloud_run_services" {
  description = "Primary Cloud Run service names."
  value = {
    api       = google_cloud_run_v2_service.api.name
    worker    = google_cloud_run_v2_service.worker.name
    dashboard = google_cloud_run_v2_service.dashboard.name
  }
}

output "cloud_run_jobs" {
  description = "Cloud Run Job names used by migrations and schedulers."
  value       = { for key, job in google_cloud_run_v2_job.jobs : key => job.name }
}

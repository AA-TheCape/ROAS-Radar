variable "project_id" {
  type        = string
  description = "GCP project that hosts one ROAS Radar environment."
}

variable "environment" {
  type        = string
  description = "Environment name used in resource names and labels."

  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "environment must be dev, staging, or production."
  }
}

variable "region" {
  type        = string
  description = "GCP region for Cloud Run, Cloud SQL, Scheduler, and Artifact Registry."
  default     = "us-central1"
}

variable "artifact_registry_repository" {
  type        = string
  description = "Artifact Registry Docker repository name."
  default     = "roas-radar"
}

variable "image_tag" {
  type        = string
  description = "Container image tag deployed by Terraform. Promotion scripts normally override runtime revisions after images are built."
  default     = "bootstrap"
}

variable "network_name" {
  type        = string
  description = "VPC network name for private service networking."
  default     = "roas-radar"
}

variable "database_name" {
  type        = string
  description = "Application database name."
  default     = "roas_radar"
}

variable "database_tier" {
  type        = string
  description = "Cloud SQL machine tier."
  default     = "db-custom-2-7680"
}

variable "database_availability_type" {
  type        = string
  description = "Cloud SQL availability type."
  default     = "ZONAL"
}

variable "database_disk_size_gb" {
  type        = number
  description = "Cloud SQL SSD disk size in GB."
  default     = 100
}

variable "database_deletion_protection" {
  type        = bool
  description = "Enable Cloud SQL deletion protection."
  default     = true
}

variable "db_app_password" {
  type        = string
  description = "Password for roas_app."
  sensitive   = true
}

variable "db_migrator_password" {
  type        = string
  description = "Password for roas_migrator."
  sensitive   = true
}

variable "db_readonly_password" {
  type        = string
  description = "Password for roas_readonly."
  sensitive   = true
}

variable "api_allowed_origins" {
  type        = string
  description = "Comma-separated CORS allow list for tracking and API requests."
}

variable "shopify_app_base_url" {
  type        = string
  description = "Public API base URL used by the Shopify app integration."
}

variable "dashboard_api_base_url" {
  type        = string
  description = "API URL used by the dashboard service."
}

variable "service_config" {
  type = object({
    api_min_instances       = number
    api_max_instances       = number
    api_concurrency         = number
    worker_min_instances    = number
    worker_max_instances    = number
    worker_concurrency      = number
    dashboard_min_instances = number
    dashboard_max_instances = number
    database_pool_max       = number
    worker_database_pool_max = number
    ads_sync_database_pool_max = number
  })
  description = "Cloud Run sizing and database pool settings."
  default = {
    api_min_instances          = 0
    api_max_instances          = 3
    api_concurrency            = 16
    worker_min_instances       = 1
    worker_max_instances       = 1
    worker_concurrency         = 2
    dashboard_min_instances    = 0
    dashboard_max_instances    = 2
    database_pool_max          = 5
    worker_database_pool_max   = 2
    ads_sync_database_pool_max = 2
  }
}

variable "schedules" {
  type = object({
    meta_ads                         = string
    meta_order_value                 = string
    google_ads                       = string
    meta_ads_metadata                = string
    google_ads_metadata              = string
    session_retention                = string
    data_quality                     = string
    identity_graph_backfill          = string
    order_attribution_materialization = string
    mmm_baseline                     = string
    mmm_bayesian                     = string
  })
  description = "Cloud Scheduler cron expressions."
  default = {
    meta_ads                          = "15 * * * *"
    meta_order_value                  = "20 * * * *"
    google_ads                        = "45 * * * *"
    meta_ads_metadata                 = "10 */6 * * *"
    google_ads_metadata               = "25 */6 * * *"
    session_retention                 = "0 3 * * *"
    data_quality                      = "20 3 * * *"
    identity_graph_backfill           = "35 3 * * *"
    order_attribution_materialization = "50 3 * * *"
    mmm_baseline                      = "15 4 * * 1"
    mmm_bayesian                      = "45 5 * * 1"
  }
}

variable "mmm_bayesian_freeze_id" {
  type        = string
  description = "Approved MMM calibration freeze id used by the bayesian_hierarchical_mmm_v1 Cloud Run Job. Leave empty until the release gate approves a freeze."
  default     = ""
}

variable "mmm_baseline_freeze_id" {
  type        = string
  description = "Approved MMM baseline calibration freeze id used by the baseline_linear_mmm_v1 Cloud Run Job. Set this to the promoted freeze for the target production calibration window before deploying the baseline job."
  default     = ""
}

variable "scheduler_time_zone" {
  type        = string
  description = "Default Cloud Scheduler timezone."
  default     = "America/Los_Angeles"
}

variable "paused_schedulers" {
  type        = set(string)
  description = "Scheduler keys to create in PAUSED state."
  default     = []
}

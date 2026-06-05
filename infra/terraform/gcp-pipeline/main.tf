locals {
  labels = {
    app         = "roas-radar"
    environment = var.environment
    managed_by  = "terraform"
  }

  service_accounts = {
    api                               = "roas-radar-api-${var.environment}"
    dashboard                         = "roas-radar-dashboard-${var.environment}"
    worker                            = "roas-radar-worker-${var.environment}"
    migrator                          = "roas-radar-migrator-${var.environment}"
    meta_ads                          = "roas-radar-meta-ads-${var.environment}"
    google_ads                        = "roas-radar-google-ads-${var.environment}"
    retention                         = "roas-radar-retention-${var.environment}"
    data_quality                      = "roas-radar-dq-${var.environment}"
    identity_graph_backfill           = "roas-radar-idbf-${var.environment}"
    order_attribution_materialization = "roas-radar-ordmat-${var.environment}"
    mmm_baseline                      = "roas-radar-mmm-${var.environment}"
    mmm_bayesian                      = "roas-radar-mmm-by-${var.environment}"
    scheduler                         = "roas-radar-scheduler-${var.environment}"
    deployer                          = "roas-radar-deployer-${var.environment}"
  }

  secret_names = toset([
    "DATABASE_URL",
    "MIGRATOR_DATABASE_URL",
    "REPORTING_API_TOKEN",
    "SHOPIFY_WEBHOOK_SECRET",
    "SHOPIFY_APP_API_KEY",
    "SHOPIFY_APP_API_SECRET",
    "SHOPIFY_APP_ENCRYPTION_KEY",
    "META_ADS_APP_SECRET",
    "META_ADS_ENCRYPTION_KEY",
    "GOOGLE_ADS_ENCRYPTION_KEY",
  ])

  backend_image   = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repository}/roas-radar-app:${var.image_tag}"
  dashboard_image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repository}/roas-radar-dashboard:${var.image_tag}"

  common_env = {
    NODE_ENV                  = "production"
    DATABASE_POOL_MIN         = "0"
    DATABASE_SSL              = "false"
    TRACKING_ALLOWED_ORIGINS  = var.api_allowed_origins
    API_JSON_BODY_LIMIT       = "20mb"
    TRACKING_BODY_LIMIT       = "20mb"
    SHOPIFY_WEBHOOK_BODY_LIMIT = "20mb"
    SHOPIFY_APP_BASE_URL      = var.shopify_app_base_url
    SHOPIFY_APP_API_VERSION   = "2026-01"
    SHOPIFY_APP_SCOPES        = "read_orders"
    META_ADS_API_VERSION      = "v25.0"
    GOOGLE_ADS_API_VERSION    = "v22"
  }

  common_secret_env = {
    DATABASE_URL              = "DATABASE_URL"
    REPORTING_API_TOKEN       = "REPORTING_API_TOKEN"
    SHOPIFY_WEBHOOK_SECRET    = "SHOPIFY_WEBHOOK_SECRET"
    SHOPIFY_APP_API_KEY       = "SHOPIFY_APP_API_KEY"
    SHOPIFY_APP_API_SECRET    = "SHOPIFY_APP_API_SECRET"
    SHOPIFY_APP_ENCRYPTION_KEY = "SHOPIFY_APP_ENCRYPTION_KEY"
    META_ADS_APP_SECRET       = "META_ADS_APP_SECRET"
    META_ADS_ENCRYPTION_KEY   = "META_ADS_ENCRYPTION_KEY"
    GOOGLE_ADS_ENCRYPTION_KEY = "GOOGLE_ADS_ENCRYPTION_KEY"
  }

  jobs = {
    migrator = {
      name            = "roas-radar-migrate-${var.environment}"
      service_account = "migrator"
      args            = ["run", "db:migrate:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = "1" }
      secrets         = { DATABASE_URL = "MIGRATOR_DATABASE_URL" }
    }
    meta_ads = {
      name            = "roas-radar-meta-ads-sync-${var.environment}"
      service_account = "meta_ads"
      args            = ["run", "meta-ads:sync:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = tostring(var.service_config.ads_sync_database_pool_max), META_ADS_WORKER_LOOP = "false", META_ADS_JOB_MODE = "spend" }
      secrets         = { DATABASE_URL = "DATABASE_URL", META_ADS_APP_SECRET = "META_ADS_APP_SECRET", META_ADS_ENCRYPTION_KEY = "META_ADS_ENCRYPTION_KEY" }
    }
    meta_order_value = {
      name            = "roas-radar-meta-order-value-sync-${var.environment}"
      service_account = "meta_ads"
      args            = ["run", "meta-ads:order-value:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = tostring(var.service_config.ads_sync_database_pool_max), META_ADS_JOB_MODE = "order_value", META_ADS_ORDER_VALUE_SYNC_ENABLED = "true", META_ADS_ORDER_VALUE_WINDOW_DAYS = "2" }
      secrets         = { DATABASE_URL = "DATABASE_URL", META_ADS_APP_SECRET = "META_ADS_APP_SECRET", META_ADS_ENCRYPTION_KEY = "META_ADS_ENCRYPTION_KEY" }
    }
    google_ads = {
      name            = "roas-radar-google-ads-sync-${var.environment}"
      service_account = "google_ads"
      args            = ["run", "google-ads:sync:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = tostring(var.service_config.ads_sync_database_pool_max), GOOGLE_ADS_WORKER_LOOP = "false" }
      secrets         = { DATABASE_URL = "DATABASE_URL", GOOGLE_ADS_ENCRYPTION_KEY = "GOOGLE_ADS_ENCRYPTION_KEY" }
    }
    meta_ads_metadata = {
      name            = "roas-radar-meta-ads-metadata-refresh-${var.environment}"
      service_account = "meta_ads"
      args            = ["run", "meta-ads:metadata-refresh:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = tostring(var.service_config.ads_sync_database_pool_max), META_ADS_METADATA_REFRESH_REQUESTED_BY = "cloud-run-scheduler-${var.environment}" }
      secrets         = { DATABASE_URL = "DATABASE_URL", META_ADS_APP_SECRET = "META_ADS_APP_SECRET", META_ADS_ENCRYPTION_KEY = "META_ADS_ENCRYPTION_KEY" }
    }
    google_ads_metadata = {
      name            = "roas-radar-google-ads-metadata-refresh-${var.environment}"
      service_account = "google_ads"
      args            = ["run", "google-ads:metadata-refresh:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = tostring(var.service_config.ads_sync_database_pool_max), GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY = "cloud-run-scheduler-${var.environment}" }
      secrets         = { DATABASE_URL = "DATABASE_URL", GOOGLE_ADS_ENCRYPTION_KEY = "GOOGLE_ADS_ENCRYPTION_KEY" }
    }
    session_retention = {
      name            = "roas-radar-session-retention-${var.environment}"
      service_account = "retention"
      args            = ["run", "session-attribution:retention:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = "1", SESSION_ATTRIBUTION_RETENTION_DAYS = "30", SESSION_ATTRIBUTION_RETENTION_BATCH_SIZE = "500", SESSION_ATTRIBUTION_RETENTION_MAX_BATCHES = "200" }
      secrets         = { DATABASE_URL = "DATABASE_URL" }
    }
    data_quality = {
      name            = "roas-radar-data-quality-${var.environment}"
      service_account = "data_quality"
      args            = ["run", "data-quality:check:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = "1", DATA_QUALITY_CHECK_LOOP = "false" }
      secrets         = { DATABASE_URL = "DATABASE_URL" }
    }
    identity_graph_backfill = {
      name            = "roas-radar-identity-graph-backfill-${var.environment}"
      service_account = "identity_graph_backfill"
      args            = ["run", "identity:backfill-graph:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = "1", IDENTITY_GRAPH_BACKFILL_REQUESTED_BY = "cloud-run-scheduler-${var.environment}", IDENTITY_GRAPH_BACKFILL_LOOKBACK_DAYS = "2", IDENTITY_GRAPH_BACKFILL_BATCH_SIZE = "250" }
      secrets         = { DATABASE_URL = "DATABASE_URL" }
    }
    order_attribution_materialization = {
      name            = "roas-radar-order-attribution-materialization-${var.environment}"
      service_account = "order_attribution_materialization"
      args            = ["run", "attribution:materialization:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = "1", ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY = "cloud-run-scheduler-${var.environment}", ORDER_ATTRIBUTION_MATERIALIZATION_LOOKBACK_DAYS = "2", ORDER_ATTRIBUTION_MATERIALIZATION_LAG_DAYS = "1", ORDER_ATTRIBUTION_MATERIALIZATION_LIMIT = "250" }
      secrets         = { DATABASE_URL = "DATABASE_URL", SHOPIFY_APP_ENCRYPTION_KEY = "SHOPIFY_APP_ENCRYPTION_KEY" }
    }
    mmm_baseline = {
      name            = "roas-radar-mmm-baseline-${var.environment}"
      service_account = "mmm_baseline"
      args            = ["run", "mmm:train-baseline:start"]
      max_retries     = 1
      env             = { DATABASE_POOL_MAX = "1", MMM_BASELINE_LOOKBACK_DAYS = "90", MMM_BASELINE_LAG_DAYS = "1", MMM_BASELINE_SUBMITTED_BY = "cloud-run-scheduler-${var.environment}", MMM_BASELINE_ATTRIBUTION_MODEL = "last_touch", MMM_BASELINE_FREEZE_ID = var.mmm_baseline_freeze_id, MMM_BASELINE_MAX_SEGMENTS = "8", MMM_BASELINE_ADSTOCK_DECAY = "0.5", MMM_BASELINE_RIDGE_LAMBDA = "1", MMM_BASELINE_HOLDOUT_RATIO = "0.2" }
      secrets         = { DATABASE_URL = "DATABASE_URL" }
    }
    mmm_bayesian = {
      name            = "roas-radar-mmm-bayesian-${var.environment}"
      service_account = "mmm_bayesian"
      args            = ["run", "mmm:train-bayesian:start"]
      max_retries     = 0
      timeout         = "3600s"
      cpu             = "1"
      memory          = "1Gi"
      env             = { DATABASE_POOL_MAX = "1", MMM_BAYESIAN_LOOKBACK_DAYS = "90", MMM_BAYESIAN_LAG_DAYS = "1", MMM_BAYESIAN_SUBMITTED_BY = "cloud-run-scheduler-${var.environment}", MMM_BAYESIAN_ATTRIBUTION_MODEL = "last_touch", MMM_BAYESIAN_FREEZE_ID = var.mmm_bayesian_freeze_id, MMM_BAYESIAN_REFRESH_MART = "false", MMM_BAYESIAN_MAX_CHANNELS = "12", MMM_BAYESIAN_POSTERIOR_CHAINS = "4", MMM_BAYESIAN_POSTERIOR_DRAWS = "1000", MMM_BAYESIAN_POSTERIOR_WARMUP_DRAWS = "500", MMM_BAYESIAN_HOLDOUT_RATIO = "0.2" }
      secrets         = { DATABASE_URL = "DATABASE_URL" }
    }
  }

  scheduler_targets = {
    meta_ads                          = { job = "meta_ads", schedule = var.schedules.meta_ads }
    meta_order_value                  = { job = "meta_order_value", schedule = var.schedules.meta_order_value }
    google_ads                        = { job = "google_ads", schedule = var.schedules.google_ads }
    meta_ads_metadata                 = { job = "meta_ads_metadata", schedule = var.schedules.meta_ads_metadata }
    google_ads_metadata               = { job = "google_ads_metadata", schedule = var.schedules.google_ads_metadata }
    session_retention                 = { job = "session_retention", schedule = var.schedules.session_retention }
    data_quality                      = { job = "data_quality", schedule = var.schedules.data_quality }
    identity_graph_backfill           = { job = "identity_graph_backfill", schedule = var.schedules.identity_graph_backfill }
    order_attribution_materialization = { job = "order_attribution_materialization", schedule = var.schedules.order_attribution_materialization }
    mmm_baseline                      = { job = "mmm_baseline", schedule = var.schedules.mmm_baseline }
    mmm_bayesian                      = { job = "mmm_bayesian", schedule = var.schedules.mmm_bayesian }
  }

  secret_access = {
    api                               = keys(local.common_secret_env)
    dashboard                         = ["REPORTING_API_TOKEN"]
    worker                            = keys(local.common_secret_env)
    migrator                          = ["MIGRATOR_DATABASE_URL"]
    meta_ads                          = ["DATABASE_URL", "META_ADS_APP_SECRET", "META_ADS_ENCRYPTION_KEY"]
    google_ads                        = ["DATABASE_URL", "GOOGLE_ADS_ENCRYPTION_KEY"]
    retention                         = ["DATABASE_URL"]
    data_quality                      = ["DATABASE_URL"]
    identity_graph_backfill           = ["DATABASE_URL"]
    order_attribution_materialization = ["DATABASE_URL", "SHOPIFY_APP_ENCRYPTION_KEY"]
    mmm_baseline                      = ["DATABASE_URL"]
    mmm_bayesian                      = ["DATABASE_URL"]
    deployer                          = ["REPORTING_API_TOKEN"]
  }
}

resource "google_project_service" "enabled" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "servicenetworking.googleapis.com",
    "sqladmin.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "docker" {
  location      = var.region
  repository_id = var.artifact_registry_repository
  description   = "ROAS Radar Docker images"
  format        = "DOCKER"
  labels        = local.labels

  depends_on = [google_project_service.enabled]
}

resource "google_compute_network" "private" {
  name                    = var.network_name
  auto_create_subnetworks = false

  depends_on = [google_project_service.enabled]
}

resource "google_compute_global_address" "private_services" {
  name          = "${var.network_name}-private-services"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.private.id
}

resource "google_service_networking_connection" "private_vpc" {
  network                 = google_compute_network.private.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_services.name]
}

resource "google_sql_database_instance" "postgres" {
  name                = "roas-radar-${var.environment}-db"
  database_version    = "POSTGRES_16"
  region              = var.region
  deletion_protection = var.database_deletion_protection

  settings {
    tier              = var.database_tier
    availability_type = var.database_availability_type
    disk_type         = "PD_SSD"
    disk_size         = var.database_disk_size_gb
    disk_autoresize   = true

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      transaction_log_retention_days = 7
    }

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.private.id
    }

    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"
    }

    user_labels = local.labels
  }

  depends_on = [google_service_networking_connection.private_vpc]
}

resource "google_sql_database" "app" {
  name     = var.database_name
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_user" "users" {
  for_each = {
    roas_app      = var.db_app_password
    roas_migrator = var.db_migrator_password
    roas_readonly = var.db_readonly_password
  }

  name     = each.key
  instance = google_sql_database_instance.postgres.name
  password = each.value
}

resource "google_secret_manager_secret" "secrets" {
  for_each  = local.secret_names
  secret_id = each.value
  labels    = local.labels

  replication {
    auto {}
  }

  depends_on = [google_project_service.enabled]
}

resource "google_service_account" "accounts" {
  for_each     = local.service_accounts
  account_id   = each.value
  display_name = "ROAS Radar ${var.environment} ${replace(each.key, "_", " ")}"
}

resource "google_project_iam_member" "runtime_roles" {
  for_each = {
    api_cloudsql                               = ["api", "roles/cloudsql.client"]
    api_logging                                = ["api", "roles/logging.logWriter"]
    api_metrics                                = ["api", "roles/monitoring.metricWriter"]
    dashboard_logging                          = ["dashboard", "roles/logging.logWriter"]
    worker_cloudsql                            = ["worker", "roles/cloudsql.client"]
    worker_logging                             = ["worker", "roles/logging.logWriter"]
    worker_metrics                             = ["worker", "roles/monitoring.metricWriter"]
    migrator_cloudsql                          = ["migrator", "roles/cloudsql.client"]
    migrator_logging                           = ["migrator", "roles/logging.logWriter"]
    meta_ads_cloudsql                          = ["meta_ads", "roles/cloudsql.client"]
    meta_ads_logging                           = ["meta_ads", "roles/logging.logWriter"]
    meta_ads_metrics                           = ["meta_ads", "roles/monitoring.metricWriter"]
    google_ads_cloudsql                        = ["google_ads", "roles/cloudsql.client"]
    google_ads_logging                         = ["google_ads", "roles/logging.logWriter"]
    google_ads_metrics                         = ["google_ads", "roles/monitoring.metricWriter"]
    retention_cloudsql                         = ["retention", "roles/cloudsql.client"]
    retention_logging                          = ["retention", "roles/logging.logWriter"]
    data_quality_cloudsql                      = ["data_quality", "roles/cloudsql.client"]
    data_quality_logging                       = ["data_quality", "roles/logging.logWriter"]
    identity_graph_backfill_cloudsql           = ["identity_graph_backfill", "roles/cloudsql.client"]
    identity_graph_backfill_logging            = ["identity_graph_backfill", "roles/logging.logWriter"]
    order_attribution_materialization_cloudsql = ["order_attribution_materialization", "roles/cloudsql.client"]
    order_attribution_materialization_logging  = ["order_attribution_materialization", "roles/logging.logWriter"]
    mmm_baseline_cloudsql                      = ["mmm_baseline", "roles/cloudsql.client"]
    mmm_baseline_logging                       = ["mmm_baseline", "roles/logging.logWriter"]
    mmm_baseline_metrics                       = ["mmm_baseline", "roles/monitoring.metricWriter"]
    mmm_bayesian_cloudsql                      = ["mmm_bayesian", "roles/cloudsql.client"]
    mmm_bayesian_logging                       = ["mmm_bayesian", "roles/logging.logWriter"]
    mmm_bayesian_metrics                       = ["mmm_bayesian", "roles/monitoring.metricWriter"]
    scheduler_logging                          = ["scheduler", "roles/logging.logWriter"]
    deployer_run_admin                         = ["deployer", "roles/run.admin"]
    deployer_scheduler_admin                   = ["deployer", "roles/cloudscheduler.admin"]
    deployer_artifact_reader                   = ["deployer", "roles/artifactregistry.reader"]
    deployer_service_account_user              = ["deployer", "roles/iam.serviceAccountUser"]
  }

  project = var.project_id
  member  = "serviceAccount:${google_service_account.accounts[each.value[0]].email}"
  role    = each.value[1]
}

resource "google_secret_manager_secret_iam_member" "secret_access" {
  for_each = {
    for pair in flatten([
      for account_key, secret_list in local.secret_access : [
        for secret_name in secret_list : {
          key        = "${account_key}.${secret_name}"
          account_key = account_key
          secret_name = secret_name
        }
      ]
    ]) : pair.key => pair
  }

  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets[each.value.secret_name].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.accounts[each.value.account_key].email}"
}

resource "google_cloud_run_v2_service" "api" {
  name     = "roas-radar-api-${var.environment}"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  labels   = local.labels

  template {
    service_account = google_service_account.accounts.api.email
    timeout         = "900s"
    max_instance_request_concurrency = var.service_config.api_concurrency

    scaling {
      min_instance_count = var.service_config.api_min_instances
      max_instance_count = var.service_config.api_max_instances
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.postgres.connection_name]
      }
    }

    containers {
      image = local.backend_image
      ports { container_port = 8080 }

      resources {
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      dynamic "env" {
        for_each = merge(local.common_env, { DATABASE_POOL_MAX = tostring(var.service_config.database_pool_max), ATTRIBUTION_WORKER_LOOP = "false" })
        content {
          name  = env.key
          value = env.value
        }
      }

      dynamic "env" {
        for_each = local.common_secret_env
        content {
          name = env.key
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.secrets[env.value].secret_id
              version = "latest"
            }
          }
        }
      }
    }
  }
}

resource "google_cloud_run_v2_service" "worker" {
  name     = "roas-radar-attribution-worker-${var.environment}"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  labels   = local.labels

  template {
    service_account = google_service_account.accounts.worker.email
    timeout         = "900s"
    max_instance_request_concurrency = var.service_config.worker_concurrency

    scaling {
      min_instance_count = var.service_config.worker_min_instances
      max_instance_count = var.service_config.worker_max_instances
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [google_sql_database_instance.postgres.connection_name]
      }
    }

    containers {
      image   = local.backend_image
      command = ["npm"]
      args    = ["run", "start:worker-service"]
      ports { container_port = 8080 }

      resources {
        cpu_idle = false
        limits = {
          cpu    = "2"
          memory = "1Gi"
        }
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      dynamic "env" {
        for_each = merge(local.common_env, { DATABASE_POOL_MAX = tostring(var.service_config.worker_database_pool_max), ATTRIBUTION_WORKER_LOOP = "true" })
        content {
          name  = env.key
          value = env.value
        }
      }

      dynamic "env" {
        for_each = local.common_secret_env
        content {
          name = env.key
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.secrets[env.value].secret_id
              version = "latest"
            }
          }
        }
      }
    }
  }
}

resource "google_cloud_run_v2_service" "dashboard" {
  name     = "roas-radar-dashboard-${var.environment}"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  labels   = local.labels

  template {
    service_account = google_service_account.accounts.dashboard.email
    timeout         = "300s"
    max_instance_request_concurrency = 80

    scaling {
      min_instance_count = var.service_config.dashboard_min_instances
      max_instance_count = var.service_config.dashboard_max_instances
    }

    containers {
      image = local.dashboard_image
      ports { container_port = 8080 }

      env {
        name  = "NODE_ENV"
        value = "production"
      }
      env {
        name  = "DASHBOARD_API_BASE_URL"
        value = var.dashboard_api_base_url
      }
      env {
        name = "DASHBOARD_REPORTING_API_TOKEN"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.secrets["REPORTING_API_TOKEN"].secret_id
            version = "latest"
          }
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job" "jobs" {
  for_each = local.jobs

  name     = each.value.name
  location = var.region
  labels   = local.labels

  lifecycle {
    precondition {
      condition     = each.key != "mmm_baseline" || trimspace(var.mmm_baseline_freeze_id) != ""
      error_message = "mmm_baseline_freeze_id must be set before deploying the baseline MMM Cloud Run Job."
    }
  }

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.accounts[each.value.service_account].email
      max_retries     = each.value.max_retries
      timeout         = lookup(each.value, "timeout", "1800s")

      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [google_sql_database_instance.postgres.connection_name]
        }
      }

      containers {
        image   = local.backend_image
        command = ["npm"]
        args    = each.value.args

        resources {
          limits = {
            cpu    = lookup(each.value, "cpu", "1")
            memory = lookup(each.value, "memory", "512Mi")
          }
        }

        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }

        dynamic "env" {
          for_each = merge(local.common_env, each.value.env)
          content {
            name  = env.key
            value = env.value
          }
        }

        dynamic "env" {
          for_each = each.value.secrets
          content {
            name = env.key
            value_source {
              secret_key_ref {
                secret  = google_secret_manager_secret.secrets[env.value].secret_id
                version = "latest"
              }
            }
          }
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  for_each = google_cloud_run_v2_job.jobs

  project  = var.project_id
  location = var.region
  name     = each.value.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.accounts.scheduler.email}"
}

resource "google_cloud_run_v2_job_iam_member" "worker_invoker" {
  for_each = google_cloud_run_v2_job.jobs

  project  = var.project_id
  location = var.region
  name     = each.value.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.accounts.worker.email}"
}

resource "google_cloud_run_v2_service_iam_member" "public_services" {
  for_each = {
    api       = google_cloud_run_v2_service.api.name
    dashboard = google_cloud_run_v2_service.dashboard.name
  }

  project  = var.project_id
  location = var.region
  name     = each.value
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_scheduler_job" "schedulers" {
  for_each = local.scheduler_targets

  name      = "roas-radar-${replace(each.key, "_", "-")}-scheduler-${var.environment}"
  region    = var.region
  schedule  = each.value.schedule
  time_zone = var.scheduler_time_zone
  paused    = contains(var.paused_schedulers, each.key)

  retry_config {
    retry_count          = 0
    min_backoff_duration = "300s"
    max_backoff_duration = "1800s"
    max_doublings        = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${local.jobs[each.value.job].name}:run"

    oauth_token {
      service_account_email = google_service_account.accounts.scheduler.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}

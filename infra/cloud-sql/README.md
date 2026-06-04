# Cloud SQL Provisioning

The supported Terraform stack is in `infra/terraform/gcp-pipeline`. It provisions the ROAS Radar PostgreSQL instance, private service networking, the application database, and three least-privilege logins:

- `roas_app`: runtime API and worker user.
- `roas_migrator`: migration-only user.
- `roas_readonly`: optional reporting/debugging user.

## What Gets Created

- Private service networking range and service networking connection.
- Cloud SQL for PostgreSQL instance with private IP only.
- Automated backups, point-in-time recovery, and deletion protection.
- One PostgreSQL database.
- Three PostgreSQL users with passwords supplied through Terraform variables.

## Usage

From the repository root:

```sh
cd infra/terraform/gcp-pipeline
terraform init
terraform plan -var-file=environments/staging.tfvars
terraform apply -var-file=environments/staging.tfvars
```

Use `environments/production.tfvars` for production after staging has been applied and smoke-tested.

After Terraform creates the database users, run `db/bootstrap/001_roles.sql` once as an admin user so `roas_migrator`, `roas_app`, and `roas_readonly` receive the expected schema privileges. Application migrations are then handled by the Cloud Run migration job during `infra/cloud-run/promote.sh`.

Terraform creates Secret Manager secret containers but does not commit secret payloads. Populate `DATABASE_URL` with the `roas_app` connection string and `MIGRATOR_DATABASE_URL` with the `roas_migrator` connection string before deploying services.

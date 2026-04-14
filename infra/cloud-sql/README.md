# Cloud SQL Provisioning

Terraform in this directory provisions the ROAS Radar PostgreSQL instance, private service networking, the application database, and three least-privilege logins:

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


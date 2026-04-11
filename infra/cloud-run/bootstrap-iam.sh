Added repeatable IAM/bootstrap script that:
- creates runtime service accounts if missing
- grants Cloud SQL client
- grants logging and monitoring writer roles
- grants Secret Manager access for configured secrets
- creates the Artifact Registry repo if needed

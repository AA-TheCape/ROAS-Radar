Added repeatable deployment script that:
- loads `staging` or `production` config
- builds the container with Cloud Build
- deploys the public API service
- deploys the internal attribution worker service
- deploys the migration Cloud Run Job
- optionally executes migrations on deploy
- wires Cloud SQL, secrets, autoscaling, and pool env vars

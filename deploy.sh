
The deploy script:

1. Builds the application and dashboard images unless `SKIP_BUILDS=true`.
2. Deploys the API, worker, and dashboard Cloud Run services.
3. Deploys the migrator, Meta Ads metadata refresh, and Google Ads metadata refresh Cloud Run jobs.
4. Executes the migrator job when `RUN_MIGRATIONS_ON_DEPLOY` is not disabled.
5. Creates or updates per-platform Cloud Scheduler HTTP jobs that invoke `projects/*/locations/*/jobs/*:run`.
6. Pauses or resumes each metadata scheduler from the `*_SCHEDULER_ENABLED` flags.

## Pause And Resume

Examples:


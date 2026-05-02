Added:
- `emitCampaignMetadataResolutionCoverageLog(...)`
- `emitCampaignMetadataFreshnessSnapshotLog(...)`
- `emitCampaignMetadataSyncJobLifecycleLog(...)`

These emit structured events:
- `campaign_metadata_resolution_coverage`
- `campaign_metadata_freshness_snapshot`
- `campaign_metadata_sync_job_lifecycle`

Also exposed them through `__observabilityTestUtils` for unit coverage.

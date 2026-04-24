# Docs Index

## Core References

- [Implementation Guide](implementation-guide.md): local setup, service responsibilities, and end-to-end validation.
- [Attribution Schema V1](attribution-schema-v1.md): shared attribution contract, normalization rules, DB mappings, Shopify keys, and rollout expectations.
- [Shopify App Setup](shopify-app-setup.md): Shopify app install flow, OAuth, and webhook provisioning.
- [Visitor Identity Stitching](visitor-identity-stitching.md): deterministic identity-linking behavior for Shopify customers, orders, and tracked sessions.
- [Analytics Playbook](analytics-playbook.md): reporting, attribution, and analytics operating model.
- [Marketing Dimensions](marketing-dimensions.md): canonical source, medium, campaign, and click-ID interpretation rules.
- [Reporting Metrics](reporting-metrics.md): dashboard and reporting metric definitions.
- [Database Operations](database-operations.md): migration, backup, and operational DB guidance.

## Runbooks

- [Attribution Completeness](runbooks/attribution-completeness.md): capture-rate, session-id, dual-write, writeback, and resolver incident response.
- [Attribution Worker Backlog](runbooks/attribution-worker-backlog.md): worker lag investigation and recovery steps.
- [Ingestion Failures](runbooks/ingestion-failures.md): failed ingestion triage and remediation.
- [API Latency](runbooks/api-latency.md): API latency investigation and recovery.

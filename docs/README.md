# Docs Index

## Start Here

### Engineers

1. Read [Implementation Guide](implementation-guide.md) for local setup, service topology, validation flow, and troubleshooting.
2. Read [Attribution Schema V1](attribution-schema-v1.md) for canonical field names, normalization rules, DB mappings, and Shopify attribute keys.
3. Read [Raw Payload Persistence Contract](raw-payload-persistence-contract.md) before changing Shopify, Meta Ads, or Google Ads raw-source ingestion behavior.
4. Read [Operational Attribution Contracts](operational-attribution-contracts.md) for resolver precedence, Shopify writeback, retention, and recovery semantics.
5. Read [Recovery Job Framework](recovery-job-framework.md) before adding automatic backfill or recovery jobs.
6. Read [Attribution QA Payload Schema V1](attribution-qa-payload-schema-v1.md) before changing per-order attribution QA exports or fixtures.

### Analysts

1. Read [Analytics Playbook](analytics-playbook.md) for how ROAS Radar stores attribution, resolves orders, and exposes reporting outputs.
2. Read [Reporting Metrics](reporting-metrics.md) for KPI formulas used by the dashboard and reporting API.
3. Read [Deterministic Attribution Behavior](deterministic-attribution-behavior.md) when Clicks, Deterministic Views, Meta view-through, or combined comparison totals differ.
4. Read [Marketing Dimensions](marketing-dimensions.md) and [Attribution Schema V1](attribution-schema-v1.md) when channel naming or field semantics matter.

### Troubleshooting

- Start with [Implementation Guide](implementation-guide.md#troubleshooting) for local setup and validation issues.
- Use [Operational Attribution Contracts](operational-attribution-contracts.md) when you need resolver, writeback, retention, or dead-letter behavior.
- Use [Attribution Completeness](runbooks/attribution-completeness.md), [Attribution QA Tooling](runbooks/attribution-qa-tooling.md), [Ingestion Failures](runbooks/ingestion-failures.md), and [Attribution Worker Backlog](runbooks/attribution-worker-backlog.md) for incident response.
- Use [Deterministic Attribution Behavior](deterministic-attribution-behavior.md) when Clicks, Deterministic Views, Meta view-through, or combined comparison totals need explanation.
- Use [Cloud Run Pipelines](runbooks/cloud-run-pipelines.md) for Cloud Run deploy, scheduler, IAM, and rollback operations.
- Use [Production Manual Backfill And Recovery](runbooks/production-manual-backfill-recovery.md) before any production manual backfill, recovery, or dead-letter replay.

## Core References

- [Implementation Guide](implementation-guide.md): local setup, service responsibilities, end-to-end validation, ad sync raw request/response audit storage, and the raw-vs-derived contract for ad spend tables.
- [Attribution Schema V1](attribution-schema-v1.md): shared attribution contract, normalization rules, DB mappings, Shopify keys, rollout expectations, and how canonical attribution fields relate to raw-source storage.
- [Attribution QA Payload Schema V1](attribution-qa-payload-schema-v1.md): per-order QA payload contract, outcome invariants, candidate groups, and success/no-match fixture guidance.
- [Raw Payload Persistence Contract](raw-payload-persistence-contract.md): exact-as-received JSONB contract for Shopify, Meta Ads, and Google Ads raw-source ingestion surfaces.
- [Operational Attribution Contracts](operational-attribution-contracts.md): resolver precedence, Shopify writeback lifecycle, retention rules, and incident-routing links.
- [Recovery Job Framework](recovery-job-framework.md): automatic recovery job lifecycle, shared contracts, committed JSON Schemas, and source precedence rules.
- [Deterministic Attribution Behavior](deterministic-attribution-behavior.md): Meta-only deterministic attribution behavior, API-only verification, reporting modes, known limitations, and support guidance.
- [Meta Deterministic View Attribution Contract V1](meta-deterministic-view-attribution-contract-v1.md): approved aggregate-only Meta API deterministic view/impression attribution design, 7-day window, quarantine rules, and non-mixing requirements.
- [Reporting API Contract](reporting-api-contract.md): reporting response versioning, campaign label enrichment, reporting modes, and layer-separation requirements.
- [Shopify App Setup](shopify-app-setup.md): Shopify app install flow, OAuth, and webhook provisioning.
- [Visitor Identity Stitching](visitor-identity-stitching.md): deterministic identity-linking behavior for Shopify customers, orders, and tracked sessions.
- [Analytics Playbook](analytics-playbook.md): reporting, attribution, and analytics operating model.
- [Last Non-Direct Touch Approval Matrix](last-non-direct-touch-approval-matrix.md): approved primary-winner rules, deterministic precedence, and Shopify synthetic fallback caveats.
- [Marketing Dimensions](marketing-dimensions.md): canonical source, medium, campaign, and click-ID interpretation rules.
- [Reporting Metrics](reporting-metrics.md): dashboard and reporting metric definitions, including deterministic view/impression layer non-mixing rules.
- [Attribution Read API OpenAPI](openapi/attribution-api.yaml): authenticated contract for attribution result summaries and per-order explainability.
- [Database Operations](database-operations.md): migration, backup, and operational DB guidance.
- [Internal Release Notes](internal-release-notes.md): operator-facing release notes for internal support handoff.

## Runbooks

- [Attribution Completeness](runbooks/attribution-completeness.md): capture-rate, session-id, dual-write, writeback, and resolver incident response.
- [Attribution QA Tooling](runbooks/attribution-qa-tooling.md): QA payload, admin debug, raw evidence retention, and snapshot write alert response.
- [Attribution Worker Backlog](runbooks/attribution-worker-backlog.md): worker lag investigation and recovery steps.
- [Ingestion Failures](runbooks/ingestion-failures.md): failed ingestion triage and remediation.
- [API Latency](runbooks/api-latency.md): API latency investigation and recovery.
- [Cloud Run Pipelines](runbooks/cloud-run-pipelines.md): staged deploy verification, scheduler validation, least-privilege IAM, and rollback steps.
- [Production Manual Backfill And Recovery](runbooks/production-manual-backfill-recovery.md): production readiness gates, dry-run-first commands, validation queries, recovery paths, and on-call handoff sign-off.
- Use [Meta Order Value Ingestion](runbooks/meta-order-value-ingestion.md) when Meta attributed-revenue syncs fail, flatline, or emit null-spike anomalies.
- [Meta Order Value Ingestion](runbooks/meta-order-value-ingestion.md): Meta attributed-revenue sync failure, zero-ingestion, and null-spike triage.
- [Meta Deterministic Ingestion](runbooks/meta-deterministic-ingestion.md): Meta deterministic view/impression scheduler, sync operations, anomaly triage, verification, bounded backfill, emergency controls, and QA checks.

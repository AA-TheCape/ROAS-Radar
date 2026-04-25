# Docs Index

## Start Here

### Engineers

1. Read [Implementation Guide](implementation-guide.md) for local setup, service topology, validation flow, and troubleshooting.
2. Read [Attribution Schema V1](attribution-schema-v1.md) for canonical field names, normalization rules, DB mappings, and Shopify attribute keys.
3. Read [Operational Attribution Contracts](operational-attribution-contracts.md) for resolver precedence, Shopify writeback, retention, and recovery semantics.

### Analysts

1. Read [Analytics Playbook](analytics-playbook.md) for how ROAS Radar stores attribution, resolves orders, and exposes reporting outputs.
2. Read [Reporting Metrics](reporting-metrics.md) for KPI formulas used by the dashboard and reporting API.
3. Read [Marketing Dimensions](marketing-dimensions.md) and [Attribution Schema V1](attribution-schema-v1.md) when channel naming or field semantics matter.

### Troubleshooting

- Start with [Implementation Guide](implementation-guide.md#troubleshooting) for local setup and validation issues.
- Use [Operational Attribution Contracts](operational-attribution-contracts.md) when you need resolver, writeback, retention, or dead-letter behavior.
- Use [Attribution Completeness](runbooks/attribution-completeness.md), [Ingestion Failures](runbooks/ingestion-failures.md), and [Attribution Worker Backlog](runbooks/attribution-worker-backlog.md) for incident response.

## Core References

- [Implementation Guide](implementation-guide.md): local setup, service responsibilities, end-to-end validation, ad sync raw request/response audit storage, and the raw-vs-derived contract for ad spend tables.
- [Attribution Schema V1](attribution-schema-v1.md): shared attribution contract, normalization rules, DB mappings, Shopify keys, rollout expectations, and raw-payload exactness rules.
- [Operational Attribution Contracts](operational-attribution-contracts.md): resolver precedence, Shopify writeback lifecycle, retention rules, and incident-routing links.
- [Shopify App Setup](shopify-app-setup.md): Shopify app install flow, OAuth, and webhook provisioning.
- [Visitor Identity Stitching](visitor-identity-stitching.md): deterministic identity-linking behavior for Shopify customers, orders, and tracked sessions.
- [Analytics Playbook](analytics-playbook.md): reporting, attribution, and analytics operating model.
- [Last Non-Direct Touch Approval Matrix](last-non-direct-touch-approval-matrix.md): approved primary-winner rules, deterministic precedence, and Shopify synthetic fallback caveats.
- [Marketing Dimensions](marketing-dimensions.md): canonical source, medium, campaign, and click-ID interpretation rules.
- [Reporting Metrics](reporting-metrics.md): dashboard and reporting metric definitions.
- [Database Operations](database-operations.md): migration, backup, and operational DB guidance.

## Runbooks

- [Attribution Completeness](runbooks/attribution-completeness.md): capture-rate, session-id, dual-write, writeback, and resolver incident response.
- [Attribution Worker Backlog](runbooks/attribution-worker-backlog.md): worker lag investigation and recovery steps.
- [Ingestion Failures](runbooks/ingestion-failures.md): failed ingestion triage and remediation.
- [API Latency](runbooks/api-latency.md): API latency investigation and recovery.

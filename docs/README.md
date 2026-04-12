# ROAS Radar Documentation

Use this index to find the right starting point without reading source first.

## Start Here

- Engineers setting up or validating the MVP should start with [Implementation Guide](implementation-guide.md).
- Analysts interpreting attribution and reporting outputs should start with [Analytics Playbook](analytics-playbook.md).
- Operators checking system freshness, health, or incident response should start with [Operations And Freshness Guide](operations-and-freshness.md).

## Core Guides

- [Implementation Guide](implementation-guide.md): local setup, service topology, environment variables, API surface, worker entrypoints, and engineer validation flow.
- [Analytics Playbook](analytics-playbook.md): event contract, warehouse objects, attribution resolution order, attribution models, reporting interpretation, and MVP limitations.
- [Operations And Freshness Guide](operations-and-freshness.md): freshness targets, worker cadences, health checks, logging signals, alert-to-runbook mapping, and troubleshooting.

## Reference Docs

- [Marketing Dimensions](marketing-dimensions.md): canonical source, medium, campaign, and click-ID normalization rules.
- [Visitor Identity Stitching](visitor-identity-stitching.md): deterministic identity linking and session-to-order stitching behavior.
- [Reporting Metrics](reporting-metrics.md): KPI definitions and reporting semantics.
- [Shopify App Setup](shopify-app-setup.md): install flow, webhook provisioning, and Shopify integration requirements.
- [Database Operations](database-operations.md): database administration and operational checks.

## Runbooks

- [Ingestion Failures](runbooks/ingestion-failures.md): `/track` and Shopify webhook troubleshooting.
- [Attribution Worker Backlog](runbooks/attribution-worker-backlog.md): queue growth, failed jobs, and stale lock recovery.
- [API Latency](runbooks/api-latency.md): API performance incident response.

## Recommended Reading Paths

### New engineer

1. [Implementation Guide](implementation-guide.md)
2. [Analytics Playbook](analytics-playbook.md)
3. [Visitor Identity Stitching](visitor-identity-stitching.md)
4. [Reporting Metrics](reporting-metrics.md)

### Analyst validating attribution

1. [Analytics Playbook](analytics-playbook.md)
2. [Marketing Dimensions](marketing-dimensions.md)
3. [Reporting Metrics](reporting-metrics.md)
4. [Operations And Freshness Guide](operations-and-freshness.md)

### Operator troubleshooting production

1. [Operations And Freshness Guide](operations-and-freshness.md)
2. [Ingestion Failures](runbooks/ingestion-failures.md)
3. [Attribution Worker Backlog](runbooks/attribution-worker-backlog.md)
4. [API Latency](runbooks/api-latency.md)

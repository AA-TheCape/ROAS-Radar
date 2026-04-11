# Database Migrations

This directory contains PostgreSQL schema migrations for ROAS Radar.

## Current schema

`migrations/0001_create_roas_radar_core_schema.sql` creates the MVP analytics schema for:

- `visitors`
- `sessions`
- `touchpoints`
- `orders`
- `order_line_items`
- `attribution_models`
- `attribution_results`
- `ad_platforms`
- `traffic_channels`
- `campaigns`
- `ad_groups`
- `creatives`
- `ad_spend_daily`

## Notes

- `orders.visitor_id`, `orders.source_session_id`, and `orders.source_touchpoint_id` are foreign-keyed so visitor-to-order lineage is enforced when attribution is known.
- `attribution_results` stores weighted allocations per `order_id`, `model_id`, and `touchpoint_id`, which supports first touch, last touch, linear, time decay, position based, and rule based weighted models.
- BRIN and composite time-series indexes are included on `sessions.started_at`, `touchpoints.occurred_at`, `orders.ordered_at`, `attribution_results.conversion_at`, and `ad_spend_daily.spend_date` to support reporting workloads.

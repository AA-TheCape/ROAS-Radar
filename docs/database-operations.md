Operational pruning now runs through the scheduled `session-attribution:retention` Cloud Run job.

The cleanup contract is:
1. delete expired `session_attribution_touch_events` rows in batches
2. delete expired `session_attribution_identities` rows in batches
3. skip rows whose `roas_radar_session_id` is still referenced by `order_attribution_links`

`order_attribution_links` rows are not pruned by the 30-day session cleanup job.

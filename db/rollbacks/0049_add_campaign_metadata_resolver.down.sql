BEGIN;

DROP INDEX IF EXISTS mmm_daily_input_mart_v1_resolver_idx;

ALTER TABLE mmm_daily_input_mart_v1
  DROP COLUMN IF EXISTS needs_metadata_qa,
  DROP COLUMN IF EXISTS resolved_hierarchy_metadata,
  DROP COLUMN IF EXISTS resolved_canonical_channel_group,
  DROP COLUMN IF EXISTS resolved_canonical_channel,
  DROP COLUMN IF EXISTS resolved_canonical_medium,
  DROP COLUMN IF EXISTS resolved_canonical_source,
  DROP COLUMN IF EXISTS resolved_canonical_campaign_name,
  DROP COLUMN IF EXISTS resolved_canonical_campaign_id,
  DROP COLUMN IF EXISTS resolver_confidence,
  DROP COLUMN IF EXISTS resolver_source,
  DROP COLUMN IF EXISTS resolver_version;

DROP TABLE IF EXISTS campaign_metadata_qa_queue;
DROP TABLE IF EXISTS campaign_metadata_resolver_rules;

COMMIT;

BEGIN;

CREATE TABLE IF NOT EXISTS campaign_metadata_resolver_rules (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  resolver_version text NOT NULL,
  rule_kind text NOT NULL DEFAULT 'rule',
  priority integer NOT NULL DEFAULT 100,
  active boolean NOT NULL DEFAULT true,
  match_platform text,
  match_source text,
  match_medium text,
  match_campaign text,
  match_content text,
  match_term text,
  match_account_id text,
  match_campaign_id text,
  match_adset_id text,
  match_ad_id text,
  match_expression jsonb NOT NULL DEFAULT '{}'::jsonb,
  canonical_campaign_id text,
  canonical_campaign_name text NOT NULL,
  canonical_source text NOT NULL,
  canonical_medium text NOT NULL,
  canonical_channel text NOT NULL,
  canonical_channel_group text NOT NULL,
  hierarchy_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  confidence numeric(5, 4) NOT NULL DEFAULT 1.0,
  source_label text NOT NULL DEFAULT 'rule',
  effective_from timestamptz NOT NULL DEFAULT '-infinity',
  effective_to timestamptz NOT NULL DEFAULT 'infinity',
  created_by text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT campaign_metadata_resolver_rules_kind_chk
    CHECK (rule_kind IN ('rule', 'override')),
  CONSTRAINT campaign_metadata_resolver_rules_confidence_chk
    CHECK (confidence >= 0 AND confidence <= 1),
  CONSTRAINT campaign_metadata_resolver_rules_version_chk
    CHECK (NULLIF(btrim(resolver_version), '') IS NOT NULL),
  CONSTRAINT campaign_metadata_resolver_rules_campaign_name_chk
    CHECK (NULLIF(btrim(canonical_campaign_name), '') IS NOT NULL),
  CONSTRAINT campaign_metadata_resolver_rules_source_chk
    CHECK (NULLIF(btrim(canonical_source), '') IS NOT NULL),
  CONSTRAINT campaign_metadata_resolver_rules_medium_chk
    CHECK (NULLIF(btrim(canonical_medium), '') IS NOT NULL),
  CONSTRAINT campaign_metadata_resolver_rules_channel_chk
    CHECK (NULLIF(btrim(canonical_channel), '') IS NOT NULL),
  CONSTRAINT campaign_metadata_resolver_rules_channel_group_chk
    CHECK (NULLIF(btrim(canonical_channel_group), '') IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS campaign_metadata_resolver_rules_lookup_idx
  ON campaign_metadata_resolver_rules (
    active,
    rule_kind,
    resolver_version,
    priority,
    match_platform,
    match_source,
    match_medium,
    match_campaign,
    match_account_id,
    match_campaign_id
  );

CREATE INDEX IF NOT EXISTS campaign_metadata_resolver_rules_effective_idx
  ON campaign_metadata_resolver_rules (effective_from, effective_to)
  WHERE active;

CREATE TABLE IF NOT EXISTS campaign_metadata_qa_queue (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  status text NOT NULL DEFAULT 'open',
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  occurrence_count integer NOT NULL DEFAULT 1,
  resolver_version text,
  reason text NOT NULL,
  platform text,
  source text,
  medium text,
  campaign text,
  content text,
  term text,
  account_id text,
  campaign_id text,
  adset_id text,
  ad_id text,
  sample_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  resolved_by_rule_id uuid REFERENCES campaign_metadata_resolver_rules(id),
  resolved_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT campaign_metadata_qa_queue_status_chk
    CHECK (status IN ('open', 'resolved', 'ignored')),
  CONSTRAINT campaign_metadata_qa_queue_occurrence_count_chk
    CHECK (occurrence_count > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS campaign_metadata_qa_queue_open_signature_uidx
  ON campaign_metadata_qa_queue (
    COALESCE(platform, ''),
    COALESCE(source, ''),
    COALESCE(medium, ''),
    COALESCE(campaign, ''),
    COALESCE(content, ''),
    COALESCE(term, ''),
    COALESCE(account_id, ''),
    COALESCE(campaign_id, ''),
    COALESCE(adset_id, ''),
    COALESCE(ad_id, ''),
    COALESCE(resolver_version, ''),
    reason
  )
  WHERE status = 'open';

CREATE INDEX IF NOT EXISTS campaign_metadata_qa_queue_status_idx
  ON campaign_metadata_qa_queue (status, last_seen_at DESC);

ALTER TABLE mmm_daily_input_mart_v1
  ADD COLUMN IF NOT EXISTS resolver_version text,
  ADD COLUMN IF NOT EXISTS resolver_source text,
  ADD COLUMN IF NOT EXISTS resolver_confidence numeric(5, 4),
  ADD COLUMN IF NOT EXISTS resolved_canonical_campaign_id text,
  ADD COLUMN IF NOT EXISTS resolved_canonical_campaign_name text,
  ADD COLUMN IF NOT EXISTS resolved_canonical_source text,
  ADD COLUMN IF NOT EXISTS resolved_canonical_medium text,
  ADD COLUMN IF NOT EXISTS resolved_canonical_channel text,
  ADD COLUMN IF NOT EXISTS resolved_canonical_channel_group text,
  ADD COLUMN IF NOT EXISTS resolved_hierarchy_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS needs_metadata_qa boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS mmm_daily_input_mart_v1_resolver_idx
  ON mmm_daily_input_mart_v1 (
    resolver_version,
    resolver_source,
    needs_metadata_qa,
    metric_date DESC
  );

COMMIT;

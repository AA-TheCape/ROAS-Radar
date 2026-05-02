BEGIN;

-- Contract reference: docs/meta-attributed-revenue-contract-v1.md
CREATE TABLE meta_ads_order_value_aggregates (
  id bigserial PRIMARY KEY,
  organization_id bigint NOT NULL,
  meta_connection_id bigint NOT NULL REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
  sync_job_id bigint NOT NULL REFERENCES meta_ads_sync_jobs(id) ON DELETE CASCADE,
  raw_record_id bigint REFERENCES meta_ads_raw_spend_records(id) ON DELETE SET NULL,
  ad_account_id text NOT NULL,
  report_date date NOT NULL,
  raw_date_start date NOT NULL,
  raw_date_stop date,
  campaign_id text NOT NULL,
  campaign_name text,
  attributed_revenue numeric(12, 2),
  purchase_count bigint,
  spend numeric(12, 2) NOT NULL DEFAULT 0,
  purchase_roas numeric(12, 6),
  currency text,
  canonical_action_type text,
  canonical_selection_mode text NOT NULL DEFAULT 'priority',
  raw_action_values jsonb NOT NULL DEFAULT '[]'::jsonb,
  raw_actions jsonb NOT NULL DEFAULT '[]'::jsonb,
  source_synced_at timestamptz NOT NULL,
  action_report_time text NOT NULL DEFAULT 'conversion',
  use_account_attribution_setting boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT meta_ads_order_value_aggregates_dedupe_key UNIQUE (
    organization_id,
    ad_account_id,
    report_date,
    campaign_id,
    action_report_time,
    use_account_attribution_setting
  ),
  CHECK (canonical_selection_mode IN ('priority', 'fallback', 'none')),
  CHECK (action_report_time IN ('conversion', 'impression', 'mixed'))
);

CREATE INDEX meta_ads_order_value_aggregates_org_account_report_date_idx
  ON meta_ads_order_value_aggregates (organization_id, ad_account_id, report_date DESC);

CREATE INDEX meta_ads_order_value_aggregates_campaign_report_date_idx
  ON meta_ads_order_value_aggregates (campaign_id, report_date DESC);

CREATE INDEX meta_ads_order_value_aggregates_connection_report_date_idx
  ON meta_ads_order_value_aggregates (meta_connection_id, report_date DESC);

CREATE INDEX meta_ads_order_value_aggregates_sync_job_idx
  ON meta_ads_order_value_aggregates (sync_job_id, source_synced_at DESC);

COMMIT;

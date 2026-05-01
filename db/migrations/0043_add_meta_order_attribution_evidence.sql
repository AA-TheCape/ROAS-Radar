BEGIN;

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS meta_attribution_evidence_id uuid,
  ADD COLUMN IF NOT EXISTS meta_attribution_evaluation_outcome text,
  ADD COLUMN IF NOT EXISTS meta_attribution_confidence_score numeric(5, 4),
  ADD COLUMN IF NOT EXISTS meta_attribution_confidence_label text,
  ADD COLUMN IF NOT EXISTS meta_attribution_present boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS meta_attribution_affected_canonical boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS latest_attribution_decision_artifact_id uuid;

ALTER TABLE attribution_results
  ADD COLUMN IF NOT EXISTS meta_attribution_evidence_id uuid,
  ADD COLUMN IF NOT EXISTS meta_attribution_evaluation_outcome text,
  ADD COLUMN IF NOT EXISTS meta_attribution_affected_canonical boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS attribution_decision_artifact_id uuid;

CREATE TABLE IF NOT EXISTS meta_order_attribution_evidence (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id bigint NOT NULL,
  shopify_order_id text NOT NULL REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  meta_connection_id bigint REFERENCES meta_ads_connections(id) ON DELETE SET NULL,
  raw_record_id bigint REFERENCES meta_ads_order_value_raw_records(id) ON DELETE SET NULL,
  sync_job_id bigint REFERENCES meta_ads_sync_jobs(id) ON DELETE SET NULL,
  ingestion_run_id bigint REFERENCES meta_ads_order_value_sync_runs(id) ON DELETE SET NULL,
  meta_signal_id text NOT NULL,
  platform text NOT NULL DEFAULT 'meta_ads',
  source_kind text NOT NULL DEFAULT 'order_scoped',
  reported_at_utc timestamptz NOT NULL,
  source_received_at timestamptz,
  normalized_at timestamptz NOT NULL DEFAULT now(),
  order_occurred_at_utc timestamptz,
  meta_touchpoint_occurred_at_utc timestamptz,
  event_or_report_timestamp_utc timestamptz,
  reported_conversion_timestamp_utc timestamptz,
  attribution_window_days integer,
  meta_attribution_reason text NOT NULL,
  campaign_id text NOT NULL,
  campaign_name text,
  ad_account_id text NOT NULL,
  ad_id text,
  ad_set_id text,
  currency_code text,
  reported_conversion_value numeric(12, 2),
  reported_event_name text,
  is_view_through boolean NOT NULL DEFAULT false,
  is_click_through boolean NOT NULL DEFAULT false,
  match_basis text,
  observed_match_bases text[] NOT NULL DEFAULT '{}'::text[],
  confidence_score numeric(5, 4),
  eligibility_outcome text NOT NULL DEFAULT 'ineligible',
  eligibility_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
  disqualification_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
  parallel_only_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
  normalization_failures jsonb NOT NULL DEFAULT '[]'::jsonb,
  eligibility_signals jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_record_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  raw_payload_reference text,
  raw_payload_hashes jsonb NOT NULL DEFAULT '[]'::jsonb,
  evidence_snapshot_hash text,
  source_snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  rule_version text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT meta_order_attribution_evidence_platform_chk
    CHECK (platform = 'meta_ads'),
  CONSTRAINT meta_order_attribution_evidence_source_kind_chk
    CHECK (source_kind IN ('order_scoped', 'order_joinable', 'aggregate_only', 'unknown')),
  CONSTRAINT meta_order_attribution_evidence_attribution_window_days_chk
    CHECK (attribution_window_days IS NULL OR attribution_window_days >= 0),
  CONSTRAINT meta_order_attribution_evidence_match_basis_chk
    CHECK (
      match_basis IS NULL
      OR match_basis IN (
        'fbclid',
        'fbc',
        'fbp',
        'external_id',
        'email_hash',
        'phone_hash',
        'meta_order_reference',
        'conversion_api_event_id'
      )
    ),
  CONSTRAINT meta_order_attribution_evidence_confidence_score_chk
    CHECK (
      confidence_score IS NULL
      OR (confidence_score >= 0 AND confidence_score <= 1)
    ),
  CONSTRAINT meta_order_attribution_evidence_eligibility_outcome_chk
    CHECK (
      eligibility_outcome IN (
        'eligible_canonical',
        'eligible_parallel_only',
        'ineligible',
        'not_evaluated'
      )
    ),
  CONSTRAINT meta_order_attribution_evidence_eligibility_reasons_json_chk
    CHECK (jsonb_typeof(eligibility_reasons) = 'array'),
  CONSTRAINT meta_order_attribution_evidence_disqualification_reasons_json_chk
    CHECK (jsonb_typeof(disqualification_reasons) = 'array'),
  CONSTRAINT meta_order_attribution_evidence_parallel_only_reasons_json_chk
    CHECK (jsonb_typeof(parallel_only_reasons) = 'array'),
  CONSTRAINT meta_order_attribution_evidence_normalization_failures_json_chk
    CHECK (jsonb_typeof(normalization_failures) = 'array'),
  CONSTRAINT meta_order_attribution_evidence_eligibility_signals_json_chk
    CHECK (jsonb_typeof(eligibility_signals) = 'object'),
  CONSTRAINT meta_order_attribution_evidence_source_record_ids_json_chk
    CHECK (jsonb_typeof(source_record_ids) = 'array'),
  CONSTRAINT meta_order_attribution_evidence_raw_payload_hashes_json_chk
    CHECK (jsonb_typeof(raw_payload_hashes) = 'array'),
  CONSTRAINT meta_order_attribution_evidence_source_snapshot_json_chk
    CHECK (jsonb_typeof(source_snapshot_json) = 'object'),
  CONSTRAINT meta_order_attribution_evidence_signal_dedupe_key
    UNIQUE (organization_id, meta_signal_id),
  CONSTRAINT meta_order_attribution_evidence_canonical_threshold_chk
    CHECK (
      eligibility_outcome <> 'eligible_canonical'
      OR (
        confidence_score IS NOT NULL
        AND confidence_score >= 0.5
        AND match_basis IS NOT NULL
        AND order_occurred_at_utc IS NOT NULL
        AND meta_touchpoint_occurred_at_utc IS NOT NULL
        AND attribution_window_days IS NOT NULL
        AND source_kind IN ('order_scoped', 'order_joinable')
        AND meta_touchpoint_occurred_at_utc <= order_occurred_at_utc
        AND meta_touchpoint_occurred_at_utc >= order_occurred_at_utc - make_interval(days => attribution_window_days)
        AND (raw_payload_reference IS NOT NULL OR raw_record_id IS NOT NULL)
        AND ingestion_run_id IS NOT NULL
      )
    ),
  CONSTRAINT meta_order_attribution_evidence_parallel_threshold_chk
    CHECK (
      eligibility_outcome <> 'eligible_parallel_only'
      OR (
        confidence_score IS NOT NULL
        AND confidence_score >= 0.35
        AND confidence_score < 0.5
      )
    ),
  CONSTRAINT meta_order_attribution_evidence_not_evaluated_reason_chk
    CHECK (
      eligibility_outcome <> 'not_evaluated'
      OR confidence_score IS NULL
    )
);

CREATE INDEX IF NOT EXISTS meta_order_attribution_evidence_order_reported_idx
  ON meta_order_attribution_evidence (shopify_order_id, reported_at_utc DESC);

CREATE INDEX IF NOT EXISTS meta_order_attribution_evidence_connection_reported_idx
  ON meta_order_attribution_evidence (meta_connection_id, reported_at_utc DESC)
  WHERE meta_connection_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS meta_order_attribution_evidence_outcome_reported_idx
  ON meta_order_attribution_evidence (eligibility_outcome, reported_at_utc DESC);

CREATE INDEX IF NOT EXISTS meta_order_attribution_evidence_campaign_touchpoint_idx
  ON meta_order_attribution_evidence (campaign_id, meta_touchpoint_occurred_at_utc DESC);

CREATE INDEX IF NOT EXISTS meta_order_attribution_evidence_ingestion_run_idx
  ON meta_order_attribution_evidence (ingestion_run_id, normalized_at DESC)
  WHERE ingestion_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS attribution_decision_artifacts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  shopify_order_id text NOT NULL REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  meta_attribution_evidence_id uuid REFERENCES meta_order_attribution_evidence(id) ON DELETE SET NULL,
  backfill_run_id text REFERENCES order_attribution_backfill_runs(id) ON DELETE SET NULL,
  resolver_run_source text NOT NULL,
  resolver_triggered_by text NOT NULL,
  resolver_timestamp timestamptz NOT NULL DEFAULT now(),
  resolver_rule_version text NOT NULL,
  resolver_model_version integer NOT NULL DEFAULT 1,
  canonical_tier_before text NOT NULL,
  canonical_tier_after text NOT NULL,
  meta_evaluation_outcome text NOT NULL,
  meta_affected_canonical boolean NOT NULL DEFAULT false,
  decision_reason text NOT NULL,
  decision_reason_detail text,
  confidence_score numeric(5, 4),
  confidence_threshold numeric(5, 4),
  rule_inputs_hash text NOT NULL,
  evidence_snapshot_hash text,
  order_occurred_at_utc timestamptz,
  order_snapshot_ref text NOT NULL,
  first_party_winner_present boolean NOT NULL DEFAULT false,
  shopify_hint_winner_present boolean NOT NULL DEFAULT false,
  ga4_fallback_candidate_present boolean NOT NULL DEFAULT false,
  canonical_winner_tier text NOT NULL,
  canonical_winner_source text NOT NULL,
  parallel_meta_available boolean NOT NULL DEFAULT false,
  replayable boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT attribution_decision_artifacts_resolver_run_source_chk
    CHECK (resolver_run_source IN ('forward_processing', 'manual_backfill')),
  CONSTRAINT attribution_decision_artifacts_meta_evaluation_outcome_chk
    CHECK (
      meta_evaluation_outcome IN (
        'eligible_canonical',
        'eligible_parallel_only',
        'ineligible',
        'not_evaluated'
      )
    ),
  CONSTRAINT attribution_decision_artifacts_confidence_score_chk
    CHECK (
      confidence_score IS NULL
      OR (confidence_score >= 0 AND confidence_score <= 1)
    ),
  CONSTRAINT attribution_decision_artifacts_confidence_threshold_chk
    CHECK (
      confidence_threshold IS NULL
      OR (confidence_threshold >= 0 AND confidence_threshold <= 1)
    ),
  CONSTRAINT attribution_decision_artifacts_canonical_tier_before_chk
    CHECK (
      canonical_tier_before IN (
        'deterministic_first_party',
        'deterministic_shopify_hint',
        'platform_reported_meta',
        'ga4_fallback',
        'unattributed'
      )
    ),
  CONSTRAINT attribution_decision_artifacts_canonical_tier_after_chk
    CHECK (
      canonical_tier_after IN (
        'deterministic_first_party',
        'deterministic_shopify_hint',
        'platform_reported_meta',
        'ga4_fallback',
        'unattributed'
      )
    ),
  CONSTRAINT attribution_decision_artifacts_canonical_winner_tier_chk
    CHECK (
      canonical_winner_tier IN (
        'deterministic_first_party',
        'deterministic_shopify_hint',
        'platform_reported_meta',
        'ga4_fallback',
        'unattributed'
      )
    ),
  CONSTRAINT attribution_decision_artifacts_reason_code_chk
    CHECK (decision_reason ~ '^meta_[a-z0-9_]+$'),
  CONSTRAINT attribution_decision_artifacts_backfill_link_chk
    CHECK (
      resolver_run_source <> 'manual_backfill'
      OR backfill_run_id IS NOT NULL
    ),
  CONSTRAINT attribution_decision_artifacts_canonical_meta_link_chk
    CHECK (
      canonical_tier_after <> 'platform_reported_meta'
      OR (
        meta_evaluation_outcome = 'eligible_canonical'
        AND meta_attribution_evidence_id IS NOT NULL
        AND meta_affected_canonical = true
      )
    ),
  CONSTRAINT attribution_decision_artifacts_meta_outcome_fields_chk
    CHECK (
      meta_evaluation_outcome = 'not_evaluated'
      OR (
        confidence_threshold IS NOT NULL
        AND evidence_snapshot_hash IS NOT NULL
      )
    )
);

CREATE INDEX IF NOT EXISTS attribution_decision_artifacts_order_resolved_idx
  ON attribution_decision_artifacts (shopify_order_id, resolver_timestamp DESC);

CREATE INDEX IF NOT EXISTS attribution_decision_artifacts_meta_outcome_idx
  ON attribution_decision_artifacts (meta_evaluation_outcome, resolver_timestamp DESC);

CREATE INDEX IF NOT EXISTS attribution_decision_artifacts_backfill_idx
  ON attribution_decision_artifacts (backfill_run_id, resolver_timestamp DESC)
  WHERE backfill_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS attribution_decision_artifacts_meta_evidence_idx
  ON attribution_decision_artifacts (meta_attribution_evidence_id, resolver_timestamp DESC)
  WHERE meta_attribution_evidence_id IS NOT NULL;

ALTER TABLE shopify_orders
  ADD CONSTRAINT shopify_orders_meta_attribution_evidence_fk
    FOREIGN KEY (meta_attribution_evidence_id)
    REFERENCES meta_order_attribution_evidence(id)
    ON DELETE SET NULL,
  ADD CONSTRAINT shopify_orders_latest_attribution_decision_artifact_fk
    FOREIGN KEY (latest_attribution_decision_artifact_id)
    REFERENCES attribution_decision_artifacts(id)
    ON DELETE SET NULL;

ALTER TABLE attribution_results
  ADD CONSTRAINT attribution_results_meta_attribution_evidence_fk
    FOREIGN KEY (meta_attribution_evidence_id)
    REFERENCES meta_order_attribution_evidence(id)
    ON DELETE SET NULL,
  ADD CONSTRAINT attribution_results_decision_artifact_fk
    FOREIGN KEY (attribution_decision_artifact_id)
    REFERENCES attribution_decision_artifacts(id)
    ON DELETE SET NULL;

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_tier_chk;

ALTER TABLE shopify_orders
  ADD CONSTRAINT shopify_orders_attribution_tier_chk
  CHECK (
    attribution_tier IN (
      'deterministic_first_party',
      'deterministic_shopify_hint',
      'platform_reported_meta',
      'ga4_fallback',
      'unattributed'
    )
  ) NOT VALID;

ALTER TABLE shopify_orders
  ADD CONSTRAINT shopify_orders_meta_attribution_evaluation_outcome_chk
    CHECK (
      meta_attribution_evaluation_outcome IS NULL
      OR meta_attribution_evaluation_outcome IN (
        'eligible_canonical',
        'eligible_parallel_only',
        'ineligible',
        'not_evaluated'
      )
    ),
  ADD CONSTRAINT shopify_orders_meta_attribution_confidence_score_chk
    CHECK (
      meta_attribution_confidence_score IS NULL
      OR (
        meta_attribution_confidence_score >= 0
        AND meta_attribution_confidence_score <= 1
      )
    ),
  ADD CONSTRAINT shopify_orders_meta_attribution_confidence_label_chk
    CHECK (
      meta_attribution_confidence_label IS NULL
      OR meta_attribution_confidence_label IN ('high', 'medium', 'low')
    ),
  ADD CONSTRAINT shopify_orders_meta_attribution_summary_link_chk
    CHECK (
      meta_attribution_affected_canonical = false
      OR (
        attribution_tier = 'platform_reported_meta'
        AND meta_attribution_evaluation_outcome = 'eligible_canonical'
        AND meta_attribution_evidence_id IS NOT NULL
      )
    );

ALTER TABLE attribution_results
  ADD CONSTRAINT attribution_results_meta_attribution_evaluation_outcome_chk
    CHECK (
      meta_attribution_evaluation_outcome IS NULL
      OR meta_attribution_evaluation_outcome IN (
        'eligible_canonical',
        'eligible_parallel_only',
        'ineligible',
        'not_evaluated'
      )
    ),
  ADD CONSTRAINT attribution_results_meta_attribution_summary_link_chk
    CHECK (
      meta_attribution_affected_canonical = false
      OR (
        meta_attribution_evaluation_outcome = 'eligible_canonical'
        AND meta_attribution_evidence_id IS NOT NULL
        AND attribution_decision_artifact_id IS NOT NULL
      )
    );

ALTER TABLE shopify_orders
  VALIDATE CONSTRAINT shopify_orders_attribution_tier_chk;

COMMIT;

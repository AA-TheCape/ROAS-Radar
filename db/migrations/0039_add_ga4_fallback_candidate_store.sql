BEGIN;

CREATE OR REPLACE FUNCTION ensure_ga4_fallback_candidate_partition(partition_month date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  partition_start date := date_trunc('month', partition_month)::date;
  partition_end date := (date_trunc('month', partition_month) + interval '1 month')::date;
  partition_suffix text := to_char(partition_start, '"y"YYYY"m"MM');
  partition_name text := format('ga4_fallback_candidates_%s', partition_suffix);
BEGIN
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF ga4_fallback_candidates
       FOR VALUES FROM (%L) TO (%L)',
    partition_name,
    partition_start,
    partition_end
  );

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON %I (customer_identity_id, occurred_at DESC, ga4_session_id ASC)
       WHERE customer_identity_id IS NOT NULL',
    partition_name || '_customer_identity_lookup_idx',
    partition_name
  );

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON %I (email_hash, occurred_at DESC, ga4_session_id ASC)
       WHERE email_hash IS NOT NULL',
    partition_name || '_email_hash_lookup_idx',
    partition_name
  );

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON %I (transaction_id, occurred_at DESC, ga4_session_id ASC)
       WHERE transaction_id IS NOT NULL',
    partition_name || '_transaction_lookup_idx',
    partition_name
  );

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON %I (retained_until ASC, occurred_at ASC, candidate_key ASC)',
    partition_name || '_retention_idx',
    partition_name
  );

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON %I (source_export_hour DESC)',
    partition_name || '_export_hour_idx',
    partition_name
  );
END;
$$;

CREATE TABLE IF NOT EXISTS ga4_fallback_candidates (
  candidate_key text NOT NULL,
  occurred_at timestamptz NOT NULL,
  ga4_user_key text NOT NULL,
  ga4_client_id text,
  ga4_session_id text,
  transaction_id text,
  email_hash text,
  customer_identity_id uuid REFERENCES customer_identities(id) ON DELETE SET NULL,
  source text,
  medium text,
  campaign text,
  content text,
  term text,
  click_id_type text,
  click_id_value text,
  session_has_required_fields boolean NOT NULL DEFAULT false,
  source_export_hour timestamptz NOT NULL,
  source_dataset text NOT NULL,
  source_table_type text NOT NULL,
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '35 days'),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (candidate_key, occurred_at),
  CHECK (char_length(candidate_key) <= 128),
  CHECK (char_length(ga4_user_key) <= 255),
  CHECK (ga4_client_id IS NULL OR char_length(ga4_client_id) <= 255),
  CHECK (ga4_session_id IS NULL OR char_length(ga4_session_id) <= 255),
  CHECK (transaction_id IS NULL OR char_length(transaction_id) <= 255),
  CHECK (email_hash IS NULL OR email_hash ~ '^[0-9a-f]{64}$'),
  CHECK (source IS NULL OR char_length(source) <= 255),
  CHECK (medium IS NULL OR char_length(medium) <= 255),
  CHECK (campaign IS NULL OR char_length(campaign) <= 255),
  CHECK (content IS NULL OR char_length(content) <= 255),
  CHECK (term IS NULL OR char_length(term) <= 255),
  CHECK (click_id_type IS NULL OR char_length(click_id_type) <= 64),
  CHECK (click_id_value IS NULL OR char_length(click_id_value) <= 255),
  CHECK (char_length(source_dataset) <= 255),
  CHECK (source_table_type IN ('events', 'intraday')),
  CHECK (retained_until >= occurred_at),
  CHECK (
    customer_identity_id IS NOT NULL
    OR email_hash IS NOT NULL
    OR transaction_id IS NOT NULL
    OR ga4_client_id IS NOT NULL
    OR ga4_session_id IS NOT NULL
  )
) PARTITION BY RANGE (occurred_at);

CREATE TABLE IF NOT EXISTS ga4_fallback_candidates_default
  PARTITION OF ga4_fallback_candidates DEFAULT;

CREATE INDEX IF NOT EXISTS ga4_fallback_candidates_default_customer_identity_lookup_idx
  ON ga4_fallback_candidates_default (customer_identity_id, occurred_at DESC, ga4_session_id ASC)
  WHERE customer_identity_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ga4_fallback_candidates_default_email_hash_lookup_idx
  ON ga4_fallback_candidates_default (email_hash, occurred_at DESC, ga4_session_id ASC)
  WHERE email_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS ga4_fallback_candidates_default_transaction_lookup_idx
  ON ga4_fallback_candidates_default (transaction_id, occurred_at DESC, ga4_session_id ASC)
  WHERE transaction_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ga4_fallback_candidates_default_retention_idx
  ON ga4_fallback_candidates_default (retained_until ASC, occurred_at ASC, candidate_key ASC);

CREATE INDEX IF NOT EXISTS ga4_fallback_candidates_default_export_hour_idx
  ON ga4_fallback_candidates_default (source_export_hour DESC);

SELECT ensure_ga4_fallback_candidate_partition(
  (
    date_trunc('month', timezone('utc', now()))::date
    + make_interval(months => offset_months)
  )::date
)
FROM generate_series(-1, 1) AS offset_months;

COMMIT;

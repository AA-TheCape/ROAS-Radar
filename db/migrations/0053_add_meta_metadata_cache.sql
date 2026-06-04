BEGIN;

CREATE TABLE IF NOT EXISTS meta_ads_metadata_cache (
  id bigserial PRIMARY KEY,
  ad_account_id text NOT NULL,
  object_type text NOT NULL,
  object_id text NOT NULL,
  object_name text,
  status text,
  last_fetched_at timestamptz,
  lookup_failed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT meta_ads_metadata_cache_scope_key UNIQUE (ad_account_id, object_type, object_id),
  CONSTRAINT meta_ads_metadata_cache_object_type_chk
    CHECK (object_type IN ('campaign', 'adset')),
  CONSTRAINT meta_ads_metadata_cache_ad_account_id_chk
    CHECK (NULLIF(btrim(ad_account_id), '') IS NOT NULL),
  CONSTRAINT meta_ads_metadata_cache_object_id_chk
    CHECK (NULLIF(btrim(object_id), '') IS NOT NULL),
  CONSTRAINT meta_ads_metadata_cache_object_name_chk
    CHECK (object_name IS NULL OR NULLIF(btrim(object_name), '') IS NOT NULL),
  CONSTRAINT meta_ads_metadata_cache_status_chk
    CHECK (status IS NULL OR NULLIF(btrim(status), '') IS NOT NULL),
  CONSTRAINT meta_ads_metadata_cache_lookup_timestamp_chk
    CHECK (last_fetched_at IS NOT NULL OR lookup_failed_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS meta_ads_metadata_cache_account_type_idx
  ON meta_ads_metadata_cache (ad_account_id, object_type, updated_at DESC);

CREATE INDEX IF NOT EXISTS meta_ads_metadata_cache_freshness_idx
  ON meta_ads_metadata_cache (object_type, last_fetched_at DESC)
  WHERE last_fetched_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS meta_ads_metadata_cache_lookup_failure_idx
  ON meta_ads_metadata_cache (ad_account_id, object_type, lookup_failed_at DESC)
  WHERE lookup_failed_at IS NOT NULL;

COMMIT;

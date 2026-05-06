BEGIN;

CREATE TABLE IF NOT EXISTS ad_platform_entity_metadata (
  id bigserial PRIMARY KEY,
  tenant_id text,
  workspace_id text,
  platform text NOT NULL,
  account_id text NOT NULL,
  entity_type text NOT NULL,
  entity_id text NOT NULL,
  latest_name text NOT NULL,
  last_seen_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ad_platform_entity_metadata_platform_chk
    CHECK (platform IN ('google_ads', 'meta_ads')),
  CONSTRAINT ad_platform_entity_metadata_entity_type_chk
    CHECK (entity_type IN ('campaign', 'adset', 'ad')),
  CONSTRAINT ad_platform_entity_metadata_account_id_chk
    CHECK (NULLIF(btrim(account_id), '') IS NOT NULL),
  CONSTRAINT ad_platform_entity_metadata_entity_id_chk
    CHECK (NULLIF(btrim(entity_id), '') IS NOT NULL),
  CONSTRAINT ad_platform_entity_metadata_latest_name_chk
    CHECK (NULLIF(btrim(latest_name), '') IS NOT NULL),
  CONSTRAINT ad_platform_entity_metadata_tenant_id_chk
    CHECK (tenant_id IS NULL OR NULLIF(btrim(tenant_id), '') IS NOT NULL),
  CONSTRAINT ad_platform_entity_metadata_workspace_id_chk
    CHECK (workspace_id IS NULL OR NULLIF(btrim(workspace_id), '') IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS ad_platform_entity_metadata_scope_key_uidx
  ON ad_platform_entity_metadata (
    platform,
    account_id,
    entity_type,
    entity_id,
    COALESCE(tenant_id, ''),
    COALESCE(workspace_id, '')
  );

CREATE INDEX IF NOT EXISTS ad_platform_entity_metadata_lookup_idx
  ON ad_platform_entity_metadata (
    platform,
    account_id,
    entity_type,
    entity_id,
    tenant_id,
    workspace_id
  );

CREATE INDEX IF NOT EXISTS ad_platform_entity_metadata_entity_lookup_idx
  ON ad_platform_entity_metadata (
    platform,
    entity_id,
    account_id,
    entity_type,
    tenant_id,
    workspace_id
  );

CREATE INDEX IF NOT EXISTS ad_platform_entity_metadata_freshness_idx
  ON ad_platform_entity_metadata (platform, entity_type, last_seen_at DESC);

COMMIT;

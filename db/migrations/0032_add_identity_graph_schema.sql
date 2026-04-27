BEGIN;

CREATE TABLE IF NOT EXISTS identity_journeys (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  authoritative_shopify_customer_id text,
  primary_email_hash text,
  primary_phone_hash text,
  status text NOT NULL DEFAULT 'active',
  merge_version integer NOT NULL DEFAULT 1,
  merged_into_journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL,
  lookback_window_started_at timestamptz NOT NULL DEFAULT now(),
  lookback_window_expires_at timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
  last_touch_eligible_at timestamptz NOT NULL DEFAULT now(),
  first_source_system text,
  first_source_table text,
  first_source_record_id text,
  last_source_system text,
  last_source_table text,
  last_source_record_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_resolved_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('active', 'quarantined', 'merged', 'conflicted')),
  CHECK (merge_version >= 1),
  CHECK (lookback_window_expires_at >= lookback_window_started_at),
  CHECK (last_touch_eligible_at >= lookback_window_started_at),
  CHECK (authoritative_shopify_customer_id IS NULL OR char_length(authoritative_shopify_customer_id) <= 255),
  CHECK (primary_email_hash IS NULL OR char_length(primary_email_hash) = 64),
  CHECK (primary_phone_hash IS NULL OR char_length(primary_phone_hash) = 64),
  CHECK (first_source_system IS NULL OR char_length(first_source_system) <= 64),
  CHECK (first_source_table IS NULL OR char_length(first_source_table) <= 128),
  CHECK (first_source_record_id IS NULL OR char_length(first_source_record_id) <= 255),
  CHECK (last_source_system IS NULL OR char_length(last_source_system) <= 64),
  CHECK (last_source_table IS NULL OR char_length(last_source_table) <= 128),
  CHECK (last_source_record_id IS NULL OR char_length(last_source_record_id) <= 255)
);

CREATE UNIQUE INDEX IF NOT EXISTS identity_journeys_authoritative_shopify_customer_uidx
  ON identity_journeys (authoritative_shopify_customer_id)
  WHERE authoritative_shopify_customer_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS identity_journeys_status_last_resolved_idx
  ON identity_journeys (status, last_resolved_at DESC);

CREATE INDEX IF NOT EXISTS identity_journeys_lookback_expires_at_idx
  ON identity_journeys (lookback_window_expires_at DESC);

CREATE INDEX IF NOT EXISTS identity_journeys_lookback_status_idx
  ON identity_journeys (status, lookback_window_expires_at DESC);

CREATE TABLE IF NOT EXISTS identity_nodes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  node_type text NOT NULL,
  node_key text NOT NULL,
  is_authoritative boolean NOT NULL DEFAULT false,
  is_ambiguous boolean NOT NULL DEFAULT false,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (node_type IN (
    'session_id',
    'checkout_token',
    'cart_token',
    'shopify_customer_id',
    'hashed_email',
    'phone_hash'
  )),
  CHECK (
    (node_type = 'shopify_customer_id' AND is_authoritative = true)
    OR (node_type <> 'shopify_customer_id' AND is_authoritative = false)
  ),
  CHECK (char_length(node_key) <= 255),
  CHECK (last_seen_at >= first_seen_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS identity_nodes_node_type_key_uidx
  ON identity_nodes (node_type, node_key);

CREATE INDEX IF NOT EXISTS identity_nodes_authoritative_lookup_idx
  ON identity_nodes (node_key)
  WHERE node_type = 'shopify_customer_id';

CREATE INDEX IF NOT EXISTS identity_nodes_hashed_email_lookup_idx
  ON identity_nodes (node_key)
  WHERE node_type = 'hashed_email';

CREATE INDEX IF NOT EXISTS identity_nodes_phone_hash_lookup_idx
  ON identity_nodes (node_key)
  WHERE node_type = 'phone_hash';

CREATE INDEX IF NOT EXISTS identity_nodes_checkout_token_lookup_idx
  ON identity_nodes (node_key)
  WHERE node_type = 'checkout_token';

CREATE INDEX IF NOT EXISTS identity_nodes_cart_token_lookup_idx
  ON identity_nodes (node_key)
  WHERE node_type = 'cart_token';

CREATE INDEX IF NOT EXISTS identity_nodes_session_id_lookup_idx
  ON identity_nodes (node_key)
  WHERE node_type = 'session_id';

CREATE TABLE IF NOT EXISTS identity_edges (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  node_id uuid NOT NULL REFERENCES identity_nodes(id) ON DELETE CASCADE,
  journey_id uuid NOT NULL REFERENCES identity_journeys(id) ON DELETE CASCADE,
  edge_type text NOT NULL,
  precedence_rank smallint NOT NULL,
  evidence_source text NOT NULL,
  source_table text,
  source_record_id text,
  is_active boolean NOT NULL DEFAULT true,
  superseded_by_edge_id uuid REFERENCES identity_edges(id) ON DELETE SET NULL,
  conflict_code text,
  first_observed_at timestamptz NOT NULL DEFAULT now(),
  last_observed_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (edge_type IN ('authoritative', 'deterministic', 'promoted', 'quarantined')),
  CHECK (precedence_rank >= 0),
  CHECK (char_length(evidence_source) <= 64),
  CHECK (source_table IS NULL OR char_length(source_table) <= 128),
  CHECK (source_record_id IS NULL OR char_length(source_record_id) <= 255),
  CHECK (conflict_code IS NULL OR char_length(conflict_code) <= 128),
  CHECK (last_observed_at >= first_observed_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS identity_edges_active_node_uidx
  ON identity_edges (node_id)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS identity_edges_journey_active_rank_idx
  ON identity_edges (journey_id, precedence_rank DESC, last_observed_at DESC)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS identity_edges_active_source_idx
  ON identity_edges (evidence_source, last_observed_at DESC)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS identity_edges_conflict_lookup_idx
  ON identity_edges (conflict_code, last_observed_at DESC)
  WHERE conflict_code IS NOT NULL;

CREATE TEMP TABLE normalized_customer_identities AS
SELECT
  ci.id,
  ci.shopify_customer_id,
  ci.hashed_email,
  ci.created_at,
  ci.updated_at,
  LEAST(ci.created_at, COALESCE(ci.last_stitched_at, ci.created_at, now())) AS normalized_first_seen_at,
  GREATEST(ci.created_at, COALESCE(ci.last_stitched_at, ci.created_at, now())) AS normalized_last_seen_at
FROM customer_identities ci;

INSERT INTO identity_journeys (
  id,
  authoritative_shopify_customer_id,
  primary_email_hash,
  status,
  merge_version,
  lookback_window_started_at,
  lookback_window_expires_at,
  last_touch_eligible_at,
  first_source_system,
  first_source_table,
  first_source_record_id,
  last_source_system,
  last_source_table,
  last_source_record_id,
  created_at,
  updated_at,
  last_resolved_at
)
SELECT
  ci.id,
  ci.shopify_customer_id,
  ci.hashed_email,
  'active',
  1,
  ci.normalized_last_seen_at,
  ci.normalized_last_seen_at + interval '30 days',
  ci.normalized_last_seen_at,
  'backfill',
  'customer_identities',
  ci.id::text,
  'backfill',
  'customer_identities',
  ci.id::text,
  ci.created_at,
  ci.updated_at,
  ci.normalized_last_seen_at
FROM normalized_customer_identities ci
ON CONFLICT (id) DO NOTHING;

INSERT INTO identity_nodes (
  node_type,
  node_key,
  is_authoritative,
  is_ambiguous,
  first_seen_at,
  last_seen_at,
  created_at,
  updated_at
)
SELECT
  'shopify_customer_id',
  ci.shopify_customer_id,
  true,
  false,
  ci.normalized_first_seen_at,
  ci.normalized_last_seen_at,
  ci.created_at,
  ci.updated_at
FROM normalized_customer_identities ci
WHERE ci.shopify_customer_id IS NOT NULL
ON CONFLICT (node_type, node_key) DO NOTHING;

INSERT INTO identity_nodes (
  node_type,
  node_key,
  is_authoritative,
  is_ambiguous,
  first_seen_at,
  last_seen_at,
  created_at,
  updated_at
)
SELECT
  'hashed_email',
  ci.hashed_email,
  false,
  false,
  ci.normalized_first_seen_at,
  ci.normalized_last_seen_at,
  ci.created_at,
  ci.updated_at
FROM normalized_customer_identities ci
WHERE ci.hashed_email IS NOT NULL
ON CONFLICT (node_type, node_key) DO NOTHING;

INSERT INTO identity_edges (
  node_id,
  journey_id,
  edge_type,
  precedence_rank,
  evidence_source,
  source_table,
  source_record_id,
  is_active,
  first_observed_at,
  last_observed_at,
  created_at,
  updated_at
)
SELECT
  nodes.id,
  ci.id,
  'authoritative',
  100,
  'backfill',
  'customer_identities',
  ci.id::text,
  true,
  ci.normalized_first_seen_at,
  ci.normalized_last_seen_at,
  ci.created_at,
  ci.updated_at
FROM normalized_customer_identities ci
INNER JOIN identity_nodes nodes
  ON nodes.node_type = 'shopify_customer_id'
 AND nodes.node_key = ci.shopify_customer_id
WHERE ci.shopify_customer_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM identity_edges existing
    WHERE existing.node_id = nodes.id
      AND existing.is_active = true
  );

INSERT INTO identity_edges (
  node_id,
  journey_id,
  edge_type,
  precedence_rank,
  evidence_source,
  source_table,
  source_record_id,
  is_active,
  first_observed_at,
  last_observed_at,
  created_at,
  updated_at
)
SELECT
  nodes.id,
  ci.id,
  'deterministic',
  70,
  'backfill',
  'customer_identities',
  ci.id::text,
  true,
  ci.normalized_first_seen_at,
  ci.normalized_last_seen_at,
  ci.created_at,
  ci.updated_at
FROM normalized_customer_identities ci
INNER JOIN identity_nodes nodes
  ON nodes.node_type = 'hashed_email'
 AND nodes.node_key = ci.hashed_email
WHERE ci.hashed_email IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM identity_edges existing
    WHERE existing.node_id = nodes.id
      AND existing.is_active = true
  );

ALTER TABLE tracking_sessions
  ADD COLUMN IF NOT EXISTS identity_journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL;

ALTER TABLE tracking_events
  ADD COLUMN IF NOT EXISTS identity_journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL;

ALTER TABLE shopify_customers
  ADD COLUMN IF NOT EXISTS identity_journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL;

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS identity_journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL;

ALTER TABLE session_attribution_identities
  ADD COLUMN IF NOT EXISTS identity_journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL;

UPDATE tracking_sessions
SET identity_journey_id = customer_identity_id
WHERE identity_journey_id IS NULL
  AND customer_identity_id IS NOT NULL;

UPDATE tracking_events
SET identity_journey_id = customer_identity_id
WHERE identity_journey_id IS NULL
  AND customer_identity_id IS NOT NULL;

UPDATE shopify_customers
SET identity_journey_id = customer_identity_id
WHERE identity_journey_id IS NULL
  AND customer_identity_id IS NOT NULL;

UPDATE shopify_orders
SET identity_journey_id = customer_identity_id
WHERE identity_journey_id IS NULL
  AND customer_identity_id IS NOT NULL;

UPDATE session_attribution_identities
SET identity_journey_id = customer_identity_id
WHERE identity_journey_id IS NULL
  AND customer_identity_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS tracking_sessions_identity_journey_id_idx
  ON tracking_sessions (identity_journey_id)
  WHERE identity_journey_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS tracking_events_identity_journey_id_idx
  ON tracking_events (identity_journey_id)
  WHERE identity_journey_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS shopify_customers_identity_journey_id_idx
  ON shopify_customers (identity_journey_id)
  WHERE identity_journey_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS shopify_orders_identity_journey_id_idx
  ON shopify_orders (identity_journey_id)
  WHERE identity_journey_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS session_attribution_identities_identity_journey_id_idx
  ON session_attribution_identities (identity_journey_id)
  WHERE identity_journey_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS session_attribution_identities_identity_journey_retention_idx
  ON session_attribution_identities (identity_journey_id, retained_until DESC)
  WHERE identity_journey_id IS NOT NULL;

COMMIT;

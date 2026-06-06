BEGIN;

CREATE OR REPLACE FUNCTION roas_attribution_tier_rank(tier text)
RETURNS integer
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE $1
    WHEN 'deterministic_first_party' THEN 1
    WHEN 'deterministic_shopify_hint' THEN 2
    WHEN 'platform_reported_meta' THEN 3
    WHEN 'ga4_fallback' THEN 4
    WHEN 'unattributed' THEN 5
    ELSE NULL
  END
$$;

CREATE OR REPLACE FUNCTION roas_canonical_tier_from_reason(reason text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN $1 IN (
      'matched_by_landing_session',
      'matched_by_checkout_token',
      'matched_by_cart_token',
      'matched_by_customer_identity'
    ) THEN 'deterministic_first_party'
    WHEN $1 = 'shopify_hint_derived' THEN 'deterministic_shopify_hint'
    WHEN $1 = 'meta_platform_reported_match' THEN 'platform_reported_meta'
    WHEN $1 = 'unattributed' THEN 'unattributed'
    WHEN $1 IS NULL THEN NULL
    ELSE 'ga4_fallback'
  END
$$;

CREATE OR REPLACE FUNCTION roas_validate_attribution_decision_artifact_precedence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.first_party_winner_present
    AND NEW.canonical_tier_after <> 'deterministic_first_party' THEN
    RAISE EXCEPTION USING
      ERRCODE = '23514',
      MESSAGE = format(
        'Cannot persist canonical tier %s for order %s because deterministic_first_party evidence is present in the decision artifact.',
        NEW.canonical_tier_after,
        NEW.shopify_order_id
      ),
      HINT = 'Set canonical_tier_after to deterministic_first_party or clear first_party_winner_present.';
  END IF;

  IF NOT NEW.first_party_winner_present
    AND NEW.shopify_hint_winner_present
    AND NEW.canonical_tier_after <> 'deterministic_shopify_hint' THEN
    RAISE EXCEPTION USING
      ERRCODE = '23514',
      MESSAGE = format(
        'Cannot persist canonical tier %s for order %s because deterministic_shopify_hint evidence is present in the decision artifact.',
        NEW.canonical_tier_after,
        NEW.shopify_order_id
      ),
      HINT = 'Set canonical_tier_after to deterministic_shopify_hint or clear shopify_hint_winner_present.';
  END IF;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION roas_validate_shopify_order_canonical_precedence(target_shopify_order_id text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  order_row RECORD;
  artifact_row RECORD;
  result_tier text;
BEGIN
  SELECT
    shopify_order_id,
    attribution_tier,
    latest_attribution_decision_artifact_id
  INTO order_row
  FROM shopify_orders
  WHERE shopify_order_id = target_shopify_order_id;

  IF NOT FOUND OR order_row.attribution_tier IS NULL THEN
    RETURN;
  END IF;

  SELECT roas_canonical_tier_from_reason(attribution_reason)
  INTO result_tier
  FROM attribution_results
  WHERE shopify_order_id = target_shopify_order_id;

  IF result_tier IS NOT NULL
    AND roas_attribution_tier_rank(result_tier) < roas_attribution_tier_rank(order_row.attribution_tier) THEN
    RAISE EXCEPTION USING
      ERRCODE = '23514',
      MESSAGE = format(
        'Cannot persist canonical tier %s for order %s because attribution_results already imply higher-precedence %s evidence.',
        order_row.attribution_tier,
        target_shopify_order_id,
        result_tier
      ),
      DETAIL = format('attribution_results tier derived from attribution_reason for order %s.', target_shopify_order_id),
      HINT = 'Persist the higher-precedence canonical tier or correct the underlying attribution_results row first.';
  END IF;

  IF order_row.latest_attribution_decision_artifact_id IS NULL THEN
    RETURN;
  END IF;

  SELECT
    id,
    shopify_order_id,
    canonical_tier_after,
    first_party_winner_present,
    shopify_hint_winner_present
  INTO artifact_row
  FROM attribution_decision_artifacts
  WHERE id = order_row.latest_attribution_decision_artifact_id;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  IF artifact_row.shopify_order_id <> target_shopify_order_id THEN
    RAISE EXCEPTION USING
      ERRCODE = '23514',
      MESSAGE = format(
        'Cannot link decision artifact %s to order %s because the artifact belongs to order %s.',
        artifact_row.id,
        target_shopify_order_id,
        artifact_row.shopify_order_id
      ),
      HINT = 'Link shopify_orders.latest_attribution_decision_artifact_id only to an artifact for the same shopify_order_id.';
  END IF;

  IF artifact_row.canonical_tier_after <> order_row.attribution_tier THEN
    RAISE EXCEPTION USING
      ERRCODE = '23514',
      MESSAGE = format(
        'Cannot persist canonical tier %s for order %s because the linked decision artifact resolves to %s.',
        order_row.attribution_tier,
        target_shopify_order_id,
        artifact_row.canonical_tier_after
      ),
      HINT = 'Keep shopify_orders.attribution_tier aligned with attribution_decision_artifacts.canonical_tier_after.';
  END IF;

  IF artifact_row.first_party_winner_present
    AND order_row.attribution_tier <> 'deterministic_first_party' THEN
    RAISE EXCEPTION USING
      ERRCODE = '23514',
      MESSAGE = format(
        'Cannot persist canonical tier %s for order %s because deterministic_first_party evidence is present in the linked decision artifact.',
        order_row.attribution_tier,
        target_shopify_order_id
      ),
      HINT = 'Persist deterministic_first_party while first_party_winner_present is true.';
  END IF;

  IF NOT artifact_row.first_party_winner_present
    AND artifact_row.shopify_hint_winner_present
    AND order_row.attribution_tier <> 'deterministic_shopify_hint' THEN
    RAISE EXCEPTION USING
      ERRCODE = '23514',
      MESSAGE = format(
        'Cannot persist canonical tier %s for order %s because deterministic_shopify_hint evidence is present in the linked decision artifact.',
        order_row.attribution_tier,
        target_shopify_order_id
      ),
      HINT = 'Persist deterministic_shopify_hint while shopify_hint_winner_present is true.';
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION roas_enforce_shopify_order_canonical_precedence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM roas_validate_shopify_order_canonical_precedence(NEW.shopify_order_id);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION roas_enforce_attribution_result_canonical_precedence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.shopify_order_id IS DISTINCT FROM NEW.shopify_order_id THEN
    PERFORM roas_validate_shopify_order_canonical_precedence(OLD.shopify_order_id);
  END IF;

  PERFORM roas_validate_shopify_order_canonical_precedence(NEW.shopify_order_id);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION roas_enforce_decision_artifact_linked_order_precedence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  linked_order RECORD;
BEGIN
  FOR linked_order IN
    SELECT shopify_order_id
    FROM shopify_orders
    WHERE latest_attribution_decision_artifact_id = NEW.id
  LOOP
    PERFORM roas_validate_shopify_order_canonical_precedence(linked_order.shopify_order_id);
  END LOOP;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS attribution_decision_artifacts_precedence_guard
  ON attribution_decision_artifacts;

CREATE TRIGGER attribution_decision_artifacts_precedence_guard
BEFORE INSERT OR UPDATE OF canonical_tier_after, first_party_winner_present, shopify_hint_winner_present
ON attribution_decision_artifacts
FOR EACH ROW
EXECUTE FUNCTION roas_validate_attribution_decision_artifact_precedence();

DROP TRIGGER IF EXISTS shopify_orders_canonical_precedence_guard
  ON shopify_orders;

CREATE CONSTRAINT TRIGGER shopify_orders_canonical_precedence_guard
AFTER INSERT OR UPDATE OF attribution_tier, latest_attribution_decision_artifact_id
ON shopify_orders
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION roas_enforce_shopify_order_canonical_precedence();

DROP TRIGGER IF EXISTS attribution_results_canonical_precedence_guard
  ON attribution_results;

CREATE CONSTRAINT TRIGGER attribution_results_canonical_precedence_guard
AFTER INSERT OR UPDATE OF attribution_reason, shopify_order_id
ON attribution_results
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION roas_enforce_attribution_result_canonical_precedence();

DROP TRIGGER IF EXISTS attribution_decision_artifacts_linked_order_precedence_guard
  ON attribution_decision_artifacts;

CREATE CONSTRAINT TRIGGER attribution_decision_artifacts_linked_order_precedence_guard
AFTER INSERT OR UPDATE OF shopify_order_id, canonical_tier_after, first_party_winner_present, shopify_hint_winner_present
ON attribution_decision_artifacts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION roas_enforce_decision_artifact_linked_order_precedence();

COMMIT;

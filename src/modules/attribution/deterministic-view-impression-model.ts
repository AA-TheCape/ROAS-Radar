import type { PoolClient } from 'pg';

export const DETERMINISTIC_VIEW_IMPRESSION_MODELS = ['deterministic_views', 'deterministic_impressions'] as const;

export type DeterministicViewImpressionModel = (typeof DETERMINISTIC_VIEW_IMPRESSION_MODELS)[number];

type DeterministicViewImpressionModelPersistResult = {
  enabled: boolean;
  insertedRows: number;
};

export function isDeterministicViewImpressionAttributionEnabled(metadata: Record<string, unknown>): boolean {
  return metadata.deterministicViewImpressionAttributionEnabled === true;
}

export async function persistDeterministicViewImpressionModelOutputs(
  client: PoolClient,
  input: {
    runId: string;
    orderId: string;
    orderOccurredAtUtc: string;
    enabled: boolean;
  }
): Promise<DeterministicViewImpressionModelPersistResult> {
  if (!input.enabled) {
    return {
      enabled: false,
      insertedRows: 0
    };
  }

  await client.query(
    `
      DELETE FROM deterministic_model_outputs
      WHERE run_id = $1::uuid
        AND order_id = $2
        AND model_key IN ('deterministic_views', 'deterministic_impressions')
    `,
    [input.runId, input.orderId]
  );

  const result = await client.query(
    `
      WITH candidate_facts AS (
        SELECT
          facts.*,
          CASE
            WHEN facts.event_type = 'view' THEN 'deterministic_views'
            WHEN facts.event_type = 'impression' THEN 'deterministic_impressions'
          END AS model_key
        FROM deterministic_event_facts facts
        WHERE facts.event_type IN ('view', 'impression')
          AND facts.platform_verified = true
          AND facts.normalization_status IN ('normalized', 'partial')
          AND facts.fact_date >= ($3::timestamptz - interval '7 days')::date
          AND facts.fact_date <= $3::date
          AND EXISTS (
            SELECT 1
            FROM attribution_touchpoint_inputs touchpoints
            WHERE touchpoints.run_id = $1::uuid
              AND touchpoints.order_id = $2
              AND touchpoints.is_eligible = true
              AND (
                (
                  facts.campaign_id IS NOT NULL
                  AND lower(btrim(facts.campaign_id)) IN (
                    lower(btrim(COALESCE(touchpoints.campaign, ''))),
                    lower(btrim(COALESCE(touchpoints.content, ''))),
                    lower(btrim(COALESCE(touchpoints.term, '')))
                  )
                )
                OR (
                  facts.adset_id IS NOT NULL
                  AND lower(btrim(facts.adset_id)) IN (
                    lower(btrim(COALESCE(touchpoints.campaign, ''))),
                    lower(btrim(COALESCE(touchpoints.content, ''))),
                    lower(btrim(COALESCE(touchpoints.term, '')))
                  )
                )
                OR (
                  facts.ad_id IS NOT NULL
                  AND lower(btrim(facts.ad_id)) IN (
                    lower(btrim(COALESCE(touchpoints.campaign, ''))),
                    lower(btrim(COALESCE(touchpoints.content, ''))),
                    lower(btrim(COALESCE(touchpoints.term, '')))
                  )
                )
              )
          )
      ),
      weighted_facts AS (
        SELECT
          candidate_facts.*,
          SUM(candidate_facts.event_count) OVER (PARTITION BY candidate_facts.model_key) AS model_event_count
        FROM candidate_facts
        WHERE candidate_facts.model_key IS NOT NULL
      )
      INSERT INTO deterministic_model_outputs (
        run_id,
        order_id,
        fact_id,
        model_key,
        output_type,
        platform,
        account_id,
        campaign_id,
        adset_id,
        ad_id,
        event_type,
        fact_date,
        evidence_origin,
        platform_verified,
        contribution_weight,
        contributed_event_count,
        output_metadata
      )
      SELECT
        $1::uuid,
        $2,
        id,
        model_key,
        'credited_input',
        platform,
        account_id,
        campaign_id,
        adset_id,
        ad_id,
        event_type,
        fact_date,
        evidence_origin,
        platform_verified,
        CASE
          WHEN model_event_count > 0 THEN event_count::numeric / model_event_count::numeric
          ELSE 0
        END,
        event_count::numeric,
        jsonb_build_object(
          'modelPath', 'deterministic_view_impression',
          'orderOccurredAtUtc', $3::text,
          'windowDays', 7,
          'creditSource', 'platform_verified_event_fact'
        )
      FROM weighted_facts
      ON CONFLICT (run_id, order_id, fact_id, model_key, output_type)
      DO UPDATE SET
        contribution_weight = EXCLUDED.contribution_weight,
        contributed_event_count = EXCLUDED.contributed_event_count,
        output_metadata = EXCLUDED.output_metadata,
        generated_at_utc = now()
    `,
    [input.runId, input.orderId, input.orderOccurredAtUtc]
  );

  return {
    enabled: true,
    insertedRows: result.rowCount ?? 0
  };
}

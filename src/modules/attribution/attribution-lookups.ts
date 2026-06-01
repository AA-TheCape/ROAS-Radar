import type { PoolClient } from 'pg';

export type AttributionLookupPair = {
  attributionSourceId: number;
  matchingMethodId: number;
};

export class AttributionLookupPairError extends Error {
  readonly code = 'attribution_lookup_pair_not_found';

  constructor(
    readonly attributionSourceCode: string,
    readonly matchingMethodCode: string
  ) {
    super(
      `Active attribution lookup pair not found for source "${attributionSourceCode}" and method "${matchingMethodCode}"`
    );
    this.name = 'AttributionLookupPairError';
  }
}

export async function resolveActiveAttributionLookupPair(
  client: PoolClient,
  input: {
    attributionSourceCode: string;
    matchingMethodCode: string;
  }
): Promise<AttributionLookupPair> {
  const result = await client.query<{
    attribution_source_id: number;
    matching_method_id: number;
  }>(
    `
      SELECT
        sources.id AS attribution_source_id,
        methods.id AS matching_method_id
      FROM attribution_sources sources
      JOIN matching_methods methods
        ON methods.attribution_source_id = sources.id
        AND methods.code = $2
        AND methods.is_active = true
      WHERE sources.code = $1
        AND sources.is_active = true
    `,
    [input.attributionSourceCode, input.matchingMethodCode]
  );

  const pair = result.rows[0];
  if (!pair) {
    throw new AttributionLookupPairError(input.attributionSourceCode, input.matchingMethodCode);
  }

  return {
    attributionSourceId: pair.attribution_source_id,
    matchingMethodId: pair.matching_method_id
  };
}

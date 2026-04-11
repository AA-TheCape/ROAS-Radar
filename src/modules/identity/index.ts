import { createHash, randomUUID } from 'node:crypto';

import { type PoolClient } from 'pg';

type IdentityRecord = {
  id: string;
  hashed_email: string | null;
  shopify_customer_id: string | null;
};

type IdentityStitchInput = {
  shopifyOrderId: string;
  shopifyCustomerId: string | null;
  email: string | null;
  landingSessionId: string | null;
  checkoutToken: string | null;
  cartToken: string | null;
};

type IdentityStitchDecision =
  | {
      outcome: 'skipped';
      reason: 'missing_identifiers';
      emailHash: string | null;
    }
  | {
      outcome: 'conflict';
      reason:
        | 'customer_id_conflicts_with_existing_email'
        | 'email_hash_conflicts_with_existing_customer_id'
        | 'identifiers_resolve_to_different_identities';
      emailHash: string | null;
    }
  | {
      outcome: 'linked';
      identityId: string | null;
      emailHash: string | null;
      shopifyCustomerId: string | null;
      operation: 'create' | 'reuse';
    };

export type IdentityStitchResult = {
  outcome: IdentityStitchDecision['outcome'];
  reason:
    | 'missing_identifiers'
    | 'customer_id_conflicts_with_existing_email'
    | 'email_hash_conflicts_with_existing_customer_id'
    | 'identifiers_resolve_to_different_identities'
    | 'create_identity'
    | 'reuse_identity';
  identityId: string | null;
  emailHash: string | null;
  linkedSessionIds: string[];
};

export function normalizeIdentityEmail(email: string | null | undefined): string | null {
  const normalized = email?.trim().toLowerCase();
  return normalized ? normalized : null;
}

export function hashIdentityEmail(email: string | null | undefined): string | null {
  const normalized = normalizeIdentityEmail(email);

  if (!normalized) {
    return null;
  }

  return createHash('sha256').update(normalized).digest('hex');
}

export function resolveIdentityStitch(
  existingIdentities: IdentityRecord[],
  input: Pick<IdentityStitchInput, 'shopifyCustomerId' | 'email'> & { emailHash?: string | null }
): IdentityStitchDecision {
  const shopifyCustomerId = normalizeNullableString(input.shopifyCustomerId);
  const emailHash = input.emailHash ?? hashIdentityEmail(input.email);

  if (!shopifyCustomerId && !emailHash) {
    return {
      outcome: 'skipped',
      reason: 'missing_identifiers',
      emailHash
    };
  }

  const identityByCustomerId = shopifyCustomerId
    ? existingIdentities.find((identity) => identity.shopify_customer_id === shopifyCustomerId) ?? null
    : null;
  const identityByEmailHash = emailHash
    ? existingIdentities.find((identity) => identity.hashed_email === emailHash) ?? null
    : null;

  if (identityByCustomerId && emailHash && identityByCustomerId.hashed_email && identityByCustomerId.hashed_email !== emailHash) {
    return {
      outcome: 'conflict',
      reason: 'customer_id_conflicts_with_existing_email',
      emailHash
    };
  }

  if (identityByEmailHash && shopifyCustomerId && identityByEmailHash.shopify_customer_id && identityByEmailHash.shopify_customer_id !== shopifyCustomerId) {
    return {
      outcome: 'conflict',
      reason: 'email_hash_conflicts_with_existing_customer_id',
      emailHash
    };
  }

  if (identityByCustomerId && identityByEmailHash && identityByCustomerId.id !== identityByEmailHash.id) {
    return {
      outcome: 'conflict',
      reason: 'identifiers_resolve_to_different_identities',
      emailHash
    };
  }

  const reusedIdentity = identityByCustomerId ?? identityByEmailHash;

  if (reusedIdentity) {
    return {
      outcome: 'linked',
      identityId: reusedIdentity.id,
      emailHash,
      shopifyCustomerId,
      operation: 'reuse'
    };
  }

  return {
    outcome: 'linked',
    identityId: null,
    emailHash,
    shopifyCustomerId,
    operation: 'create'
  };
}

export async function stitchKnownCustomerIdentity(
  client: PoolClient,
  input: IdentityStitchInput
): Promise<IdentityStitchResult> {
  const emailHash = hashIdentityEmail(input.email);
  const existingIdentities = await findExistingIdentities(client, input.shopifyCustomerId, emailHash);
  const decision = resolveIdentityStitch(existingIdentities, {
    shopifyCustomerId: input.shopifyCustomerId,
    email: input.email,
    emailHash
  });

  await client.query(
    `
      UPDATE shopify_orders
      SET email_hash = COALESCE($2, email_hash)
      WHERE shopify_order_id = $1
    `,
    [input.shopifyOrderId, emailHash]
  );

  if (decision.outcome !== 'linked') {
    return {
      outcome: decision.outcome,
      reason: decision.reason,
      identityId: null,
      emailHash: decision.emailHash,
      linkedSessionIds: []
    };
  }

  const identityId = await upsertCustomerIdentity(client, decision);
  await syncCustomerIdentityReferences(client, {
    identityId,
    shopifyOrderId: input.shopifyOrderId,
    shopifyCustomerId: input.shopifyCustomerId,
    emailHash
  });

  const linkedSessionIds = await linkCandidateSessions(client, {
    identityId,
    landingSessionId: input.landingSessionId,
    checkoutToken: input.checkoutToken,
    cartToken: input.cartToken
  });

  return {
    outcome: 'linked',
    reason: `${decision.operation}_identity`,
    identityId,
    emailHash,
    linkedSessionIds
  };
}

async function findExistingIdentities(
  client: PoolClient,
  shopifyCustomerId: string | null,
  emailHash: string | null
): Promise<IdentityRecord[]> {
  const result = await client.query<IdentityRecord>(
    `
      SELECT
        id,
        hashed_email,
        shopify_customer_id
      FROM customer_identities
      WHERE ($1::text IS NOT NULL AND shopify_customer_id = $1)
         OR ($2::text IS NOT NULL AND hashed_email = $2)
      ORDER BY created_at ASC
    `,
    [normalizeNullableString(shopifyCustomerId), emailHash]
  );

  return result.rows;
}

async function upsertCustomerIdentity(
  client: PoolClient,
  decision: Extract<IdentityStitchDecision, { outcome: 'linked' }>
): Promise<string> {
  const identityId = decision.identityId ?? randomUUID();

  try {
    await client.query(
      `
        INSERT INTO customer_identities (
          id,
          hashed_email,
          shopify_customer_id,
          created_at,
          updated_at,
          last_stitched_at
        )
        VALUES ($1::uuid, $2, $3, now(), now(), now())
        ON CONFLICT (id)
        DO UPDATE SET
          hashed_email = COALESCE(customer_identities.hashed_email, EXCLUDED.hashed_email),
          shopify_customer_id = COALESCE(customer_identities.shopify_customer_id, EXCLUDED.shopify_customer_id),
          updated_at = now(),
          last_stitched_at = now()
      `,
      [identityId, decision.emailHash, decision.shopifyCustomerId]
    );
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505') {
      const existingIdentities = await findExistingIdentities(client, decision.shopifyCustomerId, decision.emailHash);
      const resolved = resolveIdentityStitch(existingIdentities, {
        shopifyCustomerId: decision.shopifyCustomerId,
        email: null,
        emailHash: decision.emailHash
      });

      if (resolved.outcome === 'linked' && resolved.identityId) {
        await client.query(
          `
            UPDATE customer_identities
            SET
              hashed_email = COALESCE(customer_identities.hashed_email, $2),
              shopify_customer_id = COALESCE(customer_identities.shopify_customer_id, $3),
              updated_at = now(),
              last_stitched_at = now()
            WHERE id = $1::uuid
          `,
          [resolved.identityId, decision.emailHash, decision.shopifyCustomerId]
        );

        return resolved.identityId;
      }
    }

    throw error;
  }

  return identityId;
}

async function syncCustomerIdentityReferences(
  client: PoolClient,
  input: {
    identityId: string;
    shopifyOrderId: string;
    shopifyCustomerId: string | null;
    emailHash: string | null;
  }
): Promise<void> {
  await client.query(
    `
      UPDATE shopify_orders
      SET
        email_hash = COALESCE($2, email_hash),
        customer_identity_id = CASE
          WHEN customer_identity_id IS NULL OR customer_identity_id = $3::uuid THEN $3::uuid
          ELSE customer_identity_id
        END
      WHERE shopify_order_id = $1
         OR ($2::text IS NOT NULL AND email_hash = $2)
         OR ($4::text IS NOT NULL AND shopify_customer_id = $4)
    `,
    [input.shopifyOrderId, input.emailHash, input.identityId, input.shopifyCustomerId]
  );

  if (!input.shopifyCustomerId) {
    return;
  }

  await client.query(
    `
      UPDATE shopify_customers
      SET
        email_hash = COALESCE($2, email_hash),
        customer_identity_id = CASE
          WHEN customer_identity_id IS NULL OR customer_identity_id = $3::uuid THEN $3::uuid
          ELSE customer_identity_id
        END,
        updated_at = now()
      WHERE shopify_customer_id = $1
    `,
    [input.shopifyCustomerId, input.emailHash, input.identityId]
  );
}

async function linkCandidateSessions(
  client: PoolClient,
  input: {
    identityId: string;
    landingSessionId: string | null;
    checkoutToken: string | null;
    cartToken: string | null;
  }
): Promise<string[]> {
  const candidateResult = await client.query<{ session_id: string }>(
    `
      WITH candidate_sessions AS (
        SELECT $2::uuid AS session_id
        WHERE $2::uuid IS NOT NULL

        UNION

        SELECT DISTINCT e.session_id
        FROM tracking_events e
        WHERE ($3::text IS NOT NULL AND e.shopify_checkout_token = $3)
           OR ($4::text IS NOT NULL AND e.shopify_cart_token = $4)
      )
      SELECT DISTINCT s.id AS session_id
      FROM candidate_sessions c
      INNER JOIN tracking_sessions s ON s.id = c.session_id
      WHERE s.customer_identity_id IS NULL OR s.customer_identity_id = $1::uuid
    `,
    [input.identityId, input.landingSessionId, input.checkoutToken, input.cartToken]
  );

  const sessionIds = candidateResult.rows.map((row) => row.session_id);

  if (sessionIds.length === 0) {
    return [];
  }

  await client.query(
    `
      UPDATE tracking_sessions
      SET
        customer_identity_id = $1::uuid,
        updated_at = now()
      WHERE id = ANY($2::uuid[])
    `,
    [input.identityId, sessionIds]
  );

  await client.query(
    `
      UPDATE tracking_events
      SET customer_identity_id = $1::uuid
      WHERE session_id = ANY($2::uuid[])
        AND (customer_identity_id IS NULL OR customer_identity_id = $1::uuid)
    `,
    [input.identityId, sessionIds]
  );

  return sessionIds;
}

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

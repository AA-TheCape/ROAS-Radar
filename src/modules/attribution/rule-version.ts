export const SUPPORTED_ATTRIBUTION_RESOLVER_RULE_VERSIONS = [
  'attribution_resolver_v1',
  'attribution_resolver_v2'
] as const;

export type AttributionResolverRuleVersion =
  (typeof SUPPORTED_ATTRIBUTION_RESOLVER_RULE_VERSIONS)[number];

type ForwardProcessingResolverVersionOrder = {
  attributionTier: string | null;
  attributionResolverRuleVersion: string | null;
};

export const ATTRIBUTION_RESOLVER_RULE_VERSION: AttributionResolverRuleVersion =
  'attribution_resolver_v2';

export function isSupportedAttributionResolverRuleVersion(
  value: string | null | undefined
): value is AttributionResolverRuleVersion {
  return (SUPPORTED_ATTRIBUTION_RESOLVER_RULE_VERSIONS as readonly string[]).includes(value ?? '');
}

export function assertSupportedAttributionResolverRuleVersion(
  value: string | null | undefined
): AttributionResolverRuleVersion {
  if (!isSupportedAttributionResolverRuleVersion(value)) {
    throw new Error(`Unsupported attribution resolver rule version: ${value ?? 'null'}`);
  }

  return value;
}

export function selectResolverRuleVersionForForwardProcessing(
  order: ForwardProcessingResolverVersionOrder
): AttributionResolverRuleVersion {
  if (!order.attributionTier) {
    return ATTRIBUTION_RESOLVER_RULE_VERSION;
  }

  if (isSupportedAttributionResolverRuleVersion(order.attributionResolverRuleVersion)) {
    return order.attributionResolverRuleVersion;
  }

  return 'attribution_resolver_v1';
}

export const SUPPORTED_ATTRIBUTION_RESOLVER_RULE_VERSIONS = [
    'attribution_resolver_v1',
    'attribution_resolver_v2'
];
export const ATTRIBUTION_RESOLVER_RULE_VERSION = 'attribution_resolver_v2';
export function isSupportedAttributionResolverRuleVersion(value) {
    return SUPPORTED_ATTRIBUTION_RESOLVER_RULE_VERSIONS.includes(value ?? '');
}
export function assertSupportedAttributionResolverRuleVersion(value) {
    if (!isSupportedAttributionResolverRuleVersion(value)) {
        throw new Error(`Unsupported attribution resolver rule version: ${value ?? 'null'}`);
    }
    return value;
}

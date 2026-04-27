import React from 'react';

import type { AttributionTier } from '../lib/api';
import {
  ATTRIBUTION_TIER_PRECEDENCE_TOOLTIP,
  formatAttributionTierLabel,
  getAttributionTierBadgeTone,
  getAttributionTierDescription
} from '../lib/attributionTier';
import { Badge, Tooltip } from './AuthenticatedUi';

export function AttributionTierBadge({
  tier,
  withTooltip = false,
  className
}: {
  tier: AttributionTier;
  withTooltip?: boolean;
  className?: string;
}) {
  const badge = (
    <Badge tone={getAttributionTierBadgeTone(tier)} className={className}>
      {formatAttributionTierLabel(tier)}
    </Badge>
  );

  if (!withTooltip) {
    return badge;
  }

  return (
    <Tooltip content={`${getAttributionTierDescription(tier)} ${ATTRIBUTION_TIER_PRECEDENCE_TOOLTIP}`}>
      {badge}
    </Tooltip>
  );
}

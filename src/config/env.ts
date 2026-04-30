// Added Meta order-value anomaly env parsing.
  META_ADS_ORDER_VALUE_SYNC_ENABLED: booleanString.default(true),
  META_ADS_ORDER_VALUE_SYNC_INTERVAL_MS: integerString.default(60 * 60 * 1000),
  META_ADS_ORDER_VALUE_WINDOW_DAYS: integerString.default(2),
  META_ADS_ORDER_VALUE_ANOMALY_MIN_ROWS: integerString.default(5),
  META_ADS_ORDER_VALUE_NULL_SPIKE_MIN_RATIO: z.coerce.number().default(0.5),
  META_ADS_ORDER_VALUE_NULL_SPIKE_RATIO_DELTA: z.coerce.number().default(0.3),
  DEFAULT_ORGANIZATION_ID: integerString.default(1),

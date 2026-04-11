  DATA_QUALITY_TARGET_LAG_DAYS: z.coerce.number().int().nonnegative().default(1),
  DATA_QUALITY_CHECK_INTERVAL_MS: z.coerce.number().int().positive().default(24 * 60 * 60 * 1000),
  DATA_QUALITY_CHECK_LOOP: z
    .union([z.string(), z.boolean(), z.undefined()])
    .transform((value) => {
      if (typeof value === 'boolean') {
        return value;
      }

      if (typeof value !== 'string') {
        return false;
      }

      return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
    }),
  DATA_QUALITY_SPEND_LOOKBACK_DAYS: z.coerce.number().int().positive().default(3),
  DATA_QUALITY_ANOMALY_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  DATA_QUALITY_ANOMALY_THRESHOLD_RATIO: z.coerce.number().positive().default(0.35),
  DATA_QUALITY_ANOMALY_MIN_BASELINE: z.coerce.number().nonnegative().default(5),

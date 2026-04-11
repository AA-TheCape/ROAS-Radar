import { fetchDataQualityReport } from '../data-quality/index.js';

const reconciliationResponseSchema = z.object({
  version: z.string(),
  generatedAt: z.string(),
  tenantId: z.string(),
  data: z.object({
    runDate: z.string().nullable(),
    checks: z.array(
      z.object({
        checkKey: z.string(),
        status: z.enum(['healthy', 'warning', 'failed']),
        severity: z.enum(['info', 'warning', 'critical']),
        discrepancyCount: z.number(),
        summary: z.string(),
        details: z.unknown(),
        checkedAt: z.string(),
        alertEmittedAt: z.string().nullable()
      })
    ),
    totals: z.object({
      healthyChecks: z.number(),
      warningChecks: z.number(),
      failedChecks: z.number(),
      totalDiscrepancies: z.number()
    })
  })
});

router.get('/reconciliation', async (req, res, next) => {
  try {
    const principal = res.locals.reportingPrincipal as ReportingPrincipal;
    const payload = z
      .object({
        runDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional()
      })
      .parse(req.query);
    const report = await fetchDataQualityReport(payload.runDate);
    const response = reconciliationResponseSchema.parse({
      version: REPORTING_API_VERSION,
      generatedAt: new Date().toISOString(),
      tenantId: principal.tenantId,
      data: report
    });
    res.json(response);
  } catch (error) {
    next(error);
  }
});

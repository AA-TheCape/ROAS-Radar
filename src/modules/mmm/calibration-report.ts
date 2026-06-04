export const MMM_DETERMINISTIC_BASELINE_VERSION =
	"mmm_deterministic_baseline_30d_click_7d_view_v1";
export const MMM_CALIBRATION_REPORT_VERSION = "mmm_calibration_report_v1";
export const MMM_BASELINE_CLICK_LOOKBACK_DAYS = 30;
export const MMM_BASELINE_VIEW_LOOKBACK_DAYS = 7;

export type MmmContributionCalibrationSegmentInput = {
	key: string;
	source: string;
	medium: string;
	campaign: string;
	channel?: string;
	channelGroup?: string;
	spend: number;
	impressions?: number;
	clicks?: number;
	deterministicRevenue: number;
	posteriorContributionMean: number;
	posteriorContributionShareMean: number | null;
	posteriorProbabilityPositive?: number;
};

function safeShare(value: number, total: number): number | null {
	if (total <= 0) {
		return null;
	}

	return value / total;
}

function bounded(value: number): number {
	if (!Number.isFinite(value)) {
		return 0;
	}

	return Math.min(1, Math.max(0, value));
}

function calculatePosteriorTrustWeight(input: {
	deterministicShare: number | null;
	posteriorShare: number | null;
	posteriorProbabilityPositive: number;
}): number {
	if (input.deterministicShare === null || input.posteriorShare === null) {
		return 0;
	}

	const denominator = Math.max(Math.abs(input.deterministicShare), 0.01);
	const shareDivergence = Math.abs(input.posteriorShare - input.deterministicShare);
	const shareAlignment = bounded(1 - shareDivergence / denominator);

	return bounded(shareAlignment * input.posteriorProbabilityPositive);
}

export function buildMmmCalibrationReportV1(input: {
	attributionModel: string;
	deterministicAttributionUsage: string;
	totalDeterministicRevenue: number;
	totalPosteriorMediaContribution: number;
	segments: MmmContributionCalibrationSegmentInput[];
	governanceStatus?: string;
	governance?: Record<string, unknown>;
	divergenceAlerts?: unknown[];
}) {
	const totalDeterministicRevenue =
		input.totalDeterministicRevenue > 0
			? input.totalDeterministicRevenue
			: input.segments.reduce(
					(sum, segment) => sum + segment.deterministicRevenue,
					0,
				);
	const totalPosteriorMediaContribution =
		input.totalPosteriorMediaContribution > 0
			? input.totalPosteriorMediaContribution
			: input.segments.reduce(
					(sum, segment) => sum + segment.posteriorContributionMean,
					0,
				);
	const segments = input.segments.map((segment) => {
		const deterministicShare = safeShare(
			segment.deterministicRevenue,
			totalDeterministicRevenue,
		);
		const posteriorShare =
			segment.posteriorContributionShareMean ??
			safeShare(segment.posteriorContributionMean, totalPosteriorMediaContribution);
		const shareDelta =
			deterministicShare !== null && posteriorShare !== null
				? posteriorShare - deterministicShare
				: null;
		const posteriorTrustWeight = calculatePosteriorTrustWeight({
			deterministicShare,
			posteriorShare,
			posteriorProbabilityPositive: segment.posteriorProbabilityPositive ?? 1,
		});

		return {
			key: segment.key,
			source: segment.source,
			medium: segment.medium,
			campaign: segment.campaign,
			...(segment.channel ? { channel: segment.channel } : {}),
			...(segment.channelGroup ? { channelGroup: segment.channelGroup } : {}),
			spend: segment.spend,
			impressions: segment.impressions ?? 0,
			clicks: segment.clicks ?? 0,
			deterministicRevenue: segment.deterministicRevenue,
			attributedRevenue: segment.deterministicRevenue,
			deterministicContributionShare: deterministicShare,
			posteriorContributionMean: segment.posteriorContributionMean,
			modeledRevenue: segment.posteriorContributionMean,
			posteriorContributionShare: posteriorShare,
			modeledRevenueShare: posteriorShare,
			attributedRevenueShare: deterministicShare,
			shareDelta,
			absoluteShareDelta: shareDelta === null ? null : Math.abs(shareDelta),
			posteriorProbabilityPositive: segment.posteriorProbabilityPositive ?? null,
			trustWeights: {
				deterministicBaseline: 1,
				posteriorCalibration: posteriorTrustWeight,
				production: 1,
			},
			productionContributionShare: deterministicShare,
			productionContributionRevenue: segment.deterministicRevenue,
		};
	});
	const maxAbsoluteShareDelta = segments.reduce((max, segment) => {
		const value = Number(segment.absoluteShareDelta ?? 0);
		return Number.isFinite(value) ? Math.max(max, value) : max;
	}, 0);

	return {
		reportVersion: MMM_CALIBRATION_REPORT_VERSION,
		attributionModel: input.attributionModel,
		deterministicBaseline: {
			version: MMM_DETERMINISTIC_BASELINE_VERSION,
			clickLookbackWindowDays: MMM_BASELINE_CLICK_LOOKBACK_DAYS,
			viewLookbackWindowDays: MMM_BASELINE_VIEW_LOOKBACK_DAYS,
			lookbackRules: ["30d_click", "7d_view"],
			productionAlignment: "enforced",
		},
		deterministicAttributionUsage: input.deterministicAttributionUsage,
		governanceStatus:
			input.governanceStatus ??
			(input.governance?.status as string | undefined) ??
			"passed",
		totalAttributedRevenue: totalDeterministicRevenue,
		totalDeterministicRevenue,
		totalModeledMediaRevenue: totalPosteriorMediaContribution,
		totalPosteriorMediaContribution,
		maxAbsoluteShareDelta,
		trustWeightPolicy: {
			version: "mmm_calibration_trust_weights_v1",
			productionShareSource: "deterministic_baseline",
			deterministicBaselineWeight: 1,
			posteriorCalibrationWeight:
				segments.length > 0
					? segments.reduce(
							(sum, segment) =>
								sum + segment.trustWeights.posteriorCalibration,
							0,
						) / segments.length
					: 0,
		},
		segments,
		...(input.governance ? { governance: input.governance } : {}),
		divergenceAlerts: input.divergenceAlerts ?? [],
	};
}

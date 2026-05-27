import React, { useMemo, useState } from "react";

import type {
	AttributionQaCandidateV1,
	AttributionQaPayloadV1,
} from "../../../packages/attribution-schema/index.js";
import type { AttributionQaPayloadResponse } from "../lib/api";
import { formatCurrency, formatDateTimeLabel, formatNumber } from "../lib/format";
import {
	Badge,
	Card,
	CardDescription,
	CardHeader,
	CardTitle,
	DataTableToolbar,
	DetailList,
	EmptyState,
	Eyebrow,
	MetricCopy,
	MetricValue,
	PrimaryCell,
	SectionState,
	StatusPill,
	Table,
	TableBody,
	TableCell,
	TableEmptyRow,
	TableHead,
	TableHeaderCell,
	TableMeta,
	TableRow,
	TableWrap,
} from "./AuthenticatedUi";

type AsyncSection<T> = {
	data: T | null;
	loading: boolean;
	error: string | null;
};

type AttributionQaToolingViewProps = {
	selectedOrderId: string | null;
	reportingTimezone: string;
	qaPayloadSection: AsyncSection<AttributionQaPayloadResponse>;
};

const CANDIDATE_GROUP_LABELS: Record<
	keyof AttributionQaPayloadV1["candidates"],
	string
> = {
	deterministic_first_party: "First-party",
	shopify_hint: "Shopify hints",
	ga4_fallback: "GA4 fallback",
};

const SENSITIVE_URL_QUERY_KEYS = new Set([
	"access_token",
	"auth_token",
	"cart_token",
	"checkout_token",
	"client_id",
	"client_secret",
	"code",
	"email",
	"email_hash",
	"fbclid",
	"gclid",
	"gbraid",
	"id_token",
	"msclkid",
	"password",
	"refresh_token",
	"token",
	"ttclid",
	"wbraid",
]);

function formatContractValue(value: string | null | undefined): string {
	if (!value) {
		return "Not available";
	}

	return value.replace(/_/g, " ");
}

function formatOptionalValue(value: string | number | null | undefined): string {
	if (value === null || value === undefined || value === "") {
		return "Not available";
	}

	return String(value);
}

function formatDecimalCurrency(value: string | null | undefined): string {
	const parsed = value ? Number(value) : Number.NaN;
	return formatCurrency(Number.isFinite(parsed) ? parsed : null);
}

function redacted(value: string | null | undefined): string {
	if (!value) {
		return "Not available";
	}

	const normalized = value.trim();
	if (normalized.length <= 8) {
		return "Redacted";
	}

	return `${normalized.slice(0, 4)}...${normalized.slice(-4)}`;
}

function redactSensitiveUrlQueryValues(value: string): string {
	try {
		const url = new URL(value);
		let changed = false;
		for (const key of Array.from(url.searchParams.keys())) {
			if (SENSITIVE_URL_QUERY_KEYS.has(key.toLowerCase())) {
				url.searchParams.set(key, "[REDACTED]");
				changed = true;
			}
		}
		return changed ? url.toString() : value;
	} catch {
		return value.replace(
			/([?&](?:access_token|auth_token|cart_token|checkout_token|client_id|client_secret|code|email|email_hash|fbclid|gclid|gbraid|id_token|msclkid|password|refresh_token|token|ttclid|wbraid)=)[^&#\s]+/gi,
			"$1[REDACTED]",
		);
	}
}

function sanitizeUnknownForDisplay(value: unknown): unknown {
	if (typeof value === "string") {
		return redactSensitiveUrlQueryValues(value);
	}

	if (Array.isArray(value)) {
		return value.map((item) => sanitizeUnknownForDisplay(item));
	}

	if (!value || typeof value !== "object") {
		return value;
	}

	return Object.fromEntries(
		Object.entries(value).map(([key, item]) => [
			key,
			sanitizeUnknownForDisplay(item),
		]),
	);
}

function sanitizeQaPayloadForDisplay(
	payload: AttributionQaPayloadV1,
): AttributionQaPayloadV1 {
	const ga4TouchpointIds = new Map<string, string>();
	const sanitizedGa4Candidates = payload.candidates.ga4_fallback.map(
		(candidate, index) => {
			const redactedId = `ga4_fallback_candidate_${index + 1}`;
			ga4TouchpointIds.set(candidate.source_key, redactedId);
			if (candidate.touchpoint_id) {
				ga4TouchpointIds.set(candidate.touchpoint_id, redactedId);
			}

			return {
				...candidate,
				source_key: redactedId,
				touchpoint_id: redactedId,
				session_id: null,
				source_touch_event_id: null,
				click_id_value: null,
			};
		},
	);

	return {
		...payload,
		order: {
			...payload.order,
			identifiers: {
				...payload.order.identifiers,
				checkout_token: null,
				cart_token: null,
				email_hash: null,
			},
		},
		outcome: {
			...payload.outcome,
			winner_session_id: null,
		},
		candidates: {
			deterministic_first_party:
				payload.candidates.deterministic_first_party.map((candidate) => ({
					...candidate,
					click_id_value: null,
				})),
			shopify_hint: payload.candidates.shopify_hint.map((candidate) => ({
				...candidate,
				click_id_value: null,
			})),
			ga4_fallback: sanitizedGa4Candidates,
		},
		model_summaries: payload.model_summaries.map((summary) => ({
			...summary,
			winner_session_id: null,
		})),
		credits: payload.credits.map((credit) => ({
			...credit,
			session_id: null,
			click_id_value: null,
		})),
		explainability: payload.explainability.map((record) => ({
			...record,
			touchpoint_id: record.touchpoint_id
				? ga4TouchpointIds.get(record.touchpoint_id) ?? record.touchpoint_id
				: null,
			details_json: sanitizeUnknownForDisplay(record.details_json) as Record<
				string,
				unknown
			>,
		})),
		diagnostics: {
			normalization_failures: payload.diagnostics.normalization_failures.map(
				(failure) => ({
					...failure,
					source_key: failure.source_key
						? ga4TouchpointIds.get(failure.source_key) ?? failure.source_key
						: null,
				}),
			),
			notes: payload.diagnostics.notes.map((note) =>
				redactSensitiveUrlQueryValues(note),
			),
		},
	};
}

function candidateKey(
	candidate: AttributionQaCandidateV1,
	index: number,
): string {
	return `${candidate.candidate_group}-${candidate.source_key}-${candidate.touchpoint_id ?? "none"}-${index}`;
}

function candidateTone(candidate: AttributionQaCandidateV1) {
	if (candidate.selected) {
		return "success" as const;
	}

	if (!candidate.is_direct) {
		return "teal" as const;
	}

	return "neutral" as const;
}

function QaMetricCard({
	label,
	value,
	detail,
}: { label: string; value: string; detail: string }) {
	return (
		<Card padding="compact" className="border-line/70">
			<Eyebrow>{label}</Eyebrow>
			<MetricValue>{value}</MetricValue>
			<MetricCopy>{detail}</MetricCopy>
		</Card>
	);
}

function QaCard({
	title,
	description,
	children,
}: {
	title: string;
	description?: string;
	children: React.ReactNode;
}) {
	return (
		<Card className="bg-surface/88">
			<CardHeader>
				<div>
					<CardTitle>{title}</CardTitle>
					{description ? <CardDescription>{description}</CardDescription> : null}
				</div>
			</CardHeader>
			{children}
		</Card>
	);
}

function CandidateTable({
	title,
	description,
	candidates,
	reportingTimezone,
}: {
	title: string;
	description: string;
	candidates: AttributionQaCandidateV1[];
	reportingTimezone: string;
}) {
	return (
		<QaCard title={title} description={description}>
			<DataTableToolbar
				title={`${title} candidates`}
				summary={
					<TableMeta
						currentCount={candidates.length}
						totalCount={candidates.length}
						label="candidates"
					/>
				}
			/>
			<TableWrap className="mt-4 max-h-[28rem]">
				<Table caption={`${title} attribution QA candidates`}>
					<TableHead>
						<TableRow>
							<TableHeaderCell>Candidate</TableHeaderCell>
							<TableHeaderCell>Touchpoint</TableHeaderCell>
							<TableHeaderCell>Channel</TableHeaderCell>
							<TableHeaderCell>Confidence</TableHeaderCell>
							<TableHeaderCell>Reason</TableHeaderCell>
						</TableRow>
					</TableHead>
					<TableBody>
						{candidates.length === 0 ? (
							<TableEmptyRow
								colSpan={5}
								title="No candidates found"
								description="The QA payload did not include candidates for this group."
							/>
						) : null}
						{candidates.map((candidate, index) => (
							<TableRow key={candidateKey(candidate, index)}>
								<TableCell>
									<PrimaryCell>
										<strong>{candidate.source_key}</strong>
										<span>{formatContractValue(candidate.match_source)}</span>
									</PrimaryCell>
								</TableCell>
								<TableCell>
									<PrimaryCell>
										<strong>
											{formatDateTimeLabel(
												candidate.occurred_at_utc,
												reportingTimezone,
											)}
										</strong>
										<span>Session {redacted(candidate.session_id)}</span>
										<span>Click {redacted(candidate.click_id_value)}</span>
									</PrimaryCell>
								</TableCell>
								<TableCell>
									<PrimaryCell>
										<strong>{`${candidate.source ?? "unknown"} / ${candidate.medium ?? "unknown"}`}</strong>
										<span>{candidate.campaign ?? "No campaign"}</span>
									</PrimaryCell>
								</TableCell>
								<TableCell>
									<div className="flex flex-wrap gap-2">
										<Badge tone={candidateTone(candidate)}>
											{candidate.selected ? "Winner" : "Candidate"}
										</Badge>
										<Badge tone="brand">
											{formatContractValue(candidate.confidence_label)}
										</Badge>
										<StatusPill tone="neutral">
											{formatNumber(candidate.confidence_score)}
										</StatusPill>
									</div>
								</TableCell>
								<TableCell>
									<PrimaryCell>
										<strong>{formatContractValue(candidate.attribution_reason)}</strong>
										<span>
											{candidate.is_synthetic ? "Synthetic" : "Captured"};{" "}
											{candidate.is_direct ? "direct" : "non-direct"}
										</span>
									</PrimaryCell>
								</TableCell>
							</TableRow>
						))}
					</TableBody>
				</Table>
			</TableWrap>
		</QaCard>
	);
}

export default function AttributionQaToolingView({
	selectedOrderId,
	reportingTimezone,
	qaPayloadSection,
}: AttributionQaToolingViewProps) {
	const [showRawPayload, setShowRawPayload] = useState(false);
	const response = qaPayloadSection.data;
	const payload = useMemo(
		() => (response?.payload ? sanitizeQaPayloadForDisplay(response.payload) : null),
		[response?.payload],
	);
	const allCandidates = useMemo(
		() =>
			payload
				? [
						...payload.candidates.deterministic_first_party,
						...payload.candidates.shopify_hint,
						...payload.candidates.ga4_fallback,
					]
				: [],
		[payload],
	);
	const selectedCandidates = allCandidates.filter((candidate) => candidate.selected);
	const missingFields =
		payload?.diagnostics.normalization_failures.filter(
			(failure) => failure.scope === "order",
		) ?? [];
	const touchpointFailures =
		payload?.diagnostics.normalization_failures.filter(
			(failure) => failure.scope !== "order",
		) ?? [];

	return (
		<SectionState
			loading={qaPayloadSection.loading}
			error={qaPayloadSection.error}
			empty={!payload}
			emptyLabel={
				selectedOrderId
					? `No attribution QA payload was loaded for order ${selectedOrderId}.`
					: "No order selected."
			}
		>
			<React.Fragment>
				{payload ? (
					<div className="grid gap-section">
					<div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-5">
						<QaMetricCard
							label="QA source"
							value={formatContractValue(response?.source)}
							detail={`Generated ${formatDateTimeLabel(payload.generated_at_utc, reportingTimezone)}`}
						/>
						<QaMetricCard
							label="Outcome"
							value={formatContractValue(payload.outcome.status)}
							detail={formatContractValue(payload.outcome.attribution_tier)}
						/>
						<QaMetricCard
							label="Candidate matches"
							value={formatNumber(allCandidates.length)}
							detail={`${formatNumber(selectedCandidates.length)} selected winners`}
						/>
						<QaMetricCard
							label="Missing fields"
							value={formatNumber(missingFields.length)}
							detail={`${formatNumber(touchpointFailures.length)} touchpoint diagnostics`}
						/>
						<QaMetricCard
							label="GA4 fallback"
							value={formatNumber(payload.candidates.ga4_fallback.length)}
							detail={`${formatNumber(payload.candidates.ga4_fallback.filter((candidate) => candidate.selected).length)} selected`}
						/>
					</div>

					<div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
						<QaCard
							title="Winner rationale"
							description="Selected candidate, tier, match source, model key, and confidence from the schema-backed QA outcome."
						>
							<div className="grid gap-4">
								<div className="flex flex-wrap gap-2">
									<Badge tone={payload.outcome.status === "success" ? "success" : "danger"}>
										{formatContractValue(payload.outcome.status)}
									</Badge>
									<Badge tone="teal">
										{formatContractValue(payload.outcome.match_source)}
									</Badge>
									<Badge tone="brand">
										{formatContractValue(payload.outcome.confidence_label)}
									</Badge>
								</div>
								<DetailList className="xl:grid-cols-2">
									<div>
										<dt>Attribution reason</dt>
										<dd>{formatContractValue(payload.outcome.attribution_reason)}</dd>
									</div>
									<div>
										<dt>Selected model</dt>
										<dd>{formatContractValue(payload.outcome.selected_model_key)}</dd>
									</div>
									<div>
										<dt>Winner touchpoint</dt>
										<dd className="break-all">
											{formatOptionalValue(payload.outcome.winner_touchpoint_id)}
										</dd>
									</div>
									<div>
										<dt>Winner session</dt>
										<dd>{redacted(payload.outcome.winner_session_id)}</dd>
									</div>
									<div>
										<dt>Confidence score</dt>
										<dd>{formatNumber(payload.outcome.confidence_score)}</dd>
									</div>
									<div>
										<dt>Total credited</dt>
										<dd>
											{formatDecimalCurrency(
												payload.model_summaries[0]?.total_revenue_credited,
											)}
										</dd>
									</div>
								</DetailList>
							</div>
						</QaCard>

						<QaCard
							title="Redacted Shopify hints"
							description="Order identifiers are shortened for review while preserving whether Shopify linkage fields exist."
						>
							<DetailList className="xl:grid-cols-2">
								<div>
									<dt>Order</dt>
									<dd>{payload.order.order_name ?? payload.order.order_id}</dd>
								</div>
								<div>
									<dt>Timestamp source</dt>
									<dd>{formatContractValue(payload.order.order_timestamp_source)}</dd>
								</div>
								<div>
									<dt>Landing session</dt>
									<dd>{redacted(payload.order.identifiers.landing_session_id)}</dd>
								</div>
								<div>
									<dt>Checkout token</dt>
									<dd>{redacted(payload.order.identifiers.checkout_token)}</dd>
								</div>
								<div>
									<dt>Cart token</dt>
									<dd>{redacted(payload.order.identifiers.cart_token)}</dd>
								</div>
								<div>
									<dt>Email hash</dt>
									<dd>{redacted(payload.order.identifiers.email_hash)}</dd>
								</div>
								<div>
									<dt>Identity journey</dt>
									<dd>{redacted(payload.order.identifiers.identity_journey_id)}</dd>
								</div>
								<div>
									<dt>Order total</dt>
									<dd>{formatDecimalCurrency(payload.order.total_amount)}</dd>
								</div>
							</DetailList>
						</QaCard>
					</div>

					<div className="grid gap-4 xl:grid-cols-2">
						<QaCard
							title="Missing fields"
							description="Order-level normalization failures and payload notes that explain no-match or degraded-match cases."
						>
							{missingFields.length === 0 && payload.diagnostics.notes.length === 0 ? (
								<EmptyState
									title="No missing order fields"
									description="The QA payload did not report order-level normalization failures."
									compact
									tone="muted"
								/>
							) : (
								<div className="grid gap-3">
									{missingFields.map((failure) => (
										<div
											key={`${failure.scope}-${failure.reason}-${failure.source_key ?? "none"}`}
											className="rounded-card border border-danger/20 bg-danger-soft/50 px-4 py-3"
										>
											<p className="text-body font-semibold text-danger">
												{formatContractValue(failure.reason)}
											</p>
											<p className="text-body text-ink-muted">
												Source key: {formatOptionalValue(failure.source_key)}
											</p>
										</div>
									))}
									{payload.diagnostics.notes.map((note) => (
										<div
											key={note}
											className="rounded-card border border-line/60 bg-canvas-tint/80 px-4 py-3 text-body text-ink-soft"
										>
											{note}
										</div>
									))}
								</div>
							)}
						</QaCard>

						<QaCard
							title="Redacted tracking touchpoints"
							description="Candidate touchpoints and credit records with session and click identifiers shortened for QA review."
						>
							<div className="grid gap-3">
								{allCandidates.length === 0 ? (
									<EmptyState
										title="No tracking touchpoints"
										description="No deterministic, Shopify hint, or GA4 fallback candidates were included."
										compact
										tone="muted"
									/>
								) : null}
								{allCandidates.slice(0, 8).map((candidate, index) => (
									<div
										key={`touchpoint-${candidateKey(candidate, index)}`}
										className="flex flex-col gap-3 rounded-card border border-line/60 bg-canvas-tint/70 px-4 py-3 sm:flex-row sm:items-start sm:justify-between"
									>
										<PrimaryCell>
											<strong>{candidate.touchpoint_id ?? candidate.source_key}</strong>
											<span>
												{`${candidate.source ?? "unknown"} / ${candidate.medium ?? "unknown"} / ${candidate.campaign ?? "no campaign"}`}
											</span>
											<span>
												{formatDateTimeLabel(candidate.occurred_at_utc, reportingTimezone)}
											</span>
										</PrimaryCell>
										<div className="flex flex-wrap gap-2">
											<Badge tone={candidateTone(candidate)}>
												{CANDIDATE_GROUP_LABELS[candidate.candidate_group]}
											</Badge>
											<Badge tone="neutral">Session {redacted(candidate.session_id)}</Badge>
											<Badge tone="neutral">Click {redacted(candidate.click_id_value)}</Badge>
										</div>
									</div>
								))}
							</div>
						</QaCard>
					</div>

					<div className="grid gap-4">
						<CandidateTable
							title="Candidate matches"
							description="Every candidate from the shared QA payload, grouped by match class and marked when selected."
							candidates={allCandidates}
							reportingTimezone={reportingTimezone}
						/>
						<CandidateTable
							title="GA4 fallback details"
							description="Fallback candidates normalized from GA4, including source, medium, campaign, confidence, and winner state."
							candidates={payload.candidates.ga4_fallback}
							reportingTimezone={reportingTimezone}
						/>
					</div>

					<QaCard
						title="Model summaries"
						description="Allocation outputs included in the QA payload for model-level comparison without redefining backend enums."
					>
						<TableWrap className="max-h-[28rem]">
							<Table caption="Attribution QA model summaries">
								<TableHead>
									<TableRow>
										<TableHeaderCell>Model</TableHeaderCell>
										<TableHeaderCell>Status</TableHeaderCell>
										<TableHeaderCell>Winner</TableHeaderCell>
										<TableHeaderCell>Credit</TableHeaderCell>
										<TableHeaderCell>Diagnostics</TableHeaderCell>
									</TableRow>
								</TableHead>
								<TableBody>
									{payload.model_summaries.length === 0 ? (
										<TableEmptyRow
											colSpan={5}
											title="No model summaries found"
											description="The QA payload did not include attribution model summaries."
										/>
									) : null}
									{payload.model_summaries.map((summary) => (
										<TableRow key={`${summary.run_id}-${summary.model_key}`}>
											<TableCell>
												<PrimaryCell>
													<strong>{formatContractValue(summary.model_key)}</strong>
													<span>{formatContractValue(summary.winner_selection_rule)}</span>
												</PrimaryCell>
											</TableCell>
											<TableCell>{formatContractValue(summary.allocation_status)}</TableCell>
											<TableCell>
												<PrimaryCell>
													<strong>{formatOptionalValue(summary.winner_touchpoint_id)}</strong>
													<span>{formatContractValue(summary.winner_evidence_source)}</span>
												</PrimaryCell>
											</TableCell>
											<TableCell>
												<PrimaryCell>
													<strong>
														{formatDecimalCurrency(summary.total_revenue_credited)}
													</strong>
													<span>Weight {summary.total_credit_weight}</span>
												</PrimaryCell>
											</TableCell>
											<TableCell>
												<PrimaryCell>
													<strong>
														{formatNumber(summary.touchpoint_count_considered)} considered
													</strong>
													<span>
														{formatNumber(summary.normalization_failures_count)} failures
													</span>
												</PrimaryCell>
											</TableCell>
										</TableRow>
									))}
								</TableBody>
							</Table>
						</TableWrap>
					</QaCard>

					<QaCard
						title="QA payload"
						description="Raw schema-backed payload is available for contract debugging after the summarized QA sections."
					>
						<button
							type="button"
							className="rounded-pill border border-line/70 bg-surface-alt px-4 py-2 text-label uppercase text-ink-soft transition hover:border-brand/50 hover:text-brand"
							onClick={() => setShowRawPayload((value) => !value)}
						>
							{showRawPayload ? "Hide raw payload" : "Show raw payload"}
						</button>
							{showRawPayload ? (
								<pre className="mt-4 max-h-[32rem] overflow-auto rounded-card border border-white/8 bg-[#132130] p-4 font-mono text-[0.78rem] leading-6 text-slate-200 shadow-inset-soft">
									{JSON.stringify(payload, null, 2)}
								</pre>
							) : null}
						</QaCard>
					</div>
				) : null}
			</React.Fragment>
		</SectionState>
	);
}

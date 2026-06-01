import React, { useCallback, useEffect, useMemo, useState } from "react";
import { ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT } from "../../../packages/attribution-schema/index.js";
import {
	cancelRecoveryRun,
	createRecoveryRun,
	fetchRecoveryRuns,
	startRecoveryRun,
	type RecoveryJobType,
	type RecoveryRun,
	type RecoveryRunStatus,
} from "../lib/api";
import { formatDateTimeLabel, formatNumber } from "../lib/format";
import {
	Badge,
	Button,
	ButtonRow,
	Card,
	CardDescription,
	CardHeader,
	CardTitle,
	CheckboxField,
	Field,
	FieldGrid,
	Input,
	MetricCopy,
	MetricValue,
	Panel,
	SectionState,
	StatusPill,
	Table,
	TableBody,
	TableCell,
	TableEmptyRow,
	TableHead,
	TableHeaderCell,
	TableRow,
	TableWrap,
} from "./AuthenticatedUi";

type ActionKey =
	| "shopify-order-import"
	| "shopify-hint-recovery"
	| "ga4-fallback-recovery"
	| "ga4-session-enrichment"
	| "campaign-metadata-refresh"
	| "campaign-metadata-history"
	| "order-attribution-backfill";

type Feedback = {
	loading: ActionKey | "history" | null;
	error: string | null;
	message: string | null;
};

type ActionSummary = {
	label: string;
	detail: string;
	at: string;
};

const RECOVERY_JOB_LABELS: Record<RecoveryJobType, string> = {
	shopify_order_reimport: "Shopify order import",
	shopify_attribution_hint_recovery: "Shopify hint recovery",
	ga4_fallback_unattributed_recovery: "GA4 fallback recovery",
	ga4_session_enrichment_backfill: "GA4 session enrichment",
	campaign_metadata_api_refresh: "Campaign metadata refresh",
	campaign_metadata_history_backfill: "Campaign metadata history",
	order_attribution_backfill: "Order attribution backfill",
};

const statusTone: Record<RecoveryRunStatus, "brand" | "success" | "warning" | "danger" | "neutral"> = {
	queued: "warning",
	running: "brand",
	succeeded: "success",
	partial_failure: "warning",
	failed: "danger",
	cancelled: "neutral",
	dead_lettered: "danger",
};

function formatDateInput(date: Date): string {
	const year = date.getUTCFullYear();
	const month = String(date.getUTCMonth() + 1).padStart(2, "0");
	const day = String(date.getUTCDate()).padStart(2, "0");
	return `${year}-${month}-${day}`;
}

function buildLast30DayRange(): { startDate: string; endDate: string } {
	const end = new Date();
	end.setUTCHours(0, 0, 0, 0);
	end.setUTCDate(end.getUTCDate() - 1);
	const start = new Date(end);
	start.setUTCDate(start.getUTCDate() - 29);

	return {
		startDate: formatDateInput(start),
		endDate: formatDateInput(end),
	};
}

function summarizeRun(run: RecoveryRun): string {
	return [
		`${formatNumber(run.recordsProcessed)} processed`,
		`${formatNumber(run.recordsSucceeded)} succeeded`,
		`${formatNumber(run.recordsFailed)} failed`,
		`${formatNumber(run.sideEffectsSuppressed)} suppressed`,
	].join(" / ");
}

function isTerminal(status: RecoveryRunStatus): boolean {
	return ["succeeded", "partial_failure", "failed", "cancelled", "dead_lettered"].includes(status);
}

function RecoveryActionCard({
	title,
	description,
	badge,
	disabled,
	loading,
	onRun,
}: {
	title: string;
	description: string;
	badge: string;
	disabled: boolean;
	loading: boolean;
	onRun: () => void;
}) {
	return (
		<Card padding="compact" className="grid min-h-[17rem] gap-4 border-line/70 bg-surface/92">
			<CardHeader className="mb-0">
				<div>
					<Badge tone="teal">{badge}</Badge>
					<CardTitle className="mt-3">{title}</CardTitle>
				</div>
			</CardHeader>
			<CardDescription className="mt-0">{description}</CardDescription>
			<div className="mt-auto">
				<Button type="button" tone="secondary" disabled={disabled} onClick={onRun}>
					{loading ? "Launching..." : "Launch"}
				</Button>
			</div>
		</Card>
	);
}

export default function RecoveryJobsView({
	reportingTimezone,
}: {
	reportingTimezone: string;
}) {
	const defaultRange = useMemo(buildLast30DayRange, []);
	const [startDate, setStartDate] = useState(defaultRange.startDate);
	const [endDate, setEndDate] = useState(defaultRange.endDate);
	const [dryRun, setDryRun] = useState(true);
	const [chunkSize, setChunkSize] = useState("250");
	const [lookbackDays, setLookbackDays] = useState("30");
	const [limit, setLimit] = useState(String(ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT));
	const [runs, setRuns] = useState<RecoveryRun[]>([]);
	const [summary, setSummary] = useState<ActionSummary | null>(null);
	const [feedback, setFeedback] = useState<Feedback>({
		loading: "history",
		error: null,
		message: null,
	});

	const parsedChunkSize = Number(chunkSize);
	const parsedLookbackDays = Number(lookbackDays);
	const parsedLimit = Number(limit);
	const rangeInvalid = !startDate || !endDate || endDate < startDate;
	const dryRunWarning = dryRun
		? "Dry run is enabled. Write-capable launches require turning it off and confirming the exact action."
		: "Write-enabled mode is selected. Confirm prompts will appear before launch.";

	const loadRuns = useCallback(async () => {
		setFeedback((current) => ({ ...current, loading: "history", error: null }));

		try {
			const response = await fetchRecoveryRuns({ limit: 25 });
			setRuns(response.runs);
			setFeedback((current) => ({ ...current, loading: null }));
		} catch (error) {
			setFeedback({
				loading: null,
				error: error instanceof Error ? error.message : "Failed to load recovery run history",
				message: null,
			});
		}
	}, []);

	useEffect(() => {
		void loadRuns();
	}, [loadRuns]);

	async function launchRegistryRun(jobType: RecoveryJobType, action: ActionKey) {
		if (rangeInvalid || !Number.isInteger(parsedChunkSize)) {
			setFeedback({
				loading: null,
				error: "Enter a valid date range and chunk size before launching recovery.",
				message: null,
			});
			return;
		}

		if (jobType === "order_attribution_backfill" && !Number.isInteger(parsedLimit)) {
			setFeedback({
				loading: null,
				error: "Enter a valid order limit before launching order attribution backfill.",
				message: null,
			});
			return;
		}

		if (!dryRun) {
			const confirmed = window.confirm(
				`Launch write-enabled ${RECOVERY_JOB_LABELS[jobType]} for ${startDate} to ${endDate}? Run this only after a dry run for the same window.`,
			);

			if (!confirmed) {
				return;
			}
		}

		setFeedback({ loading: action, error: null, message: null });

		try {
			const created = await createRecoveryRun({
				jobType,
				startDate,
				endDate,
				dryRun,
				chunkSize: parsedChunkSize,
				...(jobType === "ga4_fallback_unattributed_recovery" && Number.isInteger(parsedLookbackDays)
					? { lookbackDays: parsedLookbackDays }
					: {}),
				...(jobType === "order_attribution_backfill" && Number.isInteger(parsedLimit)
					? {
							limit: parsedLimit,
							onlyWebOrders: true,
							skipShopifyWriteback: false,
						}
					: {}),
			});
			await startRecoveryRun(created.runId);
			setSummary({
				label: RECOVERY_JOB_LABELS[jobType],
				detail: `Run ${created.runId} queued in ${dryRun ? "dry-run" : "write-enabled"} mode.`,
				at: new Date().toISOString(),
			});
			setFeedback({
				loading: null,
				error: null,
				message: `Queued ${RECOVERY_JOB_LABELS[jobType]} run ${created.runId}.`,
			});
			await loadRuns();
		} catch (error) {
			setFeedback({
				loading: null,
				error: error instanceof Error ? error.message : "Failed to launch recovery run",
				message: null,
			});
		}
	}

	async function handleCancel(runId: string) {
		const confirmed = window.confirm(`Cancel recovery run ${runId}?`);
		if (!confirmed) {
			return;
		}

		setFeedback({ loading: "history", error: null, message: null });

		try {
			await cancelRecoveryRun(runId);
			setFeedback({ loading: null, error: null, message: `Cancelled recovery run ${runId}.` });
			await loadRuns();
		} catch (error) {
			setFeedback({
				loading: null,
				error: error instanceof Error ? error.message : "Failed to cancel recovery run",
				message: null,
			});
		}
	}

	return (
		<section className="grid gap-section">
			<Panel
				title="Manual recovery jobs"
				description="Launch controlled recovery and backfill work for a date window, with dry-run defaults for recovery-safe workflows."
				wide
			>
				<div className="grid gap-5">
					{feedback.error ? (
						<div className="rounded-card border border-danger/20 bg-danger-soft/80 px-4 py-3 text-body text-danger" role="alert">
							{feedback.error}
						</div>
					) : null}
					{feedback.message ? (
						<output className="rounded-card border border-success/20 bg-success-soft/80 px-4 py-3 text-body text-success">
							{feedback.message}
						</output>
					) : null}
					<div className="grid gap-4 rounded-card border border-line/60 bg-canvas-tint/70 p-4">
						<FieldGrid className="md:grid-cols-2 xl:grid-cols-4">
							<Field label="Start date" htmlFor="recovery-start-date" required>
								<Input
									id="recovery-start-date"
									type="date"
									value={startDate}
									onChange={(event) => setStartDate(event.target.value)}
									aria-invalid={rangeInvalid}
								/>
							</Field>
							<Field label="End date" htmlFor="recovery-end-date" required>
								<Input
									id="recovery-end-date"
									type="date"
									value={endDate}
									min={startDate}
									onChange={(event) => setEndDate(event.target.value)}
									aria-invalid={rangeInvalid}
								/>
							</Field>
							<Field label="Chunk size" htmlFor="recovery-chunk-size">
								<Input
									id="recovery-chunk-size"
									type="number"
									min="1"
									max="5000"
									value={chunkSize}
									onChange={(event) => setChunkSize(event.target.value)}
								/>
							</Field>
							<Field label="Order limit" htmlFor="recovery-order-limit">
								<Input
									id="recovery-order-limit"
									type="number"
									min="1"
									value={limit}
									onChange={(event) => setLimit(event.target.value)}
								/>
							</Field>
						</FieldGrid>
						<div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_minmax(12rem,16rem)] md:items-end">
							<CheckboxField
								label="Dry run first"
								description={dryRunWarning}
								htmlFor="recovery-dry-run"
							>
								<input
									id="recovery-dry-run"
									type="checkbox"
									checked={dryRun}
									onChange={(event) => setDryRun(event.target.checked)}
								/>
							</CheckboxField>
							<Field label="GA4 lookback days" htmlFor="recovery-lookback-days">
								<Input
									id="recovery-lookback-days"
									type="number"
									min="1"
									max="90"
									value={lookbackDays}
									onChange={(event) => setLookbackDays(event.target.value)}
								/>
							</Field>
						</div>
					</div>
					<div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
						<RecoveryActionCard
							title="Shopify order import"
							badge={dryRun ? "Dry run" : "Write-enabled"}
							description="Reimport Shopify orders for the selected window through the recovery registry before running attribution recovery."
							disabled={Boolean(feedback.loading) || rangeInvalid}
							loading={feedback.loading === "shopify-order-import"}
							onRun={() => void launchRegistryRun("shopify_order_reimport", "shopify-order-import")}
						/>
						<RecoveryActionCard
							title="Shopify hint recovery"
							badge={dryRun ? "Dry run" : "Write-enabled"}
							description="Recover attribution from Shopify web-order hints through the recovery registry and status history."
							disabled={Boolean(feedback.loading) || rangeInvalid}
							loading={feedback.loading === "shopify-hint-recovery"}
							onRun={() => void launchRegistryRun("shopify_attribution_hint_recovery", "shopify-hint-recovery")}
						/>
						<RecoveryActionCard
							title="GA4 fallback recovery"
							badge={dryRun ? "Dry run" : "Write-enabled"}
							description="Recover eligible unattributed Shopify web orders from GA4 fallback candidates after stronger matches fail."
							disabled={Boolean(feedback.loading) || rangeInvalid}
							loading={feedback.loading === "ga4-fallback-recovery"}
							onRun={() => void launchRegistryRun("ga4_fallback_unattributed_recovery", "ga4-fallback-recovery")}
						/>
						<RecoveryActionCard
							title="GA4 session enrichment"
							badge={dryRun ? "Dry run" : "Write-enabled"}
							description="Backfill GA4 session enrichment hourly windows through the shared recovery run queue."
							disabled={Boolean(feedback.loading) || rangeInvalid}
							loading={feedback.loading === "ga4-session-enrichment"}
							onRun={() => void launchRegistryRun("ga4_session_enrichment_backfill", "ga4-session-enrichment")}
						/>
						<RecoveryActionCard
							title="Campaign metadata refresh"
							badge={dryRun ? "Dry run" : "Write-enabled"}
							description="Refresh campaign metadata from connected ad-platform APIs for the selected reporting window."
							disabled={Boolean(feedback.loading) || rangeInvalid}
							loading={feedback.loading === "campaign-metadata-refresh"}
							onRun={() => void launchRegistryRun("campaign_metadata_api_refresh", "campaign-metadata-refresh")}
						/>
						<RecoveryActionCard
							title="Campaign metadata history"
							badge={dryRun ? "Dry run" : "Write-enabled"}
							description="Backfill unresolved campaign metadata history into the shared recovery run history."
							disabled={Boolean(feedback.loading) || rangeInvalid}
							loading={feedback.loading === "campaign-metadata-history"}
							onRun={() => void launchRegistryRun("campaign_metadata_history_backfill", "campaign-metadata-history")}
						/>
						<RecoveryActionCard
							title="Order attribution backfill"
							badge={dryRun ? "Dry run" : "Write-enabled"}
							description="Run the broader order attribution recovery after source import and targeted recovery checks."
							disabled={Boolean(feedback.loading) || rangeInvalid}
							loading={feedback.loading === "order-attribution-backfill"}
							onRun={() => void launchRegistryRun("order_attribution_backfill", "order-attribution-backfill")}
						/>
					</div>
					{summary ? (
						<Card padding="compact" className="border-teal/20 bg-teal-soft/50">
							<CardHeader>
								<div>
									<CardTitle>{summary.label}</CardTitle>
									<CardDescription>{summary.detail}</CardDescription>
								</div>
								<Badge tone="teal">{formatDateTimeLabel(summary.at, reportingTimezone)}</Badge>
							</CardHeader>
						</Card>
					) : null}
				</div>
			</Panel>

			<Panel
				title="Recovery run history"
				description="Manual recovery registry runs with status, counters, side-effect metrics, and terminal errors."
				wide
			>
				<div className="grid gap-4">
					<ButtonRow>
						<Button type="button" tone="secondary" disabled={feedback.loading === "history"} onClick={() => void loadRuns()}>
							{feedback.loading === "history" ? "Refreshing..." : "Refresh history"}
						</Button>
					</ButtonRow>
					<SectionState
						loading={feedback.loading === "history" && runs.length === 0}
						empty={runs.length === 0}
						error={null}
						emptyLabel="No recovery registry runs yet"
					>
						<TableWrap className="max-h-[34rem]">
							<Table caption="Recovery run history">
								<TableHead>
									<TableRow>
										<TableHeaderCell>Run</TableHeaderCell>
										<TableHeaderCell>Status</TableHeaderCell>
										<TableHeaderCell>Window</TableHeaderCell>
										<TableHeaderCell>Metrics</TableHeaderCell>
										<TableHeaderCell>Side effects</TableHeaderCell>
										<TableHeaderCell>Error</TableHeaderCell>
										<TableHeaderCell>Actions</TableHeaderCell>
									</TableRow>
								</TableHead>
								<TableBody>
									{runs.length === 0 ? (
										<TableEmptyRow colSpan={7} title="No recovery registry runs yet" />
									) : (
										runs.map((run) => (
											<TableRow key={run.id}>
												<TableCell>
													<div className="grid gap-1">
														<strong className="text-ink">{RECOVERY_JOB_LABELS[run.jobType]}</strong>
														<span className="text-ink-muted">{run.id}</span>
														<span className="text-ink-muted">Queued {formatDateTimeLabel(run.queuedAt, reportingTimezone)}</span>
													</div>
												</TableCell>
												<TableCell>
													<div className="flex flex-wrap gap-2">
														<StatusPill tone={statusTone[run.status]}>{run.status.replace("_", " ")}</StatusPill>
														<Badge tone={run.dryRun ? "warning" : "danger"}>{run.dryRun ? "Dry run" : "Write-enabled"}</Badge>
													</div>
												</TableCell>
												<TableCell>
													{formatDateTimeLabel(run.timeRangeStart, reportingTimezone)}
													<br />
													{formatDateTimeLabel(run.timeRangeEnd, reportingTimezone)}
												</TableCell>
												<TableCell>
													<MetricValue className="mt-0 text-title">{formatNumber(run.recordsDiscovered)}</MetricValue>
													<MetricCopy className="mt-1">{summarizeRun(run)}</MetricCopy>
												</TableCell>
												<TableCell>
													{formatNumber(run.sideEffectsAttempted)} attempted
													<br />
													{formatNumber(run.sideEffectsSucceeded)} succeeded
												</TableCell>
												<TableCell>
													{run.errorMessage ? (
														<div className="max-w-[20rem] text-danger">
															<strong>{run.errorCode}</strong>
															<br />
															{run.errorMessage}
														</div>
													) : (
														<span className="text-ink-muted">None</span>
													)}
												</TableCell>
												<TableCell>
													<Button
														type="button"
														tone="ghost"
														disabled={isTerminal(run.status) || feedback.loading === "history"}
														onClick={() => void handleCancel(run.id)}
													>
														Cancel
													</Button>
												</TableCell>
											</TableRow>
										))
									)}
								</TableBody>
							</Table>
						</TableWrap>
					</SectionState>
				</div>
			</Panel>
		</section>
	);
}

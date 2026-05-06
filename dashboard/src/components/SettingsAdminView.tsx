import React, { useMemo, useState, type FormEvent } from "react";
import { ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT } from "../../../packages/attribution-schema/index.js";

import type {
	AppSettings,
	AuthUser,
	CreateUserPayload,
	GoogleAdsStatusResponse,
	MetaAdsConfigSummary,
	MetaAdsConnection,
	OrderAttributionBackfillJobResponse,
	ShopifyConnectionResponse,
} from "../lib/api";
import {
	type SortState,
	matchesQuery,
	paginateRows,
	sortRows,
} from "../lib/dataTable";
import { formatDateLabel, formatDateTimeLabel } from "../lib/format";
import {
	Badge,
	Banner,
	Button,
	ButtonRow,
	Card,
	CardDescription,
	CardHeader,
	CardTitle,
	CheckboxField,
	ConnectionState,
	DataTableToolbar,
	DetailList,
	Eyebrow,
	Field,
	FieldGrid,
	Form,
	FormMessage,
	FormSection,
	HelpText,
	Input,
	MetricCopy,
	MetricValue,
	Modal,
	Panel,
	PrimaryCell,
	SectionState,
	SortableTableHeaderCell,
	StatusPill,
	Table,
	TableBody,
	TableCell,
	TableEmptyRow,
	TableFilterBar,
	TableHead,
	TableHeaderCell,
	TableMeta,
	TablePagination,
	TableRow,
	TableSearchField,
	TableWrap,
} from "./AuthenticatedUi";

type AsyncSection<T> = {
	data: T | null;
	loading: boolean;
	error: string | null;
};

type ActionFeedback = {
	context: string | null;
	loading: string | null;
	error: string | null;
	message: string | null;
};

type MetaConnectionState = {
	config: MetaAdsConfigSummary;
	connection: MetaAdsConnection | null;
};

type MetaConfigForm = {
	appId: string;
	appSecret: string;
	appBaseUrl: string;
	appScopes: string;
	adAccountId: string;
};

type SettingsForm = {
	reportingTimezone: string;
};

type GoogleConfigForm = {
	clientId: string;
	clientSecret: string;
	developerToken: string;
	appBaseUrl: string;
	appScopes: string;
};

type GoogleConnectForm = {
	customerId: string;
	loginCustomerId: string;
};

type SettingsAdminViewProps = {
	isAdmin: boolean;
	reportingTimezone: string;
	defaultReportingTimezone: string;
	reportingTimezoneOptions: readonly string[];
	filters: {
		startDate: string;
		endDate: string;
	};
	appSettings: AsyncSection<AppSettings>;
	settingsForm: SettingsForm;
	setSettingsForm: (updater: (current: SettingsForm) => SettingsForm) => void;
	usersSection: AsyncSection<AuthUser[]>;
	newUserForm: CreateUserPayload;
	setNewUserForm: (
		updater: (current: CreateUserPayload) => CreateUserPayload,
	) => void;
	shopifyConnection: AsyncSection<ShopifyConnectionResponse>;
	shopifyBackfillRange: {
		startDate: string;
		endDate: string;
	};
	setShopifyBackfillRange: (
		updater: (current: { startDate: string; endDate: string }) => {
			startDate: string;
			endDate: string;
		},
	) => void;
	shopifyOrderAttributionBackfillOptions?: Partial<{
		dryRun: boolean;
		limit: string;
		webOrdersOnly: boolean;
		skipShopifyWriteback: boolean;
	}> | null;
	setShopifyOrderAttributionBackfillOptions?: (
		updater: (current: {
			dryRun: boolean;
			limit: string;
			webOrdersOnly: boolean;
			skipShopifyWriteback: boolean;
		}) => {
			dryRun: boolean;
			limit: string;
			webOrdersOnly: boolean;
			skipShopifyWriteback: boolean;
		},
	) => void;
	orderAttributionBackfillJob: AsyncSection<OrderAttributionBackfillJobResponse>;
	metaConnection: AsyncSection<MetaConnectionState>;
	metaConfigForm: MetaConfigForm;
	setMetaConfigForm: (
		updater: (current: MetaConfigForm) => MetaConfigForm,
	) => void;
	googleConnection: AsyncSection<GoogleAdsStatusResponse>;
	googleConfigForm: GoogleConfigForm;
	setGoogleConfigForm: (
		updater: (current: GoogleConfigForm) => GoogleConfigForm,
	) => void;
	googleForm: GoogleConnectForm;
	setGoogleForm: (
		updater: (current: GoogleConnectForm) => GoogleConnectForm,
	) => void;
	actionFeedback: ActionFeedback;
	onSettingsSave: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
	onCreateUser: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
	onShopifyBackfill: (
		event: FormEvent<HTMLFormElement>,
	) => void | Promise<void>;
	onMetaConfigSave: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
	onGoogleConfigSave: (
		event: FormEvent<HTMLFormElement>,
	) => void | Promise<void>;
	onGoogleConnect: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
	onShopifyTest: () => void | Promise<void>;
	onShopifyWebhookSync: () => void | Promise<void>;
	onShopifyAttributionRecovery: () => void | Promise<void>;
	onShopifyOrderAttributionBackfill: () => void | Promise<void>;
	onOrderAttributionBackfillRefresh: () => void | Promise<void>;
	onMetaConnect: () => void | Promise<void>;
	onMetaSync: () => void | Promise<void>;
	onGoogleSync: () => void | Promise<void>;
	onGoogleReconcile: () => void | Promise<void>;
};

function formatOptionalDateTime(
	value: string | null | undefined,
	reportingTimezone: string,
): string {
	return value
		? formatDateTimeLabel(value, reportingTimezone)
		: "Not available";
}

function isValidEmail(value: string): boolean {
	return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function hasMessageForAction(
	actionFeedback: ActionFeedback,
	keys: string[],
): boolean {
	return keys.includes(actionFeedback.context ?? "");
}

function SettingsMetric({
	label,
	value,
	detail,
}: {
	label: string;
	value: string;
	detail: string;
}) {
	return (
		<Card padding="compact" className="border-line/70">
			<div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-teal via-brand/65 to-brand/90" />
			<Eyebrow>{label}</Eyebrow>
			<MetricValue>{value}</MetricValue>
			<MetricCopy>{detail}</MetricCopy>
		</Card>
	);
}

function IntegrationCard({
	eyebrow,
	title,
	description,
	status,
	accent = "brand",
	children,
}: {
	eyebrow: string;
	title: string;
	description: string;
	status: string;
	accent?: "brand" | "teal";
	children: JSX.Element;
}) {
	return (
		<Card className="overflow-hidden bg-surface/92 p-0">
			<div
				className={[
					"h-1 w-full",
					accent === "teal"
						? "bg-gradient-to-r from-teal via-teal/70 to-brand/60"
						: "bg-gradient-to-r from-brand via-brand/70 to-teal/70",
				].join(" ")}
			/>
			<div className="grid gap-6 p-panel">
				<CardHeader className="items-start gap-4">
					<div className="max-w-2xl">
						<Eyebrow>{eyebrow}</Eyebrow>
						<CardTitle className="mt-3">{title}</CardTitle>
						<CardDescription className="mt-3">{description}</CardDescription>
					</div>
					<StatusPill tone={accent === "teal" ? "teal" : "brand"}>
						{status}
					</StatusPill>
				</CardHeader>
				{children}
			</div>
		</Card>
	);
}

function DetailGrid({ children }: { children: JSX.Element[] | JSX.Element }) {
	return <DetailList className="xl:grid-cols-2">{children}</DetailList>;
}

function RecoveryAction({
	label,
	description,
	loadingLabel,
	isLoading,
	disabled,
	selected = false,
	type = "button",
	onClick,
}: {
	label: string;
	description: string;
	loadingLabel: string;
	isLoading: boolean;
	disabled: boolean;
	selected?: boolean;
	type?: "button" | "submit";
	onClick?: () => void;
}) {
	return (
		<div
			className={[
				"grid gap-3 rounded-card border bg-surface/85 p-4 shadow-inset-soft",
				selected ? "border-brand/40 bg-brand-soft/35" : "border-line/60",
			].join(" ")}
		>
			<Button
				type={type}
				tone="secondary"
				onClick={onClick}
				disabled={disabled}
			>
				{isLoading ? loadingLabel : label}
			</Button>
			<p className="text-body text-ink-muted">{description}</p>
		</div>
	);
}

function getOrderAttributionJobTone(
	status: OrderAttributionBackfillJobResponse["status"],
) {
	switch (status) {
		case "queued":
			return "warning";
		case "processing":
			return "brand";
		case "completed":
			return "success";
		case "failed":
			return "danger";
	}
}

function getOrderAttributionJobLabel(
	status: OrderAttributionBackfillJobResponse["status"],
) {
	switch (status) {
		case "queued":
			return "Queued";
		case "processing":
			return "Running";
		case "completed":
			return "Completed";
		case "failed":
			return "Failed";
	}
}

export default function SettingsAdminView({
	isAdmin,
	reportingTimezone,
	defaultReportingTimezone,
	reportingTimezoneOptions,
	filters,
	appSettings,
	settingsForm,
	setSettingsForm,
	usersSection,
	newUserForm,
	setNewUserForm,
	shopifyConnection,
	shopifyBackfillRange,
	setShopifyBackfillRange,
	shopifyOrderAttributionBackfillOptions,
	setShopifyOrderAttributionBackfillOptions,
	orderAttributionBackfillJob,
	metaConnection,
	metaConfigForm,
	setMetaConfigForm,
	googleConnection,
	googleConfigForm,
	setGoogleConfigForm,
	googleForm,
	setGoogleForm,
	actionFeedback,
	onSettingsSave,
	onCreateUser,
	onShopifyBackfill,
	onMetaConfigSave,
	onGoogleConfigSave,
	onGoogleConnect,
	onShopifyTest,
	onShopifyWebhookSync,
	onShopifyAttributionRecovery,
	onShopifyOrderAttributionBackfill,
	onOrderAttributionBackfillRefresh,
	onMetaConnect,
	onMetaSync,
	onGoogleSync,
	onGoogleReconcile,
}: SettingsAdminViewProps) {
	const normalizedOrderAttributionBackfillOptions = {
		dryRun: shopifyOrderAttributionBackfillOptions?.dryRun ?? true,
		limit: shopifyOrderAttributionBackfillOptions?.limit ?? "500",
		webOrdersOnly:
			shopifyOrderAttributionBackfillOptions?.webOrdersOnly ?? true,
		skipShopifyWriteback:
			shopifyOrderAttributionBackfillOptions?.skipShopifyWriteback ?? false,
	};
	const [userSearch, setUserSearch] = useState("");
	const [selectedRecoveryAction, setSelectedRecoveryAction] = useState<
		| "shopify-backfill"
		| "shopify-attribution-recovery"
		| "shopify-order-attribution-backfill"
		| null
	>(null);
	const [
		showOrderAttributionConfirmModal,
		setShowOrderAttributionConfirmModal,
	] = useState(false);
	const [userSort, setUserSort] = useState<
		SortState<"user" | "role" | "status" | "lastLogin">
	>({
		key: "user",
		direction: "asc",
	});
	const [userPage, setUserPage] = useState(1);
	const trimmedTimezone = settingsForm.reportingTimezone.trim();
	const timezoneError = trimmedTimezone
		? null
		: "Enter the reporting timezone used for dashboard rollups.";
	const shopifyBackfillError =
		shopifyBackfillRange.startDate &&
		shopifyBackfillRange.endDate &&
		shopifyBackfillRange.startDate > shopifyBackfillRange.endDate
			? "Backfill end must be on or after the start date."
			: null;
	const trimmedOrderAttributionLimit =
		normalizedOrderAttributionBackfillOptions.limit.trim();
	const parsedOrderAttributionLimit =
		trimmedOrderAttributionLimit.length > 0
			? Number(trimmedOrderAttributionLimit)
			: Number.NaN;
	const orderAttributionLimitError = !trimmedOrderAttributionLimit
		? "Enter the maximum number of orders to scan."
		: !Number.isInteger(parsedOrderAttributionLimit)
			? "Limit must be a whole number."
			: parsedOrderAttributionLimit <= 0
				? "Limit must be greater than 0."
				: parsedOrderAttributionLimit > ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT
					? `Limit must be ${ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT} or less.`
					: null;
	const orderAttributionBackfillError =
		shopifyBackfillError ?? orderAttributionLimitError;
	const trimmedDisplayName = newUserForm.displayName.trim();
	const trimmedEmail = newUserForm.email.trim();
	const createUserErrors = {
		displayName: trimmedDisplayName ? null : "Enter the user’s display name.",
		email: !trimmedEmail
			? "Enter the user email address."
			: isValidEmail(trimmedEmail)
				? null
				: "Enter a valid email address.",
		password: !newUserForm.password
			? "Enter a password for the new user."
			: newUserForm.password.length < 12
				? "Password must be at least 12 characters."
				: null,
	};
	const metaMissingFields = metaConnection.data?.config.missingFields ?? [];
	const metaFieldErrors = {
		appId: metaMissingFields.includes("appId")
			? "Meta app ID is required before OAuth can start."
			: null,
		appBaseUrl: metaMissingFields.includes("appBaseUrl")
			? "OAuth base URL is required before OAuth can start."
			: null,
		appScopes: metaMissingFields.includes("appScopes")
			? "Provide at least one Meta scope."
			: null,
		adAccountId: metaMissingFields.includes("adAccountId")
			? "Ad account ID is required before OAuth can start."
			: null,
	};
	const googleMissingFields = googleConnection.data?.config.missingFields ?? [];
	const googleConfigErrors = {
		developerToken: googleMissingFields.includes("developerToken")
			? "Enter the Google Ads developer token."
			: null,
		clientId: googleMissingFields.includes("clientId")
			? "Enter the OAuth client ID."
			: null,
		clientSecret: googleMissingFields.includes("clientSecret")
			? "Enter the OAuth client secret."
			: null,
		appBaseUrl: googleMissingFields.includes("appBaseUrl")
			? "Enter the OAuth base URL."
			: null,
		appScopes: googleMissingFields.includes("appScopes")
			? "Provide at least one Google Ads scope."
			: null,
	};
	const trimmedGoogleCustomerId = googleForm.customerId.trim();
	const trimmedGoogleLoginCustomerId = googleForm.loginCustomerId?.trim() ?? "";
	const googleConnectErrors = {
		customerId: trimmedGoogleCustomerId
			? null
			: "Enter the Google Ads customer ID.",
		loginCustomerId:
			trimmedGoogleLoginCustomerId &&
			!/^[\d-]+$/.test(trimmedGoogleLoginCustomerId)
				? "Use digits with optional hyphens for the login customer ID."
				: null,
	};
	const isSettingsSaving = actionFeedback.loading === "settings-save";
	const isShopifyBusy =
		actionFeedback.loading === "shopify-backfill" ||
		actionFeedback.loading === "shopify-attribution-recovery" ||
		actionFeedback.loading === "shopify-order-attribution-backfill";
	const isMetaConfigSaving = actionFeedback.loading === "meta-config-save";
	const isMetaActionBusy =
		actionFeedback.loading === "meta-connect" ||
		actionFeedback.loading === "meta-sync";
	const isGoogleBusy =
		actionFeedback.loading === "google-connect" ||
		actionFeedback.loading === "google-sync" ||
		actionFeedback.loading === "google-reconcile";
	const isUserCreateBusy = actionFeedback.loading === "user-create";
	const activeConnections = [
		shopifyConnection.data?.connected ? 1 : 0,
		metaConnection.data?.connection ? 1 : 0,
		googleConnection.data?.connection ? 1 : 0,
	].reduce((sum, value) => sum + value, 0);

	const connectionHealth = [
		metaConnection.data?.connection?.last_sync_status,
		googleConnection.data?.connection?.last_sync_status,
	].filter(Boolean);

	const timezoneUpdatedAt = appSettings.data?.updatedAt
		? formatOptionalDateTime(appSettings.data.updatedAt, reportingTimezone)
		: "Awaiting first save";
	const latestOrderAttributionJob = orderAttributionBackfillJob.data;
	const showOrderAttributionJobCard = Boolean(
		latestOrderAttributionJob ||
			orderAttributionBackfillJob.loading ||
			orderAttributionBackfillJob.error,
	);
	const users = usersSection.data ?? [];
	const filteredUsers = useMemo(
		() =>
			users.filter((user) =>
				matchesQuery(
					[
						user.displayName,
						user.email,
						user.status,
						user.isAdmin ? "Admin" : "Viewer",
					],
					userSearch,
				),
			),
		[userSearch, users],
	);
	const sortedUsers = useMemo(
		() =>
			sortRows(filteredUsers, userSort, {
				user: (user) => `${user.displayName} ${user.email}`,
				role: (user) => (user.isAdmin ? "Admin" : "Viewer"),
				status: (user) => user.status,
				lastLogin: (user) => user.lastLoginAt ?? "",
			}),
		[filteredUsers, userSort],
	);
	const paginatedUsers = useMemo(
		() => paginateRows(sortedUsers, userPage, 6),
		[sortedUsers, userPage],
	);

	function toggleUserSort(key: "user" | "role" | "status" | "lastLogin") {
		setUserSort((current) => ({
			key,
			direction:
				current.key === key && current.direction === "desc" ? "asc" : "desc",
		}));
	}

	function handleOrderAttributionQueueClick() {
		if (normalizedOrderAttributionBackfillOptions.dryRun) {
			void onShopifyOrderAttributionBackfill();
			return;
		}

		setShowOrderAttributionConfirmModal(true);
	}

	function closeOrderAttributionConfirmModal() {
		setShowOrderAttributionConfirmModal(false);
	}

	function confirmOrderAttributionBackfill() {
		setShowOrderAttributionConfirmModal(false);
		void onShopifyOrderAttributionBackfill();
	}

	return (
		<section className="grid gap-section">
			<div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
				<SettingsMetric
					label="Reporting timezone"
					value={
						appSettings.data?.reportingTimezone ?? defaultReportingTimezone
					}
					detail={timezoneUpdatedAt}
				/>
				<SettingsMetric
					label="Connected platforms"
					value={`${activeConnections}/3`}
					detail="Shopify, Meta Ads, and Google Ads"
				/>
				<SettingsMetric
					label="Sync window"
					value={formatDateLabel(filters.startDate, reportingTimezone)}
					detail={`${formatDateLabel(filters.startDate, reportingTimezone)} to ${formatDateLabel(filters.endDate, reportingTimezone)}`}
				/>
				<SettingsMetric
					label="Access control"
					value={isAdmin ? "Admin" : "Viewer"}
					detail={
						isAdmin
							? `${usersSection.data?.length ?? 0} app users provisioned`
							: "Read-only authenticated access"
					}
				/>
			</div>

			<Panel
				title="Settings operations"
				description="Manage reporting timezone, store integrations, ad platform credentials, and dashboard access from one Tailwind surface."
				wide
			>
				<div className="grid gap-3">
					{actionFeedback.error ? (
						<Banner tone="error">{actionFeedback.error}</Banner>
					) : null}
					{actionFeedback.message ? (
						<Banner tone="success">{actionFeedback.message}</Banner>
					) : null}
					{!actionFeedback.error &&
					!actionFeedback.message &&
					connectionHealth.length === 0 ? (
						<Banner>
							Save credentials here, then run targeted sync or reconciliation
							actions without leaving settings.
						</Banner>
					) : null}
				</div>
			</Panel>

			<div className="grid gap-section 2xl:grid-cols-[minmax(0,0.84fr)_minmax(0,1.16fr)]">
				<Panel
					title="Reporting timezone"
					description="Dashboard date ranges, daily aggregation, and reporting rollups all use this timezone."
					className="h-fit"
				>
					<SectionState
						loading={appSettings.loading}
						error={appSettings.error}
						empty={false}
						emptyLabel=""
					>
						<div className="grid gap-5">
							<Form onSubmit={onSettingsSave}>
								<FormSection disabled={isSettingsSaving}>
									<FieldGrid dense>
										<Field
											label="Timezone"
											htmlFor="reporting-timezone"
											required
											description="This timezone drives dashboard date filters, daily aggregation, and settings defaults."
											error={timezoneError ?? undefined}
										>
											<Input
												id="reporting-timezone"
												type="text"
												list="reporting-timezone-options"
												value={settingsForm.reportingTimezone}
												onChange={(event) =>
													setSettingsForm((current) => ({
														...current,
														reportingTimezone: event.target.value,
													}))
												}
												placeholder="America/Los_Angeles"
												aria-invalid={timezoneError ? "true" : "false"}
												required
											/>
											<datalist id="reporting-timezone-options">
												{reportingTimezoneOptions.map((option) => (
													<option key={option} value={option} />
												))}
											</datalist>
										</Field>
									</FieldGrid>
									<HelpText>
										Use a valid IANA timezone like{" "}
										<code>America/Los_Angeles</code>. Short aliases like{" "}
										<code>PST</code> and <code>UTC</code> also work here.
									</HelpText>
									{hasMessageForAction(actionFeedback, ["settings-save"]) ? (
										<FormMessage
											tone={
												actionFeedback.error
													? "error"
													: isSettingsSaving
														? "warning"
														: "success"
											}
										>
											{actionFeedback.error
												? actionFeedback.error
												: isSettingsSaving
													? "Saving the reporting timezone and refreshing dashboard windows…"
													: actionFeedback.message}
										</FormMessage>
									) : null}
									<DetailGrid>
										<div>
											<dt>Active timezone</dt>
											<dd>
												{appSettings.data?.reportingTimezone ??
													defaultReportingTimezone}
											</dd>
										</div>
										<div>
											<dt>Updated</dt>
											<dd>{timezoneUpdatedAt}</dd>
										</div>
									</DetailGrid>
									<ButtonRow>
										<Button type="submit" disabled={Boolean(timezoneError)}>
											{isSettingsSaving ? "Saving…" : "Save reporting timezone"}
										</Button>
									</ButtonRow>
								</FormSection>
							</Form>
						</div>
					</SectionState>
				</Panel>

				<div className="grid gap-section">
					<IntegrationCard
						eyebrow="Store connection"
						title="Shopify"
						description="Verify the installed Shopify app, refresh webhook subscriptions, and run order recovery tooling from one card."
						status={
							shopifyConnection.data?.status ??
							(shopifyConnection.data?.connected
								? "active"
								: shopifyConnection.loading
									? "Loading"
									: "Not connected")
						}
						accent="teal"
					>
						<ConnectionState
							loading={shopifyConnection.loading}
							error={shopifyConnection.error}
						>
							<div className="grid gap-5">
								<DetailGrid>
									<div>
										<dt>Shop</dt>
										<dd>
											{shopifyConnection.data?.shop?.name ??
												shopifyConnection.data?.shopDomain ??
												"Not connected"}
										</dd>
									</div>
									<div>
										<dt>Domain</dt>
										<dd>
											{shopifyConnection.data?.shopDomain ?? "Not available"}
										</dd>
									</div>
									<div>
										<dt>Installed</dt>
										<dd>
											{formatOptionalDateTime(
												shopifyConnection.data?.installedAt,
												reportingTimezone,
											)}
										</dd>
									</div>
									<div>
										<dt>Webhook base URL</dt>
										<dd>
											{shopifyConnection.data?.webhookBaseUrl ??
												"Not available"}
										</dd>
									</div>
								</DetailGrid>

								{shopifyConnection.data?.reconnectUrl ? (
									<HelpText>
										Reconnect URL is available if the current store installation
										needs to be reauthorized.
									</HelpText>
								) : null}

								<Form onSubmit={onShopifyBackfill}>
									<FormSection disabled={isShopifyBusy}>
										<FieldGrid>
											<Field
												label="Backfill start"
												htmlFor="shopify-backfill-start"
												required
												description="Import Shopify orders starting from this storefront date."
											>
												<Input
													id="shopify-backfill-start"
													type="date"
													value={shopifyBackfillRange.startDate}
													onChange={(event) =>
														setShopifyBackfillRange((current) => ({
															...current,
															startDate: event.target.value,
														}))
													}
													required
												/>
											</Field>
											<Field
												label="Backfill end"
												htmlFor="shopify-backfill-end"
												required
												description="Recovery and backfill actions will stop at this date."
												error={shopifyBackfillError ?? undefined}
											>
												<Input
													id="shopify-backfill-end"
													type="date"
													value={shopifyBackfillRange.endDate}
													onChange={(event) =>
														setShopifyBackfillRange((current) => ({
															...current,
															endDate: event.target.value,
														}))
													}
													aria-invalid={shopifyBackfillError ? "true" : "false"}
													required
												/>
											</Field>
										</FieldGrid>

										<Card
											padding="compact"
											className="border-line/60 bg-canvas-tint/80 shadow-none"
										>
											<Eyebrow>Recovery tools</Eyebrow>
											<p className="mt-2 text-body text-ink-soft">
												Use these in order for the selected date window: import
												Shopify orders first, recover attribution hints second,
												and queue the broader attribution backfill last only if
												the earlier steps still leave gaps.
											</p>
										</Card>

										{hasMessageForAction(actionFeedback, [
											"shopify-backfill",
											"shopify-attribution-recovery",
											"shopify-order-attribution-backfill",
										]) ? (
											<FormMessage
												tone={
													actionFeedback.error
														? "error"
														: isShopifyBusy
															? "warning"
															: "success"
												}
											>
												{actionFeedback.error
													? actionFeedback.error
													: isShopifyBusy
														? "Running the selected Shopify recovery workflow for the current date window…"
														: actionFeedback.message}
											</FormMessage>
										) : null}

										<div className="grid gap-3 xl:grid-cols-3">
											<RecoveryAction
												type="submit"
												label="Import Shopify orders"
												loadingLabel="Backfilling…"
												isLoading={
													actionFeedback.loading === "shopify-backfill"
												}
												disabled={
													Boolean(shopifyBackfillError) ||
													!shopifyConnection.data?.connected
												}
												onClick={() =>
													setSelectedRecoveryAction("shopify-backfill")
												}
												description="Run this first to import historical Shopify orders for the window; it can pull a large backlog, so start with the narrowest range that fixes the gap."
											/>
											<RecoveryAction
												label="Recover attribution hints"
												loadingLabel="Recovering…"
												isLoading={
													actionFeedback.loading ===
													"shopify-attribution-recovery"
												}
												disabled={
													Boolean(shopifyBackfillError) ||
													!shopifyConnection.data?.connected
												}
												onClick={() => {
													setSelectedRecoveryAction(
														"shopify-attribution-recovery",
													);
													void onShopifyAttributionRecovery();
												}}
												description="Run this second when imported Shopify web orders are still unattributed; it retries deterministic relinking, then applies Shopify-hint fallback only where matching still failed."
											/>
											<RecoveryAction
												label={
													selectedRecoveryAction ===
													"shopify-order-attribution-backfill"
														? "Attribution options selected"
														: "Select attribution backfill"
												}
												loadingLabel="Queueing…"
												isLoading={
													actionFeedback.loading ===
													"shopify-order-attribution-backfill"
												}
												disabled={!shopifyConnection.data?.connected}
												selected={
													selectedRecoveryAction ===
													"shopify-order-attribution-backfill"
												}
												onClick={() =>
													setSelectedRecoveryAction(
														"shopify-order-attribution-backfill",
													)
												}
												description="Run this last to queue the broader asynchronous attribution backfill for the same window; always do a dry run first before turning on write-enabled recovery."
											/>
										</div>

										{selectedRecoveryAction ===
										"shopify-order-attribution-backfill" ? (
											<Card
												padding="compact"
												className="border-brand/25 bg-brand-soft/20 shadow-none"
											>
												<div className="grid gap-4">
													<div className="grid gap-2">
														<Eyebrow>Attribution backfill options</Eyebrow>
														<p className="text-body text-ink-soft">
															Review these before queueing the asynchronous
															attribution backfill for the selected date window.
														</p>
														<p className="text-body text-ink-soft">
															Run a dry run first for this exact window, then
															keep the same dates when you queue a write-enabled
															run.
														</p>
													</div>

													<FieldGrid>
														<Field
															label="Order scan limit"
															htmlFor="shopify-order-attribution-limit"
															required
															description={`Cap the job size for this run. Maximum ${ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT} orders.`}
															error={orderAttributionLimitError ?? undefined}
														>
															<Input
																id="shopify-order-attribution-limit"
																type="number"
																inputMode="numeric"
																min={1}
																max={ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT}
																step={1}
																value={
																	normalizedOrderAttributionBackfillOptions.limit
																}
																onChange={(event) =>
																	setShopifyOrderAttributionBackfillOptions?.(
																		(current) => ({
																			...current,
																			limit: event.target.value,
																		}),
																	)
																}
																aria-invalid={
																	orderAttributionLimitError ? "true" : "false"
																}
																required
															/>
														</Field>
													</FieldGrid>

													<div className="grid gap-3 xl:grid-cols-2">
														<CheckboxField
															label="Dry run"
															htmlFor="shopify-order-attribution-dry-run"
															description="Defaults to enabled. Keep this on for the first run so the job analyzes the window without writing attribution changes."
														>
															<input
																id="shopify-order-attribution-dry-run"
																type="checkbox"
																checked={
																	normalizedOrderAttributionBackfillOptions.dryRun
																}
																onChange={(event) =>
																	setShopifyOrderAttributionBackfillOptions?.(
																		(current) => ({
																			...current,
																			dryRun: event.target.checked,
																		}),
																	)
																}
															/>
														</CheckboxField>

														<CheckboxField
															label="Web orders only"
															htmlFor="shopify-order-attribution-web-only"
															description="Defaults to enabled. Keep the backfill focused on Shopify web orders unless you explicitly need other order sources."
														>
															<input
																id="shopify-order-attribution-web-only"
																type="checkbox"
																checked={
																	normalizedOrderAttributionBackfillOptions.webOrdersOnly
																}
																onChange={(event) =>
																	setShopifyOrderAttributionBackfillOptions?.(
																		(current) => ({
																			...current,
																			webOrdersOnly: event.target.checked,
																		}),
																	)
																}
															/>
														</CheckboxField>

														<CheckboxField
															label="Skip Shopify writeback"
															htmlFor="shopify-order-attribution-skip-writeback"
															description="Defaults to off. Turn this on only when you want local attribution updates without Shopify writeback."
														>
															<input
																id="shopify-order-attribution-skip-writeback"
																type="checkbox"
																checked={
																	normalizedOrderAttributionBackfillOptions.skipShopifyWriteback
																}
																onChange={(event) =>
																	setShopifyOrderAttributionBackfillOptions?.(
																		(current) => ({
																			...current,
																			skipShopifyWriteback:
																				event.target.checked,
																		}),
																	)
																}
															/>
														</CheckboxField>
													</div>

													<ButtonRow>
														<Button
															type="button"
															onClick={handleOrderAttributionQueueClick}
															disabled={
																Boolean(orderAttributionBackfillError) ||
																!shopifyConnection.data?.connected ||
																isShopifyBusy
															}
														>
															{actionFeedback.loading ===
															"shopify-order-attribution-backfill"
																? "Queueing…"
																: "Queue order attribution backfill"}
														</Button>
													</ButtonRow>
												</div>
											</Card>
										) : null}

										{showOrderAttributionJobCard ? (
											<Card
												padding="compact"
												className="border-line/60 bg-canvas-tint/80 shadow-none"
											>
												<div className="grid gap-4">
													<div className="flex flex-wrap items-start justify-between gap-3">
														<div className="grid gap-2">
															<Eyebrow>Latest attribution backfill run</Eyebrow>
															<p className="text-body text-ink-soft">
																Reloading the page keeps tracking this job by
																its latest queued id until a newer run replaces
																it.
															</p>
														</div>
														{latestOrderAttributionJob ? (
															<StatusPill
																tone={getOrderAttributionJobTone(
																	latestOrderAttributionJob.status,
																)}
															>
																{getOrderAttributionJobLabel(
																	latestOrderAttributionJob.status,
																)}
															</StatusPill>
														) : null}
													</div>

													{orderAttributionBackfillJob.error ? (
														<Banner tone="error">
															{orderAttributionBackfillJob.error}
														</Banner>
													) : null}

													{latestOrderAttributionJob ? (
														<>
															<DetailList className="xl:grid-cols-2">
																<div>
																	<dt>Job ID</dt>
																	<dd>{latestOrderAttributionJob.jobId}</dd>
																</div>
																<div>
																	<dt>Date window</dt>
																	<dd>
																		{
																			latestOrderAttributionJob.options
																				.startDate
																		}{" "}
																		to{" "}
																		{latestOrderAttributionJob.options.endDate}
																	</dd>
																</div>
																<div>
																	<dt>Submitted</dt>
																	<dd>
																		{formatOptionalDateTime(
																			latestOrderAttributionJob.submittedAt,
																			reportingTimezone,
																		)}
																	</dd>
																</div>
																<div>
																	<dt>Submitted by</dt>
																	<dd>
																		{latestOrderAttributionJob.submittedBy}
																	</dd>
																</div>
																<div>
																	<dt>Started</dt>
																	<dd>
																		{formatOptionalDateTime(
																			latestOrderAttributionJob.startedAt,
																			reportingTimezone,
																		)}
																	</dd>
																</div>
																<div>
																	<dt>Completed</dt>
																	<dd>
																		{formatOptionalDateTime(
																			latestOrderAttributionJob.completedAt,
																			reportingTimezone,
																		)}
																	</dd>
																</div>
																<div>
																	<dt>Mode</dt>
																	<dd>
																		{latestOrderAttributionJob.options.dryRun
																			? "Dry run"
																			: "Write changes"}
																	</dd>
																</div>
																<div>
																	<dt>Order limit</dt>
																	<dd>
																		{latestOrderAttributionJob.options.limit}
																	</dd>
																</div>
																<div>
																	<dt>Web orders only</dt>
																	<dd>
																		{latestOrderAttributionJob.options
																			.webOrdersOnly
																			? "Yes"
																			: "No"}
																	</dd>
																</div>
																<div>
																	<dt>Shopify writeback</dt>
																	<dd>
																		{latestOrderAttributionJob.options
																			.skipShopifyWriteback
																			? "Skipped"
																			: "Allowed"}
																	</dd>
																</div>
															</DetailList>

															{latestOrderAttributionJob.status === "queued" ||
															latestOrderAttributionJob.status ===
																"processing" ? (
																<Banner tone="warning">
																	{latestOrderAttributionJob.status === "queued"
																		? "This backfill is queued and will update here once a worker starts it."
																		: "This backfill is currently running. The report will populate automatically when processing finishes."}
																</Banner>
															) : null}

															{latestOrderAttributionJob.error ? (
																<Banner tone="error">
																	{latestOrderAttributionJob.error.code}:{" "}
																	{latestOrderAttributionJob.error.message}
																</Banner>
															) : null}

															{latestOrderAttributionJob.report ? (
																<div className="grid gap-4">
																	<DetailList className="xl:grid-cols-2">
																		<div>
																			<dt>Orders scanned</dt>
																			<dd>
																				{
																					latestOrderAttributionJob.report
																						.scanned
																				}
																			</dd>
																		</div>
																		<div>
																			<dt>Recovered</dt>
																			<dd>
																				{
																					latestOrderAttributionJob.report
																						.recovered
																				}
																			</dd>
																		</div>
																		<div>
																			<dt>Unrecoverable</dt>
																			<dd>
																				{
																					latestOrderAttributionJob.report
																						.unrecoverable
																				}
																			</dd>
																		</div>
																		<div>
																			<dt>Shopify writebacks</dt>
																			<dd>
																				{
																					latestOrderAttributionJob.report
																						.writebackCompleted
																				}
																			</dd>
																		</div>
																		<div>
																			<dt>Failures</dt>
																			<dd>
																				{
																					latestOrderAttributionJob.report
																						.failures.length
																				}
																			</dd>
																		</div>
																	</DetailList>

																	{latestOrderAttributionJob.report.failures
																		.length ? (
																		<div className="grid gap-2">
																			<Eyebrow>Recent failures</Eyebrow>
																			<div className="grid gap-2">
																				{latestOrderAttributionJob.report.failures
																					.slice(0, 3)
																					.map((failure, index) => (
																						<Banner
																							key={`${failure.orderId ?? "unknown"}-${failure.code}-${index}`}
																							tone="warning"
																						>
																							{failure.orderId
																								? `${failure.orderId}: `
																								: ""}
																							{failure.code} ({failure.message})
																						</Banner>
																					))}
																			</div>
																		</div>
																	) : null}
																</div>
															) : null}
														</>
													) : null}

													<ButtonRow>
														<Button
															type="button"
															tone="secondary"
															onClick={() =>
																void onOrderAttributionBackfillRefresh()
															}
															disabled={
																orderAttributionBackfillJob.loading ||
																latestOrderAttributionJob == null
															}
														>
															{orderAttributionBackfillJob.loading
																? "Refreshing…"
																: "Refresh backfill status"}
														</Button>
													</ButtonRow>
												</div>
											</Card>
										) : null}
									</FormSection>
								</Form>

								<ButtonRow>
									<Button
										type="button"
										onClick={() => void onShopifyTest()}
										disabled={actionFeedback.loading !== null}
									>
										{actionFeedback.loading === "shopify-test"
											? "Testing…"
											: "Test Shopify connection"}
									</Button>
									<Button
										type="button"
										tone="secondary"
										onClick={() => void onShopifyWebhookSync()}
										disabled={
											actionFeedback.loading !== null ||
											!shopifyConnection.data?.connected
										}
									>
										{actionFeedback.loading === "shopify-webhooks"
											? "Syncing…"
											: "Sync Shopify webhooks"}
									</Button>
								</ButtonRow>
							</div>
						</ConnectionState>
					</IntegrationCard>
					<Modal
						open={showOrderAttributionConfirmModal}
						title="Confirm non-dry-run attribution backfill"
						description="This run will write recovered attribution changes instead of only analyzing the selected date window."
						onClose={closeOrderAttributionConfirmModal}
						footer={
							<>
								<Button
									type="button"
									tone="secondary"
									onClick={closeOrderAttributionConfirmModal}
								>
									No, go back
								</Button>
								<Button type="button" onClick={confirmOrderAttributionBackfill}>
									Yes, queue backfill
								</Button>
							</>
						}
					>
						<div className="grid gap-4">
							<Banner tone="warning">
								This non-dry-run backfill may update internal attribution data
								{normalizedOrderAttributionBackfillOptions.skipShopifyWriteback
									? ", but Shopify writeback is disabled for this run."
									: " and may also write updates back to Shopify when recovered attribution is available."}
							</Banner>
							<DetailList className="xl:grid-cols-2">
								<div>
									<dt>Date window</dt>
									<dd>
										{shopifyBackfillRange.startDate} to{" "}
										{shopifyBackfillRange.endDate}
									</dd>
								</div>
								<div>
									<dt>Order scan limit</dt>
									<dd>{normalizedOrderAttributionBackfillOptions.limit}</dd>
								</div>
								<div>
									<dt>Web orders only</dt>
									<dd>
										{normalizedOrderAttributionBackfillOptions.webOrdersOnly
											? "Yes"
											: "No"}
									</dd>
								</div>
								<div>
									<dt>Shopify writeback</dt>
									<dd>
										{normalizedOrderAttributionBackfillOptions.skipShopifyWriteback
											? "Skipped"
											: "Allowed"}
									</dd>
								</div>
							</DetailList>
						</div>
					</Modal>

					<IntegrationCard
						eyebrow="Ad platform"
						title="Meta Ads"
						description="Store OAuth app settings, attach the ad account through Meta OAuth, and queue spend syncs for the current reporting window."
						status={
							metaConnection.data?.connection?.status ??
							(metaConnection.data?.config.missingFields.length
								? "Needs config"
								: metaConnection.loading
									? "Loading"
									: "Not connected")
						}
					>
						<ConnectionState
							loading={metaConnection.loading}
							error={metaConnection.error}
						>
							<div className="grid gap-5">
								<DetailGrid>
									<div>
										<dt>Config source</dt>
										<dd>
											{metaConnection.data?.config.source ?? "Not available"}
										</dd>
									</div>
									<div>
										<dt>Ad account</dt>
										<dd>
											{metaConnection.data?.connection?.account_name ??
												metaConnection.data?.config.adAccountId ??
												"Not configured"}
										</dd>
									</div>
									<div>
										<dt>Last sync</dt>
										<dd>
											{formatOptionalDateTime(
												metaConnection.data?.connection?.last_sync_completed_at,
												reportingTimezone,
											)}
										</dd>
									</div>
									<div>
										<dt>Sync status</dt>
										<dd>
											{metaConnection.data?.connection?.last_sync_status ??
												"Not started"}
										</dd>
									</div>
								</DetailGrid>

								{metaConnection.data?.config.missingFields.length ? (
									<HelpText tone="error">
										Missing Meta config:{" "}
										{metaConnection.data.config.missingFields.join(", ")}
									</HelpText>
								) : null}
								{metaConnection.data?.connection?.last_sync_error ? (
									<HelpText tone="error">
										{metaConnection.data.connection.last_sync_error}
									</HelpText>
								) : null}

								<Form onSubmit={onMetaConfigSave}>
									<FormSection disabled={isMetaConfigSaving}>
										<FieldGrid>
											<Field
												label="Meta app ID"
												htmlFor="meta-app-id"
												description="Stored for Meta OAuth and reused across sync jobs."
												error={metaFieldErrors.appId ?? undefined}
											>
												<Input
													id="meta-app-id"
													type="text"
													value={metaConfigForm.appId}
													onChange={(event) =>
														setMetaConfigForm((current) => ({
															...current,
															appId: event.target.value,
														}))
													}
													placeholder="123456789012345"
													aria-invalid={
														metaFieldErrors.appId ? "true" : "false"
													}
												/>
											</Field>
											<Field
												label="Ad account ID"
												htmlFor="meta-account-id"
												description="Supports either the numeric account ID or the `act_` prefixed value."
												error={metaFieldErrors.adAccountId ?? undefined}
											>
												<Input
													id="meta-account-id"
													type="text"
													value={metaConfigForm.adAccountId}
													onChange={(event) =>
														setMetaConfigForm((current) => ({
															...current,
															adAccountId: event.target.value,
														}))
													}
													placeholder="act_123456789012345 or 123456789012345"
													aria-invalid={
														metaFieldErrors.adAccountId ? "true" : "false"
													}
												/>
											</Field>
											<Field
												label="Meta app secret"
												htmlFor="meta-app-secret"
												wide
												optional
												description="Leave blank to keep the existing stored secret."
											>
												<Input
													id="meta-app-secret"
													type="password"
													value={metaConfigForm.appSecret}
													onChange={(event) =>
														setMetaConfigForm((current) => ({
															...current,
															appSecret: event.target.value,
														}))
													}
													placeholder={
														metaConnection.data?.config.appSecretConfigured
															? "Leave blank to keep the saved secret"
															: "Paste the Meta app secret"
													}
												/>
											</Field>
											<Field
												label="OAuth base URL"
												htmlFor="meta-base-url"
												wide
												description="Base application URL used to construct Meta OAuth callbacks."
												error={metaFieldErrors.appBaseUrl ?? undefined}
											>
												<Input
													id="meta-base-url"
													type="url"
													value={metaConfigForm.appBaseUrl}
													onChange={(event) =>
														setMetaConfigForm((current) => ({
															...current,
															appBaseUrl: event.target.value,
														}))
													}
													placeholder="https://roas-radar.api.thecapemarine.com"
													aria-invalid={
														metaFieldErrors.appBaseUrl ? "true" : "false"
													}
												/>
											</Field>
											<Field
												label="Scopes"
												htmlFor="meta-scopes"
												wide
												description="Comma-separated scopes requested when an operator connects Meta Ads."
												error={metaFieldErrors.appScopes ?? undefined}
											>
												<Input
													id="meta-scopes"
													type="text"
													value={metaConfigForm.appScopes}
													onChange={(event) =>
														setMetaConfigForm((current) => ({
															...current,
															appScopes: event.target.value,
														}))
													}
													placeholder="ads_read"
													aria-invalid={
														metaFieldErrors.appScopes ? "true" : "false"
													}
												/>
											</Field>
										</FieldGrid>
										{hasMessageForAction(actionFeedback, [
											"meta-config-save",
										]) ? (
											<FormMessage
												tone={
													actionFeedback.error
														? "error"
														: isMetaConfigSaving
															? "warning"
															: "success"
												}
											>
												{actionFeedback.error
													? actionFeedback.error
													: isMetaConfigSaving
														? "Saving Meta Ads configuration…"
														: actionFeedback.message}
											</FormMessage>
										) : null}
										<ButtonRow>
											<Button type="submit">
												{actionFeedback.loading === "meta-config-save"
													? "Saving…"
													: "Save Meta config"}
											</Button>
										</ButtonRow>
									</FormSection>
								</Form>

								{hasMessageForAction(actionFeedback, [
									"meta-connect",
									"meta-sync",
								]) ? (
									<FormMessage
										tone={
											actionFeedback.error
												? "error"
												: isMetaActionBusy
													? "warning"
													: "success"
										}
									>
										{actionFeedback.error
											? actionFeedback.error
											: isMetaActionBusy
												? "Preparing the Meta Ads action you selected…"
												: actionFeedback.message}
									</FormMessage>
								) : null}

								<ButtonRow>
									<Button
										type="button"
										onClick={() => void onMetaConnect()}
										disabled={
											actionFeedback.loading !== null ||
											Boolean(metaConnection.data?.config.missingFields.length)
										}
									>
										{actionFeedback.loading === "meta-connect"
											? "Opening Meta…"
											: "Connect Meta Ads"}
									</Button>
									<Button
										type="button"
										tone="secondary"
										onClick={() => void onMetaSync()}
										disabled={
											actionFeedback.loading !== null ||
											metaConnection.data?.connection == null
										}
									>
										{actionFeedback.loading === "meta-sync" ? (
											"Queueing…"
										) : (
											<>
												<span className="sm:hidden">Sync range</span>
												<span className="hidden sm:inline">{`Sync ${filters.startDate} to ${filters.endDate}`}</span>
											</>
										)}
									</Button>
								</ButtonRow>
							</div>
						</ConnectionState>
					</IntegrationCard>

					<IntegrationCard
						eyebrow="Ad platform"
						title="Google Ads"
						description="Create the encrypted Google Ads connection, queue spend sync jobs, and trigger gap reconciliation from the same workspace."
						status={
							googleConnection.data?.connection?.status ??
							(googleConnection.loading ? "Loading" : "Not connected")
						}
						accent="teal"
					>
						<ConnectionState
							loading={googleConnection.loading}
							error={googleConnection.error}
						>
							<div className="grid gap-5">
								<DetailGrid>
									<div>
										<dt>Customer</dt>
										<dd>
											{googleConnection.data?.connection
												?.customer_descriptive_name ??
												googleConnection.data?.connection?.customer_id ??
												"Not connected"}
										</dd>
									</div>
									<div>
										<dt>Currency</dt>
										<dd>
											{googleConnection.data?.connection?.currency_code ??
												"Not available"}
										</dd>
									</div>
									<div>
										<dt>Last sync</dt>
										<dd>
											{formatOptionalDateTime(
												googleConnection.data?.connection
													?.last_sync_completed_at,
												reportingTimezone,
											)}
										</dd>
									</div>
									<div>
										<dt>Reconciliation</dt>
										<dd>
											{googleConnection.data?.reconciliation?.status ??
												"Not run"}
										</dd>
									</div>
								</DetailGrid>

								{googleConnection.data?.connection?.last_sync_error ? (
									<HelpText tone="error">
										{googleConnection.data.connection.last_sync_error}
									</HelpText>
								) : null}
								{googleMissingFields.length ? (
									<HelpText tone="error">
										Missing Google Ads config: {googleMissingFields.join(", ")}
									</HelpText>
								) : null}
								{googleConnection.data?.reconciliation?.missing_dates
									?.length ? (
									<HelpText>
										Missing dates:{" "}
										{googleConnection.data.reconciliation.missing_dates.join(
											", ",
										)}
									</HelpText>
								) : null}

								<Form onSubmit={onGoogleConfigSave}>
									<FormSection disabled={isGoogleBusy}>
										<FieldGrid>
											<Field
												label="Developer token"
												htmlFor="google-developer-token"
												required
												description="Stored encrypted and reused for sync and reconciliation jobs."
												error={googleConfigErrors.developerToken ?? undefined}
											>
												<Input
													id="google-developer-token"
													type="password"
													value={googleConfigForm.developerToken}
													onChange={(event) =>
														setGoogleConfigForm((current) => ({
															...current,
															developerToken: event.target.value,
														}))
													}
													aria-invalid={
														googleConfigErrors.developerToken ? "true" : "false"
													}
												/>
											</Field>
											<Field
												label="Client ID"
												htmlFor="google-client-id"
												required
												description="OAuth client ID from the Google Cloud project used for Ads access."
												error={googleConfigErrors.clientId ?? undefined}
											>
												<Input
													id="google-client-id"
													type="password"
													value={googleConfigForm.clientId}
													onChange={(event) =>
														setGoogleConfigForm((current) => ({
															...current,
															clientId: event.target.value,
														}))
													}
													aria-invalid={
														googleConfigErrors.clientId ? "true" : "false"
													}
												/>
											</Field>
											<Field
												label="Client secret"
												htmlFor="google-client-secret"
												required
												description="Pairs with the client ID above."
												error={googleConfigErrors.clientSecret ?? undefined}
											>
												<Input
													id="google-client-secret"
													type="password"
													value={googleConfigForm.clientSecret}
													onChange={(event) =>
														setGoogleConfigForm((current) => ({
															...current,
															clientSecret: event.target.value,
														}))
													}
													aria-invalid={
														googleConfigErrors.clientSecret ? "true" : "false"
													}
												/>
											</Field>
											<Field
												label="OAuth base URL"
												htmlFor="google-app-base-url"
												required
												description="Used to build the Google OAuth callback and redirect flow."
												error={googleConfigErrors.appBaseUrl ?? undefined}
											>
												<Input
													id="google-app-base-url"
													type="url"
													value={googleConfigForm.appBaseUrl}
													onChange={(event) =>
														setGoogleConfigForm((current) => ({
															...current,
															appBaseUrl: event.target.value,
														}))
													}
													placeholder="https://roas-radar.thecapemarine.com"
													aria-invalid={
														googleConfigErrors.appBaseUrl ? "true" : "false"
													}
												/>
											</Field>
											<Field
												label="Scopes"
												htmlFor="google-app-scopes"
												required
												description="Comma-separated scopes requested during Google Ads OAuth."
												error={googleConfigErrors.appScopes ?? undefined}
											>
												<Input
													id="google-app-scopes"
													type="text"
													value={googleConfigForm.appScopes}
													onChange={(event) =>
														setGoogleConfigForm((current) => ({
															...current,
															appScopes: event.target.value,
														}))
													}
													placeholder="https://www.googleapis.com/auth/adwords"
													aria-invalid={
														googleConfigErrors.appScopes ? "true" : "false"
													}
												/>
											</Field>
										</FieldGrid>
										{hasMessageForAction(actionFeedback, [
											"google-config-save",
										]) ? (
											<FormMessage
												tone={
													actionFeedback.error
														? "error"
														: isGoogleBusy
															? "warning"
															: "success"
												}
											>
												{actionFeedback.error
													? actionFeedback.error
													: isGoogleBusy
														? "Saving the Google Ads configuration and waiting for the API response…"
														: actionFeedback.message}
											</FormMessage>
										) : null}
										<ButtonRow>
											<Button type="submit">
												{actionFeedback.loading === "google-config-save"
													? "Saving…"
													: "Save Google Ads config"}
											</Button>
										</ButtonRow>
									</FormSection>
								</Form>

								<Form onSubmit={onGoogleConnect}>
									<FormSection disabled={isGoogleBusy}>
										<FieldGrid>
											<Field
												label="Customer ID"
												htmlFor="google-customer-id"
												required
												description="Use the destination Google Ads customer in `123-456-7890` format."
												error={googleConnectErrors.customerId ?? undefined}
											>
												<Input
													id="google-customer-id"
													type="text"
													value={googleForm.customerId}
													onChange={(event) =>
														setGoogleForm((current) => ({
															...current,
															customerId: event.target.value,
														}))
													}
													placeholder="123-456-7890"
													aria-invalid={
														googleConnectErrors.customerId ? "true" : "false"
													}
													required
												/>
											</Field>
											<Field
												label="Login customer ID"
												htmlFor="google-login-customer-id"
												optional
												description="Set this only when the OAuth credentials belong to an MCC."
												error={googleConnectErrors.loginCustomerId ?? undefined}
											>
												<Input
													id="google-login-customer-id"
													type="text"
													value={googleForm.loginCustomerId ?? ""}
													onChange={(event) =>
														setGoogleForm((current) => ({
															...current,
															loginCustomerId: event.target.value,
														}))
													}
													placeholder="Optional MCC login"
													aria-invalid={
														googleConnectErrors.loginCustomerId
															? "true"
															: "false"
													}
												/>
											</Field>
										</FieldGrid>
										{hasMessageForAction(actionFeedback, [
											"google-connect",
											"google-sync",
											"google-reconcile",
										]) ? (
											<FormMessage
												tone={
													actionFeedback.error
														? "error"
														: isGoogleBusy
															? "warning"
															: "success"
												}
											>
												{actionFeedback.error
													? actionFeedback.error
													: isGoogleBusy
														? "Submitting the Google Ads action and waiting for the API response…"
														: actionFeedback.message}
											</FormMessage>
										) : null}
										<ButtonRow>
											<Button
												type="submit"
												disabled={Boolean(
													googleConnectErrors.customerId ||
														googleConnectErrors.loginCustomerId ||
														googleMissingFields.length,
												)}
											>
												{actionFeedback.loading === "google-connect"
													? "Opening Google…"
													: "Connect Google Ads"}
											</Button>
											<Button
												type="button"
												tone="secondary"
												onClick={() => void onGoogleSync()}
												disabled={googleConnection.data?.connection == null}
											>
												{actionFeedback.loading === "google-sync" ? (
													"Queueing…"
												) : (
													<>
														<span className="sm:hidden">Sync range</span>
														<span className="hidden sm:inline">{`Sync ${filters.startDate} to ${filters.endDate}`}</span>
													</>
												)}
											</Button>
											<Button
												type="button"
												tone="secondary"
												onClick={() => void onGoogleReconcile()}
												disabled={googleConnection.data?.connection == null}
											>
												{actionFeedback.loading === "google-reconcile"
													? "Running…"
													: "Reconcile gaps"}
											</Button>
										</ButtonRow>
									</FormSection>
								</Form>
							</div>
						</ConnectionState>
					</IntegrationCard>
				</div>
			</div>

			{isAdmin ? (
				<Panel
					title="User access"
					description="Authenticated reporting and admin tools remain gated behind app-user credentials managed here."
					wide
				>
					<SectionState
						loading={usersSection.loading}
						error={usersSection.error}
						empty={false}
						emptyLabel=""
					>
						<div className="grid gap-6 xl:grid-cols-[minmax(0,0.78fr)_minmax(0,1.22fr)]">
							<div className="rounded-panel border border-line/60 bg-surface-alt/65 p-panel shadow-inset-soft">
								<Eyebrow>Provision user</Eyebrow>
								<h3 className="mt-3 font-display text-title text-ink">
									Create app access
								</h3>
								<p className="mt-3 text-body text-ink-soft">
									New users can sign into both reporting and admin surfaces
									immediately after creation.
								</p>
								<Form className="mt-5" onSubmit={onCreateUser}>
									<FormSection disabled={isUserCreateBusy}>
										<FieldGrid dense>
											<Field
												label="Display name"
												htmlFor="new-user-display-name"
												required
												description="Shown across the dashboard shell and authenticated settings views."
												error={createUserErrors.displayName ?? undefined}
											>
												<Input
													id="new-user-display-name"
													type="text"
													value={newUserForm.displayName}
													onChange={(event) =>
														setNewUserForm((current) => ({
															...current,
															displayName: event.target.value,
														}))
													}
													aria-invalid={
														createUserErrors.displayName ? "true" : "false"
													}
													required
												/>
											</Field>
											<Field
												label="Email"
												htmlFor="new-user-email"
												required
												description="Used as the sign-in identifier for reporting and admin surfaces."
												error={createUserErrors.email ?? undefined}
											>
												<Input
													id="new-user-email"
													type="email"
													value={newUserForm.email}
													onChange={(event) =>
														setNewUserForm((current) => ({
															...current,
															email: event.target.value,
														}))
													}
													aria-invalid={
														createUserErrors.email ? "true" : "false"
													}
													required
												/>
											</Field>
											<Field
												label="Password"
												htmlFor="new-user-password"
												required
												description="Minimum 12 characters. The existing password policy is unchanged."
												error={createUserErrors.password ?? undefined}
											>
												<Input
													id="new-user-password"
													type="password"
													value={newUserForm.password}
													onChange={(event) =>
														setNewUserForm((current) => ({
															...current,
															password: event.target.value,
														}))
													}
													minLength={12}
													aria-invalid={
														createUserErrors.password ? "true" : "false"
													}
													required
												/>
											</Field>
											<CheckboxField
												label="Admin access"
												htmlFor="new-user-admin"
												description="Admins can edit settings, manage connections, and provision users."
											>
												<input
													id="new-user-admin"
													type="checkbox"
													checked={Boolean(newUserForm.isAdmin)}
													onChange={(event) =>
														setNewUserForm((current) => ({
															...current,
															isAdmin: event.target.checked,
														}))
													}
												/>
											</CheckboxField>
										</FieldGrid>
										{hasMessageForAction(actionFeedback, ["user-create"]) ? (
											<FormMessage
												tone={
													actionFeedback.error
														? "error"
														: isUserCreateBusy
															? "warning"
															: "success"
												}
											>
												{actionFeedback.error
													? actionFeedback.error
													: isUserCreateBusy
														? "Creating the authenticated user account…"
														: actionFeedback.message}
											</FormMessage>
										) : null}
										<ButtonRow>
											<Button
												type="submit"
												disabled={Boolean(
													createUserErrors.displayName ||
														createUserErrors.email ||
														createUserErrors.password,
												)}
											>
												{actionFeedback.loading === "user-create"
													? "Creating…"
													: "Add user"}
											</Button>
										</ButtonRow>
									</FormSection>
								</Form>
							</div>

							<div className="grid gap-4">
								<div className="flex flex-wrap items-center justify-between gap-3">
									<div>
										<p className="text-caption uppercase tracking-[0.16em] text-ink-muted">
											Current users
										</p>
										<h3 className="mt-2 font-display text-title text-ink">
											{usersSection.data?.length ?? 0} authenticated accounts
										</h3>
									</div>
									<Badge tone="neutral" className="px-4 py-2">
										Last reviewed in {reportingTimezone}
									</Badge>
								</div>

								<DataTableToolbar
									title="Authenticated users"
									description="Shared controls keep user access review aligned with the reporting and order-detail tables."
									summary={
										<>
											<TableMeta
												currentCount={filteredUsers.length}
												totalCount={users.length}
												label="users"
											/>
											<TablePagination
												page={paginatedUsers.currentPage}
												totalPages={paginatedUsers.totalPages}
												onPageChange={setUserPage}
											/>
										</>
									}
								>
									<TableFilterBar>
										<TableSearchField
											label="Search users"
											value={userSearch}
											onChange={(value) => {
												setUserSearch(value);
												setUserPage(1);
											}}
											placeholder="Name, email, role, status"
										/>
									</TableFilterBar>
								</DataTableToolbar>

								<TableWrap className="max-h-[28rem]">
									<Table caption="Authenticated users">
										<TableHead>
											<TableRow>
												<SortableTableHeaderCell
													sorted={userSort.key === "user"}
													direction={userSort.direction}
													onSort={() => toggleUserSort("user")}
												>
													User
												</SortableTableHeaderCell>
												<SortableTableHeaderCell
													sorted={userSort.key === "role"}
													direction={userSort.direction}
													onSort={() => toggleUserSort("role")}
												>
													Role
												</SortableTableHeaderCell>
												<SortableTableHeaderCell
													sorted={userSort.key === "status"}
													direction={userSort.direction}
													onSort={() => toggleUserSort("status")}
												>
													Status
												</SortableTableHeaderCell>
												<SortableTableHeaderCell
													sorted={userSort.key === "lastLogin"}
													direction={userSort.direction}
													onSort={() => toggleUserSort("lastLogin")}
												>
													Last login
												</SortableTableHeaderCell>
											</TableRow>
										</TableHead>
										<TableBody>
											{paginatedUsers.rows.length === 0 ? (
												<TableEmptyRow
													colSpan={4}
													title="No users found"
													description="No authenticated users match the current search."
												/>
											) : null}
											{paginatedUsers.rows.map((user) => (
												<TableRow key={user.id}>
													<TableCell>
														<PrimaryCell>
															<strong>{user.displayName}</strong>
															<span>{user.email}</span>
														</PrimaryCell>
													</TableCell>
													<TableCell>
														{user.isAdmin ? "Admin" : "Viewer"}
													</TableCell>
													<TableCell>{user.status}</TableCell>
													<TableCell>
														{formatOptionalDateTime(
															user.lastLoginAt,
															reportingTimezone,
														)}
													</TableCell>
												</TableRow>
											))}
										</TableBody>
									</Table>
								</TableWrap>
							</div>
						</div>
					</SectionState>
				</Panel>
			) : null}
		</section>
	);
}

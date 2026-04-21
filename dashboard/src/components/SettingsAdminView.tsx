import React, { useMemo, useState, type FormEvent } from 'react';

import type {
  AppSettings,
  AuthUser,
  CreateUserPayload,
  GoogleAdsConnectionPayload,
  GoogleAdsStatusResponse,
  MetaAdsConfigSummary,
  MetaAdsConnection,
  ShopifyConnectionResponse
} from '../lib/api';
import { formatDateLabel, formatDateTimeLabel } from '../lib/format';
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
  DataTableToolbar,
  ConnectionState,
  DetailList,
  Eyebrow,
  Field,
  FieldGrid,
  Form,
  HelpText,
  Input,
  MetricCopy,
  MetricValue,
  Panel,
  PrimaryCell,
  SectionState,
  SortableTableHeaderCell,
  Table,
  TableBody,
  TableCell,
  TableEmptyRow,
  TableFilterBar,
  TableHead,
  TableHeaderCell,
  TableMeta,
  TablePagination,
  TableSearchField,
  TableRow,
  StatusPill,
  TableWrap
} from './AuthenticatedUi';
import { matchesQuery, paginateRows, sortRows, type SortState } from '../lib/dataTable';

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type ActionFeedback = {
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
  setNewUserForm: (updater: (current: CreateUserPayload) => CreateUserPayload) => void;
  shopifyConnection: AsyncSection<ShopifyConnectionResponse>;
  shopifyBackfillRange: {
    startDate: string;
    endDate: string;
  };
  setShopifyBackfillRange: (
    updater: (current: { startDate: string; endDate: string }) => { startDate: string; endDate: string }
  ) => void;
  metaConnection: AsyncSection<MetaConnectionState>;
  metaConfigForm: MetaConfigForm;
  setMetaConfigForm: (updater: (current: MetaConfigForm) => MetaConfigForm) => void;
  googleConnection: AsyncSection<GoogleAdsStatusResponse>;
  googleForm: GoogleAdsConnectionPayload;
  setGoogleForm: (updater: (current: GoogleAdsConnectionPayload) => GoogleAdsConnectionPayload) => void;
  actionFeedback: ActionFeedback;
  onSettingsSave: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
  onCreateUser: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
  onShopifyBackfill: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
  onMetaConfigSave: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
  onGoogleConnect: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
  onShopifyTest: () => void | Promise<void>;
  onShopifyWebhookSync: () => void | Promise<void>;
  onShopifyAttributionRecovery: () => void | Promise<void>;
  onMetaConnect: () => void | Promise<void>;
  onMetaSync: () => void | Promise<void>;
  onGoogleSync: () => void | Promise<void>;
  onGoogleReconcile: () => void | Promise<void>;
};

function formatOptionalDateTime(value: string | null | undefined, reportingTimezone: string): string {
  return value ? formatDateTimeLabel(value, reportingTimezone) : 'Not available';
}

function SettingsMetric({
  label,
  value,
  detail
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
  accent = 'brand',
  children
}: {
  eyebrow: string;
  title: string;
  description: string;
  status: string;
  accent?: 'brand' | 'teal';
  children: JSX.Element;
}) {
  return (
    <Card className="overflow-hidden bg-surface/92 p-0">
      <div
        className={[
          'h-1 w-full',
          accent === 'teal'
            ? 'bg-gradient-to-r from-teal via-teal/70 to-brand/60'
            : 'bg-gradient-to-r from-brand via-brand/70 to-teal/70'
        ].join(' ')}
      />
      <div className="grid gap-6 p-panel">
        <CardHeader className="items-start gap-4">
          <div className="max-w-2xl">
            <Eyebrow>{eyebrow}</Eyebrow>
            <CardTitle className="mt-3">{title}</CardTitle>
            <CardDescription className="mt-3">{description}</CardDescription>
          </div>
          <StatusPill tone={accent === 'teal' ? 'teal' : 'brand'}>{status}</StatusPill>
        </CardHeader>
        {children}
      </div>
    </Card>
  );
}

function DetailGrid({ children }: { children: JSX.Element[] | JSX.Element }) {
  return <DetailList className="xl:grid-cols-2">{children}</DetailList>;
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
  metaConnection,
  metaConfigForm,
  setMetaConfigForm,
  googleConnection,
  googleForm,
  setGoogleForm,
  actionFeedback,
  onSettingsSave,
  onCreateUser,
  onShopifyBackfill,
  onMetaConfigSave,
  onGoogleConnect,
  onShopifyTest,
  onShopifyWebhookSync,
  onShopifyAttributionRecovery,
  onMetaConnect,
  onMetaSync,
  onGoogleSync,
  onGoogleReconcile
}: SettingsAdminViewProps) {
  const [userSearch, setUserSearch] = useState('');
  const [userSort, setUserSort] = useState<SortState<'user' | 'role' | 'status' | 'lastLogin'>>({
    key: 'user',
    direction: 'asc'
  });
  const [userPage, setUserPage] = useState(1);
  const activeConnections = [
    shopifyConnection.data?.connected ? 1 : 0,
    metaConnection.data?.connection ? 1 : 0,
    googleConnection.data?.connection ? 1 : 0
  ].reduce((sum, value) => sum + value, 0);

  const connectionHealth = [
    metaConnection.data?.connection?.last_sync_status,
    googleConnection.data?.connection?.last_sync_status
  ].filter(Boolean);

  const timezoneUpdatedAt = appSettings.data?.updatedAt
    ? formatOptionalDateTime(appSettings.data.updatedAt, reportingTimezone)
    : 'Awaiting first save';
  const users = usersSection.data ?? [];
  const filteredUsers = useMemo(
    () => users.filter((user) => matchesQuery([user.displayName, user.email, user.status, user.isAdmin ? 'Admin' : 'Viewer'], userSearch)),
    [userSearch, users]
  );
  const sortedUsers = useMemo(
    () =>
      sortRows(filteredUsers, userSort, {
        user: (user) => `${user.displayName} ${user.email}`,
        role: (user) => (user.isAdmin ? 'Admin' : 'Viewer'),
        status: (user) => user.status,
        lastLogin: (user) => user.lastLoginAt ?? ''
      }),
    [filteredUsers, userSort]
  );
  const paginatedUsers = useMemo(() => paginateRows(sortedUsers, userPage, 6), [sortedUsers, userPage]);

  function toggleUserSort(key: 'user' | 'role' | 'status' | 'lastLogin') {
    setUserSort((current) => ({
      key,
      direction: current.key === key && current.direction === 'desc' ? 'asc' : 'desc'
    }));
  }

  return (
    <section className="grid gap-section">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <SettingsMetric
          label="Reporting timezone"
          value={appSettings.data?.reportingTimezone ?? defaultReportingTimezone}
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
          value={isAdmin ? 'Admin' : 'Viewer'}
          detail={isAdmin ? `${usersSection.data?.length ?? 0} app users provisioned` : 'Read-only authenticated access'}
        />
      </div>

      <Panel
        title="Settings operations"
        description="Manage reporting timezone, store integrations, ad platform credentials, and dashboard access from one Tailwind surface."
        wide
      >
        <div className="grid gap-3">
          {actionFeedback.error ? <Banner tone="error">{actionFeedback.error}</Banner> : null}
          {actionFeedback.message ? <Banner tone="success">{actionFeedback.message}</Banner> : null}
          {!actionFeedback.error && !actionFeedback.message && connectionHealth.length === 0 ? (
            <Banner>Save credentials here, then run targeted sync or reconciliation actions without leaving settings.</Banner>
          ) : null}
        </div>
      </Panel>

      <div className="grid gap-section 2xl:grid-cols-[minmax(0,0.84fr)_minmax(0,1.16fr)]">
        <Panel
          title="Reporting timezone"
          description="Dashboard date ranges, daily aggregation, and reporting rollups all use this timezone."
          className="h-fit"
        >
          <SectionState loading={appSettings.loading} error={appSettings.error} empty={false} emptyLabel="">
            <div className="grid gap-5">
              <Form onSubmit={onSettingsSave}>
                <FieldGrid dense>
                  <Field label="Timezone" htmlFor="reporting-timezone">
                    <Input
                      id="reporting-timezone"
                      type="text"
                      list="reporting-timezone-options"
                      value={settingsForm.reportingTimezone}
                      onChange={(event) =>
                        setSettingsForm((current) => ({ ...current, reportingTimezone: event.target.value }))
                      }
                      placeholder="America/Los_Angeles"
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
                  Use a valid IANA timezone like <code>America/Los_Angeles</code>. Short aliases like <code>PST</code> and <code>UTC</code> also work here.
                </HelpText>
                <DetailGrid>
                  <div>
                    <dt>Active timezone</dt>
                    <dd>{appSettings.data?.reportingTimezone ?? defaultReportingTimezone}</dd>
                  </div>
                  <div>
                    <dt>Updated</dt>
                    <dd>{timezoneUpdatedAt}</dd>
                  </div>
                </DetailGrid>
                <ButtonRow>
                  <Button type="submit" disabled={actionFeedback.loading !== null}>
                    {actionFeedback.loading === 'settings-save' ? 'Saving…' : 'Save reporting timezone'}
                  </Button>
                </ButtonRow>
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
              (shopifyConnection.data?.connected ? 'active' : shopifyConnection.loading ? 'Loading' : 'Not connected')
            }
            accent="teal"
          >
            <ConnectionState loading={shopifyConnection.loading} error={shopifyConnection.error}>
              <div className="grid gap-5">
                <DetailGrid>
                  <div>
                    <dt>Shop</dt>
                    <dd>{shopifyConnection.data?.shop?.name ?? shopifyConnection.data?.shopDomain ?? 'Not connected'}</dd>
                  </div>
                  <div>
                    <dt>Domain</dt>
                    <dd>{shopifyConnection.data?.shopDomain ?? 'Not available'}</dd>
                  </div>
                  <div>
                    <dt>Installed</dt>
                    <dd>{formatOptionalDateTime(shopifyConnection.data?.installedAt, reportingTimezone)}</dd>
                  </div>
                  <div>
                    <dt>Webhook base URL</dt>
                    <dd>{shopifyConnection.data?.webhookBaseUrl ?? 'Not available'}</dd>
                  </div>
                </DetailGrid>

                {shopifyConnection.data?.reconnectUrl ? (
                  <HelpText>Reconnect URL is available if the current store installation needs to be reauthorized.</HelpText>
                ) : null}

                <Form onSubmit={onShopifyBackfill}>
                  <FieldGrid>
                    <Field label="Backfill start" htmlFor="shopify-backfill-start">
                      <Input
                        id="shopify-backfill-start"
                        type="date"
                        value={shopifyBackfillRange.startDate}
                        onChange={(event) =>
                          setShopifyBackfillRange((current) => ({ ...current, startDate: event.target.value }))
                        }
                        required
                      />
                    </Field>
                    <Field label="Backfill end" htmlFor="shopify-backfill-end">
                      <Input
                        id="shopify-backfill-end"
                        type="date"
                        value={shopifyBackfillRange.endDate}
                        onChange={(event) =>
                          setShopifyBackfillRange((current) => ({ ...current, endDate: event.target.value }))
                        }
                        required
                      />
                    </Field>
                  </FieldGrid>

                  <Card padding="compact" className="border-line/60 bg-canvas-tint/80 shadow-none">
                    <Eyebrow>Recovery tools</Eyebrow>
                    <p className="mt-2 text-body text-ink-soft">
                      Backfill imports historical orders. Attribution recovery rescans unattributed web orders in the same date window.
                    </p>
                  </Card>

                  <ButtonRow>
                    <Button
                      type="submit"
                      tone="secondary"
                      disabled={actionFeedback.loading !== null || !shopifyConnection.data?.connected}
                    >
                      {actionFeedback.loading === 'shopify-backfill' ? 'Backfilling…' : 'Backfill Shopify orders'}
                    </Button>
                    <Button
                      type="button"
                      tone="secondary"
                      onClick={() => void onShopifyAttributionRecovery()}
                      disabled={actionFeedback.loading !== null || !shopifyConnection.data?.connected}
                    >
                      {actionFeedback.loading === 'shopify-attribution-recovery' ? 'Recovering…' : 'Recover attribution hints'}
                    </Button>
                  </ButtonRow>
                </Form>

                <ButtonRow>
                  <Button type="button" onClick={() => void onShopifyTest()} disabled={actionFeedback.loading !== null}>
                    {actionFeedback.loading === 'shopify-test' ? 'Testing…' : 'Test Shopify connection'}
                  </Button>
                  <Button
                    type="button"
                    tone="secondary"
                    onClick={() => void onShopifyWebhookSync()}
                    disabled={actionFeedback.loading !== null || !shopifyConnection.data?.connected}
                  >
                    {actionFeedback.loading === 'shopify-webhooks' ? 'Syncing…' : 'Sync Shopify webhooks'}
                  </Button>
                </ButtonRow>
              </div>
            </ConnectionState>
          </IntegrationCard>

          <IntegrationCard
            eyebrow="Ad platform"
            title="Meta Ads"
            description="Store OAuth app settings, attach the ad account through Meta OAuth, and queue spend syncs for the current reporting window."
            status={
              metaConnection.data?.connection?.status ??
              (metaConnection.data?.config.missingFields.length ? 'Needs config' : metaConnection.loading ? 'Loading' : 'Not connected')
            }
          >
            <ConnectionState loading={metaConnection.loading} error={metaConnection.error}>
              <div className="grid gap-5">
                <DetailGrid>
                  <div>
                    <dt>Config source</dt>
                    <dd>{metaConnection.data?.config.source ?? 'Not available'}</dd>
                  </div>
                  <div>
                    <dt>Ad account</dt>
                    <dd>{metaConnection.data?.connection?.account_name ?? metaConnection.data?.config.adAccountId ?? 'Not configured'}</dd>
                  </div>
                  <div>
                    <dt>Last sync</dt>
                    <dd>{formatOptionalDateTime(metaConnection.data?.connection?.last_sync_completed_at, reportingTimezone)}</dd>
                  </div>
                  <div>
                    <dt>Sync status</dt>
                    <dd>{metaConnection.data?.connection?.last_sync_status ?? 'Not started'}</dd>
                  </div>
                </DetailGrid>

                {metaConnection.data?.config.missingFields.length ? (
                  <HelpText tone="error">Missing Meta config: {metaConnection.data.config.missingFields.join(', ')}</HelpText>
                ) : null}
                {metaConnection.data?.connection?.last_sync_error ? (
                  <HelpText tone="error">{metaConnection.data.connection.last_sync_error}</HelpText>
                ) : null}

                <Form onSubmit={onMetaConfigSave}>
                  <FieldGrid>
                    <Field label="Meta app ID" htmlFor="meta-app-id">
                      <Input
                        id="meta-app-id"
                        type="text"
                        value={metaConfigForm.appId}
                        onChange={(event) => setMetaConfigForm((current) => ({ ...current, appId: event.target.value }))}
                        placeholder="123456789012345"
                      />
                    </Field>
                    <Field label="Ad account ID" htmlFor="meta-account-id">
                      <Input
                        id="meta-account-id"
                        type="text"
                        value={metaConfigForm.adAccountId}
                        onChange={(event) =>
                          setMetaConfigForm((current) => ({ ...current, adAccountId: event.target.value }))
                        }
                        placeholder="act_123456789012345 or 123456789012345"
                      />
                    </Field>
                    <Field label="Meta app secret" htmlFor="meta-app-secret" wide>
                      <Input
                        id="meta-app-secret"
                        type="password"
                        value={metaConfigForm.appSecret}
                        onChange={(event) =>
                          setMetaConfigForm((current) => ({ ...current, appSecret: event.target.value }))
                        }
                        placeholder={
                          metaConnection.data?.config.appSecretConfigured
                            ? 'Leave blank to keep the saved secret'
                            : 'Paste the Meta app secret'
                        }
                      />
                    </Field>
                    <Field label="OAuth base URL" htmlFor="meta-base-url" wide>
                      <Input
                        id="meta-base-url"
                        type="url"
                        value={metaConfigForm.appBaseUrl}
                        onChange={(event) =>
                          setMetaConfigForm((current) => ({ ...current, appBaseUrl: event.target.value }))
                        }
                        placeholder="https://roas-radar.api.thecapemarine.com"
                      />
                    </Field>
                    <Field label="Scopes" htmlFor="meta-scopes" wide>
                      <Input
                        id="meta-scopes"
                        type="text"
                        value={metaConfigForm.appScopes}
                        onChange={(event) => setMetaConfigForm((current) => ({ ...current, appScopes: event.target.value }))}
                        placeholder="ads_read"
                      />
                    </Field>
                  </FieldGrid>
                  <ButtonRow>
                    <Button type="submit" disabled={actionFeedback.loading !== null}>
                      {actionFeedback.loading === 'meta-config-save' ? 'Saving…' : 'Save Meta config'}
                    </Button>
                  </ButtonRow>
                </Form>

                <ButtonRow>
                  <Button
                    type="button"
                    onClick={() => void onMetaConnect()}
                    disabled={actionFeedback.loading !== null || Boolean(metaConnection.data?.config.missingFields.length)}
                  >
                    {actionFeedback.loading === 'meta-connect' ? 'Opening Meta…' : 'Connect Meta Ads'}
                  </Button>
                  <Button
                    type="button"
                    tone="secondary"
                    onClick={() => void onMetaSync()}
                    disabled={actionFeedback.loading !== null || metaConnection.data?.connection == null}
                  >
                    {actionFeedback.loading === 'meta-sync' ? 'Queueing…' : `Sync ${filters.startDate} to ${filters.endDate}`}
                  </Button>
                </ButtonRow>
              </div>
            </ConnectionState>
          </IntegrationCard>

          <IntegrationCard
            eyebrow="Ad platform"
            title="Google Ads"
            description="Create the encrypted Google Ads connection, queue spend sync jobs, and trigger gap reconciliation from the same workspace."
            status={googleConnection.data?.connection?.status ?? (googleConnection.loading ? 'Loading' : 'Not connected')}
            accent="teal"
          >
            <ConnectionState loading={googleConnection.loading} error={googleConnection.error}>
              <div className="grid gap-5">
                <DetailGrid>
                  <div>
                    <dt>Customer</dt>
                    <dd>
                      {googleConnection.data?.connection?.customer_descriptive_name ??
                        googleConnection.data?.connection?.customer_id ??
                        'Not connected'}
                    </dd>
                  </div>
                  <div>
                    <dt>Currency</dt>
                    <dd>{googleConnection.data?.connection?.currency_code ?? 'Not available'}</dd>
                  </div>
                  <div>
                    <dt>Last sync</dt>
                    <dd>{formatOptionalDateTime(googleConnection.data?.connection?.last_sync_completed_at, reportingTimezone)}</dd>
                  </div>
                  <div>
                    <dt>Reconciliation</dt>
                    <dd>{googleConnection.data?.reconciliation?.status ?? 'Not run'}</dd>
                  </div>
                </DetailGrid>

                {googleConnection.data?.connection?.last_sync_error ? (
                  <HelpText tone="error">{googleConnection.data.connection.last_sync_error}</HelpText>
                ) : null}
                {googleConnection.data?.reconciliation?.missing_dates?.length ? (
                  <HelpText>Missing dates: {googleConnection.data.reconciliation.missing_dates.join(', ')}</HelpText>
                ) : null}

                <Form onSubmit={onGoogleConnect}>
                  <FieldGrid>
                    <Field label="Customer ID" htmlFor="google-customer-id">
                      <Input
                        id="google-customer-id"
                        type="text"
                        value={googleForm.customerId}
                        onChange={(event) => setGoogleForm((current) => ({ ...current, customerId: event.target.value }))}
                        placeholder="123-456-7890"
                        required
                      />
                    </Field>
                    <Field label="Login customer ID" htmlFor="google-login-customer-id">
                      <Input
                        id="google-login-customer-id"
                        type="text"
                        value={googleForm.loginCustomerId ?? ''}
                        onChange={(event) =>
                          setGoogleForm((current) => ({ ...current, loginCustomerId: event.target.value }))
                        }
                        placeholder="Optional MCC login"
                      />
                    </Field>
                    <Field label="Developer token" htmlFor="google-developer-token">
                      <Input
                        id="google-developer-token"
                        type="password"
                        value={googleForm.developerToken}
                        onChange={(event) =>
                          setGoogleForm((current) => ({ ...current, developerToken: event.target.value }))
                        }
                        required
                      />
                    </Field>
                    <Field label="Client ID" htmlFor="google-client-id">
                      <Input
                        id="google-client-id"
                        type="password"
                        value={googleForm.clientId}
                        onChange={(event) => setGoogleForm((current) => ({ ...current, clientId: event.target.value }))}
                        required
                      />
                    </Field>
                    <Field label="Client secret" htmlFor="google-client-secret">
                      <Input
                        id="google-client-secret"
                        type="password"
                        value={googleForm.clientSecret}
                        onChange={(event) =>
                          setGoogleForm((current) => ({ ...current, clientSecret: event.target.value }))
                        }
                        required
                      />
                    </Field>
                    <Field label="Refresh token" htmlFor="google-refresh-token">
                      <Input
                        id="google-refresh-token"
                        type="password"
                        value={googleForm.refreshToken}
                        onChange={(event) =>
                          setGoogleForm((current) => ({ ...current, refreshToken: event.target.value }))
                        }
                        required
                      />
                    </Field>
                  </FieldGrid>
                  <ButtonRow>
                    <Button type="submit" disabled={actionFeedback.loading !== null}>
                      {actionFeedback.loading === 'google-connect' ? 'Saving…' : 'Connect Google Ads'}
                    </Button>
                    <Button
                      type="button"
                      tone="secondary"
                      onClick={() => void onGoogleSync()}
                      disabled={actionFeedback.loading !== null || googleConnection.data?.connection == null}
                    >
                      {actionFeedback.loading === 'google-sync' ? 'Queueing…' : `Sync ${filters.startDate} to ${filters.endDate}`}
                    </Button>
                    <Button
                      type="button"
                      tone="secondary"
                      onClick={() => void onGoogleReconcile()}
                      disabled={actionFeedback.loading !== null || googleConnection.data?.connection == null}
                    >
                      {actionFeedback.loading === 'google-reconcile' ? 'Running…' : 'Reconcile gaps'}
                    </Button>
                  </ButtonRow>
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
          <SectionState loading={usersSection.loading} error={usersSection.error} empty={false} emptyLabel="">
            <div className="grid gap-6 xl:grid-cols-[minmax(0,0.78fr)_minmax(0,1.22fr)]">
              <div className="rounded-panel border border-line/60 bg-surface-alt/65 p-panel shadow-inset-soft">
                <Eyebrow>Provision user</Eyebrow>
                <h3 className="mt-3 font-display text-title text-ink">Create app access</h3>
                <p className="mt-3 text-body text-ink-soft">
                  New users can sign into both reporting and admin surfaces immediately after creation.
                </p>
                <Form className="mt-5" onSubmit={onCreateUser}>
                  <FieldGrid dense>
                    <Field label="Display name" htmlFor="new-user-display-name">
                      <Input
                        id="new-user-display-name"
                        type="text"
                        value={newUserForm.displayName}
                        onChange={(event) =>
                          setNewUserForm((current) => ({ ...current, displayName: event.target.value }))
                        }
                        required
                      />
                    </Field>
                    <Field label="Email" htmlFor="new-user-email">
                      <Input
                        id="new-user-email"
                        type="email"
                        value={newUserForm.email}
                        onChange={(event) => setNewUserForm((current) => ({ ...current, email: event.target.value }))}
                        required
                      />
                    </Field>
                    <Field label="Password" htmlFor="new-user-password">
                      <Input
                        id="new-user-password"
                        type="password"
                        value={newUserForm.password}
                        onChange={(event) =>
                          setNewUserForm((current) => ({ ...current, password: event.target.value }))
                        }
                        minLength={12}
                        required
                      />
                    </Field>
                    <CheckboxField label="Admin access" htmlFor="new-user-admin">
                      <input
                        id="new-user-admin"
                        type="checkbox"
                        checked={Boolean(newUserForm.isAdmin)}
                        onChange={(event) =>
                          setNewUserForm((current) => ({ ...current, isAdmin: event.target.checked }))
                        }
                      />
                    </CheckboxField>
                  </FieldGrid>
                  <ButtonRow>
                    <Button type="submit" disabled={actionFeedback.loading !== null}>
                      {actionFeedback.loading === 'user-create' ? 'Creating…' : 'Add user'}
                    </Button>
                  </ButtonRow>
                </Form>
              </div>

              <div className="grid gap-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <p className="text-caption uppercase tracking-[0.16em] text-ink-muted">Current users</p>
                    <h3 className="mt-2 font-display text-title text-ink">{usersSection.data?.length ?? 0} authenticated accounts</h3>
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
                      <TableMeta currentCount={filteredUsers.length} totalCount={users.length} label="users" />
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
                          sorted={userSort.key === 'user'}
                          direction={userSort.direction}
                          onSort={() => toggleUserSort('user')}
                        >
                          User
                        </SortableTableHeaderCell>
                        <SortableTableHeaderCell
                          sorted={userSort.key === 'role'}
                          direction={userSort.direction}
                          onSort={() => toggleUserSort('role')}
                        >
                          Role
                        </SortableTableHeaderCell>
                        <SortableTableHeaderCell
                          sorted={userSort.key === 'status'}
                          direction={userSort.direction}
                          onSort={() => toggleUserSort('status')}
                        >
                          Status
                        </SortableTableHeaderCell>
                        <SortableTableHeaderCell
                          sorted={userSort.key === 'lastLogin'}
                          direction={userSort.direction}
                          onSort={() => toggleUserSort('lastLogin')}
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
                          <TableCell>{user.isAdmin ? 'Admin' : 'Viewer'}</TableCell>
                          <TableCell>{user.status}</TableCell>
                          <TableCell>{formatOptionalDateTime(user.lastLoginAt, reportingTimezone)}</TableCell>
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

# Authenticated UI Tailwind QA

## Scope

This QA pass covers every authenticated surface listed in the Authenticated UI Tailwind Migration Audit on the `tailwind` branch:

- Dashboard page
- Order details drill-in
- Settings page shell
- Reporting timezone settings
- Shopify connection card
- Meta Ads connection card
- Google Ads connection card
- User access admin section

## Coverage Summary

All authenticated surfaces now render through Tailwind-first components in `dashboard/src/components/` plus shared primitives from `dashboard/src/components/AuthenticatedUi.tsx` and `dashboard/src/components/AuthenticatedAppShell.tsx`.

### Shared authenticated primitives

- Authenticated app shell: `AuthenticatedAppShell`
- Global action feedback banner: `Banner`
- Panel container: `Panel`
- Section loading/error/empty state: `SectionState`
- Connection loading/error state: `ConnectionState`
- Summary metric card: component-local Tailwind cards in reporting, settings, and order details views
- Chart card: `ReportingDashboard` timeseries card
- Table wrapper and primary cell pattern: `TableWrap`, `PrimaryCell`
- Detail card and definition list pattern: `DetailList` plus component-local Tailwind cards
- Form field and button rows: `Field`, `FieldGrid`, `Input`, `Select`, `Button`, `ButtonRow`
- Filter bar: `ReportingDashboard`

### Authenticated page/widget coverage

| Surface or widget | Current Tailwind implementation | QA status |
| --- | --- | --- |
| Hero and signed-in status card | `AuthenticatedAppShell` header and status panel | Covered |
| Date/source/campaign/group-by filter bar | `ReportingDashboard` | Covered |
| Quick range chips | `ReportingDashboard` | Covered |
| Summary cards: Visits, Orders, Revenue, AOV | `ReportingDashboard` | Covered |
| Revenue trend chart | `ReportingDashboard` | Covered |
| Campaign performance table | `ReportingDashboard` | Covered |
| Campaign mix bars | `ReportingDashboard` | Covered |
| Attributed orders table | `ReportingDashboard` | Covered |
| Order overview card | `OrderDetailsView` | Covered |
| Customer and linkage card | `OrderDetailsView` | Covered |
| Timestamps card | `OrderDetailsView` | Covered |
| Line items table | `OrderDetailsView` | Covered |
| Attribution credits table | `OrderDetailsView` | Covered |
| Raw payload viewers | `OrderDetailsView` | Covered |
| Reporting timezone editor | `SettingsAdminView` | Covered |
| Shopify connection card | `SettingsAdminView` | Covered |
| Meta Ads connection card | `SettingsAdminView` | Covered |
| Google Ads connection card | `SettingsAdminView` | Covered |
| User access table and create-user form | `SettingsAdminView` | Covered |

## Auth And Role Gating

- Anonymous users are gated by `fetchCurrentUser()` in `dashboard/src/App.tsx` and only reach the authenticated shell after a successful response.
- Logged-in non-admin users can access the dashboard and read settings context, but admin-only forms and connection actions are guarded by `isAdmin` checks before rendering mutation controls.
- Order drill-in remains reachable only from authenticated dashboard state via in-app navigation.

## Responsive Verification Notes

- The authenticated shell provides a mobile drawer at `lg` and persistent desktop navigation above `lg`.
- Dashboard widgets collapse from multi-column layouts to single-column stacks through Tailwind breakpoint utilities in `ReportingDashboard`.
- Order details cards and settings/admin cards use `md`, `xl`, and `2xl` grid breakpoints instead of legacy CSS media queries.
- Shared authenticated table primitives now preserve column structure behind horizontal scrolling with stronger wrap behavior in dense cells, plus stacked mobile pagination and toolbar summaries.
- Shared chart wrappers now trim legends, margins, and label density below `640px`, with intermediate spacing adjustments through tablet widths so Nivo charts stay inside their cards.
- Responsive QA target breakpoints for authenticated flows are `375px`, `768px`, `1024px`, and `1440px`.

## Typography And Spacing Verification

- Authenticated page eyebrows, badges, table headers, and metric labels now use the shared `text-caption` hierarchy through `ui-eyebrow` or shared badge/table primitives.
- Form labels use `text-label`, while helper copy, detail values, table cells, and supporting widget text use `text-body`.
- KPI cards in dashboard, settings, and order details now use the shared `text-metric` scale through `MetricValue` and `MetricCopy`.
- Shared `ui-panel-header`, `ui-form`, `ui-field-grid`, `ui-detail-list`, and table primitives now enforce consistent vertical rhythm instead of page-specific ad hoc spacing.

## Legacy CSS Retirement

- Removed the dead post-Tailwind selector block from `dashboard/src/styles.css`.
- Replaced the final legacy `dashboard-grid` reference in `dashboard/src/App.tsx` with Tailwind utilities.
- Remaining stylesheet rules are limited to Tailwind imports plus shared `ui-*` component primitives still referenced by authenticated surfaces.

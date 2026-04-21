# Tailwind Design System Foundations

This token set establishes the light-only visual direction for authenticated dashboard surfaces on the `tailwind` branch. It is intended to be the baseline for shared shell primitives, reporting widgets, settings cards, forms, and tables.

## Visual Direction

- Light-only dashboard with warm brand accents and cool operational accents
- High-density layouts supported by strong panel separation and restrained shadows
- Display typography for hero and section headings, body typography for data-heavy surfaces
- Semantic spacing tokens for shell, section, panel, and card rhythm instead of one-off pixel values

## Token Reference

### Colors

| Token | Value | Usage |
| --- | --- | --- |
| `canvas` | `#f4f7f4` | App background |
| `canvas-tint` | `#edf5f2` | Inset sections and quiet surfaces |
| `surface` | `#ffffff` | Primary cards and panels |
| `surface-alt` | `#fbf7f2` | Alternate cards and supportive highlights |
| `line` | `#d7dfdb` | Default border |
| `line-strong` | `#b8c5c0` | Emphasized border |
| `ink` | `#17212b` | Primary text |
| `ink-soft` | `#314051` | Secondary headings and labels |
| `ink-muted` | `#627180` | Meta text |
| `brand` | `#cb6332` | Primary CTA and chart accent |
| `brand-strong` | `#a94b1f` | CTA hover and high-emphasis accent |
| `brand-soft` | `#f6dfd3` | Quiet brand background |
| `teal` | `#1f7a74` | Status and nav accent |
| `teal-strong` | `#145a55` | Emphasis on teal surfaces |
| `teal-soft` | `#dcefed` | Quiet teal background |
| `success` | `#2f7d60` | Positive state |
| `success-soft` | `#dcefe6` | Positive background |
| `warning` | `#b0711f` | Warning state |
| `warning-soft` | `#f8ead7` | Warning background |
| `danger` | `#b64c46` | Error state |
| `danger-soft` | `#f6dddb` | Error background |

### Typography

| Token | Value | Usage |
| --- | --- | --- |
| `font-display` | `Space Grotesk`, `IBM Plex Sans`, sans-serif | Hero and section titles |
| `font-body` | `IBM Plex Sans`, `Segoe UI`, sans-serif | Body copy, forms, tables |
| `font-mono` | `IBM Plex Mono`, `SFMono-Regular`, monospace | Payloads, ids, technical labels |
| `text-hero` | `clamp(3rem, 7vw, 5.5rem)` | App hero |
| `text-display` | `clamp(2rem, 4vw, 3rem)` | Section headers |
| `text-title` | `1.375rem` | Card and panel titles |
| `text-metric` | `clamp(1.9rem, 4vw, 2.8rem)` | KPI values and numeric callouts |
| `text-lead` | `1.0625rem` | Section intro copy |
| `text-body` | `0.9375rem` | Default application text |
| `text-label` | `0.8125rem` uppercase | Field labels and local utility headings |
| `text-caption` | `0.75rem` uppercase | Eyebrows, badges, meta labels |

### Rhythm Rules

- Use `text-caption` for page eyebrows, metric labels, badges, and table headers.
- Use `text-label` for form labels and lightweight in-card section labels.
- Use `text-body` for default copy, row metadata, helper text, and definition-list values.
- Use `text-metric` only for KPI values in summary cards and order/settings metrics.
- Prefer semantic spacing tokens (`gutter`, `section`, `panel`, `card`, `section-lg`) or shared `ui-*` flow classes instead of one-off margin values when establishing vertical rhythm.

### Spacing

| Token | Value | Usage |
| --- | --- | --- |
| `gutter` | `1rem` | Mobile shell padding |
| `section` | `1.5rem` | Default gap between related blocks |
| `panel` | `1.75rem` | Panel padding and internal grouping |
| `card` | `2.25rem` | Hero and high-importance card padding |
| `section-lg` | `3rem` | Major section separation |
| `hero` | `4.5rem` | Large top-level spacing |

### Radii, Borders, And Shadows

| Token | Value | Usage |
| --- | --- | --- |
| `rounded-card` | `1.5rem` | Cards and internal modules |
| `rounded-panel` | `1.75rem` | Dashboard panels |
| `rounded-shell` | `2rem` | Hero and outer shells |
| `rounded-pill` | `999px` | Chips and badges |
| `border` | `1px` | Default border |
| `border-strong` | `1.5px` | Emphasized border |
| `border-heavy` | `2px` | Focused or featured border |
| `shadow-panel` | `0 20px 45px rgba(23, 33, 43, 0.08)` | Standard elevation |
| `shadow-lift` | `0 28px 68px rgba(23, 33, 43, 0.14)` | Hero or hover elevation |
| `shadow-inset-soft` | `inset 0 1px 0 rgba(255, 255, 255, 0.8)` | Soft inner highlight |

### Breakpoints

| Token | Value | Usage |
| --- | --- | --- |
| `xs` | `480px` | Small-phone landscape and dense mobile grids |
| `sm` | `640px` | Large phones / small tablets |
| `md` | `768px` | Tablet layout |
| `lg` | `1040px` | Desktop shell transition |
| `xl` | `1280px` | Standard dashboard desktop |
| `2xl` | `1440px` | Wide workspace |
| `3xl` | `1680px` | Large analyst displays |

## Reference Surface

- The shared tokens are defined in `dashboard/tailwind.config.js`.
- Validate the authenticated UI directly through the dashboard surfaces in `dashboard/src/components/`.

## Component Library

The authenticated UI now exposes a reusable Tailwind component layer from `dashboard/src/components/AuthenticatedUi.tsx`.

### Standard Components

| Component | Variants | Accessibility basics |
| --- | --- | --- |
| `Button` | `primary`, `secondary`, `ghost` | Focus ring inherited from base styles, native `button` semantics |
| `Input`, `Select`, `Textarea` | Shared field styling | Label-first usage through `Field`, focus ring, native form semantics |
| `Card` | `default`, `accent`, `teal` with `compact`, `panel`, `card` padding | Semantic headings through `CardTitle` and `CardDescription` |
| `Table` | Captioned data table wrapper | Optional screen-reader caption, semantic `thead` / `tbody` |
| `Badge` / `StatusPill` | `brand`, `teal`, `success`, `warning`, `danger`, `neutral` | Text-only status indicator with strong color contrast |
| `Alert` / `Banner` | `default`, `success`, `warning`, `error` | `role="status"` for feedback messaging |
| `Modal` | Single shared dialog shell | `role="dialog"`, `aria-modal`, labelled/described content |
| `Tabs` | Shared segmented tablist | `tablist`, `tab`, and `tabpanel` roles |
| `Tooltip` | Compact helper label | Hover plus keyboard focus support, `title` fallback |
| `EmptyState` | `default`, `muted`, `danger` | Clear heading plus descriptive body copy |
| `Skeleton` | regular and `compact` | Decorative only via `aria-hidden` |

### Authenticated Surface Adoption

- `dashboard/src/components/ReportingDashboard.tsx`
- `dashboard/src/components/OrderDetailsView.tsx`
- `dashboard/src/components/SettingsAdminView.tsx`

These three authenticated pages consume the shared component library directly rather than relying on page-specific Tailwind markup.

import { useState } from 'react';

import {
  Alert,
  Badge,
  Button,
  ButtonRow,
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
  EmptyState,
  Field,
  FieldGrid,
  Form,
  Input,
  Modal,
  Panel,
  Select,
  Skeleton,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeaderCell,
  TableRow,
  Tabs,
  TableWrap,
  Textarea,
  Tooltip
} from './components/AuthenticatedUi';

const colorSections = [
  {
    title: 'Canvas',
    description: 'Light-only structural surfaces for app chrome, panels, and inset zones.',
    tokens: [
      { name: 'canvas', value: '#f4f7f4', background: 'bg-canvas', text: 'text-ink' },
      { name: 'canvas-tint', value: '#edf5f2', background: 'bg-canvas-tint', text: 'text-ink' },
      { name: 'surface', value: '#ffffff', background: 'bg-surface', text: 'text-ink' },
      { name: 'surface-alt', value: '#fbf7f2', background: 'bg-surface-alt', text: 'text-ink' }
    ]
  },
  {
    title: 'Brand',
    description: 'Primary accents for actions, data emphasis, and directional energy.',
    tokens: [
      { name: 'brand', value: '#cb6332', background: 'bg-brand', text: 'text-white' },
      { name: 'brand-strong', value: '#a94b1f', background: 'bg-brand-strong', text: 'text-white' },
      { name: 'brand-soft', value: '#f6dfd3', background: 'bg-brand-soft', text: 'text-brand-strong' },
      { name: 'teal', value: '#1f7a74', background: 'bg-teal', text: 'text-white' }
    ]
  }
] as const;

const componentUsage = [
  'Dashboard uses buttons, inputs, selects, cards, badges, tables, alerts, empty states, and skeleton loaders.',
  'Order details uses cards, badges, tooltips, tables, empty states, and skeleton-backed loading/error states.',
  'Settings uses cards, badges, alerts, forms, tables, and empty/skeleton states for connection and user-access workflows.'
] as const;

function StyleGuidePage() {
  const [modalOpen, setModalOpen] = useState(false);

  return (
    <main className="min-h-screen bg-canvas text-ink">
      <div className="absolute inset-x-0 top-0 -z-10 h-[32rem] bg-[radial-gradient(circle_at_top_left,rgba(203,99,50,0.18),transparent_28%),radial-gradient(circle_at_top_right,rgba(31,122,116,0.18),transparent_24%),linear-gradient(180deg,#f7f9f6_0%,#f4f7f4_50%,#edf5f2_100%)]" />

      <div className="mx-auto flex min-h-screen w-full max-w-[92rem] flex-col gap-section-lg px-gutter py-card sm:px-section lg:px-section-lg">
        <section className="grid gap-panel lg:grid-cols-[minmax(0,1.6fr)_20rem]">
          <Card tone="accent" padding="card" className="shadow-lift">
            <p className="font-body text-caption uppercase tracking-[0.18em] text-teal">Tailwind Component Library</p>
            <h1 className="mt-4 max-w-[12ch] font-display text-hero leading-[0.92] tracking-tight text-ink">
              Reusable authenticated UI primitives
            </h1>
            <p className="mt-5 max-w-3xl text-lead text-ink-soft">
              This page documents the standardized Tailwind components used across authenticated reporting, order
              drill-in, and settings surfaces.
            </p>
            <ButtonRow className="mt-card">
              <Badge tone="brand">Dashboard, Order details, Settings</Badge>
              <Badge tone="teal">Accessibility basics covered</Badge>
              <Badge tone="neutral">Variants documented below</Badge>
            </ButtonRow>
          </Card>

          <Panel title="Coverage" description="Required adoption and accessibility notes for the component library.">
            <div className="grid gap-3">
              {componentUsage.map((item) => (
                <Alert key={item}>{item}</Alert>
              ))}
            </div>
            <div className="mt-4 rounded-card border border-line/60 bg-canvas-tint/80 p-4 text-body text-ink-soft">
              Focus styling is applied globally to buttons, inputs, selects, textareas, tabs, and tooltip triggers.
              Tables support screen-reader captions, and modals use `role="dialog"` with labelled content.
            </div>
          </Panel>
        </section>

        <section className="grid gap-panel">
          <div>
            <p className="text-caption uppercase tracking-[0.18em] text-teal">Palette</p>
            <h2 className="mt-3 font-display text-display tracking-tight text-ink">Foundation tokens</h2>
          </div>
          <div className="grid gap-panel">
            {colorSections.map((section) => (
              <Panel key={section.title} title={section.title} description={section.description} wide>
                <div className="grid gap-4 xs:grid-cols-2 xl:grid-cols-4">
                  {section.tokens.map((token) => (
                    <div key={token.name} className="rounded-card border border-line/70 bg-canvas-tint p-3">
                      <div className={`${token.background} ${token.text} flex h-32 items-end rounded-card p-4 shadow-inset-soft`}>
                        <span className="text-body font-semibold">{token.name}</span>
                      </div>
                      <div className="mt-4 space-y-1">
                        <p className="font-body text-body font-semibold text-ink">{token.name}</p>
                        <p className="font-mono text-caption tracking-normal text-ink-muted">{token.value}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </Panel>
            ))}
          </div>
        </section>

        <section className="grid gap-panel xl:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
          <Panel title="Buttons, Badges, Alerts" description="Primary action, secondary action, neutral actions, and semantic messaging.">
            <div className="grid gap-5">
              <div className="flex flex-wrap gap-3">
                <Button>Primary action</Button>
                <Button tone="secondary">Secondary action</Button>
                <Button tone="ghost">Ghost action</Button>
              </div>
              <div className="flex flex-wrap gap-3">
                <Badge tone="brand">Brand</Badge>
                <Badge tone="teal">Teal</Badge>
                <Badge tone="success">Success</Badge>
                <Badge tone="warning">Warning</Badge>
                <Badge tone="danger">Danger</Badge>
                <Badge tone="neutral">Neutral</Badge>
              </div>
              <div className="grid gap-3">
                <Alert tone="default" title="Default alert">
                  Use for operational hints and non-blocking feedback.
                </Alert>
                <Alert tone="success" title="Success alert">
                  Use after saves, sync actions, and completed workflows.
                </Alert>
                <Alert tone="error" title="Error alert">
                  Use for failed fetches, invalid credentials, and blocked actions.
                </Alert>
              </div>
            </div>
          </Panel>

          <Panel title="Forms" description="Standardized input, select, and textarea controls with labels and hints.">
            <Form>
              <FieldGrid>
                <Field label="Campaign name" htmlFor="style-guide-campaign" hint="Use human-readable names for marketing teams.">
                  <Input id="style-guide-campaign" type="text" placeholder="spring-sale" />
                </Field>
                <Field label="Grouping" htmlFor="style-guide-grouping">
                  <Select id="style-guide-grouping" defaultValue="day">
                    <option value="day">Daily</option>
                    <option value="source">By source</option>
                    <option value="campaign">By campaign</option>
                  </Select>
                </Field>
                <Field label="Admin note" htmlFor="style-guide-note" wide>
                  <Textarea id="style-guide-note" placeholder="Explain what changed and why." />
                </Field>
              </FieldGrid>
            </Form>
          </Panel>
        </section>

        <section className="grid gap-panel xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
          <Panel title="Cards, Tables, and States" description="The same primitives used across the three authenticated pages.">
            <div className="grid gap-4">
              <Card padding="compact">
                <CardHeader>
                  <div>
                    <CardTitle>Performance card</CardTitle>
                    <CardDescription>Metric summaries and connection cards share this surface recipe.</CardDescription>
                  </div>
                  <Badge tone="teal">Healthy</Badge>
                </CardHeader>
                <p className="font-display text-display text-ink">$42.8k</p>
                <p className="mt-2 text-body text-ink-soft">Attributed revenue in the selected range.</p>
              </Card>

              <TableWrap>
                <Table caption="Example component table">
                  <TableHead>
                    <TableRow>
                      <TableHeaderCell>Component</TableHeaderCell>
                      <TableHeaderCell>Variants</TableHeaderCell>
                      <TableHeaderCell>Used on</TableHeaderCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    <TableRow>
                      <TableCell>Button</TableCell>
                      <TableCell>Primary, secondary, ghost</TableCell>
                      <TableCell>Dashboard, Settings</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Badge</TableCell>
                      <TableCell>Brand, teal, success, warning, danger, neutral</TableCell>
                      <TableCell>Dashboard, Order details, Settings</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>EmptyState / Skeleton</TableCell>
                      <TableCell>Default, muted, danger / compact + regular</TableCell>
                      <TableCell>Dashboard, Order details, Settings</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </TableWrap>
            </div>
          </Panel>

          <div className="grid gap-panel">
            <Panel title="Empty State" description="Use when no records or payloads are available.">
              <EmptyState title="No rows matched" description="Try broadening the date range or clearing source filters." compact />
            </Panel>
            <Panel title="Skeleton Loader" description="Shown during loading states via `SectionState`.">
              <Skeleton compact lines={4} />
            </Panel>
          </div>
        </section>

        <section className="grid gap-panel lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
          <Panel title="Tabs" description="For sectioned admin flows when a full page split is too heavy.">
            <Tabs
              ariaLabel="Style guide tabs"
              items={[
                {
                  value: 'reporting',
                  label: 'Reporting',
                  panel: <Alert>Use for page-level mode switches with a small number of stable sections.</Alert>
                },
                {
                  value: 'admin',
                  label: 'Admin',
                  panel: <Alert tone="warning">Avoid tabs for long, heavily interdependent forms.</Alert>
                }
              ]}
            />
          </Panel>

          <Panel title="Tooltip and Modal" description="Supplemental primitives for compact explanations and blocking workflows.">
            <div className="flex flex-wrap items-center gap-4">
              <Tooltip content="Explain compact technical labels without adding permanent visual noise.">
                <Badge tone="neutral">Hover or focus me</Badge>
              </Tooltip>
              <Button type="button" onClick={() => setModalOpen(true)}>
                Open modal preview
              </Button>
            </div>
          </Panel>
        </section>
      </div>

      <Modal
        open={modalOpen}
        title="Modal preview"
        description="Use modals for destructive confirmation or focused multi-step input, not for routine reading."
        onClose={() => setModalOpen(false)}
        footer={
          <>
            <Button tone="ghost" type="button" onClick={() => setModalOpen(false)}>
              Cancel
            </Button>
            <Button type="button" onClick={() => setModalOpen(false)}>
              Confirm
            </Button>
          </>
        }
      >
        <Alert tone="default">This preview documents the component behavior without wiring application state.</Alert>
      </Modal>
    </main>
  );
}

export default StyleGuidePage;

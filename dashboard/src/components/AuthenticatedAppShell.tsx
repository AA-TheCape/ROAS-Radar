import { useEffect, useState, type ReactNode } from 'react';

export type AppShellNavItem = {
  key: string;
  label: string;
  shortLabel?: string;
  description: string;
};

export type AppShellBreadcrumb = {
  label: string;
  current?: boolean;
  onClick?: () => void;
};

type BreadcrumbsProps = {
  items: AppShellBreadcrumb[];
};

type PageHeaderProps = {
  eyebrow: string;
  title: string;
  description: string;
  statusPanel?: ReactNode;
  actions?: ReactNode;
};

export type AuthenticatedAppShellProps = {
  navItems: AppShellNavItem[];
  activeNavKey: string;
  onNavigate: (key: string) => void;
  breadcrumbs: AppShellBreadcrumb[];
  eyebrow: string;
  title: string;
  description: string;
  topbarMeta?: ReactNode;
  headerStatus?: ReactNode;
  headerActions?: ReactNode;
  children: ReactNode;
};

function ShellNavigation({
  navItems,
  activeNavKey,
  onNavigate
}: {
  navItems: AppShellNavItem[];
  activeNavKey: string;
  onNavigate: (key: string) => void;
}) {
  return (
    <nav className="grid gap-2" aria-label="Authenticated app navigation">
      {navItems.map((item) => {
        const active = item.key === activeNavKey;

        return (
          <button
            key={item.key}
            type="button"
            onClick={() => onNavigate(item.key)}
            className={[
              'group rounded-card border px-4 py-3 text-left transition',
              active
                ? 'border-teal/30 bg-teal text-white shadow-panel'
                : 'border-line/80 bg-surface-alt text-ink hover:-translate-y-0.5 hover:border-brand/30 hover:bg-surface hover:shadow-panel'
            ].join(' ')}
            aria-current={active ? 'page' : undefined}
          >
            <div className="flex items-center justify-between gap-3">
              <span className="font-display text-title">{item.shortLabel ?? item.label}</span>
              <span
                className={[
                  'rounded-pill px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.14em]',
                  active ? 'bg-white/16 text-white' : 'bg-brand-soft text-brand-strong'
                ].join(' ')}
              >
                {active ? 'Current' : 'Open'}
              </span>
            </div>
            <p className={['mt-2 text-body', active ? 'text-white/78' : 'text-ink-muted'].join(' ')}>
              {item.description}
            </p>
          </button>
        );
      })}
    </nav>
  );
}

export function AppShellBreadcrumbs({ items }: BreadcrumbsProps) {
  return (
    <nav aria-label="Breadcrumb" className="flex flex-wrap items-center gap-2 text-body text-ink-muted">
      {items.map((item, index) => (
        <div key={`${item.label}-${index}`} className="flex items-center gap-2">
          {item.onClick && !item.current ? (
            <button
              type="button"
              onClick={item.onClick}
              className="rounded-pill px-2 py-1 transition hover:bg-surface-alt hover:text-ink"
            >
              {item.label}
            </button>
          ) : (
            <span className={item.current ? 'font-semibold text-ink' : undefined}>{item.label}</span>
          )}
          {index < items.length - 1 ? <span aria-hidden="true">/</span> : null}
        </div>
      ))}
    </nav>
  );
}

export function AppShellPageHeader({
  eyebrow,
  title,
  description,
  statusPanel,
  actions
}: PageHeaderProps) {
  return (
    <section className="grid gap-panel lg:grid-cols-[minmax(0,1.5fr)_minmax(18rem,22rem)]">
      <article className="rounded-shell border border-line/80 bg-surface/90 p-card shadow-lift backdrop-blur">
        <p className="text-caption uppercase tracking-[0.18em] text-teal">{eyebrow}</p>
        <h1 className="mt-4 font-display text-display text-ink sm:text-hero">ROAS Radar</h1>
        <h2 className="mt-4 max-w-[16ch] font-display text-title text-ink-soft sm:text-display">{title}</h2>
        <p className="mt-5 max-w-3xl text-lead text-ink-soft">{description}</p>
        {actions ? <div className="mt-card flex flex-wrap gap-3">{actions}</div> : null}
      </article>

      <aside className="rounded-shell border border-line/80 bg-surface/90 p-panel shadow-panel backdrop-blur">
        {statusPanel}
      </aside>
    </section>
  );
}

export default function AuthenticatedAppShell({
  navItems,
  activeNavKey,
  onNavigate,
  breadcrumbs,
  eyebrow,
  title,
  description,
  topbarMeta,
  headerStatus,
  headerActions,
  children
}: AuthenticatedAppShellProps) {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  useEffect(() => {
    setMobileNavOpen(false);
  }, [activeNavKey]);

  return (
    <div className="relative min-h-screen bg-canvas text-ink">
      <div className="absolute inset-x-0 top-0 -z-10 h-[34rem] bg-[radial-gradient(circle_at_top_left,rgba(203,99,50,0.18),transparent_28%),radial-gradient(circle_at_top_right,rgba(31,122,116,0.18),transparent_24%),linear-gradient(180deg,#f7f9f6_0%,#f4f7f4_55%,#edf5f2_100%)]" />

      <header className="sticky top-0 z-40 border-b border-line/70 bg-canvas/90 backdrop-blur">
        <div className="mx-auto flex w-full max-w-[92rem] items-center justify-between gap-4 px-gutter py-4 sm:px-section lg:px-section-lg">
          <div className="flex items-center gap-3">
            <button
              type="button"
              className="inline-flex h-11 w-11 items-center justify-center rounded-card border border-line/80 bg-surface text-ink shadow-inset-soft lg:hidden"
              onClick={() => setMobileNavOpen((current) => !current)}
              aria-expanded={mobileNavOpen}
              aria-controls="app-shell-mobile-nav"
              aria-label={mobileNavOpen ? 'Close navigation menu' : 'Open navigation menu'}
            >
              <span className="text-lg font-semibold">{mobileNavOpen ? '×' : '≡'}</span>
            </button>
            <div>
              <p className="text-caption uppercase tracking-[0.18em] text-teal">Authenticated UI</p>
              <p className="font-display text-title text-ink">ROAS Radar</p>
            </div>
          </div>

          <div className="hidden items-center gap-2 lg:flex">
            {navItems.map((item) => {
              const active = item.key === activeNavKey;

              return (
                <button
                  key={item.key}
                  type="button"
                  onClick={() => onNavigate(item.key)}
                  className={[
                    'rounded-pill px-4 py-2 text-body font-semibold transition',
                    active ? 'bg-teal text-white shadow-panel' : 'text-ink-soft hover:bg-surface hover:text-ink'
                  ].join(' ')}
                  aria-current={active ? 'page' : undefined}
                >
                  {item.shortLabel ?? item.label}
                </button>
              );
            })}
          </div>

          <div className="max-w-[28rem] text-right text-body text-ink-muted">{topbarMeta}</div>
        </div>
      </header>

      {mobileNavOpen ? (
        <div className="fixed inset-0 z-30 bg-ink/20 backdrop-blur-sm lg:hidden" onClick={() => setMobileNavOpen(false)}>
          <aside
            id="app-shell-mobile-nav"
            className="h-full w-[min(22rem,calc(100vw-2rem))] border-r border-line/80 bg-canvas p-gutter shadow-lift"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="mb-6 rounded-shell border border-line/80 bg-surface p-panel shadow-panel">
              <p className="text-caption uppercase tracking-[0.18em] text-teal">Navigation</p>
              <p className="mt-3 font-display text-title text-ink">Authenticated routes</p>
              <p className="mt-2 text-body text-ink-soft">
                Switch between reporting, settings, and drill-in surfaces without relying on the old page wrapper.
              </p>
            </div>
            <ShellNavigation navItems={navItems} activeNavKey={activeNavKey} onNavigate={onNavigate} />
          </aside>
        </div>
      ) : null}

      <div className="mx-auto flex w-full max-w-[92rem] gap-section px-gutter py-gutter sm:px-section sm:py-section lg:px-section-lg lg:py-section-lg">
        <aside className="sticky top-24 hidden h-fit w-[17.5rem] shrink-0 lg:block">
          <div className="rounded-shell border border-line/80 bg-surface/92 p-panel shadow-panel backdrop-blur">
            <p className="text-caption uppercase tracking-[0.18em] text-teal">Workspace</p>
            <h2 className="mt-3 font-display text-title text-ink">Authenticated shell</h2>
            <p className="mt-3 text-body text-ink-soft">
              Shared nav, breadcrumbs, and header chrome for all post-login surfaces.
            </p>
            <div className="mt-panel">
              <ShellNavigation navItems={navItems} activeNavKey={activeNavKey} onNavigate={onNavigate} />
            </div>
          </div>
        </aside>

        <main className="min-w-0 flex-1">
          <div className="grid gap-section">
            <AppShellBreadcrumbs items={breadcrumbs} />
            <AppShellPageHeader
              eyebrow={eyebrow}
              title={title}
              description={description}
              statusPanel={headerStatus}
              actions={headerActions}
            />
            <div className="grid gap-section">{children}</div>
          </div>
        </main>
      </div>
    </div>
  );
}

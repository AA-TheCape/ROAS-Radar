import React, { useEffect, useRef, useState, type ReactNode } from 'react';

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

export type AuthenticatedAppShellProps = {
  navItems: AppShellNavItem[];
  activeNavKey: string;
  onNavigate: (key: string) => void;
  breadcrumbs: AppShellBreadcrumb[];
  topbarMeta?: ReactNode;
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

export default function AuthenticatedAppShell({
  navItems,
  activeNavKey,
  onNavigate,
  breadcrumbs,
  topbarMeta,
  headerActions,
  children
}: AuthenticatedAppShellProps) {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const previousActiveNavKeyRef = useRef(activeNavKey);

  useEffect(() => {
    if (previousActiveNavKeyRef.current === activeNavKey) {
      return;
    }

    previousActiveNavKeyRef.current = activeNavKey;
    setMobileNavOpen(false);
  }, [activeNavKey]);

  return (
    <div className="relative min-h-screen bg-canvas text-ink">
      <a
        href="#app-shell-main"
        className="absolute left-4 top-4 z-50 -translate-y-24 rounded-pill bg-ink px-4 py-2 text-body font-semibold text-white transition focus:translate-y-0 focus:outline-none focus:ring-4 focus:ring-brand/35"
      >
        Skip to main content
      </a>
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
              <p className="text-caption font-semibold uppercase tracking-[0.14em] text-teal">Authenticated UI</p>
              <p className="font-display text-title text-ink">ROAS Radar</p>
            </div>
          </div>

          <nav className="hidden items-center gap-2 lg:flex" aria-label="Primary">
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
          </nav>

          <div className="ml-auto flex items-center gap-3 sm:gap-4">
            <div className="hidden max-w-[28rem] text-right text-body text-ink-muted sm:block">{topbarMeta}</div>
            {headerActions ? <div className="flex flex-wrap items-center justify-end gap-2">{headerActions}</div> : null}
          </div>
        </div>
      </header>

      {mobileNavOpen ? (
        <div
          className="fixed inset-0 z-30 bg-ink/20 backdrop-blur-sm lg:hidden"
          onClick={() => setMobileNavOpen(false)}
          onKeyDown={(event) => {
            if (event.key === 'Escape') {
              setMobileNavOpen(false);
            }
          }}
        >
          <aside
            id="app-shell-mobile-nav"
            className="h-full w-[min(22rem,calc(100vw-2rem))] border-r border-line/80 bg-canvas p-gutter shadow-lift"
            onClick={(event) => event.stopPropagation()}
            onKeyDown={(event) => {
              if (event.key === 'Enter' || event.key === ' ') {
                event.stopPropagation();
              }
            }}
          >
            <div className="mb-6 rounded-shell border border-line/80 bg-surface p-panel shadow-panel">
              <p className="text-caption font-semibold uppercase tracking-[0.14em] text-teal">Navigation</p>
              <p className="mt-3 font-display text-title text-ink">Authenticated routes</p>
              <p className="mt-2 text-body text-ink-soft">
                Switch between reporting, settings, and drill-in surfaces without relying on the old page wrapper.
              </p>
            </div>
            <ShellNavigation navItems={navItems} activeNavKey={activeNavKey} onNavigate={onNavigate} />
          </aside>
        </div>
      ) : null}

      <main id="app-shell-main" className="mx-auto w-full max-w-[92rem] px-gutter py-gutter sm:px-section sm:py-section lg:px-section-lg lg:py-section-lg" tabIndex={-1}>
        <div className="min-w-0">
          <div className="grid gap-section">
            <AppShellBreadcrumbs items={breadcrumbs} />
            <div className="grid gap-section">{children}</div>
          </div>
        </div>
      </main>
    </div>
  );
}

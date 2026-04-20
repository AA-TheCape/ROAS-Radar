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
  },
  {
    title: 'Utility',
    description: 'Supportive states for messaging, health, and interactive feedback.',
    tokens: [
      { name: 'success', value: '#2f7d60', background: 'bg-success', text: 'text-white' },
      { name: 'success-soft', value: '#dcefe6', background: 'bg-success-soft', text: 'text-success' },
      { name: 'warning', value: '#b0711f', background: 'bg-warning', text: 'text-white' },
      { name: 'danger', value: '#b64c46', background: 'bg-danger', text: 'text-white' }
    ]
  }
] as const;

const typeSamples = [
  {
    label: 'font-display / text-hero',
    className: 'font-display text-hero leading-[0.92] tracking-tight',
    preview: 'ROAS Radar'
  },
  {
    label: 'font-display / text-display',
    className: 'font-display text-display leading-tight tracking-tight',
    preview: 'Metrics with intent'
  },
  {
    label: 'font-display / text-title',
    className: 'font-display text-title leading-tight',
    preview: 'Panel and widget titles'
  },
  {
    label: 'font-body / text-lead',
    className: 'font-body text-lead text-ink-soft',
    preview: 'Use the lead size for hero support copy and section intros.'
  },
  {
    label: 'font-body / text-body',
    className: 'font-body text-body text-ink-soft',
    preview: 'Default application text balances density and readability for dashboard-heavy layouts.'
  },
  {
    label: 'font-body / text-caption',
    className: 'font-body text-caption uppercase tracking-[0.18em] text-teal',
    preview: 'Secure dashboard'
  }
] as const;

const spacingTokens = [
  { name: 'gutter', value: '1rem', className: 'w-gutter' },
  { name: 'section', value: '1.5rem', className: 'w-section' },
  { name: 'panel', value: '1.75rem', className: 'w-panel' },
  { name: 'card', value: '2.25rem', className: 'w-card' },
  { name: 'section-lg', value: '3rem', className: 'w-section-lg' },
  { name: 'hero', value: '4.5rem', className: 'w-hero' }
] as const;

const radiusTokens = [
  { name: 'card', value: '1.5rem', className: 'rounded-card' },
  { name: 'panel', value: '1.75rem', className: 'rounded-panel' },
  { name: 'shell', value: '2rem', className: 'rounded-shell' },
  { name: 'pill', value: '999px', className: 'rounded-pill' }
] as const;

const shadowTokens = [
  { name: 'shadow-panel', value: '0 20px 45px rgba(23, 33, 43, 0.08)', className: 'shadow-panel' },
  { name: 'shadow-lift', value: '0 28px 68px rgba(23, 33, 43, 0.14)', className: 'shadow-lift' },
  { name: 'shadow-inset-soft', value: 'inset 0 1px 0 rgba(255,255,255,0.8)', className: 'shadow-inset-soft' }
];

const borderTokens = [
  { name: 'border-thin', width: '1px', className: 'border border-line/80' },
  { name: 'border-strong', width: '1.5px', className: 'border-strong border-line-strong/80' },
  { name: 'border-heavy', width: '2px', className: 'border-heavy border-brand/25' }
];

const breakpointTokens = [
  { name: 'xs', value: '480px' },
  { name: 'sm', value: '640px' },
  { name: 'md', value: '768px' },
  { name: 'lg', value: '1040px' },
  { name: 'xl', value: '1280px' },
  { name: '2xl', value: '1440px' },
  { name: '3xl', value: '1680px' }
] as const;

function StyleGuidePage() {
  return (
    <main className="min-h-screen bg-canvas text-ink">
      <div className="absolute inset-x-0 top-0 -z-10 h-[32rem] bg-[radial-gradient(circle_at_top_left,rgba(203,99,50,0.18),transparent_28%),radial-gradient(circle_at_top_right,rgba(31,122,116,0.18),transparent_24%),linear-gradient(180deg,#f7f9f6_0%,#f4f7f4_50%,#edf5f2_100%)]" />

      <div className="mx-auto flex min-h-screen w-full max-w-[92rem] flex-col gap-section-lg px-gutter py-card sm:px-section lg:px-section-lg">
        <section className="grid gap-panel lg:grid-cols-[minmax(0,1.6fr)_20rem]">
          <article className="rounded-shell border border-line/80 bg-surface/90 p-card shadow-lift backdrop-blur">
            <p className="font-body text-caption uppercase tracking-[0.18em] text-teal">Tailwind Foundations</p>
            <h1 className="mt-4 max-w-[12ch] font-display text-hero leading-[0.92] tracking-tight text-ink">
              Light-mode design system for authenticated UI surfaces
            </h1>
            <p className="mt-5 max-w-3xl text-lead text-ink-soft">
              This page previews the approved Tailwind tokens for canvas, type, spacing, radii, shadows, borders,
              and responsive breakpoints. It is available at <span className="font-mono text-caption">/#style-guide</span>.
            </p>
            <div className="mt-card flex flex-wrap gap-3">
              <span className="inline-flex rounded-pill border border-line/80 bg-brand-soft px-4 py-2 text-caption font-semibold uppercase tracking-[0.12em] text-brand-strong">
                Modern light-only
              </span>
              <span className="inline-flex rounded-pill border border-line/80 bg-teal-soft px-4 py-2 text-caption font-semibold uppercase tracking-[0.12em] text-teal-strong">
                Dashboard-first density
              </span>
              <span className="inline-flex rounded-pill border border-line/80 bg-surface-alt px-4 py-2 text-caption font-semibold uppercase tracking-[0.12em] text-ink-soft">
                React + Tailwind
              </span>
            </div>
          </article>

          <aside className="rounded-shell border border-line/80 bg-surface/90 p-panel shadow-panel backdrop-blur">
            <p className="text-caption uppercase tracking-[0.14em] text-ink-muted">Foundation summary</p>
            <dl className="mt-5 grid gap-4">
              <div className="rounded-card border border-line/70 bg-canvas-tint p-4">
                <dt className="text-caption uppercase tracking-[0.12em] text-ink-muted">Primary fonts</dt>
                <dd className="mt-2 font-display text-title text-ink">Space Grotesk / IBM Plex Sans</dd>
              </div>
              <div className="rounded-card border border-line/70 bg-canvas-tint p-4">
                <dt className="text-caption uppercase tracking-[0.12em] text-ink-muted">Panel recipe</dt>
                <dd className="mt-2 text-body text-ink-soft">`rounded-panel border border-line bg-surface shadow-panel`</dd>
              </div>
              <div className="rounded-card border border-line/70 bg-canvas-tint p-4">
                <dt className="text-caption uppercase tracking-[0.12em] text-ink-muted">Responsive range</dt>
                <dd className="mt-2 text-body text-ink-soft">xs 480px through 3xl 1680px</dd>
              </div>
            </dl>
          </aside>
        </section>

        <section className="grid gap-panel">
          <div className="max-w-3xl">
            <p className="text-caption uppercase tracking-[0.18em] text-teal">Color</p>
            <h2 className="mt-3 font-display text-display tracking-tight text-ink">Palette tokens</h2>
            <p className="mt-3 text-body text-ink-soft">
              Color pairs are tuned for bright analytic surfaces with warm brand energy and cool operational accents.
            </p>
          </div>

          <div className="grid gap-panel">
            {colorSections.map((section) => (
              <article key={section.title} className="rounded-shell border border-line/80 bg-surface p-panel shadow-panel">
                <div className="mb-panel flex flex-col gap-2 lg:flex-row lg:items-end lg:justify-between">
                  <div>
                    <h3 className="font-display text-title text-ink">{section.title}</h3>
                    <p className="mt-2 max-w-2xl text-body text-ink-soft">{section.description}</p>
                  </div>
                </div>
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
              </article>
            ))}
          </div>
        </section>

        <section className="grid gap-panel xl:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
          <article className="rounded-shell border border-line/80 bg-surface p-panel shadow-panel">
            <p className="text-caption uppercase tracking-[0.18em] text-teal">Typography</p>
            <h2 className="mt-3 font-display text-display tracking-tight text-ink">Display and body scale</h2>
            <div className="mt-panel grid gap-5">
              {typeSamples.map((sample) => (
                <div key={sample.label} className="rounded-card border border-line/70 bg-canvas-tint p-5">
                  <p className="font-mono text-caption tracking-normal text-ink-muted">{sample.label}</p>
                  <p className={`mt-3 ${sample.className}`}>{sample.preview}</p>
                </div>
              ))}
            </div>
          </article>

          <article className="rounded-shell border border-line/80 bg-surface p-panel shadow-panel">
            <p className="text-caption uppercase tracking-[0.18em] text-teal">Spacing</p>
            <h2 className="mt-3 font-display text-display tracking-tight text-ink">Semantic layout rhythm</h2>
            <div className="mt-panel space-y-4">
              {spacingTokens.map((token) => (
                <div key={token.name} className="rounded-card border border-line/70 bg-canvas-tint p-4">
                  <div className="flex items-center justify-between gap-4">
                    <div>
                      <p className="text-body font-semibold text-ink">{token.name}</p>
                      <p className="font-mono text-caption tracking-normal text-ink-muted">{token.value}</p>
                    </div>
                    <div className="flex-1">
                      <div className={`${token.className} h-4 rounded-pill bg-gradient-to-r from-teal to-brand`} />
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </article>
        </section>

        <section className="grid gap-panel lg:grid-cols-3">
          <article className="rounded-shell border border-line/80 bg-surface p-panel shadow-panel">
            <p className="text-caption uppercase tracking-[0.18em] text-teal">Radii</p>
            <h2 className="mt-3 font-display text-title text-ink">Surface curvature</h2>
            <div className="mt-panel grid gap-4">
              {radiusTokens.map((token) => (
                <div key={token.name} className="rounded-card border border-line/70 bg-canvas-tint p-4">
                  <div className={`${token.className} h-20 border border-line/80 bg-surface-alt`} />
                  <div className="mt-3">
                    <p className="text-body font-semibold text-ink">{token.name}</p>
                    <p className="font-mono text-caption tracking-normal text-ink-muted">{token.value}</p>
                  </div>
                </div>
              ))}
            </div>
          </article>

          <article className="rounded-shell border border-line/80 bg-surface p-panel shadow-panel">
            <p className="text-caption uppercase tracking-[0.18em] text-teal">Elevation</p>
            <h2 className="mt-3 font-display text-title text-ink">Shadow recipes</h2>
            <div className="mt-panel grid gap-4">
              {shadowTokens.map((token) => (
                <div key={token.name} className="rounded-card border border-line/70 bg-canvas-tint p-4">
                  <div className={`${token.className} h-20 rounded-card border border-line/70 bg-surface`} />
                  <div className="mt-3">
                    <p className="text-body font-semibold text-ink">{token.name}</p>
                    <p className="font-mono text-caption tracking-normal text-ink-muted">{token.value}</p>
                  </div>
                </div>
              ))}
            </div>
          </article>

          <article className="rounded-shell border border-line/80 bg-surface p-panel shadow-panel">
            <p className="text-caption uppercase tracking-[0.18em] text-teal">Borders</p>
            <h2 className="mt-3 font-display text-title text-ink">Stroke hierarchy</h2>
            <div className="mt-panel grid gap-4">
              {borderTokens.map((token) => (
                <div key={token.name} className="rounded-card border border-line/70 bg-canvas-tint p-4">
                  <div className={`${token.className} h-20 rounded-card bg-surface`} />
                  <div className="mt-3">
                    <p className="text-body font-semibold text-ink">{token.name}</p>
                    <p className="font-mono text-caption tracking-normal text-ink-muted">{token.width}</p>
                  </div>
                </div>
              ))}
            </div>
          </article>
        </section>

        <section className="rounded-shell border border-line/80 bg-surface p-panel shadow-panel">
          <p className="text-caption uppercase tracking-[0.18em] text-teal">Breakpoints</p>
          <h2 className="mt-3 font-display text-display tracking-tight text-ink">Responsive thresholds</h2>
          <p className="mt-3 max-w-3xl text-body text-ink-soft">
            The system keeps dense reporting layouts readable by widening progressively from mobile stacks to full
            multi-panel desktop compositions.
          </p>
          <div className="mt-panel grid gap-4 xs:grid-cols-2 lg:grid-cols-4">
            {breakpointTokens.map((token) => (
              <div key={token.name} className="rounded-card border border-line/70 bg-canvas-tint p-4">
                <p className="text-caption uppercase tracking-[0.12em] text-ink-muted">{token.name}</p>
                <p className="mt-2 font-display text-title text-ink">{token.value}</p>
              </div>
            ))}
          </div>
          <div className="mt-panel rounded-panel border border-brand/20 bg-brand-soft/60 p-5">
            <p className="text-body text-brand-strong">
              Resize the viewport to see layout density change at <span className="font-semibold">xs</span>,
              <span className="font-semibold"> sm</span>, <span className="font-semibold">lg</span>, and
              <span className="font-semibold"> xl</span> grid transitions on this page.
            </p>
          </div>
        </section>
      </div>
    </main>
  );
}

export default StyleGuidePage;

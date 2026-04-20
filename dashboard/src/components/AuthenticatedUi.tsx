import type { ComponentPropsWithoutRef, ReactNode } from 'react';

function cx(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(' ');
}

type SurfaceTone = 'default' | 'error' | 'success';
type ButtonTone = 'primary' | 'secondary' | 'ghost';

export function AuthGate({
  eyebrow,
  title,
  description,
  children
}: {
  eyebrow: string;
  title: string;
  description: string;
  children?: ReactNode;
}) {
  return (
    <main className="grid min-h-screen place-items-center px-6 py-10">
      <section className="ui-surface w-full max-w-2xl p-7 sm:p-card">
        <p className="text-caption uppercase tracking-[0.18em] text-teal">{eyebrow}</p>
        <h1 className="mt-3 font-display text-display text-ink">{title}</h1>
        <p className="mt-4 max-w-2xl text-lead text-ink-soft">{description}</p>
        {children ? <div className="mt-6 grid gap-4">{children}</div> : null}
      </section>
    </main>
  );
}

export function Panel({
  title,
  description,
  wide = false,
  className,
  children
}: {
  title: string;
  description?: string;
  wide?: boolean;
  className?: string;
  children: ReactNode;
}) {
  return (
    <article className={cx('ui-panel', wide && 'ui-panel-wide', className)}>
      <div className="ui-panel-header">
        <h2 className="ui-panel-title">{title}</h2>
        {description ? <p className="ui-panel-copy">{description}</p> : null}
      </div>
      {children}
    </article>
  );
}

export function Banner({ tone = 'default', className, children }: { tone?: SurfaceTone; className?: string; children: ReactNode }) {
  return (
    <div
      className={cx(
        'ui-banner',
        tone === 'error' && 'ui-banner-error',
        tone === 'success' && 'ui-banner-success',
        className
      )}
    >
      {children}
    </div>
  );
}

export function StateBlock({
  tone = 'default',
  compact = false,
  children
}: {
  tone?: Exclude<SurfaceTone, 'success'>;
  compact?: boolean;
  children: ReactNode;
}) {
  return <div className={cx('ui-state', compact && 'ui-state-compact', tone === 'error' && 'ui-state-error')}>{children}</div>;
}

export function SectionState({
  loading,
  error,
  empty,
  emptyLabel,
  loadingLabel = 'Loading data…',
  compact = false,
  children
}: {
  loading: boolean;
  error: string | null;
  empty: boolean;
  emptyLabel: string;
  loadingLabel?: string;
  compact?: boolean;
  children: JSX.Element;
}) {
  if (loading) {
    return <StateBlock compact={compact}>{loadingLabel}</StateBlock>;
  }

  if (error) {
    return (
      <StateBlock tone="error" compact={compact}>
        {error}
      </StateBlock>
    );
  }

  if (empty) {
    return <StateBlock compact={compact}>{emptyLabel}</StateBlock>;
  }

  return children;
}

export function ConnectionState({
  loading,
  error,
  children
}: {
  loading: boolean;
  error: string | null;
  children: JSX.Element;
}) {
  return (
    <SectionState
      loading={loading}
      error={error}
      empty={false}
      emptyLabel=""
      loadingLabel="Loading connection state…"
      compact
    >
      {children}
    </SectionState>
  );
}

export function Button({
  tone = 'primary',
  className,
  children,
  ...props
}: ComponentPropsWithoutRef<'button'> & { tone?: ButtonTone }) {
  return (
    <button
      className={cx(
        tone === 'primary' && 'ui-button-primary',
        tone === 'secondary' && 'ui-button-secondary',
        tone === 'ghost' && 'ui-button-ghost',
        className
      )}
      {...props}
    >
      {children}
    </button>
  );
}

export function ButtonRow({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={cx('ui-button-row', className)}>{children}</div>;
}

export function Form({
  className,
  children,
  ...props
}: ComponentPropsWithoutRef<'form'>) {
  return (
    <form className={cx('ui-form', className)} {...props}>
      {children}
    </form>
  );
}

export function FieldGrid({
  dense = false,
  className,
  children
}: {
  dense?: boolean;
  className?: string;
  children: ReactNode;
}) {
  return <div className={cx(dense ? 'ui-field-grid-dense' : 'ui-field-grid', className)}>{children}</div>;
}

export function Field({
  label,
  htmlFor,
  wide = false,
  children
}: {
  label: string;
  htmlFor?: string;
  wide?: boolean;
  children: ReactNode;
}) {
  return (
    <label htmlFor={htmlFor} className={cx('ui-field', wide && 'ui-field-grid-wide')}>
      <span>{label}</span>
      {children}
    </label>
  );
}

export function CheckboxField({
  label,
  htmlFor,
  children
}: {
  label: string;
  htmlFor?: string;
  children: ReactNode;
}) {
  return (
    <label htmlFor={htmlFor} className="ui-checkbox-field">
      {children}
      <span>{label}</span>
    </label>
  );
}

export function Input(props: ComponentPropsWithoutRef<'input'>) {
  return <input className={cx('ui-input', props.className)} {...props} />;
}

export function Select(props: ComponentPropsWithoutRef<'select'>) {
  return <select className={cx('ui-select', props.className)} {...props} />;
}

export function HelpText({ tone = 'default', children }: { tone?: Exclude<SurfaceTone, 'success'>; children: ReactNode }) {
  return <div className={cx('ui-help', tone === 'error' && 'ui-banner-error')}>{children}</div>;
}

export function DetailList({ className, children }: { className?: string; children: ReactNode }) {
  return <dl className={cx('ui-detail-list', className)}>{children}</dl>;
}

export function TableWrap({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={cx('ui-table-wrap', className)}>{children}</div>;
}

export function PrimaryCell({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={cx('ui-primary-cell', className)}>{children}</div>;
}

export function StatusPill({ children }: { children: ReactNode }) {
  return <span className="inline-flex min-h-[30px] items-center justify-center rounded-pill bg-teal/12 px-3 text-[0.84rem] font-semibold text-teal">{children}</span>;
}

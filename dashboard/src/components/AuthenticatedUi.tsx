import { useId, useState, type ComponentPropsWithoutRef, type ReactNode } from 'react';

function cx(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(' ');
}

type SurfaceTone = 'default' | 'error' | 'success' | 'warning';
type ButtonTone = 'primary' | 'secondary' | 'ghost';
type BadgeTone = 'default' | 'brand' | 'teal' | 'success' | 'warning' | 'danger' | 'neutral';
type CardTone = 'default' | 'accent' | 'teal';
type CardPadding = 'panel' | 'card' | 'compact';
type EmptyStateTone = 'default' | 'muted' | 'danger';

const badgeToneClasses: Record<BadgeTone, string> = {
  default: 'border-line/70 bg-surface-alt text-ink-soft',
  brand: 'border-brand/15 bg-brand-soft text-brand-strong',
  teal: 'border-teal/15 bg-teal-soft text-teal-strong',
  success: 'border-success/20 bg-success-soft text-success',
  warning: 'border-warning/20 bg-warning-soft text-warning',
  danger: 'border-danger/20 bg-danger-soft text-danger',
  neutral: 'border-line/70 bg-canvas-tint text-ink-soft'
};

const alertToneClasses: Record<SurfaceTone, string> = {
  default: 'border-teal/20 bg-teal-soft/80 text-ink-soft',
  error: 'border-danger/20 bg-danger-soft/80 text-danger',
  success: 'border-success/20 bg-success-soft/80 text-success',
  warning: 'border-warning/20 bg-warning-soft/80 text-warning'
};

const cardToneClasses: Record<CardTone, string> = {
  default: 'border-line/70 bg-surface/90',
  accent: 'border-brand/15 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(246,223,211,0.5))]',
  teal: 'border-teal/15 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(220,239,237,0.58))]'
};

const cardPaddingClasses: Record<CardPadding, string> = {
  compact: 'p-4',
  panel: 'p-panel',
  card: 'p-card'
};

const emptyStateToneClasses: Record<EmptyStateTone, string> = {
  default: 'border-line/60 bg-surface-alt/70 text-ink-muted',
  muted: 'border-line/50 bg-canvas-tint/80 text-ink-muted',
  danger: 'border-danger/20 bg-danger-soft/60 text-danger'
};

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

export function Card({
  tone = 'default',
  padding = 'panel',
  className,
  children
}: {
  tone?: CardTone;
  padding?: CardPadding;
  className?: string;
  children: ReactNode;
}) {
  return (
    <article
      className={cx(
        'relative overflow-hidden rounded-panel shadow-panel backdrop-blur',
        cardToneClasses[tone],
        cardPaddingClasses[padding],
        className
      )}
    >
      {children}
    </article>
  );
}

export function CardHeader({
  className,
  children
}: {
  className?: string;
  children: ReactNode;
}) {
  return <div className={cx('mb-4 flex flex-wrap items-start justify-between gap-3', className)}>{children}</div>;
}

export function CardTitle({ className, children }: { className?: string; children: ReactNode }) {
  return <h3 className={cx('font-display text-title text-ink', className)}>{children}</h3>;
}

export function CardDescription({
  className,
  children
}: {
  className?: string;
  children: ReactNode;
}) {
  return <p className={cx('mt-2 text-body text-ink-muted', className)}>{children}</p>;
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
    <Card className={cx(wide && 'col-[1/-1]', className)}>
      <div className="mb-4">
        <h2 className="font-display text-title text-ink">{title}</h2>
        {description ? <p className="mt-1 text-body text-ink-muted">{description}</p> : null}
      </div>
      {children}
    </Card>
  );
}

export function Badge({
  tone = 'default',
  className,
  children
}: {
  tone?: BadgeTone;
  className?: string;
  children: ReactNode;
}) {
  return (
    <span
      className={cx(
        'inline-flex min-h-[30px] items-center justify-center rounded-pill border px-3 py-1 text-[0.82rem] font-semibold',
        badgeToneClasses[tone],
        className
      )}
    >
      {children}
    </span>
  );
}

export function Alert({
  tone = 'default',
  title,
  className,
  children
}: {
  tone?: SurfaceTone;
  title?: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className={cx('rounded-card border px-4 py-3 text-body', alertToneClasses[tone], className)} role="status">
      {title ? <p className="font-semibold text-current">{title}</p> : null}
      <div className={title ? 'mt-1' : undefined}>{children}</div>
    </div>
  );
}

export function Banner({
  tone = 'default',
  className,
  children
}: {
  tone?: SurfaceTone;
  className?: string;
  children: ReactNode;
}) {
  return (
    <Alert tone={tone} className={className}>
      {children}
    </Alert>
  );
}

export function EmptyState({
  title,
  description,
  tone = 'default',
  compact = false,
  action
}: {
  title: string;
  description?: string;
  tone?: EmptyStateTone;
  compact?: boolean;
  action?: ReactNode;
}) {
  return (
    <div
      className={cx(
        'grid place-items-center rounded-card border px-6 text-center',
        emptyStateToneClasses[tone],
        compact ? 'min-h-[160px] py-8' : 'min-h-[220px] py-10'
      )}
    >
      <div className="max-w-xl">
        <p className="font-display text-title text-ink">{title}</p>
        {description ? <p className="mt-3 text-body text-current">{description}</p> : null}
        {action ? <div className="mt-5 flex justify-center">{action}</div> : null}
      </div>
    </div>
  );
}

export function Skeleton({
  lines = 3,
  compact = false,
  className
}: {
  lines?: number;
  compact?: boolean;
  className?: string;
}) {
  return (
    <div
      className={cx(
        'grid gap-3 rounded-card border border-line/60 bg-surface-alt/70 px-6',
        compact ? 'min-h-[160px] py-6' : 'min-h-[220px] py-8',
        className
      )}
      aria-hidden="true"
    >
      {Array.from({ length: lines }).map((_, index) => (
        <div
          key={index}
          className={cx(
            'h-4 animate-pulse rounded-pill bg-gradient-to-r from-canvas-tint via-surface to-canvas-tint',
            index === 0 && 'w-1/2',
            index === 1 && 'w-11/12',
            index > 1 && 'w-5/6'
          )}
        />
      ))}
    </div>
  );
}

export function SkeletonBlock({ className }: { className?: string }) {
  return <div className={cx('h-4 animate-pulse rounded-pill bg-canvas-tint', className)} aria-hidden="true" />;
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
  return (
    <EmptyState
      title={tone === 'error' ? 'There was a problem' : 'Nothing to show'}
      description={typeof children === 'string' ? children : undefined}
      tone={tone === 'error' ? 'danger' : 'muted'}
      compact={compact}
      action={typeof children === 'string' ? undefined : children}
    />
  );
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
    return <Skeleton compact={compact} lines={compact ? 3 : 5} className="content-center" />;
  }

  if (error) {
    return <EmptyState title="Unable to load data" description={error} tone="danger" compact={compact} />;
  }

  if (empty) {
    return <EmptyState title="No results returned" description={emptyLabel} tone="muted" compact={compact} />;
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

export function Tooltip({
  content,
  className,
  children
}: {
  content: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <span className={cx('group relative inline-flex', className)} tabIndex={0} title={content}>
      {children}
      <span
        role="tooltip"
        className="pointer-events-none absolute bottom-[calc(100%+0.5rem)] left-1/2 z-10 hidden w-max max-w-[16rem] -translate-x-1/2 rounded-card border border-line/70 bg-ink px-3 py-2 text-[0.78rem] text-white shadow-panel group-hover:block group-focus-visible:block"
      >
        {content}
      </span>
    </span>
  );
}

export function Modal({
  open,
  title,
  description,
  onClose,
  children,
  footer
}: {
  open: boolean;
  title: string;
  description?: string;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
}) {
  const titleId = useId();
  const descriptionId = useId();

  if (!open) {
    return null;
  }

  return (
    <div className="fixed inset-0 z-50 grid place-items-center bg-ink/30 px-4 backdrop-blur-sm" onClick={onClose}>
      <div
        className="w-full max-w-xl rounded-shell border border-line/80 bg-surface p-panel shadow-lift"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={description ? descriptionId : undefined}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-3">
          <div>
            <h2 id={titleId} className="font-display text-display text-ink">
              {title}
            </h2>
            {description ? (
              <p id={descriptionId} className="mt-2 text-body text-ink-muted">
                {description}
              </p>
            ) : null}
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex h-10 w-10 items-center justify-center rounded-pill border border-line/70 bg-surface-alt text-ink"
            aria-label="Close modal"
          >
            ×
          </button>
        </div>
        <div className="mt-5">{children}</div>
        {footer ? <div className="mt-6 flex flex-wrap justify-end gap-3">{footer}</div> : null}
      </div>
    </div>
  );
}

export function Tabs({
  items,
  defaultValue,
  ariaLabel
}: {
  items: Array<{ value: string; label: string; panel: ReactNode }>;
  defaultValue?: string;
  ariaLabel: string;
}) {
  const [value, setValue] = useState(defaultValue ?? items[0]?.value ?? '');
  const activeItem = items.find((item) => item.value === value) ?? items[0];

  return (
    <div className="grid gap-4">
      <div
        className="inline-flex max-w-full flex-wrap gap-2 rounded-card border border-line/70 bg-canvas-tint/80 p-2"
        role="tablist"
        aria-label={ariaLabel}
      >
        {items.map((item) => (
          <button
            key={item.value}
            type="button"
            role="tab"
            aria-selected={item.value === activeItem?.value}
            className={cx(
              'rounded-pill px-4 py-2 text-body font-semibold transition',
              item.value === activeItem?.value ? 'bg-teal text-white shadow-panel' : 'text-ink-soft hover:bg-surface'
            )}
            onClick={() => setValue(item.value)}
          >
            {item.label}
          </button>
        ))}
      </div>
      <div role="tabpanel">{activeItem?.panel}</div>
    </div>
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
  hint,
  wide = false,
  children
}: {
  label: string;
  htmlFor?: string;
  hint?: string;
  wide?: boolean;
  children: ReactNode;
}) {
  return (
    <label htmlFor={htmlFor} className={cx('ui-field', wide && 'ui-field-grid-wide')}>
      <span>{label}</span>
      {children}
      {hint ? <span className="text-[0.78rem] font-normal text-ink-muted">{hint}</span> : null}
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

export function Textarea(props: ComponentPropsWithoutRef<'textarea'>) {
  return <textarea className={cx('ui-textarea', props.className)} {...props} />;
}

export function HelpText({
  tone = 'default',
  children
}: {
  tone?: Exclude<SurfaceTone, 'success'>;
  children: ReactNode;
}) {
  return (
    <div className={cx('ui-help', tone === 'error' && 'border-danger/20 bg-danger-soft/70 text-danger')}>
      {children}
    </div>
  );
}

export function DetailList({ className, children }: { className?: string; children: ReactNode }) {
  return <dl className={cx('ui-detail-list', className)}>{children}</dl>;
}

export function TableWrap({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={cx('ui-table-wrap', className)}>{children}</div>;
}

export function Table({
  className,
  caption,
  children
}: {
  className?: string;
  caption?: string;
  children: ReactNode;
}) {
  return (
    <table className={cx('ui-table', className)}>
      {caption ? <caption className="sr-only">{caption}</caption> : null}
      {children}
    </table>
  );
}

export function TableHead({ children }: { children: ReactNode }) {
  return <thead>{children}</thead>;
}

export function TableBody({ children }: { children: ReactNode }) {
  return <tbody>{children}</tbody>;
}

export function TableRow({ children }: { children: ReactNode }) {
  return <tr>{children}</tr>;
}

export function TableHeaderCell({
  className,
  children
}: {
  className?: string;
  children: ReactNode;
}) {
  return <th className={className}>{children}</th>;
}

export function TableCell({
  className,
  children,
  colSpan
}: {
  className?: string;
  children: ReactNode;
  colSpan?: number;
}) {
  return (
    <td className={className} colSpan={colSpan}>
      {children}
    </td>
  );
}

export function PrimaryCell({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={cx('ui-primary-cell', className)}>{children}</div>;
}

export function StatusPill({
  tone = 'teal',
  children
}: {
  tone?: BadgeTone;
  children: ReactNode;
}) {
  return <Badge tone={tone}>{children}</Badge>;
}

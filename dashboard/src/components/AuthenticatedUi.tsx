import React, {
	cloneElement,
	isValidElement,
	useEffect,
	useId,
	useMemo,
	useRef,
	useState,
	type ComponentPropsWithoutRef,
	type KeyboardEvent,
	type ReactElement,
	type ReactNode,
} from "react";

function cx(...values: Array<string | false | null | undefined>) {
	return values.filter(Boolean).join(" ");
}

const FOCUSABLE_SELECTOR =
	'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])';

type SurfaceTone = "default" | "error" | "success" | "warning";
type ButtonTone = "primary" | "secondary" | "ghost";
type BadgeTone =
	| "default"
	| "brand"
	| "teal"
	| "success"
	| "warning"
	| "danger"
	| "neutral";
type CardTone = "default" | "accent" | "teal";
type CardPadding = "panel" | "card" | "compact";
type EmptyStateTone = "default" | "muted" | "danger";
export type SortDirection = "asc" | "desc";

const badgeToneClasses: Record<BadgeTone, string> = {
	default: "border-line/70 bg-surface-alt text-ink-soft",
	brand: "border-brand/15 bg-brand-soft text-brand-strong",
	teal: "border-teal/15 bg-teal-soft text-teal-strong",
	success: "border-success/20 bg-success-soft text-success",
	warning: "border-warning/20 bg-warning-soft text-warning",
	danger: "border-danger/20 bg-danger-soft text-danger",
	neutral: "border-line/70 bg-canvas-tint text-ink-soft",
};

const alertToneClasses: Record<SurfaceTone, string> = {
	default: "border-teal/20 bg-teal-soft/80 text-ink-soft",
	error: "border-danger/20 bg-danger-soft/80 text-danger",
	success: "border-success/20 bg-success-soft/80 text-success",
	warning: "border-warning/20 bg-warning-soft/80 text-warning",
};

const cardToneClasses: Record<CardTone, string> = {
	default: "border-line/70 bg-surface/90",
	accent:
		"border-brand/15 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(246,223,211,0.5))]",
	teal: "border-teal/15 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(220,239,237,0.58))]",
};

const cardPaddingClasses: Record<CardPadding, string> = {
	compact: "p-4",
	panel: "p-panel",
	card: "p-card",
};

const emptyStateToneClasses: Record<EmptyStateTone, string> = {
	default: "border-line/60 bg-surface-alt/70 text-ink-muted",
	muted: "border-line/50 bg-canvas-tint/80 text-ink-muted",
	danger: "border-danger/20 bg-danger-soft/60 text-danger",
};

export function AuthGate({
	eyebrow,
	title,
	description,
	children,
}: {
	eyebrow: string;
	title: string;
	description: string;
	children?: ReactNode;
}) {
	return (
		<main className="grid min-h-screen place-items-center px-6 py-10">
			<section className="w-full max-w-2xl rounded-shell border border-line/80 bg-surface/90 p-7 shadow-panel backdrop-blur sm:p-card">
				<Eyebrow className="text-teal">{eyebrow}</Eyebrow>
				<h1 className="mt-3 font-display text-display text-ink">{title}</h1>
				<p className="mt-4 max-w-2xl text-lead text-ink-soft">{description}</p>
				{children ? <div className="mt-6 grid gap-4">{children}</div> : null}
			</section>
		</main>
	);
}

export function Card({
	tone = "default",
	padding = "panel",
	className,
	children,
}: {
	tone?: CardTone;
	padding?: CardPadding;
	className?: string;
	children: ReactNode;
}) {
	return (
		<article
			className={cx(
				"relative overflow-hidden rounded-panel shadow-panel backdrop-blur",
				cardToneClasses[tone],
				cardPaddingClasses[padding],
				className,
			)}
		>
			{children}
		</article>
	);
}

export function CardHeader({
	className,
	children,
}: {
	className?: string;
	children: ReactNode;
}) {
	return (
		<div
			className={cx(
				"mb-4 flex flex-wrap items-start justify-between gap-3",
				className,
			)}
		>
			{children}
		</div>
	);
}

export function CardTitle({
	className,
	children,
}: { className?: string; children: ReactNode }) {
	return (
		<h3 className={cx("font-display text-title text-ink", className)}>
			{children}
		</h3>
	);
}

export function CardDescription({
	className,
	children,
}: {
	className?: string;
	children: ReactNode;
}) {
	return (
		<p className={cx("mt-2 text-body text-ink-muted", className)}>{children}</p>
	);
}

export function Eyebrow({
	className,
	children,
}: { className?: string; children: ReactNode }) {
	return (
		<p
			className={cx(
				"text-caption font-semibold uppercase tracking-[0.14em] text-ink-muted",
				className,
			)}
		>
			{children}
		</p>
	);
}

export function MetricValue({
	className,
	children,
}: { className?: string; children: ReactNode }) {
	return (
		<p className={cx("mt-4 font-display text-metric text-ink", className)}>
			{children}
		</p>
	);
}

export function MetricCopy({
	className,
	children,
}: { className?: string; children: ReactNode }) {
	return (
		<p className={cx("mt-3 text-body text-ink-soft", className)}>{children}</p>
	);
}

export function Panel({
	title,
	description,
	wide = false,
	className,
	children,
}: {
	title: string;
	description?: string;
	wide?: boolean;
	className?: string;
	children: ReactNode;
}) {
	return (
		<Card className={cx(wide && "col-[1/-1]", className)}>
			<div className="mb-5 grid gap-2">
				<h2 className="font-display text-title text-ink">{title}</h2>
				{description ? (
					<p className="mt-1 text-body text-ink-muted">{description}</p>
				) : null}
			</div>
			{children}
		</Card>
	);
}

export function Badge({
	tone = "default",
	className,
	children,
}: {
	tone?: BadgeTone;
	className?: string;
	children: ReactNode;
}) {
	return (
		<span
			className={cx(
				"inline-flex min-h-[30px] items-center justify-center rounded-pill border px-3 py-1 text-label uppercase",
				badgeToneClasses[tone],
				className,
			)}
		>
			{children}
		</span>
	);
}

export function Alert({
	tone = "default",
	title,
	className,
	children,
}: {
	tone?: SurfaceTone;
	title?: string;
	className?: string;
	children: ReactNode;
}) {
	return (
		<div
			className={cx(
				"rounded-card border px-4 py-3 text-body",
				alertToneClasses[tone],
				className,
			)}
			role={tone === "error" ? "alert" : "status"}
			aria-live={tone === "error" ? "assertive" : "polite"}
		>
			{title ? <p className="font-semibold text-current">{title}</p> : null}
			<div className={title ? "mt-1" : undefined}>{children}</div>
		</div>
	);
}

export function Banner({
	tone = "default",
	className,
	children,
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
	tone = "default",
	compact = false,
	action,
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
				"grid place-items-center rounded-card border px-6 text-center",
				emptyStateToneClasses[tone],
				compact ? "min-h-[160px] py-8" : "min-h-[220px] py-10",
			)}
		>
			<div className="max-w-xl">
				<p className="font-display text-title text-ink">{title}</p>
				{description ? (
					<p className="mt-3 text-body text-current">{description}</p>
				) : null}
				{action ? (
					<div className="mt-5 flex justify-center">{action}</div>
				) : null}
			</div>
		</div>
	);
}

export function Skeleton({
	lines = 3,
	compact = false,
	className,
}: {
	lines?: number;
	compact?: boolean;
	className?: string;
}) {
	return (
		<div
			className={cx(
				"grid gap-3 rounded-card border border-line/60 bg-surface-alt/70 px-6",
				compact ? "min-h-[160px] py-6" : "min-h-[220px] py-8",
				className,
			)}
			aria-hidden="true"
		>
			{Array.from(
				{ length: lines },
				(_, index) => `skeleton-line-${index}`,
			).map((lineKey, index) => (
				<div
					key={lineKey}
					className={cx(
						"h-4 animate-pulse rounded-pill bg-gradient-to-r from-canvas-tint via-surface to-canvas-tint",
						index === 0 && "w-1/2",
						index === 1 && "w-11/12",
						index > 1 && "w-5/6",
					)}
				/>
			))}
		</div>
	);
}

export function SkeletonBlock({ className }: { className?: string }) {
	return (
		<div
			className={cx("h-4 animate-pulse rounded-pill bg-canvas-tint", className)}
			aria-hidden="true"
		/>
	);
}

export function StateBlock({
	tone = "default",
	compact = false,
	children,
}: {
	tone?: Exclude<SurfaceTone, "success">;
	compact?: boolean;
	children: ReactNode;
}) {
	return (
		<EmptyState
			title={tone === "error" ? "There was a problem" : "Nothing to show"}
			description={typeof children === "string" ? children : undefined}
			tone={tone === "error" ? "danger" : "muted"}
			compact={compact}
			action={typeof children === "string" ? undefined : children}
		/>
	);
}

export function SectionState({
	loading,
	error,
	empty,
	emptyLabel,
	loadingLabel = "Loading data…",
	compact = false,
	children,
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
		return (
			<Skeleton
				compact={compact}
				lines={compact ? 3 : 5}
				className="content-center"
			/>
		);
	}

	if (error) {
		return (
			<EmptyState
				title="Unable to load data"
				description={error}
				tone="danger"
				compact={compact}
			/>
		);
	}

	if (empty) {
		return (
			<EmptyState
				title="No results returned"
				description={emptyLabel}
				tone="muted"
				compact={compact}
			/>
		);
	}

	return children;
}

export function ConnectionState({
	loading,
	error,
	children,
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
	children,
}: {
	content: string;
	className?: string;
	children: ReactNode;
}) {
	const tooltipId = useId();

	return (
		<span
			className={cx("group relative inline-flex", className)}
			aria-describedby={tooltipId}
		>
			{children}
			<span
				id={tooltipId}
				role="tooltip"
				className="pointer-events-none absolute bottom-[calc(100%+0.5rem)] left-1/2 z-10 hidden w-max max-w-[16rem] -translate-x-1/2 rounded-card border border-line/70 bg-ink px-3 py-2 text-caption text-white shadow-panel group-hover:block group-focus-visible:block"
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
	footer,
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
	const dialogRef = useRef<HTMLDialogElement | null>(null);
	const lastFocusedElementRef = useRef<HTMLElement | null>(null);

	useEffect(() => {
		if (!open || typeof document === "undefined") {
			return;
		}

		lastFocusedElementRef.current =
			document.activeElement instanceof HTMLElement
				? document.activeElement
				: null;
		const previousOverflow = document.body.style.overflow;
		document.body.style.overflow = "hidden";

		const dialog = dialogRef.current;
		const focusableElements = dialog
			? Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR))
			: [];
		const initialFocusTarget = focusableElements[0] ?? dialog;
		initialFocusTarget?.focus();

		function handleKeyDown(event: globalThis.KeyboardEvent) {
			if (!dialog) {
				return;
			}

			if (event.key === "Escape") {
				event.preventDefault();
				onClose();
				return;
			}

			if (event.key !== "Tab") {
				return;
			}

			const currentFocusableElements = Array.from(
				dialog.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR),
			);

			if (currentFocusableElements.length === 0) {
				event.preventDefault();
				dialog.focus();
				return;
			}

			const firstElement = currentFocusableElements[0];
			const lastElement =
				currentFocusableElements[currentFocusableElements.length - 1];
			const activeElement =
				document.activeElement instanceof HTMLElement
					? document.activeElement
					: null;

			if (event.shiftKey && activeElement === firstElement) {
				event.preventDefault();
				lastElement.focus();
			} else if (!event.shiftKey && activeElement === lastElement) {
				event.preventDefault();
				firstElement.focus();
			}
		}

		document.addEventListener("keydown", handleKeyDown);

		return () => {
			document.removeEventListener("keydown", handleKeyDown);
			document.body.style.overflow = previousOverflow;
			lastFocusedElementRef.current?.focus();
		};
	}, [open, onClose]);

	if (!open) {
		return null;
	}

	return (
		<dialog
			ref={dialogRef}
			className="fixed inset-0 z-50 m-0 grid h-full max-h-none w-full max-w-none place-items-center border-0 bg-ink/30 px-4 backdrop-blur-sm"
			open
			aria-labelledby={titleId}
			aria-describedby={description ? descriptionId : undefined}
			onClick={onClose}
			onKeyDown={(event) => {
				if (event.key === "Enter" || event.key === " ") {
					onClose();
				}
			}}
			onCancel={(event) => {
				event.preventDefault();
				onClose();
			}}
		>
			<div
				className="w-full max-w-xl rounded-shell border border-line/80 bg-surface p-panel shadow-lift"
				tabIndex={-1}
				onClick={(event) => event.stopPropagation()}
				onKeyDown={(event) => {
					if (event.key === "Enter" || event.key === " ") {
						event.stopPropagation();
					}
				}}
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
				{footer ? (
					<div className="mt-6 flex flex-wrap justify-end gap-3">{footer}</div>
				) : null}
			</div>
		</dialog>
	);
}

export function Tabs({
	items,
	defaultValue,
	ariaLabel,
}: {
	items: Array<{ value: string; label: string; panel: ReactNode }>;
	defaultValue?: string;
	ariaLabel: string;
}) {
	const [value, setValue] = useState(defaultValue ?? items[0]?.value ?? "");
	const activeItem = items.find((item) => item.value === value) ?? items[0];
	const tabsInstanceId = useId();
	const tabIds = useMemo(
		() =>
			items.reduce<Record<string, string>>((accumulator, item) => {
				accumulator[item.value] = `tab-${tabsInstanceId}-${item.value}`;
				return accumulator;
			}, {}),
		[items, tabsInstanceId],
	);
	const panelIds = useMemo(
		() =>
			items.reduce<Record<string, string>>((accumulator, item) => {
				accumulator[item.value] = `panel-${tabsInstanceId}-${item.value}`;
				return accumulator;
			}, {}),
		[items, tabsInstanceId],
	);

	function focusTab(nextValue: string) {
		const tabId = tabIds[nextValue];
		if (typeof document === "undefined" || !tabId) {
			return;
		}

		document.getElementById(tabId)?.focus();
	}

	function handleKeyDown(
		event: KeyboardEvent<HTMLButtonElement>,
		index: number,
	) {
		if (
			![
				"ArrowLeft",
				"ArrowRight",
				"ArrowUp",
				"ArrowDown",
				"Home",
				"End",
			].includes(event.key)
		) {
			return;
		}

		event.preventDefault();

		if (items.length === 0) {
			return;
		}

		let nextIndex = index;

		if (event.key === "Home") {
			nextIndex = 0;
		} else if (event.key === "End") {
			nextIndex = items.length - 1;
		} else {
			const delta =
				event.key === "ArrowLeft" || event.key === "ArrowUp" ? -1 : 1;
			nextIndex = (index + delta + items.length) % items.length;
		}

		const nextValue = items[nextIndex]?.value;
		if (!nextValue) {
			return;
		}

		setValue(nextValue);
		focusTab(nextValue);
	}

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
						id={tabIds[item.value]}
						type="button"
						role="tab"
						aria-selected={item.value === activeItem?.value}
						aria-controls={panelIds[item.value]}
						className={cx(
							"rounded-pill px-4 py-2 text-body font-semibold transition",
							item.value === activeItem?.value
								? "bg-teal text-white shadow-panel"
								: "text-ink-soft hover:bg-surface",
						)}
						onClick={() => setValue(item.value)}
						onKeyDown={(event) =>
							handleKeyDown(
								event,
								items.findIndex((entry) => entry.value === item.value),
							)
						}
						tabIndex={item.value === activeItem?.value ? 0 : -1}
					>
						{item.label}
					</button>
				))}
			</div>
			<div
				id={activeItem ? panelIds[activeItem.value] : undefined}
				role="tabpanel"
				aria-labelledby={activeItem ? tabIds[activeItem.value] : undefined}
			>
				{activeItem?.panel}
			</div>
		</div>
	);
}

export function Button({
	tone = "primary",
	className,
	children,
	...props
}: ComponentPropsWithoutRef<"button"> & { tone?: ButtonTone }) {
	return (
		<button
			className={cx(
				"inline-flex min-h-[42px] items-center justify-center rounded-pill px-4 py-2.5 text-body font-semibold transition duration-150 disabled:cursor-wait disabled:opacity-60",
				tone === "primary" &&
					"bg-brand text-white shadow-panel hover:-translate-y-0.5 hover:bg-brand-strong",
				tone === "secondary" &&
					"border border-teal/15 bg-teal/10 text-teal hover:-translate-y-0.5 hover:bg-teal/15",
				tone === "ghost" &&
					"border border-line/80 bg-surface-alt text-ink hover:-translate-y-0.5 hover:bg-surface",
				className,
			)}
			{...props}
		>
			{children}
		</button>
	);
}

export function ButtonRow({
	className,
	children,
}: { className?: string; children: ReactNode }) {
	return (
		<div className={cx("grid gap-3 sm:flex sm:flex-wrap", className)}>
			{children}
		</div>
	);
}

export function Form({
	className,
	children,
	...props
}: ComponentPropsWithoutRef<"form">) {
	return (
		<form className={cx("grid gap-5", className)} {...props}>
			{children}
		</form>
	);
}

export function FormSection({
	className,
	disabled = false,
	children,
	...props
}: ComponentPropsWithoutRef<"fieldset">) {
	return (
		<fieldset
			className={cx(
				"grid gap-5 disabled:cursor-wait disabled:opacity-80",
				className,
			)}
			disabled={disabled}
			{...props}
		>
			{children}
		</fieldset>
	);
}

export function FormMessage({
	tone = "default",
	title,
	children,
	className,
}: {
	tone?: SurfaceTone;
	title?: string;
	children: ReactNode;
	className?: string;
}) {
	return (
		<Alert
			tone={tone}
			title={title}
			className={cx("rounded-[18px]", className)}
		>
			{children}
		</Alert>
	);
}

export function FieldGrid({
	dense = false,
	className,
	children,
}: {
	dense?: boolean;
	className?: string;
	children: ReactNode;
}) {
	return (
		<div
			className={cx(
				dense ? "grid gap-4" : "grid gap-4 md:grid-cols-2",
				className,
			)}
		>
			{children}
		</div>
	);
}

export function Field({
	label,
	htmlFor,
	hint,
	error,
	description,
	required = false,
	optional = false,
	wide = false,
	children,
}: {
	label: string;
	htmlFor?: string;
	hint?: string;
	error?: string;
	description?: string;
	required?: boolean;
	optional?: boolean;
	wide?: boolean;
	children: ReactNode;
}) {
	const generatedId = useId();
	const controlId = htmlFor ?? generatedId;
	const descriptionId = description ? `${controlId}-description` : undefined;
	const hintId = hint ? `${controlId}-hint` : undefined;
	const errorId = error ? `${controlId}-error` : undefined;
	const describedBy =
		[descriptionId, hintId, errorId].filter(Boolean).join(" ") || undefined;
	let content = children;

	if (isValidElement(children)) {
		const child = children as ReactElement<Record<string, unknown>>;
		const childProps = child.props ?? {};
		const mergedDescribedBy =
			[childProps["aria-describedby"], describedBy].filter(Boolean).join(" ") ||
			undefined;

		content = cloneElement(child, {
			id: (childProps.id as string | undefined) ?? controlId,
			"aria-describedby": mergedDescribedBy,
			"aria-invalid": error ? true : childProps["aria-invalid"],
		});
	}

	return (
		<label
			htmlFor={controlId}
			className={cx(
				"grid gap-2 text-label font-semibold uppercase tracking-[0.08em] text-ink-soft",
				wide && "md:col-[1/-1]",
			)}
		>
			<span className="flex flex-wrap items-center gap-2">
				<span>{label}</span>
				{required ? (
					<span className="text-[0.7rem] tracking-[0.12em] text-brand">
						Required
					</span>
				) : null}
				{optional ? (
					<span className="text-[0.7rem] tracking-[0.12em] text-ink-muted">
						Optional
					</span>
				) : null}
			</span>
			{description ? (
				<span
					id={descriptionId}
					className="text-body normal-case tracking-normal text-ink-muted"
				>
					{description}
				</span>
			) : null}
			{content}
			{hint ? (
				<span
					id={hintId}
					className="text-body normal-case tracking-normal text-ink-muted"
				>
					{hint}
				</span>
			) : null}
			{error ? (
				<span
					id={errorId}
					className="text-body normal-case tracking-normal text-danger"
					role="alert"
				>
					{error}
				</span>
			) : null}
		</label>
	);
}

export function CheckboxField({
	label,
	htmlFor,
	description,
	error,
	children,
}: {
	label: string;
	htmlFor?: string;
	description?: string;
	error?: string;
	children: ReactNode;
}) {
	const generatedId = useId();
	const controlId = htmlFor ?? generatedId;
	const descriptionId = description ? `${controlId}-description` : undefined;
	const errorId = error ? `${controlId}-error` : undefined;
	let content = children;

	if (isValidElement(children)) {
		const child = children as ReactElement<Record<string, unknown>>;
		const childProps = child.props ?? {};
		const mergedDescribedBy =
			[childProps["aria-describedby"], descriptionId, errorId]
				.filter(Boolean)
				.join(" ") || undefined;

		content = cloneElement(child, {
			id: (childProps.id as string | undefined) ?? controlId,
			"aria-describedby": mergedDescribedBy,
			"aria-invalid": error ? true : childProps["aria-invalid"],
		});
	}

	return (
		<label
			htmlFor={controlId}
			className={cx(
				"flex items-start gap-3 rounded-card border border-line/60 bg-surface-alt/60 px-4 py-3 text-label font-semibold uppercase tracking-[0.08em] text-ink-soft",
				error && "border-danger/30 bg-danger-soft/40",
			)}
		>
			{content}
			<span className="grid gap-1.5">
				<span>{label}</span>
				{description ? (
					<span
						id={descriptionId}
						className="text-body normal-case tracking-normal text-ink-muted"
					>
						{description}
					</span>
				) : null}
				{error ? (
					<span
						id={errorId}
						className="text-body normal-case tracking-normal text-danger"
						role="alert"
					>
						{error}
					</span>
				) : null}
			</span>
		</label>
	);
}

export function Input({
	className,
	...props
}: ComponentPropsWithoutRef<"input">) {
	return (
		<input
			className={cx(
				"min-h-[44px] w-full rounded-[14px] border border-line-strong/45 bg-white/95 px-4 py-3 text-body font-normal text-ink shadow-inset-soft transition placeholder:text-ink-muted/80 focus:border-brand/60 focus:outline-none focus:ring-4 focus:ring-brand/10 disabled:cursor-not-allowed disabled:border-line/40 disabled:bg-surface-alt disabled:text-ink-muted aria-[invalid=true]:border-danger/45 aria-[invalid=true]:bg-danger-soft/30 aria-[invalid=true]:text-ink",
				className,
			)}
			{...props}
		/>
	);
}

export function Select({
	className,
	...props
}: ComponentPropsWithoutRef<"select">) {
	return (
		<select
			className={cx(
				"min-h-[44px] w-full rounded-[14px] border border-line-strong/45 bg-white/95 px-4 py-3 text-body font-normal text-ink shadow-inset-soft transition placeholder:text-ink-muted/80 focus:border-brand/60 focus:outline-none focus:ring-4 focus:ring-brand/10 disabled:cursor-not-allowed disabled:border-line/40 disabled:bg-surface-alt disabled:text-ink-muted aria-[invalid=true]:border-danger/45 aria-[invalid=true]:bg-danger-soft/30 aria-[invalid=true]:text-ink",
				className,
			)}
			{...props}
		/>
	);
}

export function Textarea({
	className,
	...props
}: ComponentPropsWithoutRef<"textarea">) {
	return (
		<textarea
			className={cx(
				"min-h-[120px] w-full rounded-[14px] border border-line-strong/45 bg-white/95 px-4 py-3 text-body font-normal text-ink shadow-inset-soft transition placeholder:text-ink-muted/80 focus:border-brand/60 focus:outline-none focus:ring-4 focus:ring-brand/10 disabled:cursor-not-allowed disabled:border-line/40 disabled:bg-surface-alt disabled:text-ink-muted aria-[invalid=true]:border-danger/45 aria-[invalid=true]:bg-danger-soft/30 aria-[invalid=true]:text-ink",
				className,
			)}
			{...props}
		/>
	);
}

export function HelpText({
	tone = "default",
	children,
}: {
	tone?: Exclude<SurfaceTone, "success">;
	children: ReactNode;
}) {
	return (
		<div
			className={cx(
				"rounded-card border border-line/60 bg-surface-alt/70 px-4 py-3 text-body text-ink-muted [overflow-wrap:anywhere]",
				tone === "error" && "border-danger/20 bg-danger-soft/70 text-danger",
			)}
		>
			{children}
		</div>
	);
}

export function DetailList({
	className,
	children,
}: { className?: string; children: ReactNode }) {
	return (
		<dl
			className={cx(
				"m-0 grid gap-4 md:grid-cols-2 [&>div]:min-w-0 [&>div]:rounded-card [&>div]:border [&>div]:border-line/50 [&>div]:bg-surface/80 [&>div]:p-4 [&_dd]:m-0 [&_dd]:text-body [&_dd]:font-semibold [&_dd]:text-ink [&_dd]:[overflow-wrap:anywhere] [&_dt]:mb-2 [&_dt]:text-caption [&_dt]:font-semibold [&_dt]:uppercase [&_dt]:tracking-[0.14em] [&_dt]:text-ink-muted",
				className,
			)}
		>
			{children}
		</dl>
	);
}

export function TableWrap({
	className,
	children,
}: { className?: string; children: ReactNode }) {
	return (
		<div
			className={cx(
				"min-w-0 overflow-auto overscroll-x-contain rounded-card border border-line/60 bg-surface/65 supports-[backdrop-filter]:backdrop-blur [&_thead_th]:sticky [&_thead_th]:top-0 [&_thead_th]:z-[1] [&_thead_th]:bg-surface/95",
				className,
			)}
		>
			{children}
		</div>
	);
}

export function Table({
	className,
	caption,
	children,
}: {
	className?: string;
	caption?: string;
	children: ReactNode;
}) {
	return (
		<table
			className={cx(
				"min-w-[42rem] border-collapse sm:min-w-[48rem] [&_td]:border-b [&_td]:border-line/50 [&_td]:px-4 [&_td]:py-4 [&_td]:text-left [&_td]:align-top [&_td]:text-body [&_td]:[overflow-wrap:anywhere] [&_th]:border-b [&_th]:border-line/50 [&_th]:px-4 [&_th]:py-4 [&_th]:text-left [&_th]:align-top [&_th]:text-caption [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.14em] [&_th]:text-ink-muted [&_tbody_tr:last-child_td]:border-b-0",
				className,
			)}
		>
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
	scope = "col",
	children,
}: {
	className?: string;
	scope?: ComponentPropsWithoutRef<"th">["scope"];
	children: ReactNode;
}) {
	return (
		<th className={className} scope={scope}>
			{children}
		</th>
	);
}

export function SortableTableHeaderCell({
	className,
	children,
	sorted = false,
	direction = "asc",
	onSort,
}: {
	className?: string;
	children: ReactNode;
	sorted?: boolean;
	direction?: SortDirection;
	onSort: () => void;
}) {
	return (
		<TableHeaderCell
			className={className}
			aria-sort={
				sorted ? (direction === "asc" ? "ascending" : "descending") : "none"
			}
		>
			<button
				type="button"
				onClick={onSort}
				className={cx(
					"inline-flex items-center gap-2 rounded-pill px-2 py-1 text-left transition hover:bg-canvas-tint/80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/30",
					sorted && "bg-canvas-tint text-ink",
				)}
				aria-label={`Sort by ${typeof children === "string" ? children : "column"} ${sorted && direction === "asc" ? "descending" : "ascending"}`}
			>
				<span>{children}</span>
				<span
					className={cx("text-[0.7rem] text-ink-muted", sorted && "text-brand")}
				>
					{sorted ? (direction === "asc" ? "↑" : "↓") : "↕"}
				</span>
			</button>
		</TableHeaderCell>
	);
}

export function TableCell({
	className,
	children,
	colSpan,
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

export function TableEmptyRow({
	colSpan,
	title,
	description,
}: {
	colSpan: number;
	title: string;
	description?: string;
}) {
	return (
		<TableRow>
			<TableCell colSpan={colSpan} className="px-4 py-10">
				<EmptyState
					title={title}
					description={description}
					compact
					tone="muted"
				/>
			</TableCell>
		</TableRow>
	);
}

export function PrimaryCell({
	className,
	children,
}: { className?: string; children: ReactNode }) {
	return (
		<div
			className={cx(
				"grid min-w-0 gap-1.5 [&_strong]:block [&_strong]:text-body [&_strong]:font-semibold [&_strong]:text-ink [&_strong]:[overflow-wrap:anywhere] [&_span]:text-body [&_span]:text-ink-muted [&_span]:[overflow-wrap:anywhere]",
				className,
			)}
		>
			{children}
		</div>
	);
}

export function StatusPill({
	tone = "teal",
	children,
}: {
	tone?: BadgeTone;
	children: ReactNode;
}) {
	return <Badge tone={tone}>{children}</Badge>;
}

export function DataTableToolbar({
	title,
	description,
	summary,
	children,
	actions,
	className,
}: {
	title: string;
	description?: string;
	summary?: ReactNode;
	children?: ReactNode;
	actions?: ReactNode;
	className?: string;
}) {
	return (
		<div
			className={cx(
				"grid gap-4 rounded-card border border-line/60 bg-canvas-tint/70 p-4",
				className,
			)}
		>
			<div className="flex flex-wrap items-start justify-between gap-3">
				<div className="max-w-2xl">
					<p className="text-caption font-semibold uppercase tracking-[0.14em] text-ink-muted">
						{title}
					</p>
					{description ? (
						<p className="mt-2 text-body text-ink-soft">{description}</p>
					) : null}
				</div>
				{actions ? <div className="flex flex-wrap gap-2">{actions}</div> : null}
			</div>
			{children ? (
				<div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
					{children}
				</div>
			) : null}
			{summary ? (
				<div className="grid gap-3 text-body text-ink-muted sm:flex sm:flex-wrap sm:items-center sm:justify-between">
					{summary}
				</div>
			) : null}
		</div>
	);
}

export function TableFilterBar({
	className,
	children,
}: { className?: string; children: ReactNode }) {
	return (
		<div
			className={cx(
				"grid gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(10rem,12rem)]",
				className,
			)}
		>
			{children}
		</div>
	);
}

export function TableSearchField({
	label,
	value,
	onChange,
	placeholder,
}: {
	label: string;
	value: string;
	onChange: (value: string) => void;
	placeholder: string;
}) {
	const inputId = useId();

	return (
		<Field label={label} htmlFor={inputId}>
			<Input
				id={inputId}
				type="search"
				value={value}
				placeholder={placeholder}
				onChange={(event) => onChange(event.target.value)}
			/>
		</Field>
	);
}

export function TableMeta({
	currentCount,
	totalCount,
	label,
}: {
	currentCount: number;
	totalCount: number;
	label: string;
}) {
	return (
		<span>
			Showing <strong className="text-ink">{currentCount}</strong> of{" "}
			<strong className="text-ink">{totalCount}</strong> {label}
		</span>
	);
}

export function TablePagination({
	page,
	totalPages,
	onPageChange,
}: {
	page: number;
	totalPages: number;
	onPageChange: (page: number) => void;
}) {
	if (totalPages <= 1) {
		return null;
	}

	return (
		<div className="grid gap-2 sm:flex sm:items-center">
			<Button
				type="button"
				tone="ghost"
				className="w-full sm:w-auto"
				onClick={() => onPageChange(page - 1)}
				disabled={page <= 1}
			>
				Previous
			</Button>
			<span className="min-w-[5.5rem] text-center text-body text-ink-muted">
				Page {page} / {totalPages}
			</span>
			<Button
				type="button"
				tone="ghost"
				className="w-full sm:w-auto"
				onClick={() => onPageChange(page + 1)}
				disabled={page >= totalPages}
			>
				Next
			</Button>
		</div>
	);
}

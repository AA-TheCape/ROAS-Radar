import React, { useEffect, useId, useState, type ReactNode } from 'react';

import { ResponsiveBar, type BarDatum, type BarSvgProps } from '@nivo/bar';
import { type Margin } from '@nivo/core';
import type { LegendProps } from '@nivo/legends';
import { ResponsiveLine, type LineSeries, type LineSvgProps } from '@nivo/line';
import { ResponsivePie, type DefaultRawDatum, type PieSvgProps } from '@nivo/pie';

import { EmptyState, Skeleton } from '../AuthenticatedUi';

type ChartFrameProps = {
  loading?: boolean;
  error?: string | null;
  empty?: boolean;
  emptyLabel?: string;
  height?: number;
  className?: string;
  label: string;
  description?: string;
  summary?: ReactNode;
  children: ReactNode;
};

type ChartTooltipRow = {
  label: string;
  value: string;
  color?: string;
};

type SharedChartProps = {
  loading?: boolean;
  error?: string | null;
  empty?: boolean;
  emptyLabel?: string;
  height?: number;
  className?: string;
  label: string;
  description?: string;
  summary?: ReactNode;
};

type SharedLineChartProps = SharedChartProps & {
  data: LineSeries[];
  axisBottomLegend?: ReactNode;
  axisLeftLegend?: ReactNode;
  enableArea?: boolean;
  yFormat?: (value: number) => string;
  legends?: LineSvgProps<LineSeries>['legends'];
  margin?: Partial<Margin>;
};

type SharedBarChartProps<Datum extends BarDatum> = SharedChartProps & {
  data: Datum[];
  keys: Array<Extract<keyof Datum, string>>;
  indexBy: Extract<keyof Datum, string>;
  axisBottomLegend?: ReactNode;
  axisLeftLegend?: ReactNode;
  layout?: 'horizontal' | 'vertical';
  valueFormat?: (value: number) => string;
  legends?: BarSvgProps<Datum>['legends'];
  margin?: Partial<Margin>;
};

type SharedPieDatum = DefaultRawDatum & {
  revenueLabel?: string;
  label?: string;
};

type SharedPieChartProps = SharedChartProps & {
  data: SharedPieDatum[];
  valueFormat?: (value: number) => string;
  legends?: PieSvgProps<SharedPieDatum>['legends'];
  margin?: Partial<Margin>;
};

const chartPalette = ['#cb6332', '#1f7a74', '#d8a542', '#7c8aa5', '#5c6f7b', '#b64c46'];

const sharedTheme = {
  background: 'transparent',
  text: {
    fontSize: 12,
    fill: '#314051',
    fontFamily: '"IBM Plex Sans", "Segoe UI", sans-serif'
  },
  axis: {
    domain: {
      line: {
        stroke: '#b8c5c0',
        strokeWidth: 1
      }
    },
    ticks: {
      line: {
        stroke: '#d7dfdb',
        strokeWidth: 1
      },
      text: {
        fill: '#627180',
        fontSize: 11
      }
    },
    legend: {
      text: {
        fill: '#627180',
        fontSize: 11,
        fontWeight: 600
      }
    }
  },
  grid: {
    line: {
      stroke: 'rgba(23, 33, 43, 0.08)',
      strokeWidth: 1,
      strokeDasharray: '4 6'
    }
  },
  crosshair: {
    line: {
      stroke: '#cb6332',
      strokeWidth: 1,
      strokeOpacity: 0.35,
      strokeDasharray: '3 4'
    }
  },
  tooltip: {
    container: {
      background: 'transparent',
      boxShadow: 'none',
      padding: 0
    }
  },
  legends: {
    text: {
      fill: '#314051',
      fontSize: 12
    }
  }
} satisfies LineSvgProps<LineSeries>['theme'];

const baseLineMargin: Margin = {
  top: 20,
  right: 24,
  bottom: 56,
  left: 72
};

const baseBarMargin: Margin = {
  top: 20,
  right: 24,
  bottom: 56,
  left: 72
};

const basePieMargin: Margin = {
  top: 20,
  right: 24,
  bottom: 56,
  left: 24
};

const bottomLegend: LegendProps[] = [
  {
    anchor: 'bottom',
    direction: 'row',
    justify: false,
    translateY: 48,
    itemsSpacing: 12,
    itemWidth: 80,
    itemHeight: 18,
    itemDirection: 'left-to-right',
    itemOpacity: 0.85,
    symbolSize: 10,
    symbolShape: 'circle'
  }
];

function cx(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(' ');
}

function formatMetric(value: number, formatter?: (value: number) => string) {
  return formatter ? formatter(value) : value.toLocaleString('en-US');
}

function useChartViewport() {
  const [width, setWidth] = useState<number | null>(null);

  useEffect(() => {
    function handleResize() {
      setWidth(window.innerWidth);
    }

    handleResize();
    window.addEventListener('resize', handleResize);

    return () => window.removeEventListener('resize', handleResize);
  }, []);

  return {
    isCompact: width !== null && width < 640,
    isTablet: width !== null && width >= 640 && width < 1024
  };
}

function ChartTooltip({ title, rows }: { title: string; rows: ChartTooltipRow[] }) {
  return (
    <div className="min-w-[11rem] rounded-card border border-line/80 bg-surface/95 px-4 py-3 shadow-panel backdrop-blur">
      <p className="text-label uppercase text-ink-muted">{title}</p>
      <div className="mt-3 grid gap-2">
        {rows.map((row) => (
          <div key={`${row.label}-${row.value}`} className="flex items-center justify-between gap-4 text-body text-ink-soft">
            <span className="inline-flex items-center gap-2">
              {row.color ? (
                <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: row.color }} aria-hidden="true" />
              ) : null}
              {row.label}
            </span>
            <strong className="text-ink">{row.value}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

function ChartFrame({
  loading = false,
  error = null,
  empty = false,
  emptyLabel = 'No chart data returned yet.',
  height = 320,
  className,
  label,
  description,
  summary,
  children
}: ChartFrameProps) {
  const titleId = useId();
  const descriptionId = description || summary ? `${titleId}-description` : undefined;

  if (loading) {
    return <Skeleton compact className="min-h-[220px]" />;
  }

  if (error) {
    return <EmptyState title="Unable to load chart" description={error} tone="danger" compact />;
  }

  if (empty) {
    return <EmptyState title="No chart data" description={emptyLabel} tone="muted" compact />;
  }

  return (
    <figure
      className={cx(
        'w-full rounded-card focus-visible:outline-none focus-visible:ring-4 focus-visible:ring-brand/20 focus-visible:ring-offset-2 focus-visible:ring-offset-surface',
        className
      )}
      role="group"
      aria-labelledby={titleId}
      aria-describedby={descriptionId}
      tabIndex={0}
    >
      <figcaption className="sr-only">
        <span id={titleId}>{label}</span>
        {description || summary ? (
          <span id={descriptionId}>
            {[description, typeof summary === 'string' ? summary : null].filter(Boolean).join(' ')}
          </span>
        ) : null}
      </figcaption>
      <div aria-hidden="true" style={{ height }}>
        {children}
      </div>
      {summary && typeof summary !== 'string' ? <div className="sr-only">{summary}</div> : null}
    </figure>
  );
}

export function NivoLineChart({
  data,
  loading,
  error,
  empty = data.length === 0 || data.every((series) => series.data.length === 0),
  emptyLabel,
  height = 320,
  className,
  label,
  description,
  summary,
  axisBottomLegend,
  axisLeftLegend,
  yFormat,
  legends = bottomLegend,
  margin
}: SharedLineChartProps) {
  const { isCompact, isTablet } = useChartViewport();
  const effectiveLegends = isCompact ? [] : legends;
  const effectiveHeight = isCompact ? Math.max(260, height - 36) : height;

  return (
    <ChartFrame
      loading={loading}
      error={error}
      empty={empty}
      emptyLabel={emptyLabel}
      height={effectiveHeight}
      className={className}
      label={label}
      description={description}
      summary={summary}
    >
      <ResponsiveLine
        data={data}
        theme={sharedTheme}
        colors={chartPalette}
        margin={{
          ...baseLineMargin,
          ...(isCompact ? { left: 52, right: 12, bottom: 44, top: 16 } : isTablet ? { left: 60, right: 18, bottom: 50 } : {}),
          ...margin
        }}
        curve="monotoneX"
        enablePoints
        pointSize={isCompact ? 6 : 9}
        pointBorderWidth={2}
        pointColor="#ffffff"
        pointBorderColor={{ from: 'seriesColor' }}
        enableGridX={false}
        enableArea={false}
        useMesh
        areaOpacity={0.12}
        lineWidth={3}
        axisBottom={{
          tickSize: 0,
          tickPadding: isCompact ? 10 : 14,
          legend: isCompact ? undefined : axisBottomLegend,
          legendOffset: isCompact ? 28 : 42,
          legendPosition: 'middle'
        }}
        axisLeft={{
          tickSize: 0,
          tickPadding: isCompact ? 8 : 12,
          tickValues: isCompact ? 4 : 5,
          legend: isCompact ? undefined : axisLeftLegend,
          legendOffset: isCompact ? -40 : -54,
          legendPosition: 'middle',
          format: (value: number | string) => formatMetric(Number(value), yFormat)
        }}
        legends={effectiveLegends}
        tooltip={({ point }: any) => (
          <ChartTooltip
            title={String(point.data.xFormatted ?? point.data.x)}
            rows={[
              {
                label: String(point.serieId),
                value: formatMetric(Number(point.data.y), yFormat),
                color: point.serieColor
              }
            ]}
          />
        )}
      />
    </ChartFrame>
  );
}

export function NivoAreaChart({
  legends = [],
  ...props
}: Omit<SharedLineChartProps, 'enableArea'>) {
  const { isCompact, isTablet } = useChartViewport();
  const effectiveLegends = isCompact ? [] : legends;
  const effectiveHeight = isCompact ? Math.max(260, (props.height ?? 320) - 36) : props.height;

  return (
    <ChartFrame
      loading={props.loading}
      error={props.error}
      empty={props.empty ?? (props.data.length === 0 || props.data.every((series) => series.data.length === 0))}
      emptyLabel={props.emptyLabel}
      height={effectiveHeight}
      className={props.className}
      label={props.label}
      description={props.description}
      summary={props.summary}
    >
      <ResponsiveLine
        data={props.data}
        theme={sharedTheme}
        colors={chartPalette}
        margin={{
          ...baseLineMargin,
          ...(isCompact ? { left: 52, right: 12, bottom: 44, top: 16 } : isTablet ? { left: 60, right: 18, bottom: 50 } : {}),
          ...props.margin
        }}
        curve="monotoneX"
        enablePoints
        pointSize={isCompact ? 6 : 9}
        pointBorderWidth={2}
        pointColor="#ffffff"
        pointBorderColor={{ from: 'seriesColor' }}
        enableGridX={false}
        enableArea
        areaOpacity={0.14}
        useMesh
        lineWidth={3}
        axisBottom={{
          tickSize: 0,
          tickPadding: isCompact ? 10 : 14,
          legend: isCompact ? undefined : props.axisBottomLegend,
          legendOffset: isCompact ? 28 : 42,
          legendPosition: 'middle'
        }}
        axisLeft={{
          tickSize: 0,
          tickPadding: isCompact ? 8 : 12,
          tickValues: isCompact ? 4 : 5,
          legend: isCompact ? undefined : props.axisLeftLegend,
          legendOffset: isCompact ? -40 : -54,
          legendPosition: 'middle',
          format: (value: number | string) => formatMetric(Number(value), props.yFormat)
        }}
        defs={[
          {
            id: 'areaGradient',
            type: 'linearGradient',
            colors: [
              { offset: 0, color: '#cb6332' },
              { offset: 100, color: '#cb6332', opacity: 0.08 }
            ]
          }
        ]}
        fill={[{ match: '*', id: 'areaGradient' }]}
        legends={effectiveLegends}
        tooltip={({ point }: any) => (
          <ChartTooltip
            title={String(point.data.xFormatted ?? point.data.x)}
            rows={[
              {
                label: String(point.serieId),
                value: formatMetric(Number(point.data.y), props.yFormat),
                color: point.serieColor
              }
            ]}
          />
        )}
      />
    </ChartFrame>
  );
}

export function NivoBarChart<Datum extends BarDatum>({
  data,
  keys,
  indexBy,
  loading,
  error,
  empty = data.length === 0,
  emptyLabel,
  height = 320,
  className,
  label,
  description,
  summary,
  axisBottomLegend,
  axisLeftLegend,
  layout = 'vertical',
  valueFormat,
  legends = [],
  margin
}: SharedBarChartProps<Datum>) {
  const { isCompact, isTablet } = useChartViewport();
  const effectiveLegends = isCompact ? [] : legends;
  const effectiveHeight = isCompact ? Math.max(260, height - 24) : height;

  return (
    <ChartFrame
      loading={loading}
      error={error}
      empty={empty}
      emptyLabel={emptyLabel}
      height={effectiveHeight}
      className={className}
      label={label}
      description={description}
      summary={summary}
    >
      <ResponsiveBar
        data={data}
        keys={keys}
        indexBy={indexBy}
        theme={sharedTheme}
        colors={chartPalette}
        margin={{
          ...baseBarMargin,
          ...(isCompact
            ? layout === 'horizontal'
              ? { left: 96, right: 12, bottom: 40, top: 16 }
              : { left: 52, right: 12, bottom: 44, top: 16 }
            : isTablet
              ? layout === 'horizontal'
                ? { left: 112, right: 18, bottom: 48 }
                : { left: 60, right: 18, bottom: 52 }
              : {}),
          ...margin
        }}
        padding={0.28}
        innerPadding={4}
        borderRadius={10}
        enableGridX={layout === 'horizontal'}
        enableGridY={layout !== 'horizontal'}
        layout={layout}
        labelSkipWidth={18}
        labelSkipHeight={18}
        labelTextColor="#314051"
        axisBottom={{
          tickSize: 0,
          tickPadding: isCompact ? 8 : 12,
          truncateTickAt: 0,
          legend: isCompact ? undefined : axisBottomLegend,
          legendOffset: isCompact ? 28 : 42,
          legendPosition: 'middle',
          format: (value: string | number) => String(value)
        }}
        axisLeft={{
          tickSize: 0,
          tickPadding: isCompact ? 8 : 12,
          legend: isCompact ? undefined : axisLeftLegend,
          legendOffset: isCompact ? -40 : -56,
          legendPosition: 'middle'
        }}
        valueFormat={(value: string | number) => formatMetric(Number(value), valueFormat)}
        legends={effectiveLegends}
        tooltip={({ id, indexValue, value, color, data: datum }: any) => (
          <ChartTooltip
            title={String(indexValue)}
            rows={[
              {
                label: String(id),
                value: formatMetric(Number(value), valueFormat),
                color
              },
              ...(datum.sourceMedium
                ? [{ label: 'Traffic', value: String(datum.sourceMedium) }]
                : [])
            ]}
          />
        )}
      />
    </ChartFrame>
  );
}

export function NivoPieChart({
  data,
  loading,
  error,
  empty = data.length === 0,
  emptyLabel,
  height = 320,
  className,
  label,
  description,
  summary,
  valueFormat,
  legends = bottomLegend,
  margin
}: SharedPieChartProps) {
  const { isCompact, isTablet } = useChartViewport();
  const effectiveLegends = isCompact ? [] : legends;
  const effectiveHeight = isCompact ? Math.max(260, height - 20) : height;

  return (
    <ChartFrame
      loading={loading}
      error={error}
      empty={empty}
      emptyLabel={emptyLabel}
      height={effectiveHeight}
      className={className}
      label={label}
      description={description}
      summary={summary}
    >
      <ResponsivePie
        data={data}
        theme={sharedTheme}
        colors={chartPalette}
        margin={{
          ...basePieMargin,
          ...(isCompact ? { left: 8, right: 8, bottom: 18, top: 12 } : isTablet ? { left: 16, right: 16, bottom: 42 } : {}),
          ...margin
        }}
        sortByValue
        innerRadius={isCompact ? 0.58 : 0.62}
        padAngle={0.8}
        cornerRadius={4}
        activeOuterRadiusOffset={8}
        arcLinkLabelsSkipAngle={isCompact ? 360 : 12}
        arcLinkLabelsThickness={1}
        arcLinkLabelsColor="#627180"
        arcLabelsSkipAngle={isCompact ? 360 : 12}
        arcLabelsTextColor="#17212b"
        legends={effectiveLegends}
        tooltip={({ datum }: any) => (
          <ChartTooltip
            title={String(datum.label ?? datum.id)}
            rows={[
              {
                label: 'Value',
                value: formatMetric(Number(datum.value), valueFormat),
                color: datum.color
              },
              ...(datum.data?.revenueLabel ? [{ label: 'Revenue', value: String(datum.data.revenueLabel) }] : [])
            ]}
          />
        )}
      />
    </ChartFrame>
  );
}

export { chartPalette };

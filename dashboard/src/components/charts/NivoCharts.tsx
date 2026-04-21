import React, { type ReactNode } from 'react';

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
  children
}: ChartFrameProps) {
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
    <div className={cx('w-full', className)} style={{ height }}>
      {children}
    </div>
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
  axisBottomLegend,
  axisLeftLegend,
  yFormat,
  legends = bottomLegend,
  margin
}: SharedLineChartProps) {
  return (
    <ChartFrame
      loading={loading}
      error={error}
      empty={empty}
      emptyLabel={emptyLabel}
      height={height}
      className={className}
    >
      <ResponsiveLine
        data={data}
        theme={sharedTheme}
        colors={chartPalette}
        margin={{ ...baseLineMargin, ...margin }}
        curve="monotoneX"
        enablePoints
        pointSize={9}
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
          tickPadding: 14,
          legend: axisBottomLegend,
          legendOffset: 42,
          legendPosition: 'middle'
        }}
        axisLeft={{
          tickSize: 0,
          tickPadding: 12,
          tickValues: 5,
          legend: axisLeftLegend,
          legendOffset: -54,
          legendPosition: 'middle',
          format: (value) => formatMetric(Number(value), yFormat)
        }}
        legends={legends}
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
  return (
    <ChartFrame
      loading={props.loading}
      error={props.error}
      empty={props.empty ?? (props.data.length === 0 || props.data.every((series) => series.data.length === 0))}
      emptyLabel={props.emptyLabel}
      height={props.height}
      className={props.className}
    >
      <ResponsiveLine
        data={props.data}
        theme={sharedTheme}
        colors={chartPalette}
        margin={{ ...baseLineMargin, ...props.margin }}
        curve="monotoneX"
        enablePoints
        pointSize={9}
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
          tickPadding: 14,
          legend: props.axisBottomLegend,
          legendOffset: 42,
          legendPosition: 'middle'
        }}
        axisLeft={{
          tickSize: 0,
          tickPadding: 12,
          tickValues: 5,
          legend: props.axisLeftLegend,
          legendOffset: -54,
          legendPosition: 'middle',
          format: (value) => formatMetric(Number(value), props.yFormat)
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
        legends={legends}
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
  axisBottomLegend,
  axisLeftLegend,
  layout = 'vertical',
  valueFormat,
  legends = [],
  margin
}: SharedBarChartProps<Datum>) {
  return (
    <ChartFrame
      loading={loading}
      error={error}
      empty={empty}
      emptyLabel={emptyLabel}
      height={height}
      className={className}
    >
      <ResponsiveBar
        data={data}
        keys={keys}
        indexBy={indexBy}
        theme={sharedTheme}
        colors={chartPalette}
        margin={{ ...baseBarMargin, ...margin }}
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
          tickPadding: 12,
          truncateTickAt: 0,
          legend: axisBottomLegend,
          legendOffset: 42,
          legendPosition: 'middle',
          format: (value) => String(value)
        }}
        axisLeft={{
          tickSize: 0,
          tickPadding: 12,
          legend: axisLeftLegend,
          legendOffset: -56,
          legendPosition: 'middle'
        }}
        valueFormat={(value) => formatMetric(Number(value), valueFormat)}
        legends={legends}
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
  valueFormat,
  legends = bottomLegend,
  margin
}: SharedPieChartProps) {
  return (
    <ChartFrame
      loading={loading}
      error={error}
      empty={empty}
      emptyLabel={emptyLabel}
      height={height}
      className={className}
    >
      <ResponsivePie
        data={data}
        theme={sharedTheme}
        colors={chartPalette}
        margin={{ ...basePieMargin, ...margin }}
        sortByValue
        innerRadius={0.62}
        padAngle={0.8}
        cornerRadius={4}
        activeOuterRadiusOffset={8}
        arcLinkLabelsSkipAngle={12}
        arcLinkLabelsThickness={1}
        arcLinkLabelsColor="#627180"
        arcLabelsSkipAngle={12}
        arcLabelsTextColor="#17212b"
        legends={legends}
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

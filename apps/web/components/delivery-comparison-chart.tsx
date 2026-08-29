"use client";

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  Brush,
  CartesianGrid,
  Cell,
  ComposedChart,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { TooltipContentProps } from "recharts";
import { DashboardSelect } from "@/components/dashboard-select";
import type { DeliveryInterval } from "@/lib/contracts";
import {
  formatGovernedEnergy,
  formatOperatingDateTime,
  formatOperatingTime,
  formatStatus,
} from "@/lib/format";

type ChartMode = "delivery" | "exceptions" | "evidence";
type EvidenceKind = "commitment" | "delivery" | "capacity" | "financial";
type EvidenceTone = "final" | "not-applicable" | "waiting" | "missing" | "invalid";
const HALF_HOUR_MS = 30 * 60 * 1000;

export const DELIVERY_PROFILE_INTERPOLATION = "stepAfter" as const;

export interface ChartDatum {
  interval: DeliveryInterval | null;
  intervalKey: string;
  timestamp: number;
  timeLabel: string;
  isGap: boolean;
  committed: number | null;
  delivered: number | null;
  capacity: number | null;
  exception: number | null;
  exceptionKind: "shortfall" | "excess" | "none" | "unavailable";
}

export interface DeliveryPointSeries {
  key: string;
  label: string;
  intervals: DeliveryInterval[];
}

type ActualChartDatum = ChartDatum & {
  interval: DeliveryInterval;
  isGap: false;
};

function isActualChartDatum(datum: ChartDatum): datum is ActualChartDatum {
  return datum.interval !== null && !datum.isGap;
}

function numeric(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function governedException(interval: DeliveryInterval): Pick<
  ChartDatum,
  "exception" | "exceptionKind"
> {
  const shortfall = numeric(interval.shortfall_mwh_th);
  const excess = numeric(interval.excess_mwh_th);
  if (shortfall !== null && shortfall > 0) {
    return { exception: -shortfall, exceptionKind: "shortfall" };
  }
  if (excess !== null && excess > 0) {
    return { exception: excess, exceptionKind: "excess" };
  }
  if (shortfall !== null && excess !== null) {
    return { exception: 0, exceptionKind: "none" };
  }
  return { exception: null, exceptionKind: "unavailable" };
}

export function groupDeliveryPointSeries(
  intervals: DeliveryInterval[],
): DeliveryPointSeries[] {
  const grouped = new Map<string, DeliveryPointSeries>();
  for (const interval of intervals) {
    const key = [
      interval.customer_id,
      interval.site_id,
      interval.delivery_point_id,
    ].join("::");
    const current = grouped.get(key) ?? {
      key,
      label: `${interval.customer_name} · ${interval.site_name} · ${interval.delivery_point_name}`,
      intervals: [],
    };
    current.intervals.push(interval);
    grouped.set(key, current);
  }
  return Array.from(grouped.values())
    .map((series) => ({
      ...series,
      intervals: [...series.intervals].sort((left, right) =>
        left.interval_start_at.localeCompare(right.interval_start_at),
      ),
    }))
    .sort((left, right) => left.label.localeCompare(right.label));
}

function evidenceTone(kind: EvidenceKind, status: string): EvidenceTone {
  const normalized = status.toLowerCase();
  if (kind === "commitment") {
    if (normalized === "committed") return "final";
    if (normalized === "no_commitment") return "not-applicable";
    if (["missing", "withdrawn"].includes(normalized)) return "missing";
    return "invalid";
  }
  if (kind === "delivery") {
    if (normalized === "accepted") return "final";
    if (["meter_assignment_missing", "boundary_missing", "boundary_withdrawn"].includes(normalized)) {
      return "missing";
    }
    return "invalid";
  }
  if (kind === "capacity") {
    if (normalized === "final") return "final";
    if (normalized === "provisional") return "waiting";
    if (["missing", "withdrawn"].includes(normalized)) return "missing";
    return "invalid";
  }
  if (normalized === "final") return "final";
  if (normalized === "not_applicable") return "not-applicable";
  if (normalized === "provisional") return "waiting";
  if (normalized === "no_data") return "missing";
  return "invalid";
}

export function buildDeliveryChartData(intervals: DeliveryInterval[]): ChartDatum[] {
  const data: ChartDatum[] = [];
  let previous: DeliveryInterval | null = null;
  for (const interval of intervals) {
    const timestamp = Date.parse(interval.interval_start_at);
    if (previous) {
      const previousTimestamp = Date.parse(previous.interval_start_at);
      for (
        let gapTimestamp = previousTimestamp + HALF_HOUR_MS;
        gapTimestamp < timestamp;
        gapTimestamp += HALF_HOUR_MS
      ) {
        const gapDateTime = new Date(gapTimestamp).toISOString();
        data.push({
          interval: null,
          intervalKey: `gap-${gapDateTime}`,
          timestamp: gapTimestamp,
          timeLabel: formatOperatingTime(gapDateTime),
          isGap: true,
          committed: null,
          delivered: null,
          capacity: null,
          exception: null,
          exceptionKind: "unavailable",
        });
      }
    }
    const firstVisibleIntervalOfDay = previous?.reporting_date !== interval.reporting_date;
    const reportingDate = new Date(`${interval.reporting_date}T00:00:00Z`).toLocaleDateString(
      "en-GB",
      { day: "2-digit", month: "short", timeZone: "UTC" },
    );
    data.push({
      interval,
      intervalKey: interval.interval_key,
      timestamp,
      timeLabel: firstVisibleIntervalOfDay
        ? `${reportingDate} · ${formatOperatingTime(interval.interval_start_at)}`
        : formatOperatingTime(interval.interval_start_at),
      isGap: false,
      committed: numeric(interval.committed_mwh_th),
      delivered: numeric(interval.delivered_mwh_th),
      capacity: numeric(interval.deliverable_capacity_mwh_th),
      ...governedException(interval),
    });
    previous = interval;
  }
  return data;
}

function axisEnergy(value: number): string {
  return new Intl.NumberFormat("en-GB", {
    maximumFractionDigits: 2,
  }).format(value);
}

function ChartTooltip({ active, payload }: TooltipContentProps) {
  if (!active || payload.length === 0) return null;
  const datum = payload[0]?.payload as ChartDatum | undefined;
  if (!datum) return null;
  const interval = datum.interval;
  if (!interval) return null;

  return (
    <div className="analysis-tooltip" role="status" aria-live="polite">
      <p className="analysis-tooltip__time">
        {formatOperatingDateTime(interval.interval_start_at)} · Period {interval.local_period_number}
      </p>
      <p className="analysis-tooltip__identity">
        {interval.site_name} · {interval.delivery_point_name}
      </p>
      <dl>
        <div><dt>Committed</dt><dd>{formatGovernedEnergy(interval.committed_mwh_th)}</dd></div>
        <div><dt>Delivered</dt><dd>{formatGovernedEnergy(interval.delivered_mwh_th)}</dd></div>
        <div><dt>Capacity</dt><dd>{formatGovernedEnergy(interval.deliverable_capacity_mwh_th)}</dd></div>
        <div><dt>Shortfall</dt><dd>{formatGovernedEnergy(interval.shortfall_mwh_th)}</dd></div>
        <div><dt>Excess</dt><dd>{formatGovernedEnergy(interval.excess_mwh_th)}</dd></div>
      </dl>
      <p className="analysis-tooltip__status">
        Commitment {formatStatus(interval.commitment_status)} · Delivery {formatStatus(interval.delivery_measurement_status)} · Capacity {formatStatus(interval.capacity_status)}
      </p>
    </div>
  );
}

function EvidenceTracks({ data }: { data: ActualChartDatum[] }) {
  const rows = [
    {
      label: "Commitment",
      kind: "commitment" as const,
      status: (interval: DeliveryInterval) => interval.commitment_status,
    },
    {
      label: "Delivery",
      kind: "delivery" as const,
      status: (interval: DeliveryInterval) => interval.delivery_measurement_status,
    },
    {
      label: "Capacity",
      kind: "capacity" as const,
      status: (interval: DeliveryInterval) => interval.capacity_status,
    },
    {
      label: "Financial result",
      kind: "financial" as const,
      status: (interval: DeliveryInterval) => interval.financial_result_status,
    },
  ];
  const width = Math.max(760, data.length * 34);

  return (
    <div
      className="evidence-chart__scroll"
      role="region"
      aria-label="Evidence status by interval; scroll horizontally for more intervals"
      tabIndex={0}
    >
      <div className="evidence-chart" style={{ width }}>
        <div className="evidence-chart__corner">Evidence</div>
        <div className="evidence-chart__times" aria-hidden="true">
          {data.map((datum) => (
            <span key={datum.intervalKey}>{datum.timeLabel}</span>
          ))}
        </div>
        {rows.map((row) => (
          <div className="evidence-track" key={row.label}>
            <strong>{row.label}</strong>
            <div className="evidence-track__cells">
              {data.map((datum) => {
                const status = row.status(datum.interval);
                const description = `${row.label}, ${formatOperatingDateTime(
                  datum.interval.interval_start_at,
                )}: ${formatStatus(status)}`;
                return (
                  <span
                    key={datum.intervalKey}
                    className={`evidence-cell evidence-cell--${evidenceTone(row.kind, status)}`}
                    aria-label={description}
                    title={description}
                    role="img"
                  />
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function ChartLegend({ mode }: { mode: ChartMode }) {
  const entries = mode === "delivery"
    ? [
        ["commitment", "Committed"],
        ["delivery", "Delivered"],
        ["capacity", "Final capacity"],
      ]
    : mode === "exceptions"
      ? [
          ["shortfall", "Shortfall"],
          ["excess", "Excess"],
          ["zero", "Recorded zero"],
          ["unavailable", "Unavailable"],
        ]
      : [
          ["final", "Final / accepted"],
          ["not-applicable", "Not applicable"],
          ["waiting", "Waiting for data"],
          ["missing", "Missing / withdrawn"],
          ["invalid", "Rejected evidence"],
        ];
  return (
    <ul className="analysis-legend" aria-label="Chart legend">
      {entries.map(([tone, label]) => (
        <li key={tone}>
          <span className={`analysis-legend__mark analysis-legend__mark--${tone}`} />
          {label}
        </li>
      ))}
    </ul>
  );
}

export function DeliveryComparisonChart({
  intervals,
  total,
}: {
  intervals: DeliveryInterval[];
  total: number;
}) {
  const series = useMemo(() => groupDeliveryPointSeries(intervals), [intervals]);
  const [selectedSeriesKey, setSelectedSeriesKey] = useState(series[0]?.key ?? "");
  const [mode, setMode] = useState<ChartMode>("delivery");
  const selectedSeries = series.find((candidate) => candidate.key === selectedSeriesKey) ?? series[0];
  const data = useMemo(
    () => buildDeliveryChartData(selectedSeries?.intervals ?? []),
    [selectedSeries],
  );
  const actualData = data.filter(isActualChartDatum);
  const shortfallCount = actualData.filter((datum) => datum.exceptionKind === "shortfall").length;
  const excessCount = actualData.filter((datum) => datum.exceptionKind === "excess").length;
  const waitingCount = actualData.filter((datum) =>
    [
      evidenceTone("commitment", datum.interval.commitment_status),
      evidenceTone("delivery", datum.interval.delivery_measurement_status),
      evidenceTone("capacity", datum.interval.capacity_status),
      evidenceTone("financial", datum.interval.financial_result_status),
    ].some((tone) => !["final", "not-applicable"].includes(tone)),
  ).length;
  const unavailableExceptionMarkers = actualData
    .filter((datum) => datum.exceptionKind === "unavailable")
    .map((datum) => ({ ...datum, exception: 0 }));
  const zeroExceptionMarkers = actualData
    .filter((datum) => datum.exceptionKind === "none")
    .map((datum) => ({ ...datum, exception: 0 }));

  if (!selectedSeries) return null;

  return (
    <section className="comparison-panel" aria-labelledby="comparison-heading">
      <div className="analysis-chart__header">
        <div>
          <p className="section-kicker">Interval analysis</p>
          <h2 id="comparison-heading">Delivery performance over time</h2>
          <p className="comparison-panel__description">
            Explore the full bounded chart scope independently of the paginated
            interval register below. Gaps mean evidence is unavailable; they are
            never drawn as zero.
          </p>
        </div>
        <div className="analysis-chart__series-picker">
          <label htmlFor="chart-delivery-point">Delivery point</label>
          <DashboardSelect
            id="chart-delivery-point"
            value={selectedSeries.key}
            groups={[{
              options: series.map((candidate) => ({
                value: candidate.key,
                label: candidate.label,
              })),
            }]}
            onValueChange={setSelectedSeriesKey}
          />
        </div>
      </div>

      <div className="analysis-chart__toolbar">
        <div className="analysis-tabs" role="group" aria-label="Chart view">
          {([
            ["delivery", "Delivery"],
            ["exceptions", "Exceptions"],
            ["evidence", "Evidence"],
          ] as const).map(([value, label]) => (
            <button
              key={value}
              type="button"
              aria-pressed={mode === value}
              aria-controls="delivery-analysis-chart"
              onClick={() => setMode(value)}
            >
              {label}
            </button>
          ))}
        </div>
        <ChartLegend mode={mode} />
      </div>

      <dl className="analysis-chart__signals">
        <div><dt>Intervals shown</dt><dd>{actualData.length}</dd></div>
        <div><dt>With shortfall</dt><dd>{shortfallCount}</dd></div>
        <div><dt>With excess</dt><dd>{excessCount}</dd></div>
        <div><dt>Waiting for evidence</dt><dd>{waitingCount}</dd></div>
      </dl>

      {total > intervals.length ? (
        <p className="analysis-chart__coverage" role="status">
          The chart request loaded the first {intervals.length} of {total}{" "}
          matching intervals. This delivery-point series may therefore be
          incomplete. Narrow the filters to inspect every interval in one
          continuous view.
        </p>
      ) : null}

      <div
        id="delivery-analysis-chart"
        role="region"
        aria-label={`${mode} chart for ${selectedSeries.label}`}
        className="analysis-chart__body"
      >
        {mode === "delivery" ? (
          <ResponsiveContainer width="100%" height={390}>
            <ComposedChart
              data={data}
              accessibilityLayer
              role="img"
              title={`Committed, delivered and final capacity for ${selectedSeries.label}`}
              desc="Thirty-minute energy profile. Missing values break the line instead of being shown as zero."
              margin={{ top: 18, right: 22, bottom: 12, left: 6 }}
            >
              <CartesianGrid stroke="#d8ddd8" strokeDasharray="3 6" vertical={false} />
              <XAxis
                dataKey="timeLabel"
                minTickGap={28}
                tick={{ fill: "#435049", fontSize: 11 }}
                tickLine={false}
                axisLine={{ stroke: "#9aa39e" }}
              />
              <YAxis
                width={54}
                tickFormatter={axisEnergy}
                tick={{ fill: "#435049", fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                label={{ value: "MWhₜₕ", angle: -90, position: "insideLeft", fill: "#435049", fontSize: 11 }}
              />
              <Tooltip content={ChartTooltip} cursor={{ stroke: "#2b826b", strokeWidth: 1 }} />
              <Line
                type={DELIVERY_PROFILE_INTERPOLATION}
                dataKey="committed"
                name="Committed"
                stroke="#606c65"
                strokeWidth={2}
                strokeDasharray="6 4"
                dot={{ r: 2, fill: "#fbfaf5", strokeWidth: 2 }}
                activeDot={{ r: 5 }}
                connectNulls={false}
                isAnimationActive={false}
              />
              <Line
                type={DELIVERY_PROFILE_INTERPOLATION}
                dataKey="delivered"
                name="Delivered"
                stroke="#17221d"
                strokeWidth={3}
                dot={{ r: 2.5, fill: "#d9f45f", stroke: "#17221d", strokeWidth: 1.5 }}
                activeDot={{ r: 5, fill: "#d9f45f", stroke: "#17221d" }}
                connectNulls={false}
                isAnimationActive={false}
              />
              <Line
                type="stepAfter"
                dataKey="capacity"
                name="Final capacity"
                stroke="#2b826b"
                strokeWidth={2}
                strokeDasharray="2 5"
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
              {data.length > 18 ? (
                <Brush
                  dataKey="timeLabel"
                  ariaLabel="Choose visible delivery time range"
                  height={44}
                  travellerWidth={44}
                  stroke="#2b826b"
                  fill="#f0ede3"
                />
              ) : null}
            </ComposedChart>
          </ResponsiveContainer>
        ) : mode === "exceptions" ? (
          <ResponsiveContainer width="100%" height={390}>
            <BarChart
              data={data}
              accessibilityLayer
              role="img"
              title={`Governed shortfall and excess for ${selectedSeries.label}`}
              desc="Excess appears above zero, shortfall below zero, real zero as an outlined circle, and unavailable evidence as a diamond."
              margin={{ top: 18, right: 22, bottom: 12, left: 6 }}
            >
              <CartesianGrid stroke="#d8ddd8" strokeDasharray="3 6" vertical={false} />
              <XAxis
                dataKey="timeLabel"
                minTickGap={28}
                tick={{ fill: "#435049", fontSize: 11 }}
                tickLine={false}
                axisLine={{ stroke: "#9aa39e" }}
              />
              <YAxis
                width={54}
                domain={([dataMinimum, dataMaximum]) => {
                  const bound = Math.max(
                    Math.abs(Number(dataMinimum)),
                    Math.abs(Number(dataMaximum)),
                  );
                  return bound === 0 ? [-1, 1] : [-bound, bound];
                }}
                tickFormatter={(value) => axisEnergy(Math.abs(value))}
                tick={{ fill: "#435049", fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                label={{ value: "MWhₜₕ", angle: -90, position: "insideLeft", fill: "#435049", fontSize: 11 }}
              />
              <ReferenceLine y={0} stroke="#17221d" strokeWidth={1.5} />
              <Tooltip content={ChartTooltip} cursor={{ fill: "rgb(43 130 107 / 8%)" }} />
              <Bar dataKey="exception" name="Governed exception" maxBarSize={28} isAnimationActive={false}>
                {data.map((datum) => (
                  <Cell
                    key={datum.intervalKey}
                    fill={datum.exceptionKind === "shortfall" ? "#b9382e" : datum.exceptionKind === "excess" ? "#567111" : "#aeb7b1"}
                    stroke={datum.exceptionKind === "unavailable" ? "#606c65" : "none"}
                    strokeDasharray={datum.exceptionKind === "unavailable" ? "3 2" : undefined}
                  />
                ))}
              </Bar>
              <Scatter
                name="Recorded zero"
                data={zeroExceptionMarkers}
                dataKey="exception"
                fill="#f7f7f2"
                stroke="#17221d"
                strokeWidth={2}
                shape="circle"
                isAnimationActive={false}
              />
              <Scatter
                name="Unavailable"
                data={unavailableExceptionMarkers}
                dataKey="exception"
                fill="#f5e7c9"
                stroke="#7a5212"
                strokeWidth={2}
                shape="diamond"
                isAnimationActive={false}
              />
              {data.length > 18 ? (
                <Brush
                  dataKey="timeLabel"
                  ariaLabel="Choose visible exception time range"
                  height={44}
                  travellerWidth={44}
                  stroke="#2b826b"
                  fill="#f0ede3"
                />
              ) : null}
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <EvidenceTracks data={actualData} />
        )}
      </div>

      <div className="sr-only" role="list" aria-label="Exact interval chart values">
        {actualData.map((datum) => (
          <p role="listitem" key={datum.intervalKey}>
            {formatOperatingDateTime(datum.interval.interval_start_at)}.
            Committed {formatGovernedEnergy(datum.interval.committed_mwh_th)}.
            Delivered {formatGovernedEnergy(datum.interval.delivered_mwh_th)}.
            Capacity {formatGovernedEnergy(datum.interval.deliverable_capacity_mwh_th)}.
            Shortfall {formatGovernedEnergy(datum.interval.shortfall_mwh_th)}.
            Excess {formatGovernedEnergy(datum.interval.excess_mwh_th)}.
          </p>
        ))}
      </div>
      <p className="chart-note">
        Hover, tap, or use the chart keyboard controls to inspect exact governed
        values. The navigator beneath longer series lets you focus on a smaller
        time window without changing the filters.
      </p>
    </section>
  );
}

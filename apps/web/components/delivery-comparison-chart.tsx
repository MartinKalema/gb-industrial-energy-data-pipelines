import type { CSSProperties } from "react";
import type { DeliveryInterval } from "@/lib/contracts";
import {
  formatGovernedEnergy,
  formatOperatingDateTime,
  formatOperatingTime,
} from "@/lib/format";

interface ChartValue {
  raw: string | null;
  numeric: number | null;
}

function chartValue(value: string | null): ChartValue {
  if (value === null) return { raw: null, numeric: null };
  const numeric = Number(value);
  return Number.isFinite(numeric) ? { raw: value, numeric } : { raw: null, numeric: null };
}

export function deliveryChartScale(intervals: DeliveryInterval[]): number | null {
  return deliveryChartMaximum(intervals).numeric;
}

function deliveryChartMaximum(intervals: DeliveryInterval[]): ChartValue {
  const available = intervals
    .flatMap((interval) => [
      chartValue(interval.committed_mwh_th),
      chartValue(interval.delivered_mwh_th),
    ])
    .filter((value): value is { raw: string; numeric: number } =>
      value.raw !== null && value.numeric !== null,
    );
  if (available.length === 0) return { raw: null, numeric: null };
  const maximum = available.reduce((current, candidate) =>
    candidate.numeric > current.numeric ? candidate : current,
  );
  return maximum.numeric < 0 ? { raw: "0", numeric: 0 } : maximum;
}

function barHeight(value: number | null, pageMaximum: number | null): string {
  if (value === null || pageMaximum === null || pageMaximum === 0) return "0%";
  return `${Math.min(100, Math.max(0, (value / pageMaximum) * 100))}%`;
}

function ComparisonBar({
  kind,
  value,
  pageMaximum,
}: {
  kind: "Committed" | "Delivered";
  value: ChartValue;
  pageMaximum: number | null;
}) {
  const unavailable = value.numeric === null;
  const label = `${kind}: ${formatGovernedEnergy(value.raw)}`;
  return (
    <span className="comparison-bar__slot">
      <span
        aria-hidden="true"
        className={`comparison-bar comparison-bar--${kind.toLowerCase()}${
          unavailable ? " comparison-bar--unavailable" : ""
        }`}
        style={
          { "--bar-height": barHeight(value.numeric, pageMaximum) } as CSSProperties
        }
        title={label}
      >
        {unavailable ? <span className="comparison-bar__na">N/A</span> : null}
      </span>
      <span className="sr-only">{label}</span>
    </span>
  );
}

export function DeliveryComparisonChart({
  intervals,
}: {
  intervals: DeliveryInterval[];
}) {
  const maximum = deliveryChartMaximum(intervals);
  const pageMaximum = maximum.numeric;
  const scaleMaximum = formatGovernedEnergy(maximum.raw);
  const chartWidth = Math.max(780, intervals.length * 78);

  return (
    <section className="comparison-panel" aria-labelledby="comparison-heading">
      <div className="section-heading-row comparison-panel__heading">
        <div>
          <p className="section-kicker">Energy comparison</p>
          <h2 id="comparison-heading">Committed versus delivered steam</h2>
        </div>
        <ul className="comparison-legend" aria-label="Energy series legend">
          <li><span className="comparison-swatch comparison-swatch--committed" />Committed</li>
          <li><span className="comparison-swatch comparison-swatch--delivered" />Delivered</li>
          <li><span className="comparison-swatch comparison-swatch--unavailable" />Unavailable</li>
        </ul>
      </div>
      <p id="comparison-description" className="comparison-panel__description">
        Each group is one visible 30-minute interval. Bar heights use a zero to
        page-maximum scale for visual comparison only. Exact labels and
        descriptions preserve the governed decimal values returned by the API.
      </p>

      <div className="comparison-chart">
        <div className="comparison-axis" aria-hidden="true">
          <span>{scaleMaximum}</span>
          <span>0 MWhₜₕ</span>
        </div>
        <div
          className="comparison-chart__scroll"
          role="region"
          aria-label="Committed and delivered energy chart; scroll horizontally for more intervals"
          aria-describedby="comparison-description"
          tabIndex={0}
        >
          <div
            className="comparison-chart__plot"
            role="list"
            aria-label="Visible delivery intervals"
            style={{ "--chart-width": `${chartWidth}px` } as CSSProperties}
          >
            <span className="comparison-gridline comparison-gridline--maximum" aria-hidden="true" />
            <span className="comparison-gridline comparison-gridline--middle" aria-hidden="true" />
            <span className="comparison-gridline comparison-gridline--zero" aria-hidden="true" />
            {intervals.map((interval) => {
              const committed = chartValue(interval.committed_mwh_th);
              const delivered = chartValue(interval.delivered_mwh_th);
              const operatingTime = formatOperatingDateTime(interval.interval_start_at);
              const identity = `${interval.customer_name} (${interval.customer_id}), ${interval.site_name} (${interval.site_id}), ${interval.delivery_point_name} (${interval.delivery_point_id})`;
              const description = `${identity}. ${operatingTime}, local period ${interval.local_period_number}. Committed ${formatGovernedEnergy(
                interval.committed_mwh_th,
              )}. Delivered ${formatGovernedEnergy(interval.delivered_mwh_th)}.`;
              return (
                <div
                  className="comparison-group"
                  key={interval.interval_key}
                  role="listitem"
                  aria-label={description}
                  title={description}
                >
                  <div className="comparison-group__bars">
                    <ComparisonBar
                      kind="Committed"
                      value={committed}
                      pageMaximum={pageMaximum}
                    />
                    <ComparisonBar
                      kind="Delivered"
                      value={delivered}
                      pageMaximum={pageMaximum}
                    />
                  </div>
                  <time
                    className="comparison-group__time"
                    dateTime={interval.interval_start_at}
                  >
                    {formatOperatingTime(interval.interval_start_at)}
                  </time>
                  <span className="comparison-group__period">
                    P{interval.local_period_number}
                  </span>
                  <span
                    className="comparison-group__point"
                    title={`${interval.delivery_point_name} · ${interval.site_name}`}
                  >
                    {interval.delivery_point_id}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
      {pageMaximum === null ? (
        <p className="comparison-panel__unavailable" role="status">
          Committed and delivered values are unavailable for every interval on
          this page, so a numeric scale cannot be drawn.
        </p>
      ) : pageMaximum === 0 ? (
        <p className="comparison-panel__zero" role="status">
          Every available committed and delivered value on this page is a real
          zero, so all available bars sit on the baseline.
        </p>
      ) : null}
      <p className="chart-note">
        Times use the Europe/London operating clock. Hover an interval group for
        its full identity, local timestamp, and exact governed values; the same
        details are included in its screen-reader description.
      </p>
    </section>
  );
}

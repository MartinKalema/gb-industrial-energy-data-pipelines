import Link from "next/link";
import type { DashboardFilters, DeliveryInterval } from "@/lib/contracts";
import { dashboardQuery } from "@/lib/filters";
import { formatOperatingDateTime, formatStatus } from "@/lib/format";

export function outcomeClass(interval: DeliveryInterval): string {
  const states = [
    interval.delivery_measurement_status,
    interval.sla_result_status,
  ]
    .join(" ")
    .toLowerCase();
  if (states.includes("missing")) return "missing";
  if (states.includes("provisional") || states.includes("pending")) {
    return "provisional";
  }
  const shortfall = Number(interval.shortfall_mwh_th);
  const excess = Number(interval.excess_mwh_th);
  if (Number.isFinite(shortfall) && shortfall > 0) return "shortfall";
  if (Number.isFinite(excess) && excess > 0) return "excess";
  return "accepted";
}

export function OutcomeRibbon({
  intervals,
  filters,
}: {
  intervals: DeliveryInterval[];
  filters: DashboardFilters;
}) {
  return (
    <section className="outcome-panel" aria-labelledby="outcome-heading">
      <div className="section-heading-row">
        <div>
          <p className="section-kicker">Interval signal</p>
          <h2 id="outcome-heading">Delivery outcomes across this page</h2>
        </div>
        <ul className="outcome-legend" aria-label="Delivery outcome legend">
          <li><span className="legend-swatch legend-swatch--accepted" />Accepted</li>
          <li><span className="legend-swatch legend-swatch--shortfall" />Shortfall</li>
          <li><span className="legend-swatch legend-swatch--provisional" />Waiting for data</li>
          <li><span className="legend-swatch legend-swatch--missing" />Missing</li>
        </ul>
      </div>
      <p id="outcome-scroll-description" className="outcome-panel__description">
        Oldest interval first. Scroll horizontally to follow every visible
        result in chronological order.
      </p>
      <span id="outcome-scroll-label" className="sr-only">
        Chronological delivery outcomes
      </span>
      <div
        className="outcome-ribbon__scroll"
        role="region"
        aria-labelledby="outcome-scroll-label"
        aria-describedby="outcome-scroll-description"
        tabIndex={0}
      >
        <div className="outcome-ribbon" role="list" aria-label="Chronological delivery states">
          {intervals.map((interval) => {
            const state = outcomeClass(interval);
            const identity = `${interval.customer_name} (${interval.customer_id}), ${interval.site_name} (${interval.site_id}), ${interval.delivery_point_name} (${interval.delivery_point_id})`;
            const label = `${identity}. ${formatOperatingDateTime(interval.interval_start_at)}: ${formatStatus(
              interval.sla_result_status,
            )}, measurement ${formatStatus(interval.delivery_measurement_status)}`;
            return (
              <Link
                role="listitem"
                key={interval.interval_key}
                className={`outcome-segment outcome-segment--${state}`}
                href={`/delivery-intervals/${encodeURIComponent(interval.interval_key)}?${dashboardQuery(filters)}`}
                aria-label={label}
                title={label}
              >
                <span className="sr-only">{label}</span>
              </Link>
            );
          })}
        </div>
      </div>
      <p className="chart-note">
        Each block is one governed interval result. Select a block to inspect its
        revision history.
      </p>
    </section>
  );
}

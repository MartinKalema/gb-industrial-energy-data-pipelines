import Link from "next/link";
import { StateBadge } from "@/components/state-badge";
import type { DashboardFilters, DeliveryInterval } from "@/lib/contracts";
import { withProductDataVersion } from "@/lib/data-version";
import { dashboardQuery } from "@/lib/filters";
import {
  formatCurrency,
  formatDateTime,
  formatEnergy,
  formatOperatingDateTime,
} from "@/lib/format";

export function IntervalTable({
  intervals,
  filters,
  currency,
  financialColumnLabel,
  dataVersion,
}: {
  intervals: DeliveryInterval[];
  filters: DashboardFilters;
  currency: string | null;
  financialColumnLabel: string;
  dataVersion: string | undefined;
}) {
  if (intervals.length === 0) {
    return (
      <section className="empty-state" aria-labelledby="empty-heading">
        <p className="empty-state__marker" aria-hidden="true">00</p>
        <div>
          <p className="section-kicker">No matching evidence</p>
          <h2 id="empty-heading">No 30-minute delivery records match these filters</h2>
          <p>
            Widen the date range or remove a customer, site, or delivery-state
            filter. No zero values have been substituted for absent records.
          </p>
        </div>
      </section>
    );
  }

  return (
    <section className="table-panel" aria-labelledby="intervals-heading">
      <div className="section-heading-row">
        <div>
          <p className="section-kicker">Thirty-minute evidence</p>
          <h2 id="intervals-heading">30-minute delivery records</h2>
        </div>
        <p className="table-panel__hint">Times shown in UTC</p>
      </div>
      <div className="table-scroll" tabIndex={0}>
        <table>
          <thead>
            <tr>
              <th scope="col">Time period</th>
              <th scope="col">Site / point</th>
              <th scope="col">Committed</th>
              <th scope="col">Delivered</th>
              <th scope="col">Shortfall</th>
              <th scope="col">Excess</th>
              <th scope="col">{financialColumnLabel}</th>
              <th scope="col">SLA result</th>
              <th scope="col">Record state</th>
              <th scope="col"><span className="sr-only">Open history</span></th>
            </tr>
          </thead>
          <tbody>
            {intervals.map((interval) => (
              <tr key={interval.interval_key}>
                <td>
                  <time dateTime={interval.interval_start_at}>
                    {formatOperatingDateTime(interval.interval_start_at)}
                  </time>
                  <span className="table-subline">
                    {formatDateTime(interval.interval_start_at)} UTC
                  </span>
                </td>
                <td>
                  <strong>{interval.site_name}</strong>
                  <span className="table-subline">
                    {interval.delivery_point_name}
                  </span>
                </td>
                <td>{formatEnergy(interval.committed_mwh_th)}</td>
                <td>{formatEnergy(interval.delivered_mwh_th)}</td>
                <td>{formatEnergy(interval.shortfall_mwh_th)}</td>
                <td>{formatEnergy(interval.excess_mwh_th)}</td>
                <td>{formatCurrency(interval.net_earned_revenue_gbp, currency)}</td>
                <td><StateBadge status={interval.sla_result_status} /></td>
                <td><StateBadge status={interval.correction_status} /></td>
                <td>
                  <Link
                    className="row-link"
                    href={`/delivery-intervals/${encodeURIComponent(interval.interval_key)}?${withProductDataVersion(
                      dashboardQuery(filters),
                      dataVersion,
                    )}`}
                    aria-label={`Inspect revision history for the 30-minute period beginning ${formatDateTime(interval.interval_start_at)}`}
                  >
                    Inspect <span aria-hidden="true">→</span>
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

import { FilterPanel } from "@/components/filter-panel";
import { DeliveryComparisonChart } from "@/components/delivery-comparison-chart";
import { DataStatusNotice } from "@/components/data-status-notice";
import { IntervalTable } from "@/components/interval-table";
import { MetricCard } from "@/components/metric-card";
import { Pagination } from "@/components/pagination";
import { TermGuide } from "@/components/term-guide";
import { notFound } from "next/navigation";
import { parseProductDataVersion } from "@/lib/data-version";
import type { SearchParams } from "@/lib/filters";
import { parseDashboardFilters } from "@/lib/filters";
import {
  financialLabelsFromContract,
  formatCount,
  formatCurrency,
  formatDateTime,
  formatEnergy,
  formatPercent,
  formatStatus,
} from "@/lib/format";
import {
  getDeliveryIntervals,
  getDeliverySummary,
  getProductContext,
} from "@/lib/product-api";
import { pendingDataReasons } from "@/lib/summary-status";

export const dynamic = "force-dynamic";

interface PageProps {
  searchParams: Promise<SearchParams>;
}

export default async function DeliveryPerformancePage({ searchParams }: PageProps) {
  const params = await searchParams;
  const initialFilters = parseDashboardFilters(params);
  const requestedDataVersion = parseProductDataVersion(params.data_version);
  if (
    params.data_version !== undefined &&
    requestedDataVersion === undefined
  ) {
    notFound();
  }
  const contextResult = await getProductContext(initialFilters.actor, {
    dataVersion: requestedDataVersion,
  });
  const filters = parseDashboardFilters(
    params,
    new Date(),
    contextResult.data.available_reporting_dates ?? undefined,
  );
  const dataVersion =
    requestedDataVersion ?? contextResult.data.data_version ?? undefined;
  const [summaryResult, intervalsResult, chartIntervalsResult] = await Promise.all([
    getDeliverySummary(filters, { dataVersion }),
    getDeliveryIntervals(filters, { dataVersion }),
    getDeliveryIntervals(
      { ...filters, page: 1, limit: 200 },
      { dataVersion },
    ),
  ]);
  const summary = summaryResult.data;
  const intervals = intervalsResult.data;
  const chartIntervals = chartIntervalsResult.data;
  const labels = financialLabelsFromContract(summary.financial_labels);
  const waitingForDataReasons = pendingDataReasons(summary);

  return (
    <div className="app-shell">
      <header className="masthead">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>
          <div>
            <h1 id="page-title" className="brand-name">Steam Delivery Performance</h1>
          </div>
        </div>
      </header>

      <main id="main-content" aria-labelledby="page-title">
        <div className="workspace-grid">
          <FilterPanel
            filters={filters}
            customers={contextResult.data.customers}
            dataVersion={dataVersion}
          />

          <div className="report-column">
            <TermGuide />
            <section className="metric-section metric-section--overview" aria-labelledby="summary-heading">
              <div className="section-heading-row">
                <div>
                  <p className="section-kicker">Selected scope</p>
                  <h2 id="summary-heading">Performance at a glance</h2>
                </div>
              </div>
              <DataStatusNotice reasons={waitingForDataReasons} />
              <div className="metric-grid metric-grid--overview">
                <MetricCard
                  eyebrow="Delivered energy"
                  value={formatEnergy(summary.known_delivered_mwh_th)}
                  note="Subtotal of 30-minute periods with accepted delivery evidence."
                  featured
                />
                <MetricCard
                  eyebrow="Committed energy"
                  value={formatEnergy(summary.committed_mwh_th)}
                  note={`${formatCount(summary.commitment_record_count)} governed commitment records.`}
                />
                <MetricCard
                  eyebrow="Known shortfall"
                  value={formatEnergy(summary.known_shortfall_mwh_th)}
                  note="Governed shortfall subtotal; no missing period is treated as zero."
                />
                <MetricCard
                  eyebrow="Known excess delivery"
                  value={formatEnergy(summary.known_excess_mwh_th)}
                  note="Governed excess subtotal for accepted 30-minute period evidence."
                />
              </div>
            </section>

            {chartIntervals.items.length > 0 ? (
              <DeliveryComparisonChart
                intervals={chartIntervals.items}
                total={chartIntervals.total}
              />
            ) : null}

            <section className="commercial-strip" aria-labelledby="commercial-heading">
              <div className="commercial-strip__heading">
                <p className="section-kicker">Known commercial position</p>
                <h2 id="commercial-heading">Billable subtotal</h2>
              </div>
              <MetricCard
                eyebrow="Known billable energy"
                value={formatEnergy(summary.known_billable_mwh_th)}
                note="Billable subtotal returned by the governed mart."
              />
              <MetricCard
                eyebrow={labels.gross}
                value={formatCurrency(
                  summary.known_gross_earned_revenue_gbp,
                  summary.currency_code,
                )}
                note="Known subtotal before an official financial result."
              />
            </section>

            <section className="official-panel" aria-labelledby="official-heading">
              <div className="official-panel__intro">
                <p className="section-kicker">Decision gate</p>
                <h2 id="official-heading">Official contractual outcomes</h2>
                <p>
                  These figures remain unavailable until all required commitments
                  are present, delivery limits are confirmed, and delivery records
                  are accepted. Known subtotals above can still support investigation.
                </p>
              </div>
              <div className="official-grid">
                <MetricCard
                  eyebrow="Delivery completeness"
                  value={formatPercent(summary.delivery_data_completeness_percent)}
                  note={`${formatCount(summary.accepted_applicable_delivery_count)} of ${formatCount(summary.applicable_interval_count)} applicable 30-minute periods accepted.`}
                />
                <MetricCard
                  eyebrow="Commitment completeness"
                  value={formatPercent(summary.commitment_completeness_percent)}
                  note={`${formatCount(summary.commitment_record_count)} of ${formatCount(summary.expected_interval_count)} expected 30-minute commitments present.`}
                />
                <MetricCard
                  eyebrow="SLA attainment"
                  value={formatPercent(summary.sla_attainment_percent)}
                  note="Official percentage; never calculated in this interface."
                />
                <MetricCard
                  eyebrow="Contractual availability"
                  value={formatPercent(summary.contractual_availability_percent)}
                  note="Official percentage from the governed mart."
                />
                <MetricCard
                  eyebrow={labels.adjustment}
                  value={formatCurrency(
                    summary.accrued_sla_penalty_gbp,
                    summary.currency_code,
                  )}
                  note="Unavailable until the financial result is final."
                />
                <MetricCard
                  eyebrow={labels.net}
                  value={formatCurrency(
                    summary.net_earned_revenue_gbp,
                    summary.currency_code,
                  )}
                  note="Official result after the contractual decision gate."
                />
              </div>
            </section>

            <IntervalTable
              intervals={intervals.items}
              filters={filters}
              currency={summary.currency_code}
              financialColumnLabel={labels.shortNet}
              dataVersion={dataVersion}
            />
            <Pagination
              filters={filters}
              total={intervals.total}
              dataVersion={dataVersion}
            />

            <section className="provenance" aria-labelledby="provenance-heading">
              <div>
                <p className="section-kicker">Traceability</p>
                <h2 id="provenance-heading">Evidence provenance and freshness</h2>
              </div>
              <dl>
                <div>
                  <dt>Governed source</dt>
                  <dd>Tested Iceberg dimensional mart</dd>
                </div>
                <div>
                  <dt>Published to the frontend database</dt>
                  <dd>{formatDateTime(contextResult.data.data_published_at_utc)} UTC</dd>
                </div>
                <div>
                  <dt>Frontend data version</dt>
                  <dd><code>{contextResult.data.data_version ?? "Direct mart query"}</code></dd>
                </div>
                <div>
                  <dt>Latest coverage publication</dt>
                  <dd>{formatDateTime(summary.latest_coverage_published_at_utc)} UTC</dd>
                </div>
                <div>
                  <dt>API response</dt>
                  <dd>{formatDateTime(summaryResult.respondedAt)} UTC</dd>
                </div>
                <div>
                  <dt>Request trace</dt>
                  <dd><code>{summaryResult.requestId ?? "Unavailable"}</code></dd>
                </div>
                <div>
                  <dt>Result state</dt>
                  <dd>{formatStatus(summary.completeness_status)}</dd>
                </div>
              </dl>
            </section>
          </div>
        </div>
      </main>

    </div>
  );
}

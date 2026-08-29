import { FilterPanel } from "@/components/filter-panel";
import { DeliveryComparisonChart } from "@/components/delivery-comparison-chart";
import { IntervalTable } from "@/components/interval-table";
import { MetricCard } from "@/components/metric-card";
import { OutcomeRibbon } from "@/components/outcome-ribbon";
import { Pagination } from "@/components/pagination";
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
  const contextResult = await getProductContext(initialFilters.actor);
  const filters = parseDashboardFilters(
    params,
    new Date(),
    contextResult.data.available_reporting_dates ?? undefined,
  );
  const [summaryResult, intervalsResult] = await Promise.all([
    getDeliverySummary(filters),
    getDeliveryIntervals(filters),
  ]);
  const summary = summaryResult.data;
  const intervals = intervalsResult.data;
  const labels = financialLabelsFromContract(summary.financial_labels);
  const waitingForDataReasons = pendingDataReasons(summary);

  return (
    <div className="app-shell">
      <header className="masthead">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>
          <div>
            <p className="brand-name">Steam Delivery Performance</p>
          </div>
        </div>
        <div className="masthead__status">
          <span className="live-indicator" aria-hidden="true" />
          <div>
            <strong>Data ready</strong>
            <span>Historical and read-only</span>
          </div>
        </div>
      </header>

      <main id="main-content">
        <section className="hero" aria-labelledby="page-title">
          <div className="hero__copy">
            <p className="section-kicker">Delivery review</p>
            <h1 id="page-title">Compare committed and delivered steam.</h1>
            <p className="hero__lede">
              Review each 30-minute interval, find shortfalls and excess, and see
              how corrections changed the result.
            </p>
          </div>
          <dl className="hero__scope">
            <div>
              <dt>Dates</dt>
              <dd>{filters.start} <span aria-hidden="true">→</span> {filters.end}</dd>
            </div>
            <div>
              <dt>Customers</dt>
              <dd>{formatCount(contextResult.data.customers.length)}</dd>
            </div>
            <div>
              <dt>Intervals</dt>
              <dd>{formatCount(summary.interval_count)}</dd>
            </div>
          </dl>
        </section>

        <div className="workspace-grid">
          <FilterPanel
            filters={filters}
            customers={contextResult.data.customers}
            identityRole={contextResult.data.identity.role}
          />

          <div className="report-column">
            <section className="metric-section" aria-labelledby="summary-heading">
              <div className="section-heading-row">
                <div>
                  <p className="section-kicker">Available data</p>
                  <h2 id="summary-heading">Known totals</h2>
                </div>
              </div>
              {waitingForDataReasons.length > 0 ? (
                <div className="data-status-notice" role="status">
                  <strong>Some official results are waiting for data.</strong>
                  <span>
                    Why: {waitingForDataReasons.join("; ")}. Known totals
                    below remain available.
                  </span>
                </div>
              ) : null}
              <div className="metric-grid">
                <MetricCard
                  eyebrow="Delivered energy"
                  value={formatEnergy(summary.known_delivered_mwh_th)}
                  note="Subtotal of intervals with accepted delivery evidence."
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
                  note="Governed shortfall subtotal; no missing interval is treated as zero."
                />
                <MetricCard
                  eyebrow="Known excess delivery"
                  value={formatEnergy(summary.known_excess_mwh_th)}
                  note="Governed excess subtotal for accepted interval evidence."
                />
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
              </div>
            </section>

            <section className="official-panel" aria-labelledby="official-heading">
              <div className="official-panel__intro">
                <p className="section-kicker">Decision gate</p>
                <h2 id="official-heading">Official contractual outcomes</h2>
                <p>
                  These figures remain unavailable until all required commitments,
                  applicable capacity and accepted delivery records pass their gates.
                  Known subtotals above can still support investigation.
                </p>
              </div>
              <div className="official-grid">
                <MetricCard
                  eyebrow="Delivery completeness"
                  value={formatPercent(summary.delivery_data_completeness_percent)}
                  note={`${formatCount(summary.accepted_applicable_delivery_count)} of ${formatCount(summary.applicable_interval_count)} applicable intervals accepted.`}
                />
                <MetricCard
                  eyebrow="Commitment completeness"
                  value={formatPercent(summary.commitment_completeness_percent)}
                  note={`${formatCount(summary.commitment_record_count)} of ${formatCount(summary.expected_interval_count)} expected interval commitments present.`}
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

            {intervals.items.length > 0 ? (
              <>
                <DeliveryComparisonChart intervals={intervals.items} />
                <OutcomeRibbon intervals={intervals.items} filters={filters} />
              </>
            ) : null}
            <IntervalTable
              intervals={intervals.items}
              filters={filters}
              currency={summary.currency_code}
              financialColumnLabel={labels.shortNet}
            />
            <Pagination filters={filters} total={intervals.total} />

            <section className="provenance" aria-labelledby="provenance-heading">
              <div>
                <p className="section-kicker">Traceability</p>
                <h2 id="provenance-heading">Evidence provenance and freshness</h2>
              </div>
              <dl>
                <div>
                  <dt>Governed source</dt>
                  <dd>Iceberg dimensional mart, served read-only through the product API</dd>
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

      <footer className="product-footer">
        <p>Historical Steam Delivery Performance</p>
        <p>Simulated business data · Europe/London operating calendar · UTC evidence timestamps</p>
      </footer>
    </div>
  );
}

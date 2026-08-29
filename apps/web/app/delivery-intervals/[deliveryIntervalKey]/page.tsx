import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { MetricCard } from "@/components/metric-card";
import { StateBadge } from "@/components/state-badge";
import type { DeliveryIntervalHistoryItem } from "@/lib/contracts";
import type { SearchParams } from "@/lib/filters";
import { dashboardQuery, parseDashboardFilters } from "@/lib/filters";
import {
  financialLabelsForRole,
  formatCurrency,
  formatDateTime,
  formatEnergy,
  formatOperatingDateTime,
} from "@/lib/format";
import {
  getDeliveryIntervalHistory,
  getProductContext,
  ProductApiError,
} from "@/lib/product-api";

export const dynamic = "force-dynamic";

interface DetailPageProps {
  params: Promise<{ deliveryIntervalKey: string }>;
  searchParams: Promise<SearchParams>;
}

export async function generateMetadata({ params }: DetailPageProps): Promise<Metadata> {
  const { deliveryIntervalKey } = await params;
  return {
    title: `Interval ${deliveryIntervalKey}`,
    description: `Governed revision history for steam-delivery interval ${deliveryIntervalKey}.`,
    openGraph: { images: [] },
    twitter: { images: [] },
  };
}

function RevisionCard({
  revision,
  position,
  currency,
  identityRole,
}: {
  revision: DeliveryIntervalHistoryItem;
  position: number;
  currency: string | null;
  identityRole: string;
}) {
  const labels = financialLabelsForRole(identityRole);
  const current = revision.is_current_knowledge_state;
  return (
    <article className={`revision-card${current ? " revision-card--current" : ""}`}>
      <div className="revision-card__rail" aria-hidden="true">
        <span>{String(position).padStart(2, "0")}</span>
      </div>
      <div className="revision-card__body">
        <header>
          <div>
            <p className="revision-card__eyebrow">
              {current ? "Current known version" : "Superseded version"}
            </p>
            <h2>Revision known from {formatDateTime(revision.known_from_at)} UTC</h2>
          </div>
          <StateBadge status={revision.correction_status} />
        </header>

        <dl className="revision-validity">
          <div>
            <dt>Known from</dt>
            <dd>{formatDateTime(revision.known_from_at)} UTC</dd>
          </div>
          <div>
            <dt>Known until</dt>
            <dd>{revision.known_to_at ? `${formatDateTime(revision.known_to_at)} UTC` : "Current"}</dd>
          </div>
          <div>
            <dt>History key</dt>
            <dd><code>{revision.history_key}</code></dd>
          </div>
        </dl>

        <div className="revision-metrics">
          <MetricCard
            eyebrow="Committed"
            value={formatEnergy(revision.committed_mwh_th)}
            note="Governed interval commitment"
            state={revision.commitment_status ?? "Recorded"}
          />
          <MetricCard
            eyebrow="Delivered"
            value={formatEnergy(revision.delivered_mwh_th)}
            note="Accepted measurement only"
            state={revision.delivery_measurement_status}
          />
          <MetricCard
            eyebrow="Shortfall"
            value={formatEnergy(revision.shortfall_mwh_th)}
            note="Governed result, not calculated here"
            state={revision.sla_result_status}
          />
          <MetricCard
            eyebrow={labels.shortNet}
            value={formatCurrency(revision.net_earned_revenue_gbp, currency)}
            note="Official only when financial state is final"
            state={revision.financial_result_status}
          />
        </div>

        <div className="revision-status-grid">
          <div><span>Measurement</span><StateBadge status={revision.delivery_measurement_status} /></div>
          <div><span>SLA</span><StateBadge status={revision.sla_result_status} /></div>
          <div><span>Availability</span><StateBadge status={revision.availability_result_status} /></div>
          <div><span>Financial</span><StateBadge status={revision.financial_result_status} /></div>
        </div>
      </div>
    </article>
  );
}

export default async function DeliveryIntervalHistoryPage({
  params,
  searchParams,
}: DetailPageProps) {
  const [{ deliveryIntervalKey }, rawSearchParams] = await Promise.all([
    params,
    searchParams,
  ]);
  const filters = parseDashboardFilters(rawSearchParams);

  let historyResult;
  let contextResult;
  try {
    [historyResult, contextResult] = await Promise.all([
      getDeliveryIntervalHistory(deliveryIntervalKey, filters),
      getProductContext(filters.actor),
    ]);
  } catch (error) {
    if (error instanceof ProductApiError && (error.status === 403 || error.status === 404)) {
      notFound();
    }
    throw error;
  }

  const history = historyResult.data;
  if (history.items.length === 0) notFound();
  const current =
    history.items.find((item) => item.is_current_knowledge_state) ?? history.items.at(-1)!;
  const currency = current.currency_code;

  return (
    <div className="app-shell detail-shell">
      <header className="masthead">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>
          <div>
            <p className="brand-name">Steam Delivery Performance</p>
          </div>
        </div>
        <Link className="back-link" href={`/?${dashboardQuery(filters)}`}>
          <span aria-hidden="true">←</span> Back to interval register
        </Link>
      </header>

      <main id="main-content">
        <section className="detail-hero" aria-labelledby="detail-title">
          <div>
            <p className="hero__index">EVIDENCE / REVISION HISTORY</p>
            <p className="section-kicker">Delivery interval</p>
            <h1 id="detail-title">
              {current.site_name} · period {current.local_period_number}
            </h1>
            <p className="detail-hero__time">
              {formatOperatingDateTime(current.interval_start_at)}
              <span>{formatDateTime(current.interval_start_at)} UTC</span>
            </p>
          </div>
          <div className="detail-hero__identity">
            <dl>
              <div><dt>Interval key</dt><dd><code>{history.interval_key}</code></dd></div>
              <div><dt>Delivery point</dt><dd>{current.delivery_point_name}</dd></div>
              <div><dt>Operating date</dt><dd>{current.reporting_date}</dd></div>
              <div><dt>Current record</dt><dd><StateBadge status={current.correction_status} /></dd></div>
            </dl>
          </div>
        </section>

        <section className="history-intro" aria-labelledby="history-heading">
          <div>
            <p className="section-kicker">As-known chronology</p>
            <h2 id="history-heading">How this result changed over time</h2>
          </div>
          <p>
            Each card preserves what the business knew during that validity window.
            Superseded evidence remains visible; only the open-ended record is current.
          </p>
        </section>

        {history.truncated ? (
          <p className="history-limit-notice" role="status">
            Showing the 200 most recent revisions. Earlier revisions exist but are
            not included in this response.
          </p>
        ) : null}

        <div className="revision-list">
          {history.items.map((revision, index) => (
            <RevisionCard
              key={revision.history_key}
              revision={revision}
              position={index + 1}
              currency={currency}
              identityRole={contextResult.data.identity.role}
            />
          ))}
        </div>

        <section className="provenance detail-provenance" aria-labelledby="trace-heading">
          <div>
            <p className="section-kicker">Traceability</p>
            <h2 id="trace-heading">History request</h2>
          </div>
          <dl>
            <div><dt>Governed source</dt><dd>Iceberg fact history, served read-only</dd></div>
            <div><dt>API response</dt><dd>{formatDateTime(historyResult.respondedAt)} UTC</dd></div>
            <div><dt>Request trace</dt><dd><code>{historyResult.requestId ?? "Unavailable"}</code></dd></div>
          </dl>
        </section>
      </main>
    </div>
  );
}

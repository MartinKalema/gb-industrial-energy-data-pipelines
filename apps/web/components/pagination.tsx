import Link from "next/link";
import type { DashboardFilters } from "@/lib/contracts";
import { withProductDataVersion } from "@/lib/data-version";
import { dashboardQuery } from "@/lib/filters";
import { formatCount } from "@/lib/format";

export function Pagination({
  filters,
  total,
  dataVersion,
}: {
  filters: DashboardFilters;
  total: number;
  dataVersion: string | undefined;
}) {
  const pageCount = Math.max(1, Math.ceil(total / filters.limit));
  const from = total === 0 ? 0 : (filters.page - 1) * filters.limit + 1;
  const to = Math.min(filters.page * filters.limit, total);

  return (
    <nav className="pagination" aria-label="Delivery intervals pages">
      <p>
        Showing <strong>{formatCount(from)}–{formatCount(to)}</strong> of{" "}
        <strong>{formatCount(total)}</strong> intervals
      </p>
      <div className="pagination__actions">
        {filters.page > 1 ? (
          <Link href={`/?${withProductDataVersion(
            dashboardQuery(filters, { page: filters.page - 1 }),
            dataVersion,
          )}`}>
            <span aria-hidden="true">←</span> Previous
          </Link>
        ) : (
          <span aria-disabled="true"><span aria-hidden="true">←</span> Previous</span>
        )}
        <span>Page {filters.page} of {pageCount}</span>
        {filters.page < pageCount ? (
          <Link href={`/?${withProductDataVersion(
            dashboardQuery(filters, { page: filters.page + 1 }),
            dataVersion,
          )}`}>
            Next <span aria-hidden="true">→</span>
          </Link>
        ) : (
          <span aria-disabled="true">Next <span aria-hidden="true">→</span></span>
        )}
      </div>
    </nav>
  );
}

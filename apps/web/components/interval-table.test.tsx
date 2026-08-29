import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { IntervalTable } from "@/components/interval-table";
import type { DashboardFilters, DeliveryInterval } from "@/lib/contracts";

const filters: DashboardFilters = {
  actor: "customer-cust-001",
  start: "2026-08-26",
  end: "2026-08-26",
  page: 1,
  limit: 25,
};
const dataVersion = `publication-${"a".repeat(32)}`;

const interval: DeliveryInterval = {
  interval_key: "interval-001",
  customer_id: "CUST-001",
  customer_name: "North Foundry",
  site_id: "SITE-001",
  site_name: "North Foundry Works",
  delivery_point_id: "DP-001",
  delivery_point_name: "Main steam header",
  reporting_date: "2026-08-26",
  local_period_number: 1,
  interval_start_at: "2026-08-25T23:00:00Z",
  interval_end_at: "2026-08-25T23:30:00Z",
  interval_start_local: "2026-08-26T00:00:00",
  interval_end_local: "2026-08-26T00:30:00",
  operating_timezone: "Europe/London",
  utc_offset_minutes: 60,
  is_daylight_saving_time: true,
  committed_mwh_th: "10.0",
  delivered_mwh_th: null,
  shortfall_mwh_th: null,
  excess_mwh_th: null,
  deliverable_capacity_mwh_th: "12.0",
  billable_mwh_th: null,
  gross_earned_revenue_gbp: null,
  accrued_sla_penalty_gbp: null,
  net_earned_revenue_gbp: null,
  currency_code: "GBP",
  delivery_measurement_status: "missing",
  commitment_status: "final",
  capacity_status: "final",
  sla_result_status: "provisional",
  availability_result_status: "provisional",
  financial_result_status: "provisional",
  correction_status: "current",
};

describe("IntervalTable", () => {
  it("uses customer language and never substitutes zero for missing measures", () => {
    render(
      <IntervalTable
        intervals={[interval]}
        filters={filters}
        currency="GBP"
        financialColumnLabel="Projected service charge"
        dataVersion={dataVersion}
      />,
    );

    expect(screen.getByRole("columnheader", { name: "Projected service charge" })).toBeVisible();
    expect(screen.getAllByText("Unavailable").length).toBeGreaterThan(0);
    expect(screen.queryByText("0 MWhₜₕ")).not.toBeInTheDocument();
    expect(screen.getByText("26 Aug 2026, 00:00 BST")).toBeVisible();
    const detailUrl = new URL(
      screen.getByRole("link", { name: /inspect revision history/i }).getAttribute("href")!,
      "http://product.local",
    );
    expect(detailUrl.searchParams.get("data_version")).toBe(dataVersion);
    expect(detailUrl.searchParams.get("actor")).toBe(filters.actor);
    expect(detailUrl.searchParams.get("start_date")).toBe(filters.start);
    expect(detailUrl.searchParams.get("end_date")).toBe(filters.end);
  });

  it("provides an instructive empty state", () => {
    render(
      <IntervalTable
        intervals={[]}
        filters={filters}
        currency="GBP"
        financialColumnLabel="Projected service charge"
        dataVersion={dataVersion}
      />,
    );
    expect(screen.getByRole("heading", { name: /no delivery intervals match/i })).toBeVisible();
    expect(screen.getByText(/no zero values have been substituted/i)).toBeVisible();
  });
});

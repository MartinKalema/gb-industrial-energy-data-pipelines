import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
  DeliveryComparisonChart,
  deliveryChartScale,
} from "@/components/delivery-comparison-chart";
import type { DeliveryInterval } from "@/lib/contracts";

function interval(
  key: string,
  period: number,
  start: string,
  committed: string | null,
  delivered: string | null,
): DeliveryInterval {
  return {
    interval_key: key,
    customer_id: "CUST-001",
    customer_name: "North Foundry",
    site_id: "SITE-001",
    site_name: "North Foundry Works",
    delivery_point_id: "DP-001",
    delivery_point_name: "Main steam header",
    reporting_date: "2026-08-26",
    local_period_number: period,
    interval_start_at: start,
    interval_end_at: "2026-08-25T23:30:00Z",
    interval_start_local: "2026-08-26T00:00:00",
    interval_end_local: "2026-08-26T00:30:00",
    operating_timezone: "Europe/London",
    utc_offset_minutes: 60,
    is_daylight_saving_time: true,
    committed_mwh_th: committed,
    delivered_mwh_th: delivered,
    shortfall_mwh_th: null,
    excess_mwh_th: null,
    deliverable_capacity_mwh_th: "12.0",
    billable_mwh_th: null,
    gross_earned_revenue_gbp: null,
    accrued_sla_penalty_gbp: null,
    net_earned_revenue_gbp: null,
    currency_code: "GBP",
    delivery_measurement_status: delivered === null ? "missing" : "accepted",
    commitment_status: "final",
    capacity_status: "final",
    sla_result_status: delivered === null ? "provisional" : "final",
    availability_result_status: "final",
    financial_result_status: delivered === null ? "provisional" : "final",
    correction_status: "current",
  };
}

const visibleIntervals = [
  interval("interval-001", 1, "2026-08-25T23:00:00Z", "10.0", "8.0"),
  {
    ...interval("interval-002", 2, "2026-08-25T23:30:00Z", null, "12.0"),
    customer_id: "CUST-002",
    customer_name: "South Ceramics",
    site_id: "SITE-002",
    site_name: "South Ceramics Works",
    delivery_point_id: "DP-002",
    delivery_point_name: "Kiln steam header",
  },
];

describe("DeliveryComparisonChart", () => {
  it("uses the largest governed visible value as its page scale", () => {
    expect(deliveryChartScale(visibleIntervals)).toBe(12);
  });

  it("renders exact accessible comparisons and a visible unavailable marker", () => {
    const { container } = render(
      <DeliveryComparisonChart intervals={visibleIntervals} />,
    );

    expect(
      screen.getByRole("heading", { name: "Committed versus delivered steam" }),
    ).toBeVisible();
    expect(screen.getByText("12.0 MWhₜₕ")).toBeVisible();
    expect(screen.getByText("00:00")).toBeVisible();
    expect(screen.getByText("00:30")).toBeVisible();

    const intervalGroup = screen.getByRole("listitem", {
      name: /local period 1\. Committed 10\.0 MWhₜₕ\. Delivered 8\.0 MWhₜₕ\./,
    });
    expect(intervalGroup).toHaveAttribute("title", expect.stringContaining("26 Aug 2026"));
    expect(intervalGroup).not.toHaveAttribute("tabindex");

    const missingGroup = screen.getByRole("listitem", {
      name: /South Ceramics \(CUST-002\), South Ceramics Works \(SITE-002\), Kiln steam header \(DP-002\).*local period 2\. Committed Unavailable\. Delivered 12\.0 MWhₜₕ\./,
    });
    expect(screen.getByText("DP-002")).toBeVisible();
    expect(missingGroup).toHaveAttribute(
      "title",
      expect.stringContaining("Kiln steam header (DP-002)"),
    );
    expect(within(missingGroup).getByText("N/A")).toBeVisible();
    expect(
      container.querySelector(".comparison-bar--unavailable"),
    ).toHaveClass("comparison-bar--unavailable");
    expect(
      screen.getByRole("region", { name: /scroll horizontally/i }),
    ).toHaveAttribute("tabindex", "0");
  });

  it("explains when every chart value is unavailable", () => {
    render(
      <DeliveryComparisonChart
        intervals={[
          interval("interval-003", 3, "2026-08-26T00:00:00Z", null, null),
        ]}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent(
      /values are unavailable for every interval/i,
    );
  });

  it("keeps a real zero distinct from an unavailable value", () => {
    render(
      <DeliveryComparisonChart
        intervals={[
          interval("interval-004", 4, "2026-08-26T00:30:00Z", "0.0", "0.0"),
        ]}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent(/real zero/i);
    expect(screen.queryByText("N/A")).not.toBeInTheDocument();
  });

  it("preserves six-decimal governed values in the scale, tooltip, and description", () => {
    render(
      <DeliveryComparisonChart
        intervals={[
          interval(
            "interval-005",
            5,
            "2026-08-26T01:00:00Z",
            "0.004000",
            "0.003500",
          ),
        ]}
      />,
    );

    expect(screen.getByText("0.004000 MWhₜₕ")).toBeVisible();
    const group = screen.getByRole("listitem", {
      name: /Committed 0\.004000 MWhₜₕ\. Delivered 0\.003500 MWhₜₕ\./,
    });
    expect(group).toHaveAttribute(
      "title",
      expect.stringContaining("Committed 0.004000 MWhₜₕ"),
    );
    expect(screen.getByText(/Exact labels and descriptions preserve/)).toBeVisible();
  });
});

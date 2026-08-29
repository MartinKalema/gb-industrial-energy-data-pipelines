import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { OutcomeRibbon, outcomeClass } from "@/components/outcome-ribbon";
import type { DashboardFilters, DeliveryInterval } from "@/lib/contracts";

const baseInterval = {
  delivery_measurement_status: "accepted",
  sla_result_status: "final",
  shortfall_mwh_th: "0.0",
  excess_mwh_th: "0.0",
} as DeliveryInterval;

describe("outcome ribbon classification", () => {
  it("uses the governed shortfall amount when statuses are otherwise final", () => {
    expect(outcomeClass({ ...baseInterval, shortfall_mwh_th: "0.3" })).toBe(
      "shortfall",
    );
  });

  it("uses the governed excess amount when statuses are otherwise final", () => {
    expect(outcomeClass({ ...baseInterval, excess_mwh_th: "0.3" })).toBe("excess");
  });

  it("keeps missing and provisional states ahead of amount styling", () => {
    expect(
      outcomeClass({
        ...baseInterval,
        delivery_measurement_status: "missing",
        shortfall_mwh_th: null,
      }),
    ).toBe("missing");
    expect(
      outcomeClass({
        ...baseInterval,
        sla_result_status: "provisional",
        shortfall_mwh_th: "0.3",
      }),
    ).toBe("provisional");
  });

  it("distinguishes same-time outcomes from different delivery points", () => {
    const filters: DashboardFilters = {
      actor: "commercial-manager",
      start: "2026-08-26",
      end: "2026-08-26",
      page: 1,
      limit: 25,
    };
    const shared = {
      ...baseInterval,
      customer_id: "CUST-001",
      customer_name: "Northstar Ceramics",
      interval_start_at: "2026-08-25T23:00:00Z",
    };
    const intervals = [
      {
        ...shared,
        interval_key: "interval-001",
        site_id: "SITE-001",
        site_name: "Sheffield Works",
        delivery_point_id: "DP-001",
        delivery_point_name: "Main steam header",
      },
      {
        ...shared,
        interval_key: "interval-002",
        site_id: "SITE-002",
        site_name: "Leeds Works",
        delivery_point_id: "DP-002",
        delivery_point_name: "East steam header",
      },
    ] as DeliveryInterval[];

    render(
      OutcomeRibbon({
        intervals,
        filters,
        dataVersion: `publication-${"a".repeat(32)}`,
      }),
    );

    expect(
      screen.getByRole("listitem", { name: /Sheffield Works \(SITE-001\).*Main steam header \(DP-001\)/ }),
    ).toBeVisible();
    expect(
      screen.getByRole("listitem", { name: /Leeds Works \(SITE-002\).*East steam header \(DP-002\)/ }),
    ).toBeVisible();
  });
});

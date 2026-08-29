import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { OutcomeRibbon } from "@/components/outcome-ribbon";
import type { DashboardFilters, DeliveryInterval } from "@/lib/contracts";

const filters: DashboardFilters = {
  actor: "commercial-manager",
  start: "2026-08-26",
  end: "2026-08-26",
  page: 1,
  limit: 25,
};

function ribbonInterval(key: string, start: string): DeliveryInterval {
  return {
    interval_key: key,
    interval_start_at: start,
    delivery_measurement_status: "accepted",
    sla_result_status: "final",
    shortfall_mwh_th: "0.0",
    excess_mwh_th: "0.0",
  } as DeliveryInterval;
}

describe("OutcomeRibbon scroll region", () => {
  it("labels its local scroll and preserves chronological link order", () => {
    render(
      <OutcomeRibbon
        filters={filters}
        intervals={[
          ribbonInterval("interval-first", "2026-08-25T23:00:00Z"),
          ribbonInterval("interval-second", "2026-08-25T23:30:00Z"),
        ]}
      />,
    );

    const region = screen.getByRole("region", {
      name: "Chronological delivery outcomes",
    });
    expect(region).toHaveAttribute("tabindex", "0");
    expect(region).toHaveAttribute("aria-describedby", "outcome-scroll-description");
    expect(screen.getByText(/Oldest interval first/)).toBeVisible();

    const outcomes = within(region).getAllByRole("listitem");
    expect(outcomes).toHaveLength(2);
    expect(outcomes[0]).toHaveAttribute(
      "href",
      expect.stringContaining("interval-first"),
    );
    expect(outcomes[1]).toHaveAttribute(
      "href",
      expect.stringContaining("interval-second"),
    );
  });
});

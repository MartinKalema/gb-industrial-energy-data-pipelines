import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { FilterPanel } from "@/components/filter-panel";

describe("FilterPanel", () => {
  it("switches persona in a separate form so stale scope is reset", () => {
    const { container } = render(
      <FilterPanel
        filters={{
          actor: "commercial-manager",
          start: "2026-08-26",
          end: "2026-08-26",
          customerId: "CUST-002",
          page: 1,
          limit: 25,
        }}
        customers={[]}
        identityRole="commercial_manager"
      />,
    );

    const forms = container.querySelectorAll("form");
    expect(forms).toHaveLength(2);
    expect(forms[0]).toContainElement(screen.getByLabelText("View as"));
    expect(forms[0].querySelector("[name='customer_id']")).toBeNull();
    expect(forms[1].querySelector("input[name='actor']")).toHaveValue(
      "commercial-manager",
    );
    expect(
      screen.getByRole("option", { name: "Commercial manager" }),
    ).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Customer 001" })).toBeInTheDocument();
    expect(screen.queryByText(/Portfolio-wide commercial view/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Switch view" })).toBeVisible();
  });
});

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { MetricCard } from "@/components/metric-card";
import { StateBadge } from "@/components/state-badge";

describe("MetricCard", () => {
  it("makes an unavailable governed metric explicit", () => {
    const { container } = render(
      <MetricCard
        eyebrow="SLA attainment"
        value="Unavailable"
        note="Official percentage"
        state="provisional"
      />,
    );

    expect(screen.getByText("Unavailable")).toBeVisible();
    expect(screen.queryByText("0%")).not.toBeInTheDocument();
    expect(container.firstChild).toHaveAttribute("data-unavailable", "true");
  });
});

describe("StateBadge", () => {
  it("renders a readable status instead of relying only on colour", () => {
    render(<StateBadge status="not_applicable" />);
    expect(screen.getByText("Not Applicable")).toBeVisible();
  });

  it("explains a provisional business result in plain English", () => {
    render(<StateBadge status="provisional" />);
    expect(screen.getByText("Waiting for data")).toBeVisible();
    expect(screen.queryByText("Provisional")).not.toBeInTheDocument();
  });
});

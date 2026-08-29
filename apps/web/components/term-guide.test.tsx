import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { TermGuide } from "@/components/term-guide";

describe("TermGuide", () => {
  it("explains governed result terms without treating unavailable as zero", () => {
    render(<TermGuide />);

    expect(screen.getByText("What do these terms mean?")).toBeVisible();
    expect(screen.getByText("Accepted")).toBeInTheDocument();
    expect(screen.getByText("Final")).toBeInTheDocument();
    expect(screen.getByText("Missing")).toBeInTheDocument();
    expect(screen.getByText("Corrected")).toBeInTheDocument();
    expect(screen.getByText("Not applicable")).toBeInTheDocument();
    expect(screen.getByText(/Unavailable never means zero/)).toBeInTheDocument();
  });

  it("explains the customer-facing delivery limit and 30-minute period terms", () => {
    render(<TermGuide />);

    expect(screen.getByText("Confirmed delivery limit")).toBeInTheDocument();
    expect(
      screen.getByText(/maximum steam energy the delivery point was confirmed able to supply/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/one 30-minute period/i)).toBeInTheDocument();
  });
});

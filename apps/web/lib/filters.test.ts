import { describe, expect, it } from "vitest";
import { dashboardQuery, parseDashboardFilters } from "@/lib/filters";

describe("dashboard filters", () => {
  it("uses available Europe/London reporting dates when no dates are supplied", () => {
    const filters = parseDashboardFilters(
      { actor: "commercial-manager" },
      new Date("2030-01-01T00:00:00Z"),
      { start: "2026-08-26", end: "2026-08-28" },
    );

    expect(filters.start).toBe("2026-08-26");
    expect(filters.end).toBe("2026-08-28");
  });

  it("defaults a long available history to its latest seven operating dates", () => {
    const filters = parseDashboardFilters(
      {},
      new Date("2030-01-01T00:00:00Z"),
      { start: "2026-01-01", end: "2026-08-28" },
    );

    expect(filters.start).toBe("2026-08-22");
    expect(filters.end).toBe("2026-08-28");
  });

  it("uses the London operating date across the UTC midnight boundary", () => {
    const filters = parseDashboardFilters(
      {},
      new Date("2026-08-29T23:30:00Z"),
    );

    // London is on BST, so this instant belongs to 30 August operationally.
    expect(filters.start).toBe("2026-08-24");
    expect(filters.end).toBe("2026-08-30");
  });

  it("preserves accepted status filters and rejects unknown values", () => {
    expect(parseDashboardFilters({ status: "corrected" }).status).toBe("corrected");
    expect(parseDashboardFilters({ status: "accepted" }).status).toBeUndefined();
  });

  it("emits inclusive reporting-date parameters, not UTC timestamp boundaries", () => {
    const query = dashboardQuery({
      actor: "commercial-manager",
      start: "2026-08-26",
      end: "2026-08-26",
      page: 1,
      limit: 25,
    });
    const params = new URLSearchParams(query);

    expect(params.get("start_date")).toBe("2026-08-26");
    expect(params.get("end_date")).toBe("2026-08-26");
    expect(query).not.toContain("T00");
    expect(query).not.toContain("23%3A59");
  });
});

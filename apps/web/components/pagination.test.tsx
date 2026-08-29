import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Pagination } from "@/components/pagination";
import type { DashboardFilters } from "@/lib/contracts";

const dataVersion = `publication-${"d".repeat(32)}`;
const filters: DashboardFilters = {
  actor: "customer-cust-001",
  start: "2026-08-26",
  end: "2026-08-28",
  customerId: "CUST-001",
  siteId: "SITE-001",
  deliveryPointId: "DP-001",
  status: "shortfall",
  page: 2,
  limit: 25,
};

describe("Pagination", () => {
  it("carries the resolved publication and investigation filters both ways", () => {
    render(
      <Pagination filters={filters} total={100} dataVersion={dataVersion} />,
    );

    for (const [name, page] of [
      ["Previous", "1"],
      ["Next", "3"],
    ] as const) {
      const url = new URL(
        screen.getByRole("link", { name }).getAttribute("href")!,
        "http://product.local",
      );
      expect(url.searchParams.get("data_version")).toBe(dataVersion);
      expect(url.searchParams.get("actor")).toBe(filters.actor);
      expect(url.searchParams.get("start_date")).toBe(filters.start);
      expect(url.searchParams.get("end_date")).toBe(filters.end);
      expect(url.searchParams.get("customer_id")).toBe(filters.customerId);
      expect(url.searchParams.get("site_id")).toBe(filters.siteId);
      expect(url.searchParams.get("delivery_point_id")).toBe(
        filters.deliveryPointId,
      );
      expect(url.searchParams.get("status")).toBe(filters.status);
      expect(url.searchParams.get("page")).toBe(page);
      expect(url.searchParams.get("limit")).toBe("25");
    }
  });
});

import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const api = vi.hoisted(() => ({
  getProductContext: vi.fn(),
  getDeliveryIntervalHistory: vi.fn(),
}));

vi.mock("@/lib/product-api", () => ({
  ...api,
  ProductApiError: class ProductApiError extends Error {
    constructor(
      message: string,
      readonly status: number,
      readonly requestId: string | null,
    ) {
      super(message);
    }
  },
}));

import DeliveryIntervalHistoryPage from "@/app/delivery-intervals/[deliveryIntervalKey]/page";

const requestedVersion = `publication-${"a".repeat(32)}`;
const latestVersion = `publication-${"b".repeat(32)}`;

describe("delivery interval history page", () => {
  beforeEach(() => {
    api.getProductContext.mockReset();
    api.getDeliveryIntervalHistory.mockReset();
    api.getProductContext.mockResolvedValue({
      data: {
        data_version: latestVersion,
        identity: { role: "customer" },
      },
    });
    api.getDeliveryIntervalHistory.mockResolvedValue({
      data: {
        interval_key: "a".repeat(64),
        items: [
          {
            interval_key: "a".repeat(64),
            history_key: "history-001",
            customer_id: "CUST-001",
            customer_name: "Northstar Ceramics",
            site_id: "SITE-001",
            site_name: "Sheffield Works",
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
            delivered_mwh_th: "9.8",
            shortfall_mwh_th: "0.2",
            excess_mwh_th: "0.0",
            deliverable_capacity_mwh_th: "12.0",
            billable_mwh_th: "9.8",
            gross_earned_revenue_gbp: "100.0",
            accrued_sla_penalty_gbp: "2.0",
            net_earned_revenue_gbp: "98.0",
            currency_code: "GBP",
            delivery_measurement_status: "accepted",
            commitment_status: "final",
            capacity_status: "final",
            sla_result_status: "final",
            availability_result_status: "final",
            financial_result_status: "final",
            correction_status: "current",
            known_from_at: "2026-08-29T10:00:00Z",
            known_to_at: null,
            is_current_knowledge_state: true,
          },
        ],
        truncated: false,
      },
      requestId: "request-001",
      respondedAt: "2026-08-29T12:00:00Z",
    });
  });

  it("uses the publication carried by the detail link instead of switching to the latest version", async () => {
    const page = await DeliveryIntervalHistoryPage({
      params: Promise.resolve({ deliveryIntervalKey: "a".repeat(64) }),
      searchParams: Promise.resolve({
        actor: "customer-cust-001",
        start_date: "2026-08-26",
        end_date: "2026-08-26",
        customer_id: "CUST-001",
        site_id: "SITE-001",
        page: "2",
        limit: "25",
        data_version: requestedVersion,
      }),
    });

    expect(api.getDeliveryIntervalHistory).toHaveBeenCalledWith(
      "a".repeat(64),
      expect.objectContaining({
        actor: "customer-cust-001",
        customerId: "CUST-001",
        siteId: "SITE-001",
        page: 2,
      }),
      { dataVersion: requestedVersion },
    );
    expect(api.getProductContext).toHaveBeenCalledWith(
      "customer-cust-001",
      { dataVersion: requestedVersion },
    );

    render(page);
    const backUrl = new URL(
      screen.getByRole("link", { name: /back to delivery records/i }).getAttribute("href")!,
      "http://product.local",
    );
    expect(backUrl.searchParams.get("data_version")).toBe(requestedVersion);
    expect(backUrl.searchParams.get("actor")).toBe("customer-cust-001");
    expect(backUrl.searchParams.get("customer_id")).toBe("CUST-001");
    expect(backUrl.searchParams.get("site_id")).toBe("SITE-001");
    expect(backUrl.searchParams.get("page")).toBe("2");
  });

  it("rejects a malformed publication before making a product API request", async () => {
    await expect(
      DeliveryIntervalHistoryPage({
        params: Promise.resolve({ deliveryIntervalKey: "a".repeat(64) }),
        searchParams: Promise.resolve({
          actor: "customer-cust-001",
          data_version: "latest",
        }),
      }),
    ).rejects.toThrow();

    expect(api.getProductContext).not.toHaveBeenCalled();
    expect(api.getDeliveryIntervalHistory).not.toHaveBeenCalled();
  });
});

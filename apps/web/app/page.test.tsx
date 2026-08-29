import { beforeEach, describe, expect, it, vi } from "vitest";

const api = vi.hoisted(() => ({
  getProductContext: vi.fn(),
  getDeliverySummary: vi.fn(),
  getDeliveryIntervals: vi.fn(),
}));

vi.mock("@/lib/product-api", () => api);

import DeliveryPerformancePage from "@/app/page";

const requestedVersion = `publication-${"e".repeat(32)}`;
const latestVersion = `publication-${"f".repeat(32)}`;

describe("delivery performance page publication", () => {
  beforeEach(() => {
    Object.values(api).forEach((mock) => mock.mockReset());
    api.getProductContext.mockResolvedValue({
      data: {
        identity: { actor_id: "commercial-manager", role: "commercial_manager" },
        data_version: latestVersion,
        data_published_at_utc: "2026-08-29T12:00:00Z",
        customers: [],
        available_reporting_dates: null,
      },
    });
    api.getDeliverySummary.mockResolvedValue({
      data: {
        interval_count: 0,
        expected_interval_count: 0,
        commitment_record_count: 0,
        missing_commitment_count: 0,
        applicable_interval_count: 0,
        accepted_applicable_delivery_count: 0,
        final_applicable_capacity_count: 0,
        non_final_financial_count: 0,
        completeness_status: "no_data",
        sla_result_status: "no_data",
        availability_result_status: "no_data",
        financial_result_status: "no_data",
        financial_labels: {
          gross_amount: "earned_revenue",
          deduction: "sla_penalty",
          net_amount: "net_earned_revenue",
        },
      },
      requestId: "request-001",
      respondedAt: "2026-08-29T12:00:00Z",
    });
    api.getDeliveryIntervals.mockResolvedValue({
      data: { items: [], page: 1, limit: 25, total: 0 },
    });
  });

  it("pins context, summary and intervals to an incoming valid publication", async () => {
    await DeliveryPerformancePage({
      searchParams: Promise.resolve({
        actor: "commercial-manager",
        start_date: "2026-08-26",
        end_date: "2026-08-26",
        data_version: requestedVersion,
      }),
    });

    expect(api.getProductContext).toHaveBeenCalledWith("commercial-manager", {
      dataVersion: requestedVersion,
    });
    expect(api.getDeliverySummary).toHaveBeenCalledWith(
      expect.objectContaining({
        actor: "commercial-manager",
        start: "2026-08-26",
        end: "2026-08-26",
      }),
      { dataVersion: requestedVersion },
    );
    expect(api.getDeliveryIntervals).toHaveBeenCalledWith(
      expect.any(Object),
      { dataVersion: requestedVersion },
    );
  });

  it("fails closed on a malformed incoming publication", async () => {
    await expect(
      DeliveryPerformancePage({
        searchParams: Promise.resolve({ data_version: "latest" }),
      }),
    ).rejects.toThrow();

    expect(api.getProductContext).not.toHaveBeenCalled();
    expect(api.getDeliverySummary).not.toHaveBeenCalled();
    expect(api.getDeliveryIntervals).not.toHaveBeenCalled();
  });
});

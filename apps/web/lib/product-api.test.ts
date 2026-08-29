import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getDeliverySummary, getProductContext, ProductApiError } from "@/lib/product-api";
import type { DashboardFilters } from "@/lib/contracts";
import { deliveryIntervalHistorySchema } from "@/lib/schemas";

const filters: DashboardFilters = {
  actor: "commercial-manager",
  start: "2026-08-26",
  end: "2026-08-26",
  status: "provisional",
  page: 2,
  limit: 25,
};

function summaryBody() {
  return {
    scope: {
      start_date: "2026-08-26",
      end_date: "2026-08-26",
      customer_id: null,
      site_id: null,
      delivery_point_id: null,
      status: "provisional",
    },
    interval_count: 48,
    expected_interval_count: 48,
    commitment_record_count: 48,
    missing_commitment_count: 0,
    commitment_completeness_percent: "100.0",
    applicable_interval_count: 48,
    accepted_applicable_delivery_count: 47,
    final_applicable_capacity_count: 48,
    non_final_financial_count: 1,
    completeness_status: "provisional",
    delivery_data_completeness_percent: "97.9",
    known_delivered_mwh_th: "470.0",
    committed_mwh_th: "480.0",
    known_shortfall_mwh_th: "10.0",
    known_excess_mwh_th: "0.0",
    known_billable_mwh_th: "470.0",
    sla_attainment_percent: null,
    sla_result_status: "provisional",
    contractual_availability_percent: null,
    availability_result_status: "provisional",
    known_gross_earned_revenue_gbp: "2500.0",
    accrued_sla_penalty_gbp: null,
    net_earned_revenue_gbp: null,
    financial_result_status: "provisional",
    currency_code: "GBP",
    latest_coverage_published_at_utc: "2026-08-28T12:00:00Z",
    financial_labels: {
      gross_amount: "earned_revenue",
      deduction: "sla_penalty",
      net_amount: "net_earned_revenue",
    },
  };
}

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json",
      "x-request-id": "request-123",
      date: "Sat, 29 Aug 2026 01:00:00 GMT",
    },
  });
}

function mockFetch(body: unknown, status = 200) {
  return vi.fn(async () => response(body, status));
}

describe("product API client", () => {
  beforeEach(() => {
    process.env.PRODUCT_API_URL = "http://product-api:8000";
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete process.env.PRODUCT_API_URL;
  });

  it("sends demo identity only in the server-side header", async () => {
    const fetchImpl = mockFetch({
      identity: { actor_id: "commercial-manager", role: "commercial_manager" },
      customers: [],
      available_reporting_dates: null,
    });

    const result = await getProductContext(
      "commercial-manager",
      fetchImpl as unknown as typeof fetch,
    );
    const [url, init] = fetchImpl.mock.calls[0] as unknown as [URL, RequestInit];

    expect(url.pathname).toBe("/api/v1/context");
    expect(url.searchParams.has("actor")).toBe(false);
    expect(init.headers).toMatchObject({ "X-Demo-Actor": "commercial-manager" });
    expect(result.requestId).toBe("request-123");
  });

  it("uses inclusive reporting dates and preserves null official metrics", async () => {
    const fetchImpl = mockFetch(summaryBody());

    const result = await getDeliverySummary(filters, fetchImpl as unknown as typeof fetch);
    const [url] = fetchImpl.mock.calls[0] as unknown as [URL];

    expect(url.searchParams.get("start_date")).toBe("2026-08-26");
    expect(url.searchParams.get("end_date")).toBe("2026-08-26");
    expect(url.search).not.toContain("T00");
    expect(result.data.sla_attainment_percent).toBeNull();
    expect(result.data.net_earned_revenue_gbp).toBeNull();
  });

  it("rejects non-decimal strings at the service boundary", async () => {
    const body = summaryBody();
    body.known_delivered_mwh_th = "not-a-number";
    const fetchImpl = mockFetch(body);

    await expect(
      getDeliverySummary(filters, fetchImpl as unknown as typeof fetch),
    ).rejects.toMatchObject({
      status: 502,
      requestId: "request-123",
    });
  });

  it("fails clearly when a successful response drifts from the contract", async () => {
    const fetchImpl = mockFetch({
      identity: { actor_id: "commercial-manager" },
      customers: [],
    });

    await expect(
      getProductContext("commercial-manager", fetchImpl as unknown as typeof fetch),
    ).rejects.toMatchObject({
      name: "ProductApiError",
      status: 502,
      requestId: "request-123",
      message: "The delivery data service response did not match the expected contract.",
    } satisfies Partial<ProductApiError>);
  });

  it("surfaces the governed API error message and request id", async () => {
    const fetchImpl = mockFetch(
      { detail: { code: "scope_denied", message: "Customer is outside actor scope." } },
      403,
    );

    await expect(
      getProductContext("commercial-manager", fetchImpl as unknown as typeof fetch),
    ).rejects.toMatchObject({
      status: 403,
      requestId: "request-123",
      message: "Customer is outside actor scope.",
    });
  });

  it("requires an explicit history truncation signal", () => {
    expect(
      deliveryIntervalHistorySchema.safeParse({
        interval_key: "a".repeat(64),
        items: [],
        truncated: false,
      }).success,
    ).toBe(true);
    expect(
      deliveryIntervalHistorySchema.safeParse({
        interval_key: "a".repeat(64),
        items: [],
      }).success,
    ).toBe(false);
  });
});

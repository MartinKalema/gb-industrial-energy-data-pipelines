import { describe, expect, it } from "vitest";
import type { DeliveryPerformanceSummary } from "@/lib/contracts";
import { pendingDataReasons } from "@/lib/summary-status";

function summary(
  overrides: Partial<DeliveryPerformanceSummary> = {},
): DeliveryPerformanceSummary {
  return {
    scope: {
      start_date: "2026-08-26",
      end_date: "2026-08-26",
      customer_id: null,
      site_id: null,
      delivery_point_id: null,
      status: null,
    },
    interval_count: 48,
    expected_interval_count: 48,
    commitment_record_count: 48,
    missing_commitment_count: 0,
    commitment_completeness_percent: "100.000000",
    applicable_interval_count: 48,
    accepted_applicable_delivery_count: 48,
    final_applicable_capacity_count: 48,
    non_final_financial_count: 0,
    completeness_status: "final",
    delivery_data_completeness_percent: "100.000000",
    known_delivered_mwh_th: "192.960000",
    committed_mwh_th: "192.000000",
    known_shortfall_mwh_th: "0.000000",
    known_excess_mwh_th: "0.960000",
    known_billable_mwh_th: "192.000000",
    sla_attainment_percent: "100.000000",
    sla_result_status: "final",
    contractual_availability_percent: "100.000000",
    availability_result_status: "final",
    known_gross_earned_revenue_gbp: "9984.000000000000",
    accrued_sla_penalty_gbp: "0.000000000000",
    net_earned_revenue_gbp: "9984.000000000000",
    financial_result_status: "final",
    currency_code: "GBP",
    latest_coverage_published_at_utc: "2026-08-28T12:00:00Z",
    financial_labels: {
      gross_amount: "earned_revenue",
      deduction: "sla_penalty",
      net_amount: "net_earned_revenue",
    },
    ...overrides,
  };
}

describe("pendingDataReasons", () => {
  it("uses the governed missing commitment count, including withdrawals", () => {
    expect(
      pendingDataReasons(
        summary({
          commitment_record_count: 48,
          missing_commitment_count: 1,
          completeness_status: "provisional",
          sla_result_status: "provisional",
        }),
      ),
    ).toContain("1 30-minute period with a missing or withdrawn commitment");
  });

  it("explains a financial-only blocker", () => {
    expect(
      pendingDataReasons(
        summary({
          non_final_financial_count: 2,
          financial_result_status: "provisional",
        }),
      ),
    ).toEqual([
      "2 30-minute periods with a financial result still awaiting confirmation",
    ]);
  });

  it("describes incomplete capacity evidence as an unconfirmed delivery limit", () => {
    expect(
      pendingDataReasons(
        summary({
          final_applicable_capacity_count: 46,
          availability_result_status: "provisional",
        }),
      ),
    ).toContain("2 30-minute periods without a confirmed delivery limit");
  });

  it("returns no blockers for final results", () => {
    expect(pendingDataReasons(summary())).toEqual([]);
  });
});

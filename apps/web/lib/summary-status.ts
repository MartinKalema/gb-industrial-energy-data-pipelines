import type { DeliveryPerformanceSummary } from "@/lib/contracts";

function intervalLabel(count: number): string {
  return `interval${count === 1 ? "" : "s"}`;
}

/**
 * Explain governed result blockers using counts returned by the product API.
 * These are evidence gaps, not page-loading states.
 */
export function pendingDataReasons(
  summary: DeliveryPerformanceSummary,
): string[] {
  const missingAcceptedDeliveryCount = Math.max(
    0,
    summary.applicable_interval_count -
      summary.accepted_applicable_delivery_count,
  );
  const nonFinalCapacityCount = Math.max(
    0,
    summary.applicable_interval_count - summary.final_applicable_capacity_count,
  );

  const reasons = [
    summary.missing_commitment_count > 0
      ? `${summary.missing_commitment_count} ${intervalLabel(summary.missing_commitment_count)} with a missing or withdrawn commitment`
      : null,
    missingAcceptedDeliveryCount > 0
      ? `${missingAcceptedDeliveryCount} ${intervalLabel(missingAcceptedDeliveryCount)} without accepted delivery evidence`
      : null,
    nonFinalCapacityCount > 0
      ? `${nonFinalCapacityCount} ${intervalLabel(nonFinalCapacityCount)} without final capacity evidence`
      : null,
    summary.non_final_financial_count > 0
      ? `${summary.non_final_financial_count} ${intervalLabel(summary.non_final_financial_count)} with a financial result still awaiting final evidence`
      : null,
  ].filter((reason): reason is string => reason !== null);

  const hasWaitingResult = [
    summary.completeness_status,
    summary.sla_result_status,
    summary.availability_result_status,
    summary.financial_result_status,
  ].some((status) => status === "provisional");

  if (hasWaitingResult && reasons.length === 0) {
    return ["one or more official evidence checks have not yet passed"];
  }
  return reasons;
}

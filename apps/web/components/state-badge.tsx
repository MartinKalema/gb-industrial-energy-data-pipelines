import { formatStatus } from "@/lib/format";

function toneForStatus(status: string | null | undefined) {
  const value = status?.toLowerCase() ?? "";
  if (
    ["shortfall", "missing", "failed", "rejected", "incomplete", "unavailable"].some(
      (term) => value.includes(term),
    )
  ) {
    return "critical";
  }
  if (
    ["provisional", "pending", "estimated", "corrected", "superseded"].some(
      (term) => value.includes(term),
    )
  ) {
    return "attention";
  }
  if (
    ["accepted", "complete", "final", "met", "available", "current"].some(
      (term) => value.includes(term),
    )
  ) {
    return "positive";
  }
  return "neutral";
}

export function StateBadge({ status }: { status: string | null | undefined }) {
  const label = formatStatus(status);
  return (
    <span className={`state-badge state-badge--${toneForStatus(status)}`}>
      <span aria-hidden="true" className="state-badge__dot" />
      {label}
    </span>
  );
}

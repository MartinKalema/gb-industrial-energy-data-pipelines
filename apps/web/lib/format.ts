const UNAVAILABLE = "Unavailable";

function parseDecimal(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function formatNumber(
  value: string | number | null | undefined,
  maximumFractionDigits = 1,
): string {
  const parsed = parseDecimal(value);
  if (parsed === null) return UNAVAILABLE;
  return new Intl.NumberFormat("en-GB", {
    maximumFractionDigits,
    minimumFractionDigits: 0,
  }).format(parsed);
}

export function formatEnergy(
  value: string | number | null | undefined,
): string {
  const formatted = formatNumber(value, 2);
  return formatted === UNAVAILABLE ? formatted : `${formatted} MWhₜₕ`;
}

export function formatGovernedEnergy(
  value: string | number | null | undefined,
): string {
  const parsed = parseDecimal(value);
  if (parsed === null) return UNAVAILABLE;
  const exact = typeof value === "string" ? value.trim() : String(value);
  return `${exact} MWhₜₕ`;
}

export function formatPercent(
  value: string | number | null | undefined,
): string {
  const formatted = formatNumber(value, 1);
  return formatted === UNAVAILABLE ? formatted : `${formatted}%`;
}

export function formatCurrency(
  value: string | number | null | undefined,
  currency: string | null | undefined = "GBP",
): string {
  const parsed = parseDecimal(value);
  if (parsed === null || !currency) return UNAVAILABLE;
  return new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format(parsed);
}

export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return UNAVAILABLE;
  }
  return new Intl.NumberFormat("en-GB").format(value);
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return UNAVAILABLE;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) return UNAVAILABLE;
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "UTC",
  }).format(parsed);
}

export function formatOperatingDateTime(
  value: string | null | undefined,
): string {
  if (!value) return UNAVAILABLE;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) return UNAVAILABLE;
  return new Intl.DateTimeFormat("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "Europe/London",
    timeZoneName: "short",
  }).format(parsed);
}

export function formatOperatingTime(value: string | null | undefined): string {
  if (!value) return UNAVAILABLE;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) return UNAVAILABLE;
  return new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZone: "Europe/London",
  }).format(parsed);
}

export function financialLabels(actor: string) {
  if (actor.startsWith("customer-")) {
    return {
      gross: "Projected service charge before credits",
      adjustment: "Projected SLA credit",
      net: "Projected service charge after credits",
      shortNet: "Projected service charge",
    };
  }
  return {
    gross: "Gross earned revenue",
    adjustment: "Accrued SLA penalty",
    net: "Net earned revenue",
    shortNet: "Net revenue",
  };
}

export function financialLabelsForRole(role: string) {
  return financialLabels(role === "customer" ? "customer-authorized" : "commercial-manager");
}

export function financialLabelsFromContract(labels: {
  gross_amount: "earned_revenue" | "projected_service_charge";
  deduction: "sla_penalty" | "projected_sla_credit";
  net_amount: "net_earned_revenue" | "projected_net_service_charge";
}) {
  return {
    gross:
      labels.gross_amount === "projected_service_charge"
        ? "Projected service charge before credits"
        : "Gross earned revenue",
    adjustment:
      labels.deduction === "projected_sla_credit"
        ? "Projected SLA credit"
        : "Accrued SLA penalty",
    net:
      labels.net_amount === "projected_net_service_charge"
        ? "Projected service charge after credits"
        : "Net earned revenue",
    shortNet:
      labels.net_amount === "projected_net_service_charge"
        ? "Projected service charge"
        : "Net revenue",
  };
}

export function formatStatus(value: string | null | undefined): string {
  if (!value) return UNAVAILABLE;
  if (value.toLowerCase() === "provisional") return "Waiting for data";
  return value
    .toLowerCase()
    .replaceAll("_", " ")
    .replace(/(^|\s)\p{L}/gu, (letter) => letter.toUpperCase());
}

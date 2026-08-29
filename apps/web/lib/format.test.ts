import { describe, expect, it } from "vitest";
import {
  financialLabels,
  financialLabelsFromContract,
  formatCurrency,
  formatEnergy,
  formatGovernedEnergy,
  formatOperatingDateTime,
  formatOperatingTime,
  formatPercent,
} from "@/lib/format";

describe("governed value formatting", () => {
  it("renders absent values as unavailable rather than zero", () => {
    expect(formatEnergy(null)).toBe("Unavailable");
    expect(formatPercent(null)).toBe("Unavailable");
    expect(formatCurrency(null, "GBP")).toBe("Unavailable");
    expect(formatEnergy("0")).toBe("0 MWhₜₕ");
  });

  it("does not invent a currency when the API omits it", () => {
    expect(formatCurrency("500.00", null)).toBe("Unavailable");
  });

  it("preserves governed energy precision without rounding a small value to zero", () => {
    expect(formatGovernedEnergy("0.004000")).toBe("0.004000 MWhₜₕ");
    expect(formatGovernedEnergy(null)).toBe("Unavailable");
  });

  it("labels a British summer interval in its operating timezone", () => {
    expect(formatOperatingDateTime("2026-08-25T23:00:00Z")).toContain("BST");
    expect(formatOperatingDateTime("2026-08-25T23:00:00Z")).toContain("26 Aug");
  });

  it("formats compact Europe/London chart labels", () => {
    expect(formatOperatingTime("2026-08-25T23:00:00Z")).toBe("00:00");
  });

  it("uses customer-facing financial language for customer personas", () => {
    expect(financialLabels("customer-cust-001")).toEqual({
      gross: "Projected service charge before credits",
      adjustment: "Projected SLA credit",
      net: "Projected service charge after credits",
      shortNet: "Projected service charge",
    });
  });

  it("uses the API-authoritative financial vocabulary", () => {
    expect(
      financialLabelsFromContract({
        gross_amount: "earned_revenue",
        deduction: "sla_penalty",
        net_amount: "net_earned_revenue",
      }).net,
    ).toBe("Net earned revenue");
  });
});

import { describe, expect, it } from "vitest";
import {
  parseProductDataVersion,
  withProductDataVersion,
} from "@/lib/data-version";

const dataVersion = `publication-${"c".repeat(32)}`;

describe("product data version navigation", () => {
  it("accepts only an immutable publication identifier", () => {
    expect(parseProductDataVersion(dataVersion)).toBe(dataVersion);
    expect(parseProductDataVersion("version-001")).toBeUndefined();
    expect(parseProductDataVersion(`publication-${"g".repeat(32)}`)).toBeUndefined();
    expect(parseProductDataVersion(undefined)).toBeUndefined();
  });

  it("adds the version without dropping the existing investigation scope", () => {
    const result = withProductDataVersion(
      "actor=customer-cust-001&start_date=2026-08-26&end_date=2026-08-26&page=2",
      dataVersion,
    );
    const params = new URLSearchParams(result);

    expect(params.get("data_version")).toBe(dataVersion);
    expect(params.get("actor")).toBe("customer-cust-001");
    expect(params.get("start_date")).toBe("2026-08-26");
    expect(params.get("end_date")).toBe("2026-08-26");
    expect(params.get("page")).toBe("2");
  });
});

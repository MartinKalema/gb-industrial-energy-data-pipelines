export const PRODUCT_DATA_VERSION_PATTERN = /^publication-[a-f0-9]{32}$/;

type SearchValue = string | string[] | undefined;

export function parseProductDataVersion(value: SearchValue): string | undefined {
  const candidate = Array.isArray(value) ? value[0] : value;
  return candidate && PRODUCT_DATA_VERSION_PATTERN.test(candidate)
    ? candidate
    : undefined;
}

export function withProductDataVersion(
  query: string,
  dataVersion: string | undefined,
): string {
  if (dataVersion === undefined) return query;
  if (!PRODUCT_DATA_VERSION_PATTERN.test(dataVersion)) {
    throw new Error("Product data version has an invalid format");
  }
  const versionedQuery = new URLSearchParams(query);
  versionedQuery.set("data_version", dataVersion);
  return versionedQuery.toString();
}

import type {
  ApiEnvelope,
  DashboardFilters,
  DeliveryIntervalHistory,
  DeliveryIntervalsPage,
  DeliveryPerformanceSummary,
  DemoActor,
  ProductContext,
} from "@/lib/contracts";
import {
  deliveryIntervalHistorySchema,
  deliveryIntervalsPageSchema,
  deliveryPerformanceSummarySchema,
  productContextSchema,
} from "@/lib/schemas";
import type { z } from "zod";

type Fetch = typeof fetch;

interface RequestOptions {
  actor: DemoActor;
  params?: Record<string, string | number | undefined>;
  dataVersion?: string;
  fetchImpl?: Fetch;
}

interface VersionedRequestOptions {
  dataVersion?: string;
  fetchImpl?: Fetch;
}

export class ProductApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly requestId: string | null,
  ) {
    super(message);
    this.name = "ProductApiError";
  }
}

function productApiUrl(): URL {
  const configured = process.env.PRODUCT_API_URL;
  if (!configured) {
    throw new ProductApiError(
      "The delivery data service has not been configured.",
      500,
      null,
    );
  }
  try {
    const url = new URL(configured);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error();
    return url;
  } catch {
    throw new ProductApiError(
      "The delivery data service address is invalid.",
      500,
      null,
    );
  }
}

function productApiTimeoutMs(): number {
  const value = Number.parseInt(process.env.PRODUCT_API_TIMEOUT_MS ?? "90000", 10);
  return Number.isFinite(value) && value >= 1000 && value <= 120_000
    ? value
    : 90_000;
}

function errorDetail(body: unknown): string | null {
  if (!body || typeof body !== "object" || !("detail" in body)) return null;
  const detail = (body as { detail: unknown }).detail;
  if (typeof detail === "string") return detail;
  if (detail && typeof detail === "object" && "message" in detail) {
    const message = (detail as { message: unknown }).message;
    return typeof message === "string" ? message : null;
  }
  return null;
}

export async function requestProductApi<T>(
  path: string,
  schema: z.ZodType<T>,
  { actor, params = {}, dataVersion, fetchImpl = fetch }: RequestOptions,
): Promise<ApiEnvelope<T>> {
  const url = new URL(path.replace(/^\//, ""), productApiUrl().toString().replace(/\/?$/, "/"));
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== "") url.searchParams.set(key, String(value));
  });

  const response = await fetchImpl(url, {
    method: "GET",
    headers: {
      Accept: "application/json",
      "X-Demo-Actor": actor,
      ...(dataVersion ? { "X-Product-Data-Version": dataVersion } : {}),
    },
    cache: "no-store",
    signal: AbortSignal.timeout(productApiTimeoutMs()),
  });
  const requestId = response.headers.get("x-request-id");

  if (!response.ok) {
    let body: unknown = null;
    try {
      body = await response.json();
    } catch {
      // The status code and request id remain useful when the service did not
      // return a JSON error document.
    }
    throw new ProductApiError(
      errorDetail(body) ?? `The delivery data service returned ${response.status}.`,
      response.status,
      requestId,
    );
  }

  const result = schema.safeParse(await response.json());
  if (!result.success) {
    throw new ProductApiError(
      "The delivery data service response did not match the expected contract.",
      502,
      requestId,
    );
  }

  return {
    data: result.data,
    requestId,
    respondedAt: response.headers.get("date"),
  };
}

function scopedParams(filters: DashboardFilters) {
  return {
    start_date: filters.start,
    end_date: filters.end,
    customer_id: filters.customerId,
    site_id: filters.siteId,
    delivery_point_id: filters.deliveryPointId,
    status: filters.status,
  };
}

export function getProductContext(
  actor: DemoActor,
  { dataVersion, fetchImpl }: VersionedRequestOptions = {},
) {
  return requestProductApi<ProductContext>(
    "/api/v1/context",
    productContextSchema,
    { actor, dataVersion, fetchImpl },
  );
}

export function getDeliverySummary(
  filters: DashboardFilters,
  { dataVersion, fetchImpl }: VersionedRequestOptions = {},
) {
  return requestProductApi<DeliveryPerformanceSummary>(
    "/api/v1/delivery-performance/summary",
    deliveryPerformanceSummarySchema,
    {
      actor: filters.actor,
      params: scopedParams(filters),
      dataVersion,
      fetchImpl,
    },
  );
}

export function getDeliveryIntervals(
  filters: DashboardFilters,
  { dataVersion, fetchImpl }: VersionedRequestOptions = {},
) {
  return requestProductApi<DeliveryIntervalsPage>(
    "/api/v1/delivery-performance/intervals",
    deliveryIntervalsPageSchema,
    {
      actor: filters.actor,
      params: {
        ...scopedParams(filters),
        page: filters.page,
        limit: filters.limit,
      },
      dataVersion,
      fetchImpl,
    },
  );
}

export function getDeliveryIntervalHistory(
  intervalKey: string,
  filters: DashboardFilters,
  { dataVersion, fetchImpl }: VersionedRequestOptions = {},
) {
  return requestProductApi<DeliveryIntervalHistory>(
    `/api/v1/delivery-performance/intervals/${encodeURIComponent(intervalKey)}/history`,
    deliveryIntervalHistorySchema,
    {
      actor: filters.actor,
      dataVersion,
      fetchImpl,
    },
  );
}

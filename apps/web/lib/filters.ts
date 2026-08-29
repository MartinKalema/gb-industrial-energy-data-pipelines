import type { DashboardFilters, DemoActor } from "@/lib/contracts";
import { DEMO_ACTORS } from "@/lib/contracts";

export type SearchValue = string | string[] | undefined;
export type SearchParams = Record<string, SearchValue>;

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;
const OPERATING_TIME_ZONE = "Europe/London";
const VALID_STATUSES = new Set([
  "final",
  "provisional",
  "missing",
  "corrected",
  "shortfall",
  "excess",
]);

function first(value: SearchValue): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function isActor(value: string | undefined): value is DemoActor {
  return DEMO_ACTORS.some((actor) => actor.id === value);
}

function validDate(value: string | undefined): value is string {
  if (!value || !ISO_DATE.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(parsed.valueOf()) && parsed.toISOString().startsWith(value);
}

function currentDateRange(now: Date): { start: string; end: string } {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: OPERATING_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const operatingDate = `${values.year}-${values.month}-${values.day}`;
  const end = new Date(`${operatingDate}T00:00:00Z`);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - 6);
  return {
    start: start.toISOString().slice(0, 10),
    end: end.toISOString().slice(0, 10),
  };
}

function recentAvailableRange(available: { start: string; end: string }) {
  const availableStart = new Date(`${available.start}T00:00:00Z`);
  const end = new Date(`${available.end}T00:00:00Z`);
  const recentStart = new Date(end);
  recentStart.setUTCDate(recentStart.getUTCDate() - 6);
  const start = recentStart < availableStart ? availableStart : recentStart;
  return {
    start: start.toISOString().slice(0, 10),
    end: available.end,
  };
}

function boundedInteger(
  value: string | undefined,
  fallback: number,
  maximum: number,
): number {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) && parsed >= 1
    ? Math.min(parsed, maximum)
    : fallback;
}

export function parseDashboardFilters(
  params: SearchParams,
  now = new Date(),
  availableDates?: { start: string; end: string },
): DashboardFilters {
  const defaults = availableDates
    ? recentAvailableRange(availableDates)
    : currentDateRange(now);
  const actor = first(params.actor);
  const start = first(params.start_date);
  const end = first(params.end_date);
  const status = first(params.status);

  return {
    actor: isActor(actor)
      ? actor
      : isActor(process.env.DEFAULT_DEMO_ACTOR)
        ? process.env.DEFAULT_DEMO_ACTOR
        : "commercial-manager",
    start: validDate(start) ? start : defaults.start,
    end: validDate(end) ? end : defaults.end,
    customerId: first(params.customer_id) || undefined,
    siteId: first(params.site_id) || undefined,
    deliveryPointId: first(params.delivery_point_id) || undefined,
    status: status && VALID_STATUSES.has(status) ? status : undefined,
    page: boundedInteger(first(params.page), 1, 100_000),
    limit: boundedInteger(first(params.limit), 25, 200),
  };
}

export function dashboardQuery(
  filters: DashboardFilters,
  overrides: Partial<DashboardFilters> = {},
): string {
  const value = { ...filters, ...overrides };
  const query = new URLSearchParams({
    actor: value.actor,
    start_date: value.start,
    end_date: value.end,
    page: String(value.page),
    limit: String(value.limit),
  });
  if (value.customerId) query.set("customer_id", value.customerId);
  if (value.siteId) query.set("site_id", value.siteId);
  if (value.deliveryPointId) {
    query.set("delivery_point_id", value.deliveryPointId);
  }
  if (value.status) query.set("status", value.status);
  return query.toString();
}

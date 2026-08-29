import { z } from "zod";

const decimal = z
  .string()
  .regex(
    /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/,
    "Expected a finite decimal string",
  )
  .nullable();
const utcTimestamp = z.iso.datetime({ offset: true });
const aggregateStatus = z.enum(["final", "provisional", "not_applicable", "no_data"]);

const deliveryPointSchema = z
  .object({
    delivery_point_id: z.string(),
    delivery_point_name: z.string(),
  })
  .strict();

const siteSchema = z
  .object({
    site_id: z.string(),
    site_name: z.string(),
    delivery_points: z.array(deliveryPointSchema),
  })
  .strict();

const customerSchema = z
  .object({
    customer_id: z.string(),
    display_name: z.string(),
    sites: z.array(siteSchema),
  })
  .strict();

export const productContextSchema = z
  .object({
    identity: z
      .object({
        actor_id: z.string(),
        role: z.enum(["commercial_manager", "customer"]),
      })
      .strict(),
    customers: z.array(customerSchema),
    available_reporting_dates: z
      .object({
        start: z.iso.date(),
        end: z.iso.date(),
        time_zone: z.literal("Europe/London"),
      })
      .strict()
      .nullable(),
  })
  .strict();

const queryScopeSchema = z
  .object({
    start_date: z.iso.date(),
    end_date: z.iso.date(),
    customer_id: z.string().nullable(),
    site_id: z.string().nullable(),
    delivery_point_id: z.string().nullable(),
    status: z
      .enum(["final", "provisional", "missing", "corrected", "shortfall", "excess"])
      .nullable(),
  })
  .strict();

export const deliveryPerformanceSummarySchema = z
  .object({
    scope: queryScopeSchema,
    interval_count: z.number().int().nonnegative(),
    expected_interval_count: z.number().int().nonnegative(),
    commitment_record_count: z.number().int().nonnegative(),
    missing_commitment_count: z.number().int().nonnegative(),
    commitment_completeness_percent: decimal,
    applicable_interval_count: z.number().int().nonnegative(),
    accepted_applicable_delivery_count: z.number().int().nonnegative(),
    final_applicable_capacity_count: z.number().int().nonnegative(),
    non_final_financial_count: z.number().int().nonnegative(),
    completeness_status: aggregateStatus,
    delivery_data_completeness_percent: decimal,
    known_delivered_mwh_th: decimal,
    committed_mwh_th: decimal,
    known_shortfall_mwh_th: decimal,
    known_excess_mwh_th: decimal,
    known_billable_mwh_th: decimal,
    sla_attainment_percent: decimal,
    sla_result_status: aggregateStatus,
    contractual_availability_percent: decimal,
    availability_result_status: aggregateStatus,
    known_gross_earned_revenue_gbp: decimal,
    accrued_sla_penalty_gbp: decimal,
    net_earned_revenue_gbp: decimal,
    financial_result_status: z.enum(["final", "provisional", "no_data"]),
    currency_code: z.string().nullable(),
    latest_coverage_published_at_utc: utcTimestamp.nullable(),
    financial_labels: z
      .object({
        gross_amount: z.enum(["earned_revenue", "projected_service_charge"]),
        deduction: z.enum(["sla_penalty", "projected_sla_credit"]),
        net_amount: z.enum(["net_earned_revenue", "projected_net_service_charge"]),
      })
      .strict(),
  })
  .strict();

export const deliveryIntervalSchema = z
  .object({
    interval_key: z.string(),
    customer_id: z.string(),
    customer_name: z.string(),
    site_id: z.string(),
    site_name: z.string(),
    delivery_point_id: z.string(),
    delivery_point_name: z.string(),
    reporting_date: z.iso.date(),
    local_period_number: z.number().int().min(1).max(50),
    interval_start_at: utcTimestamp,
    interval_end_at: utcTimestamp,
    interval_start_local: z.string().min(1),
    interval_end_local: z.string().min(1),
    operating_timezone: z.string().min(1),
    utc_offset_minutes: z.number().int(),
    is_daylight_saving_time: z.boolean(),
    committed_mwh_th: decimal,
    delivered_mwh_th: decimal,
    shortfall_mwh_th: decimal,
    excess_mwh_th: decimal,
    deliverable_capacity_mwh_th: decimal,
    billable_mwh_th: decimal,
    gross_earned_revenue_gbp: decimal,
    accrued_sla_penalty_gbp: decimal,
    net_earned_revenue_gbp: decimal,
    currency_code: z.string().nullable(),
    delivery_measurement_status: z.string(),
    commitment_status: z.string(),
    capacity_status: z.string(),
    sla_result_status: z.string(),
    availability_result_status: z.string(),
    financial_result_status: z.string(),
    correction_status: z.string(),
  })
  .strict();

export const deliveryIntervalsPageSchema = z
  .object({
    items: z.array(deliveryIntervalSchema),
    page: z.number().int().positive(),
    limit: z.number().int().min(1).max(200),
    total: z.number().int().nonnegative(),
  })
  .strict();

export const deliveryIntervalHistorySchema = z
  .object({
    interval_key: z.string(),
    items: z.array(
      deliveryIntervalSchema.extend({
        history_key: z.string(),
        known_from_at: utcTimestamp,
        known_to_at: utcTimestamp.nullable(),
        is_current_knowledge_state: z.boolean(),
      }),
    ),
    truncated: z.boolean(),
  })
  .strict();

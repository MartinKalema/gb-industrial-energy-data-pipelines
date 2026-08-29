export type DemoActor =
  | "commercial-manager"
  | "customer-cust-001"
  | "customer-cust-002";

export const DEMO_ACTORS: ReadonlyArray<{
  id: DemoActor;
  label: string;
  description: string;
}> = [
  {
    id: "commercial-manager",
    label: "Commercial manager",
    description: "All customers",
  },
  {
    id: "customer-cust-001",
    label: "Customer 001",
    description: "Customer 001 only",
  },
  {
    id: "customer-cust-002",
    label: "Customer 002",
    description: "Customer 002 only",
  },
];

export type DecimalValue = string | null;

export interface SiteOption {
  site_id: string;
  site_name: string;
  delivery_points: Array<{
    delivery_point_id: string;
    delivery_point_name: string;
  }>;
}

export interface CustomerOption {
  customer_id: string;
  display_name: string;
  sites: SiteOption[];
}

export interface ProductContext {
  identity: {
    actor_id: string;
    role: string;
  };
  data_version: string | null;
  data_published_at_utc: string | null;
  customers: CustomerOption[];
  available_reporting_dates: {
    start: string;
    end: string;
    time_zone: string;
  } | null;
}

export interface DeliveryPerformanceSummary {
  scope: {
    start_date: string;
    end_date: string;
    customer_id: string | null;
    site_id: string | null;
    delivery_point_id: string | null;
    status: string | null;
  };
  interval_count: number;
  expected_interval_count: number;
  applicable_interval_count: number;
  accepted_applicable_delivery_count: number;
  commitment_record_count: number;
  missing_commitment_count: number;
  final_applicable_capacity_count: number;
  non_final_financial_count: number;
  completeness_status: string;
  delivery_data_completeness_percent: DecimalValue;
  commitment_completeness_percent: DecimalValue;
  known_delivered_mwh_th: DecimalValue;
  committed_mwh_th: DecimalValue;
  known_shortfall_mwh_th: DecimalValue;
  known_excess_mwh_th: DecimalValue;
  known_billable_mwh_th: DecimalValue;
  sla_attainment_percent: DecimalValue;
  contractual_availability_percent: DecimalValue;
  known_gross_earned_revenue_gbp: DecimalValue;
  accrued_sla_penalty_gbp: DecimalValue;
  net_earned_revenue_gbp: DecimalValue;
  currency_code: string | null;
  sla_result_status: string;
  availability_result_status: string;
  financial_result_status: string;
  latest_coverage_published_at_utc: string | null;
  financial_labels: {
    gross_amount: "earned_revenue" | "projected_service_charge";
    deduction: "sla_penalty" | "projected_sla_credit";
    net_amount: "net_earned_revenue" | "projected_net_service_charge";
  };
}

export interface DeliveryInterval {
  interval_key: string;
  customer_id: string;
  site_id: string;
  delivery_point_id: string;
  interval_start_at: string;
  interval_end_at: string;
  reporting_date: string;
  local_period_number: number;
  customer_name: string;
  site_name: string;
  delivery_point_name: string;
  interval_start_local: string;
  interval_end_local: string;
  operating_timezone: string;
  utc_offset_minutes: number;
  is_daylight_saving_time: boolean;
  deliverable_capacity_mwh_th: DecimalValue;
  billable_mwh_th: DecimalValue;
  commitment_status: string;
  capacity_status: string;
  committed_mwh_th: DecimalValue;
  delivered_mwh_th: DecimalValue;
  shortfall_mwh_th: DecimalValue;
  excess_mwh_th: DecimalValue;
  gross_earned_revenue_gbp: DecimalValue;
  accrued_sla_penalty_gbp: DecimalValue;
  net_earned_revenue_gbp: DecimalValue;
  currency_code: string | null;
  delivery_measurement_status: string;
  sla_result_status: string;
  availability_result_status: string;
  financial_result_status: string;
  correction_status: string;
}

export interface DeliveryIntervalsPage {
  items: DeliveryInterval[];
  page: number;
  limit: number;
  total: number;
}

export interface DeliveryIntervalHistoryItem extends DeliveryInterval {
  history_key: string;
  known_from_at: string;
  known_to_at: string | null;
  is_current_knowledge_state: boolean;
}

export interface DeliveryIntervalHistory {
  interval_key: string;
  items: DeliveryIntervalHistoryItem[];
  truncated: boolean;
}

export interface ApiEnvelope<T> {
  data: T;
  requestId: string | null;
  respondedAt: string | null;
}

export interface DashboardFilters {
  actor: DemoActor;
  start: string;
  end: string;
  customerId?: string;
  siteId?: string;
  status?: string;
  page: number;
  limit: number;
}

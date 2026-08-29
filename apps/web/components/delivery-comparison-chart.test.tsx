import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import {
  buildDeliveryChartData,
  DELIVERY_PROFILE_INTERPOLATION,
  DeliveryComparisonChart,
  groupDeliveryPointSeries,
  RecordedZeroCircle,
  shouldShowChartNavigator,
  UnavailableDiamond,
} from "@/components/delivery-comparison-chart";
import type { DeliveryInterval } from "@/lib/contracts";

function interval(
  key: string,
  period: number,
  start: string,
  committed: string | null,
  delivered: string | null,
): DeliveryInterval {
  return {
    interval_key: key,
    customer_id: "CUST-001",
    customer_name: "North Foundry",
    site_id: "SITE-001",
    site_name: "North Foundry Works",
    delivery_point_id: "DP-001",
    delivery_point_name: "Main steam header",
    reporting_date: "2026-08-26",
    local_period_number: period,
    interval_start_at: start,
    interval_end_at: "2026-08-25T23:30:00Z",
    interval_start_local: "2026-08-26T00:00:00",
    interval_end_local: "2026-08-26T00:30:00",
    operating_timezone: "Europe/London",
    utc_offset_minutes: 60,
    is_daylight_saving_time: true,
    committed_mwh_th: committed,
    delivered_mwh_th: delivered,
    shortfall_mwh_th: null,
    excess_mwh_th: null,
    deliverable_capacity_mwh_th: "12.0",
    billable_mwh_th: null,
    gross_earned_revenue_gbp: null,
    accrued_sla_penalty_gbp: null,
    net_earned_revenue_gbp: null,
    currency_code: "GBP",
    delivery_measurement_status: delivered === null ? "missing" : "accepted",
    commitment_status: committed === null ? "missing" : "committed",
    capacity_status: "final",
    sla_result_status: delivered === null ? "provisional" : "final",
    availability_result_status: "final",
    financial_result_status: delivered === null ? "provisional" : "final",
    correction_status: "current",
  };
}

const visibleIntervals = [
  {
    ...interval("interval-002", 2, "2026-08-25T23:30:00Z", null, "12.0"),
    shortfall_mwh_th: null,
    excess_mwh_th: null,
  },
  {
    ...interval("interval-001", 1, "2026-08-25T23:00:00Z", "10.0", "8.0"),
    shortfall_mwh_th: "2.000000",
    excess_mwh_th: "0.000000",
  },
  {
    ...interval("interval-003", 1, "2026-08-25T23:00:00Z", "4.0", "4.5"),
    customer_id: "CUST-002",
    customer_name: "South Ceramics",
    site_id: "SITE-002",
    site_name: "South Ceramics Works",
    delivery_point_id: "DP-002",
    delivery_point_name: "Kiln steam header",
    shortfall_mwh_th: "0.000000",
    excess_mwh_th: "0.500000",
  },
];

Object.defineProperties(HTMLElement.prototype, {
  hasPointerCapture: { configurable: true, value: vi.fn(() => false) },
  releasePointerCapture: { configurable: true, value: vi.fn() },
  scrollIntoView: { configurable: true, value: vi.fn() },
});

describe("delivery chart data", () => {
  it("separates delivery points and sorts each series chronologically", () => {
    const series = groupDeliveryPointSeries(visibleIntervals);

    expect(series).toHaveLength(2);
    expect(series[0].intervals.map((item) => item.interval_key)).toEqual([
      "interval-001",
      "interval-002",
    ]);
    expect(series[1].intervals).toHaveLength(1);
  });

  it("uses governed exception fields and keeps missing distinct from zero", () => {
    const data = buildDeliveryChartData(visibleIntervals);

    expect(data[0]).toMatchObject({
      exception: null,
      exceptionKind: "unavailable",
      zeroMarker: null,
      unavailableMarker: 0,
    });
    expect(data[1]).toMatchObject({
      exception: -2,
      exceptionKind: "shortfall",
      zeroMarker: null,
      unavailableMarker: null,
    });
    expect(data[2]).toMatchObject({
      exception: 0.5,
      exceptionKind: "excess",
      zeroMarker: null,
      unavailableMarker: null,
    });
  });

  it("assigns zero and unavailable markers only to their own interval row", () => {
    const recordedZero = {
      ...interval("recorded-zero", 1, "2026-08-25T23:00:00Z", "5.0", "5.0"),
      shortfall_mwh_th: "0.000000",
      excess_mwh_th: "0.000000",
    };
    const excess = {
      ...interval("excess", 2, "2026-08-25T23:30:00Z", "5.0", "5.1"),
      shortfall_mwh_th: "0.000000",
      excess_mwh_th: "0.100000",
    };
    const unavailable = interval(
      "unavailable",
      3,
      "2026-08-26T00:00:00Z",
      null,
      "5.0",
    );

    const data = buildDeliveryChartData([recordedZero, excess, unavailable]);

    expect(data.map(({ intervalKey, exceptionKind, zeroMarker, unavailableMarker }) => ({
      intervalKey,
      exceptionKind,
      zeroMarker,
      unavailableMarker,
    }))).toEqual([
      {
        intervalKey: "recorded-zero",
        exceptionKind: "none",
        zeroMarker: 0,
        unavailableMarker: null,
      },
      {
        intervalKey: "excess",
        exceptionKind: "excess",
        zeroMarker: null,
        unavailableMarker: null,
      },
      {
        intervalKey: "unavailable",
        exceptionKind: "unavailable",
        zeroMarker: null,
        unavailableMarker: 0,
      },
    ]);
  });

  it("renders marker shapes only when Recharts supplies a real zero marker", () => {
    const { container, rerender } = render(
      <UnavailableDiamond cx={20} cy={30} value={null} />,
    );
    expect(container.querySelector("rect")).toBeNull();

    rerender(<UnavailableDiamond cx={20} cy={30} value={0} />);
    expect(container.querySelector("rect")).toHaveAttribute("x", "16");

    rerender(<RecordedZeroCircle cx={20} cy={30} value={null} />);
    expect(container.querySelector("circle")).toBeNull();

    rerender(<RecordedZeroCircle cx={20} cy={30} value={0} />);
    expect(container.querySelector("circle")).toHaveAttribute("cx", "20");
  });

  it("uses proportionate half-hour gap buckets and discrete step interpolation", () => {
    const oneHourGap = buildDeliveryChartData([
      interval("period-3", 3, "2026-08-26T00:00:00Z", "5.0", "4.8"),
      interval("period-5", 5, "2026-08-26T01:00:00Z", "5.0", "4.7"),
    ]);
    const twelveHourGap = buildDeliveryChartData([
      interval("period-3", 3, "2026-08-26T00:00:00Z", "5.0", "4.8"),
      interval("period-27", 27, "2026-08-26T12:00:00Z", "5.0", "4.7"),
    ]);

    expect(oneHourGap.filter((datum) => datum.isGap)).toHaveLength(1);
    expect(twelveHourGap.filter((datum) => datum.isGap)).toHaveLength(23);
    expect(twelveHourGap).toHaveLength(25);
    expect(twelveHourGap[1]).toMatchObject({
      timestamp: Date.parse("2026-08-26T00:30:00Z"),
      isGap: true,
      committed: null,
      delivered: null,
      capacity: null,
    });
    expect(DELIVERY_PROFILE_INTERPOLATION).toBe("stepAfter");
  });

  it("does not insert a gap between adjacent half-hour intervals", () => {
    const adjacent = buildDeliveryChartData([
      interval("period-3", 3, "2026-08-26T00:00:00Z", "5.0", "4.8"),
      interval("period-4", 4, "2026-08-26T00:30:00Z", "5.0", "4.7"),
    ]);

    expect(adjacent.filter((datum) => datum.isGap)).toHaveLength(0);
  });

  it("omits the navigator for normal one-day scopes and keeps it for long series", () => {
    expect(shouldShowChartNavigator(48)).toBe(false);
    expect(shouldShowChartNavigator(96)).toBe(false);
    expect(shouldShowChartNavigator(97)).toBe(true);
  });
});

describe("DeliveryComparisonChart", () => {
  it("provides delivery, exception and evidence views with exact accessible values", async () => {
    const user = userEvent.setup();
    render(<DeliveryComparisonChart intervals={visibleIntervals} total={3} />);

    expect(
      screen.getByRole("heading", { name: "Delivery performance over time" }),
    ).toBeVisible();
    expect(screen.getByRole("combobox", { name: "Delivery point" })).toHaveTextContent(
      "North Foundry · North Foundry Works · Main steam header",
    );
    expect(screen.getByRole("list", { name: "Exact interval chart values" })).toHaveTextContent(
      "Committed 10.0 MWhₜₕ. Delivered 8.0 MWhₜₕ",
    );

    await user.click(screen.getByRole("button", { name: "Exceptions" }));
    expect(screen.getByRole("button", { name: "Exceptions" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );

    await user.click(screen.getByRole("button", { name: "Evidence" }));
    expect(
      screen.getByRole("region", { name: /evidence status by interval/i }),
    ).toBeVisible();
    expect(screen.getByRole("img", { name: /Commitment.*Missing/i })).toBeVisible();

    await user.click(screen.getByRole("combobox", { name: "Delivery point" }));
    await user.click(
      screen.getByRole("option", {
        name: "South Ceramics · South Ceramics Works · Kiln steam header",
      }),
    );
    expect(screen.getByRole("combobox", { name: "Delivery point" })).toHaveTextContent(
      "South Ceramics",
    );
  });

  it("states when the bounded chart does not cover every matching interval", () => {
    render(<DeliveryComparisonChart intervals={visibleIntervals} total={250} />);

    expect(screen.getByRole("status")).toHaveTextContent(
      /first 3 of 250 matching intervals/i,
    );
  });

  it("distinguishes not-applicable, waiting, missing and rejected evidence", async () => {
    const user = userEvent.setup();
    const governedStates = {
      ...interval("interval-004", 4, "2026-08-26T00:30:00Z", "0.0", null),
      commitment_status: "no_commitment",
      delivery_measurement_status: "unit_mismatch",
      capacity_status: "provisional",
      financial_result_status: "not_applicable",
    };
    render(<DeliveryComparisonChart intervals={[governedStates]} total={1} />);

    await user.click(screen.getByRole("button", { name: "Evidence" }));

    expect(screen.getByRole("img", { name: /Commitment.*No Commitment/i })).toHaveClass(
      "evidence-cell--not-applicable",
    );
    expect(screen.getByRole("img", { name: /Delivery.*Unit Mismatch/i })).toHaveClass(
      "evidence-cell--invalid",
    );
    expect(screen.getByRole("img", { name: /Capacity.*Waiting for data/i })).toHaveClass(
      "evidence-cell--waiting",
    );
    const waitingSignal = screen.getByText("Waiting for evidence").closest("div");
    expect(waitingSignal).not.toBeNull();
    expect(within(waitingSignal!).getByText("1")).toBeVisible();
  });
});

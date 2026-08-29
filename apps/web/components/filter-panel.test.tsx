import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

const router = vi.hoisted(() => ({ push: vi.fn() }));
vi.mock("next/navigation", () => ({
  useRouter: () => router,
}));

import { FilterPanel } from "@/components/filter-panel";

const customers = [
  {
    customer_id: "CUST-001",
    display_name: "Northstar Ceramics",
    sites: [
      {
        site_id: "SITE-001",
        site_name: "Sheffield Works",
        delivery_points: [
          {
            delivery_point_id: "DP-001",
            delivery_point_name: "Main steam header",
          },
        ],
      },
    ],
  },
  {
    customer_id: "CUST-002",
    display_name: "Southbank Textiles",
    sites: [
      {
        site_id: "SITE-002",
        site_name: "Leeds Works",
        delivery_points: [
          {
            delivery_point_id: "DP-002",
            delivery_point_name: "Process steam header",
          },
        ],
      },
    ],
  },
];
const dataVersion = `publication-${"a".repeat(32)}`;

Object.defineProperties(HTMLElement.prototype, {
  hasPointerCapture: { configurable: true, value: vi.fn(() => false) },
  releasePointerCapture: { configurable: true, value: vi.fn() },
  scrollIntoView: { configurable: true, value: vi.fn() },
});

async function chooseOption(
  user: ReturnType<typeof userEvent.setup>,
  label: string,
  option: string,
) {
  await user.click(screen.getByRole("combobox", { name: label }));
  await user.click(screen.getByRole("option", { name: option }));
}

describe("FilterPanel", () => {
  beforeEach(() => {
    router.push.mockReset();
  });

  it("places persona and analytical scope in one horizontal filter form", () => {
    const { container } = render(
      <FilterPanel
        filters={{
          actor: "commercial-manager",
          start: "2026-08-26",
          end: "2026-08-26",
          customerId: "CUST-001",
          siteId: "SITE-001",
          deliveryPointId: "DP-001",
          page: 1,
          limit: 25,
        }}
        customers={customers}
        dataVersion={dataVersion}
      />,
    );

    expect(container.querySelectorAll("form")).toHaveLength(1);
    expect(screen.getByRole("combobox", { name: "View" })).toHaveTextContent(
      "Commercial manager",
    );
    expect(screen.getByRole("combobox", { name: "Delivery point" })).toHaveTextContent(
      "Main steam header",
    );
    expect(container.querySelector('input[name="actor"]')).toHaveValue("commercial-manager");
    expect(container.querySelector('input[name="delivery_point_id"]')).toHaveValue("DP-001");
    expect(screen.getByText("3 active")).toBeVisible();
    expect(screen.getByRole("button", { name: "Update analysis" })).toBeVisible();
  });

  it("uses Next navigation to apply a view change without stale dependent filters", async () => {
    const user = userEvent.setup();
    render(
      <FilterPanel
        filters={{
          actor: "commercial-manager",
          start: "2026-08-26",
          end: "2026-08-26",
          customerId: "CUST-001",
          siteId: "SITE-001",
          deliveryPointId: "DP-001",
          page: 1,
          limit: 25,
        }}
        customers={customers}
        dataVersion={dataVersion}
      />,
    );

    await chooseOption(user, "View", "Customer 001");

    expect(screen.getByRole("combobox", { name: "Customer" })).toHaveTextContent("All customers");
    expect(screen.getByRole("combobox", { name: "Industrial site" })).toHaveTextContent("All sites");
    expect(screen.getByRole("combobox", { name: "Delivery point" })).toHaveTextContent(
      "All delivery points",
    );
    await user.click(screen.getByRole("combobox", { name: "Customer" }));
    expect(screen.queryByRole("option", { name: "Southbank Textiles" })).not.toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(router.push).toHaveBeenCalledWith(
      expect.stringMatching(/actor=customer-cust-001/),
    );
    expect(router.push).toHaveBeenCalledWith(
      expect.stringMatching(/data_version=publication-/),
    );
  });

  it("clears optional filters without losing dates or the publication version", () => {
    render(
      <FilterPanel
        filters={{
          actor: "commercial-manager",
          start: "2026-08-26",
          end: "2026-08-27",
          customerId: "CUST-001",
          page: 1,
          limit: 25,
        }}
        customers={customers}
        dataVersion={dataVersion}
      />,
    );

    const clearUrl = new URL(
      screen.getByRole("link", { name: "Clear optional filters" }).getAttribute("href")!,
      "http://product.local",
    );
    expect(clearUrl.searchParams.get("start_date")).toBe("2026-08-26");
    expect(clearUrl.searchParams.get("end_date")).toBe("2026-08-27");
    expect(clearUrl.searchParams.get("data_version")).toBe(dataVersion);
    expect(clearUrl.searchParams.has("customer_id")).toBe(false);
  });
});

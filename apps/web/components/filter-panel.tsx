"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { DashboardSelect } from "@/components/dashboard-select";
import type { CustomerOption, DashboardFilters } from "@/lib/contracts";
import { DEMO_ACTORS } from "@/lib/contracts";

interface FilterPanelProps {
  filters: DashboardFilters;
  customers: CustomerOption[];
  dataVersion: string | undefined;
}

const STATUS_OPTIONS = [
  ["", "All delivery states"],
  ["final", "Final result"],
  ["provisional", "Waiting for data"],
  ["missing", "Missing measurement"],
  ["corrected", "Corrected record"],
  ["shortfall", "Shortfall"],
  ["excess", "Excess delivery"],
] as const;

export function FilterPanel({
  filters,
  customers,
  dataVersion,
}: FilterPanelProps) {
  const router = useRouter();
  const [selectedActor, setSelectedActor] = useState(filters.actor);
  const [selectedCustomer, setSelectedCustomer] = useState(filters.customerId ?? "");
  const [selectedSite, setSelectedSite] = useState(filters.siteId ?? "");
  const [selectedDeliveryPoint, setSelectedDeliveryPoint] = useState(
    filters.deliveryPointId ?? "",
  );
  const [selectedStatus, setSelectedStatus] = useState(filters.status ?? "");
  const actorCustomerId = selectedActor === "customer-cust-001"
    ? "CUST-001"
    : selectedActor === "customer-cust-002"
      ? "CUST-002"
      : null;
  const authorizedCustomers = actorCustomerId
    ? customers.filter((customer) => customer.customer_id === actorCustomerId)
    : customers;
  const customersInScope = selectedCustomer
    ? authorizedCustomers.filter((customer) => customer.customer_id === selectedCustomer)
    : authorizedCustomers;
  const deliveryPointSites = customersInScope.flatMap((customer) =>
    customer.sites.filter(
      (site) => !selectedSite || site.site_id === selectedSite,
    ),
  );
  const deliveryPoints = deliveryPointSites.flatMap((site) =>
    site.delivery_points.map((deliveryPoint) => {
      const customer = authorizedCustomers.find((candidate) =>
        candidate.sites.some((candidateSite) => candidateSite.site_id === site.site_id),
      );
      return {
        ...deliveryPoint,
        siteName: site.site_name,
        customerName: customer?.display_name ?? "Customer",
      };
    }),
  );
  const activeFilterCount = [
    selectedCustomer,
    selectedSite,
    selectedDeliveryPoint,
    selectedStatus,
  ].filter(Boolean).length;
  const clearQuery = new URLSearchParams({
    actor: filters.actor,
    start_date: filters.start,
    end_date: filters.end,
    page: "1",
    limit: String(filters.limit),
  });
  if (dataVersion) clearQuery.set("data_version", dataVersion);

  function switchActor(actor: typeof selectedActor) {
    setSelectedActor(actor);
    const nextScope = new URLSearchParams({
      actor,
      start_date: filters.start,
      end_date: filters.end,
      page: "1",
      limit: String(filters.limit),
    });
    if (dataVersion) nextScope.set("data_version", dataVersion);
    router.push(`/?${nextScope.toString()}`);
  }

  const clearFilterLabel = `Clear ${activeFilterCount} ${
    activeFilterCount === 1 ? "filter" : "filters"
  }`;

  return (
    <aside className="filter-panel" aria-label="Analysis filters">
      <form action="/" method="get" className="filter-form" aria-label="Filter delivery analysis">
        {dataVersion ? <input type="hidden" name="data_version" value={dataVersion} /> : null}
        <div className="filter-field">
          <label className="field-label" htmlFor="actor">
            View
          </label>
          <DashboardSelect
            id="actor"
            name="actor"
            value={selectedActor}
            groups={[{
              options: DEMO_ACTORS.map((actor) => ({
                value: actor.id,
                label: actor.label,
              })),
            }]}
            onValueChange={(actor) => {
              setSelectedCustomer("");
              setSelectedSite("");
              setSelectedDeliveryPoint("");
              switchActor(actor as typeof selectedActor);
            }}
          />
        </div>

        <div className="filter-field filter-field--date">
          <label className="field-label" htmlFor="start">
            From
          </label>
            <input
              id="start"
              name="start_date"
              type="date"
              defaultValue={filters.start}
              required
            />
        </div>
        <div className="filter-field filter-field--date">
          <label className="field-label" htmlFor="end">
            Through
          </label>
            <input
              id="end"
              name="end_date"
              type="date"
              defaultValue={filters.end}
              required
            />
        </div>

        <div className="filter-field">
          <label className="field-label" htmlFor="customer_id">
            Customer
          </label>
          <DashboardSelect
            id="customer_id"
            name="customer_id"
            value={selectedCustomer}
            groups={[{
              options: [
                { value: "", label: "All customers" },
                ...authorizedCustomers.map((customer) => ({
                  value: customer.customer_id,
                  label: customer.display_name,
                })),
              ],
            }]}
            onValueChange={(customerId) => {
              setSelectedCustomer(customerId);
              setSelectedSite("");
              setSelectedDeliveryPoint("");
            }}
          />
        </div>

        <div className="filter-field">
          <label className="field-label" htmlFor="site_id">
            Industrial site
          </label>
          <DashboardSelect
            id="site_id"
            name="site_id"
            value={selectedSite}
            groups={[
              { options: [{ value: "", label: "All sites" }] },
              ...customersInScope.map((customer) => ({
                label: customer.display_name,
                options: customer.sites.map((site) => ({
                  value: site.site_id,
                  label: site.site_name,
                })),
              })),
            ]}
            onValueChange={(siteId) => {
              setSelectedSite(siteId);
              setSelectedDeliveryPoint("");
            }}
          />
        </div>

        <div className="filter-field">
          <label className="field-label" htmlFor="delivery_point_id">
            Delivery point
          </label>
          <DashboardSelect
            id="delivery_point_id"
            name="delivery_point_id"
            value={selectedDeliveryPoint}
            groups={[{
              options: [
                { value: "", label: "All delivery points" },
                ...deliveryPoints.map((deliveryPoint) => ({
                  value: deliveryPoint.delivery_point_id,
                  label: `${deliveryPoint.customerName} · ${deliveryPoint.siteName} · ${deliveryPoint.delivery_point_name}`,
                })),
              ],
            }]}
            onValueChange={setSelectedDeliveryPoint}
          />
        </div>

        <div className="filter-field">
          <label className="field-label" htmlFor="status">
            Delivery state
          </label>
          <DashboardSelect
            id="status"
            name="status"
            value={selectedStatus}
            groups={[{
              options: STATUS_OPTIONS.map(([value, label]) => ({ value, label })),
            }]}
            onValueChange={setSelectedStatus}
          />
        </div>

        <input type="hidden" name="page" value="1" />
        <input type="hidden" name="limit" value={String(filters.limit)} />
        <div className="filter-actions">
          <span className="field-label">Actions</span>
          <div className="filter-actions__controls">
            <button className="primary-button" type="submit">
              Apply filters
            </button>
            {activeFilterCount > 0 ? (
              <a
                className="clear-filter-link"
                href={`/?${clearQuery.toString()}`}
                aria-label={`${clearFilterLabel} and keep the selected dates`}
              >
                {clearFilterLabel}
              </a>
            ) : null}
          </div>
        </div>
      </form>
    </aside>
  );
}

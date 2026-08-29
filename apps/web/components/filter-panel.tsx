import type { CustomerOption, DashboardFilters } from "@/lib/contracts";
import { DEMO_ACTORS } from "@/lib/contracts";

interface FilterPanelProps {
  filters: DashboardFilters;
  customers: CustomerOption[];
  identityRole: string;
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
  identityRole,
}: FilterPanelProps) {
  const selectedActor =
    DEMO_ACTORS.find((actor) => actor.id === filters.actor) ?? DEMO_ACTORS[0];

  return (
    <aside className="filter-panel" aria-labelledby="scope-heading">
      <div className="filter-panel__heading">
        <p className="section-kicker">Investigation scope</p>
        <h2 id="scope-heading">Choose the evidence window</h2>
      </div>

      <form action="/" method="get" className="persona-form">
        <fieldset className="persona-fieldset">
          <legend>Demo persona</legend>
          <p className="field-help">
            Local authorization demonstration — this is not a production login.
          </p>
          <label className="field-label" htmlFor="actor">
            View as
          </label>
          <select id="actor" name="actor" defaultValue={filters.actor}>
            {DEMO_ACTORS.map((actor) => (
              <option key={actor.id} value={actor.id}>
                {actor.label}
              </option>
            ))}
          </select>
          <p className="actor-scope">
            Scope: <strong>{selectedActor.description}</strong>
            <span className="sr-only">. API role: {identityRole}</span>
          </p>
          <button className="secondary-button" type="submit">
            Switch view
          </button>
        </fieldset>
      </form>

      <form action="/" method="get" className="filter-form">
        <input type="hidden" name="actor" value={filters.actor} />
        <div className="date-pair">
          <div>
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
          <div>
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
        </div>
        <p className="field-help">
          Dates are inclusive Europe/London operating dates. Choose no more than
          31 days.
        </p>

        <div>
          <label className="field-label" htmlFor="customer_id">
            Customer
          </label>
          <select
            id="customer_id"
            name="customer_id"
            defaultValue={filters.customerId ?? ""}
          >
            <option value="">All customers in scope</option>
            {customers.map((customer) => (
              <option key={customer.customer_id} value={customer.customer_id}>
                {customer.display_name}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="field-label" htmlFor="site_id">
            Industrial site
          </label>
          <select
            id="site_id"
            name="site_id"
            defaultValue={filters.siteId ?? ""}
          >
            <option value="">All sites in scope</option>
            {customers.map((customer) => (
              <optgroup key={customer.customer_id} label={customer.display_name}>
                {customer.sites.map((site) => (
                  <option key={site.site_id} value={site.site_id}>
                    {site.site_name}
                  </option>
                ))}
              </optgroup>
            ))}
          </select>
        </div>

        <div>
          <label className="field-label" htmlFor="status">
            Delivery state
          </label>
          <select id="status" name="status" defaultValue={filters.status ?? ""}>
            {STATUS_OPTIONS.map(([value, label]) => (
              <option key={label} value={value}>
                {label}
              </option>
            ))}
          </select>
        </div>

        <input type="hidden" name="page" value="1" />
        <input type="hidden" name="limit" value={String(filters.limit)} />
        <button className="primary-button" type="submit">
          Apply investigation scope
        </button>
      </form>
    </aside>
  );
}

import { StateBadge } from "@/components/state-badge";

interface MetricCardProps {
  eyebrow: string;
  value: string;
  note: string;
  state?: string | null;
  featured?: boolean;
}

export function MetricCard({
  eyebrow,
  value,
  note,
  state,
  featured = false,
}: MetricCardProps) {
  const unavailable = value === "Unavailable";
  return (
    <article
      className={`metric-card${featured ? " metric-card--featured" : ""}`}
      data-unavailable={unavailable ? "true" : undefined}
    >
      <div className="metric-card__header">
        <p className="metric-card__eyebrow">{eyebrow}</p>
        {state ? <StateBadge status={state} /> : null}
      </div>
      <p className="metric-card__value">{value}</p>
      <p className="metric-card__note">{note}</p>
    </article>
  );
}

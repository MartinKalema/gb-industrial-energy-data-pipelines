const TERMS = [
  {
    term: "Committed",
    meaning: "The amount of steam energy agreed for a delivery point during one 30-minute period.",
  },
  {
    term: "Delivered",
    meaning: "The steam energy accepted from the official revenue meter for that 30-minute period.",
  },
  {
    term: "Confirmed delivery limit",
    meaning: "The maximum steam energy the delivery point was confirmed able to supply during that 30-minute period. It is not the amount actually delivered.",
  },
  {
    term: "Accepted",
    meaning: "The evidence passed the agreed quality rules and can be used in governed calculations.",
  },
  {
    term: "Final",
    meaning: "All evidence required for that result is complete and approved, so it can be used for official reporting.",
  },
  {
    term: "Waiting for data",
    meaning: "Some required evidence is missing or not final yet. Known values may still appear, but official results are withheld.",
  },
  {
    term: "Unavailable",
    meaning: "There is not enough governed evidence to calculate the value. Unavailable never means zero.",
  },
  {
    term: "Missing",
    meaning: "An expected source record or measurement has not been received. The system keeps the result unknown instead of assuming zero.",
  },
  {
    term: "Corrected",
    meaning: "A later approved revision changed earlier evidence. The current result uses that revision while the earlier version remains traceable.",
  },
  {
    term: "Not applicable",
    meaning: "The rule does not apply, for example when an approved 30-minute period has no commitment. It is not missing data.",
  },
  {
    term: "Shortfall / excess",
    meaning: "Shortfall means accepted delivery was below the commitment. Excess means it was above the commitment.",
  },
] as const;

export function TermGuide() {
  return (
    <details className="term-guide">
      <summary>
        <span>
          <strong>What do these terms mean?</strong>
          <small>Committed, delivered, delivery limits, unavailable and other result labels</small>
        </span>
        <span className="term-guide__action" aria-hidden="true">View definitions</span>
      </summary>
      <dl>
        {TERMS.map(({ term, meaning }) => (
          <div key={term}>
            <dt>{term}</dt>
            <dd>{meaning}</dd>
          </div>
        ))}
      </dl>
    </details>
  );
}

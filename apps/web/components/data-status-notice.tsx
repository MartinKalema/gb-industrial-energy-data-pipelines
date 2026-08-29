export function DataStatusNotice({ reasons }: { reasons: string[] }) {
  if (reasons.length === 0) return null;

  const checkLabel = `${reasons.length} item${reasons.length === 1 ? "" : "s"} to review`;

  return (
    <section
      className="data-status-notice"
      aria-labelledby="data-status-heading"
    >
      <div className="data-status-notice__summary">
        <div className="data-status-notice__kicker">
          <span aria-hidden="true">!</span>
          <p>Evidence check</p>
        </div>
        <h3 id="data-status-heading">Some official results are not ready</h3>
        <p>Known totals below are still available.</p>
      </div>
      <div className="data-status-notice__details">
        <span className="data-status-notice__count">{checkLabel}</span>
        <ul>
          {reasons.map((reason, index) => (
            <li key={reason}>
              <span aria-hidden="true">{String(index + 1).padStart(2, "0")}</span>
              <p>{reason}</p>
            </li>
          ))}
        </ul>
      </div>
    </section>
  );
}

import Link from "next/link";

export default function NotFound() {
  return (
    <main id="main-content" className="state-page">
      <p className="state-page__code" aria-hidden="true">404 / NO RECORD</p>
      <p className="section-kicker">Delivery record not found</p>
      <h1>This 30-minute delivery record is not available in your scope.</h1>
      <p>It may not exist, or the selected demo persona may not be authorized to see it.</p>
      <Link className="primary-button" href="/">Return to delivery performance</Link>
    </main>
  );
}

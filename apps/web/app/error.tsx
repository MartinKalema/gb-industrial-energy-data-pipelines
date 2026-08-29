"use client";

import { useEffect } from "react";

export default function ErrorPage({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error(error);
  }, [error]);

  return (
    <main id="main-content" className="state-page">
      <p className="state-page__code" aria-hidden="true">SERVICE / HOLD</p>
      <p className="section-kicker">Delivery evidence unavailable</p>
      <h1>The investigation could not be loaded.</h1>
      <p>
        The read-only product API did not return a usable response. The source data
        has not been changed.
      </p>
      <button className="primary-button" type="button" onClick={reset}>Try again</button>
    </main>
  );
}

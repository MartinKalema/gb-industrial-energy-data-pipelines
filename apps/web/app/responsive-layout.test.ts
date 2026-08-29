import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const css = readFileSync(resolve(process.cwd(), "app/globals.css"), "utf8");

describe("responsive containment rules", () => {
  it("allows native controls and grid children to shrink inside the filter column", () => {
    expect(css).toMatch(
      /\.filter-form input\s*\{[^}]*min-width:\s*0;[^}]*max-width:\s*100%;/s,
    );
    expect(css).toMatch(
      /\.workspace-grid > \*,[\s\S]*?\.date-pair > \*\s*\{[^}]*min-width:\s*0;[^}]*max-width:\s*100%;/,
    );
    expect(css).toMatch(
      /\.date-pair\s*\{[^}]*grid-template-columns:\s*minmax\(0, 1fr\);/s,
    );
  });

  it("contains wide chart content in its own horizontal scroll region", () => {
    expect(css).toMatch(
      /\.comparison-chart__scroll\s*\{[^}]*max-width:\s*100%;[^}]*overflow-x:\s*auto;/s,
    );
    expect(css).toMatch(
      /\.comparison-panel\s*\{[^}]*overflow:\s*hidden;/s,
    );
    expect(css).toMatch(
      /\.comparison-bar--unavailable\s*\{[^}]*height:\s*28px;/s,
    );
  });

  it("contains the chronological outcome ribbon in a local scroll region", () => {
    expect(css).toMatch(
      /\.outcome-panel\s*\{[^}]*overflow:\s*hidden;/s,
    );
    expect(css).toMatch(
      /\.outcome-ribbon__scroll\s*\{[^}]*max-width:\s*100%;[^}]*overflow-x:\s*auto;/s,
    );
    expect(css).toMatch(
      /\.outcome-ribbon\s*\{[^}]*width:\s*max-content;[^}]*min-width:\s*100%;/s,
    );
    expect(css).toMatch(
      /\.outcome-segment\s*\{[^}]*min-width:\s*14px;[^}]*flex:\s*0 0 14px;/s,
    );
  });
});

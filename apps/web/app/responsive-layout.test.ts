import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const css = readFileSync(resolve(process.cwd(), "app/globals.css"), "utf8");
const page = readFileSync(resolve(process.cwd(), "app/page.tsx"), "utf8");

describe("responsive containment rules", () => {
  it("opens directly on filters and analysis without a hero or footer", () => {
    expect(page).not.toContain('className="hero"');
    expect(page).not.toContain("<footer");
    expect(page).toContain("<FilterPanel");
  });

  it("allows native controls and grid children to shrink inside the filter column", () => {
    expect(css).toMatch(
      /\.filter-form input\s*\{[^}]*min-width:\s*0;[^}]*max-width:\s*100%;/s,
    );
    expect(css).toMatch(
      /\.workspace-grid > \*,[\s\S]*?\.filter-form > \*\s*\{[^}]*min-width:\s*0;[^}]*max-width:\s*100%;/,
    );
  });

  it("uses a full-width horizontal filter bar above the graph", () => {
    expect(css).toMatch(
      /\.workspace-grid\s*\{[^}]*grid-template-columns:\s*minmax\(0, 1fr\);/s,
    );
    expect(css).toMatch(
      /\.comparison-panel\s*\{[^}]*overflow:\s*hidden;/s,
    );
    expect(css).toMatch(
      /\.filter-form\s*\{[^}]*display:\s*grid;[^}]*grid-template-columns:/s,
    );
    expect(css).toMatch(
      /\.dashboard-select__trigger\s*\{[^}]*min-height:\s*46px;[^}]*display:\s*flex;/s,
    );
    expect(css).toMatch(
      /\.filter-form \.dashboard-select__trigger\s*\{[^}]*display:\s*flex;/s,
    );
    expect(css).toMatch(
      /\.dashboard-select__content\s*\{[^}]*max-width:\s*min\(480px, calc\(100vw - 24px\)\);/s,
    );
    expect(css).toMatch(
      /@media \(max-width:\s*1520px\)[\s\S]*?\.filter-form\s*\{[^}]*grid-template-columns:\s*repeat\(4, minmax\(0, 1fr\)\);/,
    );
    expect(css).toMatch(
      /@media \(max-width:\s*900px\)[\s\S]*?\.filter-actions\s*\{[^}]*grid-column:\s*1 \/ -1;/,
    );
  });

  it("contains the evidence matrix in its own horizontal scroll region", () => {
    expect(css).toMatch(
      /\.evidence-chart__scroll\s*\{[^}]*max-width:\s*100%;[^}]*overflow-x:\s*auto;/s,
    );
    expect(css).toMatch(
      /\.analysis-chart__body\s*\{[^}]*min-width:\s*0;[^}]*min-height:\s*330px;/s,
    );
    expect(css).toMatch(
      /\.evidence-cell--missing\s*\{[^}]*repeating-linear-gradient/s,
    );
  });

  it("enlarges chart legend symbols without enlarging legend text", () => {
    expect(css).toMatch(
      /\.analysis-legend\s*\{[^}]*font-size:\s*0\.67rem;/s,
    );
    expect(css).toMatch(
      /\.analysis-legend__mark\s*\{[^}]*width:\s*24px;[^}]*height:\s*4px;/s,
    );
    expect(css).toMatch(
      /\.analysis-legend__mark--zero\s*\{[^}]*width:\s*14px;[^}]*height:\s*14px;/s,
    );
    expect(css).toMatch(
      /\.analysis-legend__mark--unavailable\s*\{[^}]*width:\s*14px;[^}]*height:\s*14px;/s,
    );
  });

  it("lays out evidence gaps as cards and stacks them on small screens", () => {
    expect(css).toMatch(
      /\.data-status-notice\s*\{[^}]*grid-template-columns:\s*minmax\(220px, 0\.62fr\) minmax\(0, 1\.38fr\);/s,
    );
    expect(css).toMatch(
      /\.data-status-notice ul\s*\{[^}]*grid-template-columns:\s*repeat\(2, minmax\(0, 1fr\)\);/s,
    );
    expect(css).toMatch(
      /@media \(max-width:\s*640px\)[\s\S]*?\.data-status-notice\s*\{[^}]*grid-template-columns:\s*1fr;/,
    );
    expect(css).toMatch(
      /@media \(max-width:\s*640px\)[\s\S]*?\.analysis-tabs\s*\{[^}]*grid-template-columns:\s*repeat\(3, minmax\(0, 1fr\)\);/,
    );
  });
});

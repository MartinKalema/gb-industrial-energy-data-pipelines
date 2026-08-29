import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DataStatusNotice } from "@/components/data-status-notice";

describe("DataStatusNotice", () => {
  it("presents evidence gaps as separate, scannable items", () => {
    render(
      <DataStatusNotice
        reasons={[
          "1 30-minute period with a missing commitment",
          "2 30-minute periods without a confirmed delivery limit",
        ]}
      />,
    );

    const status = screen.getByRole("region", {
      name: "Some official results are not ready",
    });
    expect(
      within(status).getByRole("heading", {
        name: "Some official results are not ready",
      }),
    ).toBeVisible();
    expect(within(status).getByText("2 items to review")).toBeVisible();
    expect(within(status).getAllByRole("listitem")).toHaveLength(2);
    expect(status).toHaveTextContent(
      "Known totals below are still available.",
    );
  });

  it("uses the singular item label for one reason", () => {
    render(<DataStatusNotice reasons={["1 missing commitment"]} />);
    expect(screen.getByText("1 item to review")).toBeVisible();
  });

  it("renders nothing when every evidence check has passed", () => {
    const { container } = render(<DataStatusNotice reasons={[]} />);
    expect(container).toBeEmptyDOMElement();
  });
});

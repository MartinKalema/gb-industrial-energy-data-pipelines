import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { DashboardSelect } from "@/components/dashboard-select";

Object.defineProperties(HTMLElement.prototype, {
  hasPointerCapture: { configurable: true, value: vi.fn(() => false) },
  releasePointerCapture: { configurable: true, value: vi.fn() },
  scrollIntoView: { configurable: true, value: vi.fn() },
});

function SelectHarness() {
  const [value, setValue] = useState("");
  return (
    <form aria-label="Test filters">
      <label htmlFor="delivery-state">Delivery state</label>
      <DashboardSelect
        id="delivery-state"
        name="status"
        value={value}
        groups={[{
          options: [
            { value: "", label: "All delivery states" },
            { value: "shortfall", label: "Shortfall" },
            { value: "disabled", label: "Unavailable choice", disabled: true },
          ],
        }]}
        onValueChange={setValue}
      />
    </form>
  );
}

describe("DashboardSelect", () => {
  it("opens an accessible portal menu and preserves exact form values", async () => {
    const user = userEvent.setup();
    const { container } = render(<SelectHarness />);
    const trigger = screen.getByRole("combobox", { name: "Delivery state" });

    expect(trigger).toHaveTextContent("All delivery states");
    expect(trigger).toHaveAttribute("aria-expanded", "false");
    expect(container.querySelector('input[name="status"]')).toHaveValue("");

    await user.click(trigger);

    expect(screen.getByRole("listbox")).toBeVisible();
    expect(screen.getByRole("option", { name: "All delivery states" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByRole("option", { name: "Unavailable choice" })).toHaveAttribute(
      "aria-disabled",
      "true",
    );

    await user.click(screen.getByRole("option", { name: "Shortfall" }));

    expect(trigger).toHaveTextContent("Shortfall");
    expect(trigger).toHaveAttribute("aria-expanded", "false");
    expect(container.querySelector('input[name="status"]')).toHaveValue("shortfall");
  });

  it("supports keyboard selection and escape without changing the value", async () => {
    const user = userEvent.setup();
    const { container } = render(<SelectHarness />);
    const trigger = screen.getByRole("combobox", { name: "Delivery state" });

    trigger.focus();
    await user.keyboard("{Enter}{ArrowDown}{Enter}");
    expect(container.querySelector('input[name="status"]')).toHaveValue("shortfall");

    await user.keyboard("{Enter}{Escape}");
    expect(trigger).toHaveAttribute("aria-expanded", "false");
    expect(container.querySelector('input[name="status"]')).toHaveValue("shortfall");
  });
});

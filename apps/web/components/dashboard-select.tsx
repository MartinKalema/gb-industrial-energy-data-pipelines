"use client";

import * as Select from "@radix-ui/react-select";

const VALUE_PREFIX = "dashboard-select:";

export interface DashboardSelectOption {
  value: string;
  label: string;
  disabled?: boolean;
}

export interface DashboardSelectGroup {
  label?: string;
  options: readonly DashboardSelectOption[];
}

interface DashboardSelectProps {
  id: string;
  name?: string;
  value: string;
  groups: readonly DashboardSelectGroup[];
  onValueChange: (value: string) => void;
  disabled?: boolean;
  ariaDescribedBy?: string;
}

function encodeValue(value: string): string {
  return `${VALUE_PREFIX}${value}`;
}

function decodeValue(value: string): string {
  return value.startsWith(VALUE_PREFIX) ? value.slice(VALUE_PREFIX.length) : value;
}

export function DashboardSelect({
  id,
  name,
  value,
  groups,
  onValueChange,
  disabled = false,
  ariaDescribedBy,
}: DashboardSelectProps) {
  const selectedOption = groups
    .flatMap((group) => group.options)
    .find((option) => option.value === value);

  return (
    <>
      <Select.Root
        value={encodeValue(value)}
        onValueChange={(nextValue) => onValueChange(decodeValue(nextValue))}
        disabled={disabled}
      >
        <Select.Trigger
          id={id}
          className="dashboard-select__trigger"
          aria-describedby={ariaDescribedBy}
        >
          <Select.Value>{selectedOption?.label ?? value}</Select.Value>
          <Select.Icon className="dashboard-select__icon" aria-hidden="true">
            <span />
          </Select.Icon>
        </Select.Trigger>

        <Select.Portal>
          <Select.Content
            className="dashboard-select__content"
            position="popper"
            sideOffset={6}
            collisionPadding={12}
          >
            <Select.ScrollUpButton className="dashboard-select__scroll-button" aria-label="Scroll up">
              <span aria-hidden="true">▲</span>
            </Select.ScrollUpButton>
            <Select.Viewport className="dashboard-select__viewport">
              {groups.map((group, groupIndex) => (
                <Select.Group key={group.label ?? `group-${groupIndex}`}>
                  {group.label ? (
                    <Select.Label className="dashboard-select__group-label">
                      {group.label}
                    </Select.Label>
                  ) : null}
                  {group.options.map((option) => (
                    <Select.Item
                      key={option.value}
                      value={encodeValue(option.value)}
                      disabled={option.disabled}
                      className="dashboard-select__option"
                    >
                      <Select.ItemText>{option.label}</Select.ItemText>
                      <Select.ItemIndicator className="dashboard-select__check" aria-hidden="true">
                        ✓
                      </Select.ItemIndicator>
                    </Select.Item>
                  ))}
                </Select.Group>
              ))}
            </Select.Viewport>
            <Select.ScrollDownButton className="dashboard-select__scroll-button" aria-label="Scroll down">
              <span aria-hidden="true">▼</span>
            </Select.ScrollDownButton>
          </Select.Content>
        </Select.Portal>
      </Select.Root>
      {name ? <input type="hidden" name={name} value={value} disabled={disabled} /> : null}
    </>
  );
}

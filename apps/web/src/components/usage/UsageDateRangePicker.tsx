import { UsageDay } from "@t3tools/contracts";
import { formatDayShort } from "@t3tools/shared/usageFormat";
import { CalendarIcon } from "lucide-react";
import { useState } from "react";
import type { DateRange } from "@daypicker/react";

import { weekStartsOn } from "../../timestampFormat";
import { Button } from "../ui/button";
import { Calendar } from "../ui/calendar";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";

/** Usage history older than this is pruned on the server, so it cannot be picked. */
const MAX_RANGE_DAYS = 90;

function toUsageDay(date: Date): UsageDay {
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return UsageDay.make(`${date.getFullYear()}-${month}-${day}`);
}

function fromUsageDay(day: string): Date {
  const [year = 0, month = 1, dayOfMonth = 1] = day.split("-").map(Number);
  return new Date(year, month - 1, dayOfMonth);
}

export function formatUsageDayRange(sinceDay: string, untilDay: string): string {
  return sinceDay === untilDay
    ? formatDayShort(sinceDay)
    : `${formatDayShort(sinceDay)} – ${formatDayShort(untilDay)}`;
}

/**
 * Picks an inclusive calendar-day range for Usage. The trigger shows the range
 * only while it is the active period, so the presets beside it stay the default.
 */
export function UsageDateRangePicker({
  sinceDay,
  untilDay,
  active,
  disabled,
  onSelect,
}: {
  sinceDay: string;
  untilDay: string;
  active: boolean;
  disabled: boolean;
  onSelect: (sinceDay: UsageDay, untilDay: UsageDay) => void;
}) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<DateRange | undefined>();
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const earliest = new Date(today);
  earliest.setDate(earliest.getDate() - (MAX_RANGE_DAYS - 1));

  return (
    <Popover
      open={open}
      onOpenChange={(nextOpen) => {
        if (nextOpen) setDraft({ from: fromUsageDay(sinceDay), to: fromUsageDay(untilDay) });
        setOpen(nextOpen);
      }}
    >
      <PopoverTrigger
        render={
          <Button
            variant={active ? "secondary" : "ghost"}
            size={active ? "sm" : "icon-sm"}
            disabled={disabled}
            aria-label={
              active ? `Custom range, ${formatUsageDayRange(sinceDay, untilDay)}` : "Custom range"
            }
            title="Custom range"
          />
        }
      >
        <CalendarIcon />
        {active ? formatUsageDayRange(sinceDay, untilDay) : null}
      </PopoverTrigger>
      <PopoverPopup align="end" aria-label="Choose usage date range">
        <Calendar
          mode="range"
          resetOnSelect
          selected={draft}
          defaultMonth={draft?.to ?? today}
          startMonth={earliest}
          endMonth={today}
          disabled={{ before: earliest, after: today }}
          {...(weekStartsOn === undefined ? {} : { weekStartsOn })}
          onSelect={(range) => {
            setDraft(range);
            if (range?.from === undefined || range.to === undefined) return;
            onSelect(toUsageDay(range.from), toUsageDay(range.to));
            setOpen(false);
          }}
        />
      </PopoverPopup>
    </Popover>
  );
}

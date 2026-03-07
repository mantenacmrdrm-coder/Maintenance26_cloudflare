"use client"

import * as React from "react"
import { ChevronLeft, ChevronRight } from "lucide-react"
import { DayPicker, useDayPicker, useNavigation } from "react-day-picker"
import { fr } from "date-fns/locale"
import { format } from "date-fns"

import { cn } from "@/lib/utils"
import { Button, buttonVariants } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"


export type CalendarProps = React.ComponentProps<typeof DayPicker>

function Calendar({
  className,
  classNames,
  showOutsideDays = true,
  ...props
}: CalendarProps) {
  return (
    <DayPicker
      locale={fr}
      showOutsideDays={showOutsideDays}
      className={cn("p-3", className)}
      classNames={{
        months: "flex flex-col sm:flex-row space-y-4 sm:space-x-4 sm:space-y-0",
        month: "space-y-4",
        caption: "hidden", // We are replacing the caption entirely
        table: "w-full border-collapse space-y-1",
        head_row: "flex",
        head_cell:
          "text-muted-foreground rounded-md w-9 font-normal text-[0.8rem]",
        row: "flex w-full mt-2",
        cell: "h-9 w-9 text-center text-sm p-0 relative [&:has([aria-selected].day-range-end)]:rounded-r-md [&:has([aria-selected].day-outside)]:bg-accent/50 [&:has([aria-selected])]:bg-accent first:[&:has([aria-selected])]:rounded-l-md last:[&:has([aria-selected])]:rounded-r-md focus-within:relative focus-within:z-20",
        day: cn(
          buttonVariants({ variant: "ghost" }),
          "h-9 w-9 p-0 font-normal aria-selected:opacity-100"
        ),
        day_range_end: "day-range-end",
        day_selected:
          "bg-primary text-primary-foreground hover:bg-primary hover:text-primary-foreground focus:bg-primary focus:text-primary-foreground",
        day_today: "bg-accent text-accent-foreground",
        day_outside:
          "day-outside text-muted-foreground opacity-50 aria-selected:bg-accent/50 aria-selected:text-muted-foreground",
        day_disabled: "text-muted-foreground opacity-50",
        day_range_middle:
          "aria-selected:bg-accent aria-selected:text-accent-foreground",
        day_hidden: "invisible",
        ...classNames,
      }}
      components={{
        IconLeft: () => <ChevronLeft className="h-4 w-4" />,
        IconRight: () => <ChevronRight className="h-4 w-4" />,
        Caption: ({ displayMonth }) => {
          const { goToMonth, nextMonth, previousMonth } = useNavigation();
          const { fromDate, toDate } = useDayPicker();

          const startYear = props.fromYear || fromDate?.getFullYear() || new Date().getFullYear() - 80;
          const endYear = props.toYear || toDate?.getFullYear() || new Date().getFullYear() + 5;
          const years = Array.from({ length: endYear - startYear + 1 }, (_, i) => startYear + i);

          const months = Array.from({ length: 12 }, (_, i) => ({
            value: i,
            label: format(new Date(2000, i), 'MMMM', { locale: fr }),
          }));
          
          return (
            <div className="flex items-center justify-between px-1 mb-4">
              <Button
                variant="outline"
                size="icon"
                className="h-7 w-7"
                onClick={() => previousMonth && goToMonth(previousMonth)}
                disabled={!previousMonth}
              >
                <ChevronLeft className="h-4 w-4" />
              </Button>

              <div className="flex gap-2">
                <Select
                    value={displayMonth.getMonth().toString()}
                    onValueChange={(value) => {
                        goToMonth(new Date(displayMonth.getFullYear(), parseInt(value, 10)));
                    }}
                >
                    <SelectTrigger className="w-fit capitalize text-sm h-7 px-2">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                        {months.map((month) => (
                            <SelectItem key={month.value} value={month.value.toString()} className="text-sm capitalize">
                                {month.label}
                            </SelectItem>
                        ))}
                    </SelectContent>
                </Select>
                <Select
                    value={displayMonth.getFullYear().toString()}
                    onValueChange={(value) => {
                        goToMonth(new Date(parseInt(value, 10), displayMonth.getMonth()));
                    }}
                >
                    <SelectTrigger className="w-fit text-sm h-7 px-2">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                        {years.map((year) => (
                            <SelectItem key={year} value={year.toString()} className="text-sm">
                                {year}
                            </SelectItem>
                        ))}
                    </SelectContent>
                </Select>
              </div>

              <Button
                variant="outline"
                size="icon"
                className="h-7 w-7"
                onClick={() => nextMonth && goToMonth(nextMonth)}
                disabled={!nextMonth}
              >
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          );
        },
      }}
      {...props}
    />
  )
}
Calendar.displayName = "Calendar"

export { Calendar }

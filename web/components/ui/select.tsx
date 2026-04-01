import * as React from "react";

import { cn } from "@/lib/utils";

export const Select = React.forwardRef<HTMLSelectElement, React.ComponentProps<"select">>(function Select(
  { children, className, ...props },
  ref,
) {
  return (
    <div className="relative">
      <select
        className={cn(
          "flex h-7 w-full appearance-none rounded-md border border-input bg-background px-2.5 pr-7 text-[11px] text-foreground outline-none transition-[border-color,box-shadow,background-color,color] focus-visible:border-[#cfd4ff] focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:bg-muted disabled:text-muted-foreground disabled:opacity-100",
          className,
        )}
        ref={ref}
        {...props}
      >
        {children}
      </select>
      <span className="pointer-events-none absolute inset-y-0 right-2 flex items-center text-[9px] text-muted-foreground">
        ▾
      </span>
    </div>
  );
});

import * as React from "react";

import { cn } from "@/lib/utils";

type BadgeVariant = "default" | "secondary" | "outline" | "success" | "warning" | "destructive";

const variantClasses: Record<BadgeVariant, string> = {
  default: "border-transparent bg-primary text-primary-foreground",
  secondary: "border-border bg-muted text-muted-foreground",
  outline: "border-border bg-card text-foreground",
  success: "border-[#d5ebe7] bg-[#f5fbfa] text-[#0f766e]",
  warning: "border-[#f6e4c7] bg-[#fffbf5] text-[#a16207]",
  destructive: "border-[#f3d3da] bg-[#fff5f7] text-[#c43d57]",
};

export function Badge({
  className,
  variant = "default",
  ...props
}: React.ComponentProps<"span"> & { variant?: BadgeVariant }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md border px-2 py-0.5 text-[11px] font-medium",
        variantClasses[variant],
        className,
      )}
      {...props}
    />
  );
}

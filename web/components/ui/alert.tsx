import * as React from "react";

import { cn } from "@/lib/utils";

type AlertVariant = "default" | "destructive";

const variantClasses: Record<AlertVariant, string> = {
  default: "border-border bg-card text-card-foreground",
  destructive: "border-[#f1d5dc] bg-[#fff7f9] text-[#c43d57]",
};

export function Alert({
  className,
  variant = "default",
  ...props
}: React.ComponentProps<"div"> & { variant?: AlertVariant }) {
  return <div className={cn("rounded-lg border px-3 py-2.5", variantClasses[variant], className)} role="alert" {...props} />;
}

export function AlertTitle({ className, ...props }: React.ComponentProps<"h5">) {
  return <h5 className={cn("mb-1 font-medium leading-none tracking-tight", className)} {...props} />;
}

export function AlertDescription({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn("text-xs leading-5", className)} {...props} />;
}

import * as React from "react";

import { cn } from "@/lib/utils";

type ButtonVariant = "default" | "outline" | "secondary" | "destructive" | "ghost";
type ButtonSize = "default" | "sm" | "lg";

type ButtonVariantOptions = {
  variant?: ButtonVariant;
  size?: ButtonSize;
  className?: string;
};

const variantClasses: Record<ButtonVariant, string> = {
  default: "border border-black bg-black text-white hover:bg-[#111111]",
  outline: "border border-border bg-card text-foreground hover:bg-muted",
  secondary: "border border-border bg-card text-foreground hover:bg-muted",
  destructive: "border border-[#f1d5dc] bg-[#fff7f9] text-[#c43d57] hover:bg-[#fff1f4]",
  ghost: "border border-transparent bg-transparent text-muted-foreground hover:bg-muted hover:text-foreground",
};

const sizeClasses: Record<ButtonSize, string> = {
  default: "h-7 px-2.5 text-[11px] leading-none",
  sm: "h-6 px-2 text-[11px] leading-none",
  lg: "h-8 px-3 text-[12px] leading-none",
};

export function buttonVariants({ variant = "default", size = "default", className }: ButtonVariantOptions = {}) {
  return cn(
    "inline-flex cursor-pointer appearance-none items-center justify-center gap-1 whitespace-nowrap rounded-md font-medium no-underline transition-[border-color,background-color,color] duration-150 ease-out outline-none select-none focus-visible:ring-2 focus-visible:ring-ring/20 focus-visible:ring-offset-2 focus-visible:ring-offset-background disabled:pointer-events-none disabled:opacity-50",
    variantClasses[variant],
    sizeClasses[size],
    className,
  );
}

export type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
  size?: ButtonSize;
};

export function Button({ className, variant = "default", size = "default", type = "button", ...props }: ButtonProps) {
  return <button className={buttonVariants({ variant, size, className })} type={type} {...props} />;
}



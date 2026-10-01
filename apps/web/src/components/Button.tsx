import type { ButtonHTMLAttributes, PropsWithChildren } from "react";
import { cn } from "../lib/utils";

type ButtonProps = PropsWithChildren<ButtonHTMLAttributes<HTMLButtonElement>> & {
  variant?: "primary" | "secondary" | "ghost" | "danger";
};

export function Button({ children, className, variant = "primary", type = "button", ...props }: ButtonProps) {
  const variants = {
    primary: "bg-blue-600 hover:bg-blue-500 text-white border-blue-500/40",
    secondary: "bg-slate-800 hover:bg-slate-700 text-slate-100 border-slate-700",
    ghost: "bg-transparent hover:bg-slate-800 text-slate-300 border-transparent",
    danger: "bg-red-500/15 hover:bg-red-500/25 text-red-300 border-red-500/30",
  };
  return <button type={type} className={cn("rounded-lg border px-3 py-2 text-xs font-bold transition-colors disabled:cursor-not-allowed disabled:opacity-50", variants[variant], className)} {...props}>{children}</button>;
}

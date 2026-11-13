import type { ComponentProps } from "react"

import { cn } from "@/lib/utils"

export function Card({ className, ...props }: ComponentProps<"section">) {
  return <section data-slot="card" className={cn("min-w-0 rounded-xl border border-border bg-card text-card-foreground", className)} {...props} />
}

import { cva, type VariantProps } from "class-variance-authority"
import type { ComponentProps } from "react"

import { cn } from "@/lib/utils"

const buttonVariants = cva(
  "inline-flex shrink-0 items-center justify-center gap-2 rounded-md px-3.5 text-sm font-medium transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring disabled:pointer-events-none disabled:opacity-45",
  {
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground hover:bg-primary/85",
        outline: "border border-border bg-card text-foreground hover:bg-muted",
        ghost: "text-muted-foreground hover:bg-muted hover:text-foreground",
      },
      size: { default: "h-9", small: "h-7 px-2.5 text-xs" },
    },
    defaultVariants: { variant: "default", size: "default" },
  },
)

export function Button({
  className, variant, size, type = "button", ...props
}: ComponentProps<"button"> & VariantProps<typeof buttonVariants>) {
  return <button data-slot="button" type={type} className={cn(buttonVariants({ variant, size }), className)} {...props} />
}

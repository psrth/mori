"use client"

import { Toaster as Sonner, type ToasterProps } from "sonner"

const Toaster = ({ ...props }: ToasterProps) => {
  return (
    <Sonner
      theme="dark"
      className="toaster group"
      position="bottom-right"
      style={
        {
          "--normal-bg": "#191816",
          "--normal-text": "#D9D9D9",
          "--normal-border": "rgba(255, 255, 255, 0.1)",
          "--border-radius": "0.625rem",
        } as React.CSSProperties
      }
      {...props}
    />
  )
}

export { Toaster }

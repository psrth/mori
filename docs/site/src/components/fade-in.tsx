"use client";

import { motion, useReducedMotion } from "framer-motion";
import { ReactNode } from "react";

// ease-out-quint: strong deceleration, feels snappy and responsive
const easeOutQuint: [number, number, number, number] = [0.23, 1, 0.32, 1];

interface FadeInProps {
  children: ReactNode;
  direction?: "up" | "down" | "left" | "right";
  delay?: number;
  duration?: number;
  className?: string;
}

const directionOffset = {
  up: { y: 16, x: 0 },
  down: { y: -16, x: 0 },
  left: { x: 16, y: 0 },
  right: { x: -16, y: 0 },
};

export function FadeIn({
  children,
  direction = "up",
  delay = 0,
  duration = 0.4,
  className,
}: FadeInProps) {
  const shouldReduceMotion = useReducedMotion();
  const offset = directionOffset[direction];

  return (
    <motion.div
      initial={
        shouldReduceMotion ? { opacity: 0 } : { opacity: 0, x: offset.x, y: offset.y }
      }
      whileInView={{ opacity: 1, x: 0, y: 0 }}
      viewport={{ once: true, margin: "-80px" }}
      transition={{
        duration: shouldReduceMotion ? 0.15 : duration,
        delay,
        ease: easeOutQuint,
      }}
      className={className}
    >
      {children}
    </motion.div>
  );
}

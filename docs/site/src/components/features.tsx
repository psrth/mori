"use client";

import Image from "next/image";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { FadeIn } from "./fade-in";

const features = [
  {
    number: "01",
    title: "zero-config provider auth",
    description:
      "auto-detects credentials for AWS RDS, GCP Cloud SQL, Supabase, Neon, PlanetScale, etc — or bring your own connection string.",
  },
  {
    number: "02",
    title: "copy-on-write isolation",
    description:
      "reads from prod, writes to ephemeral sandbox. you and your agents get live production data, and the confidence that your tests will never mutate your database.",
  },
  {
    number: "03",
    title: "migration testing",
    description:
      "validate schema changes by running your migrations against a copy of real data before deploying. let that intern drop the unique constraint on emails — who cares?",
  },
  {
    number: "04",
    title: "model context protocol",
    description:
      "exposes the mori proxy as an mcp server out-of-the-box. drop it into claude code or cursor, and help your agent write more intelligent code with better validation.",
  },
  {
    number: "05",
    title: "tui dashboard support",
    description:
      "run in headless mode for pipeline integration, or use the built-in terminal dashboard with real-time query logs, delta tracking, and session metrics.",
  },
];

export function Features() {
  return (
    <section className="py-24 px-6">
      <div className="max-w-6xl mx-auto">
        <FadeIn>
          <h2 className="font-[family-name:var(--font-manrope)] font-bold text-3xl md:text-4xl text-header-white tracking-[-0.05em] mb-12">
            features
          </h2>
        </FadeIn>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-12">
          <FadeIn delay={0.1}>
            <Accordion type="single" collapsible defaultValue="item-0">
              {features.map((feature, i) => (
                <AccordionItem
                  key={feature.number}
                  value={`item-${i}`}
                  className="border-white/10"
                >
                  <AccordionTrigger className="hover:no-underline py-5">
                    <div className="flex items-center gap-4">
                      <span className="font-[family-name:var(--font-dm-mono)] text-brand-purple text-sm tracking-[-0.02em]">
                        {feature.number}
                      </span>
                      <span className="font-[family-name:var(--font-manrope)] font-bold text-header-white text-base tracking-[-0.03em]">
                        {feature.title}
                      </span>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    <p className="font-[family-name:var(--font-inter)] text-card-white/70 text-sm leading-relaxed tracking-[-0.01em] pl-12">
                      {feature.description}
                    </p>
                  </AccordionContent>
                </AccordionItem>
              ))}
            </Accordion>
          </FadeIn>

          <FadeIn delay={0.2} className="hidden lg:block">
            <div className="lg:sticky lg:top-24">
              <Image
                src="/images/feature-0.png"
                alt="mori feature preview"
                width={600}
                height={500}
                className="w-full rounded-xl"
              />
            </div>
          </FadeIn>
        </div>
      </div>
    </section>
  );
}

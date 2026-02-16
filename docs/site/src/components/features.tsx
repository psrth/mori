"use client";

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

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {features.map((feature, i) => (
            <FadeIn key={feature.number} delay={0.1 + i * 0.05}>
              <div className="bg-card-gray border border-card-white/10 rounded-xl p-6 h-full hover:border-card-white/20 transition-colors">
                <span className="font-[family-name:var(--font-dm-mono)] text-brand-purple text-sm tracking-[-0.02em]">
                  {feature.number}
                </span>
                <h3 className="font-[family-name:var(--font-manrope)] font-bold text-header-white text-base tracking-[-0.03em] mt-3">
                  {feature.title}
                </h3>
                <p className="font-[family-name:var(--font-inter)] text-card-white/70 text-sm leading-relaxed tracking-[-0.01em] mt-3">
                  {feature.description}
                </p>
              </div>
            </FadeIn>
          ))}
        </div>
      </div>
    </section>
  );
}

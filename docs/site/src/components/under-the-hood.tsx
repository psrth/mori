"use client";

import { FiDatabase } from "react-icons/fi";
import { FadeIn } from "./fade-in";

const steps = [
  {
    step: 1,
    title: "connect to prod.",
    description:
      "point mori at your production database. it authenticates with your existing provider credentials — no config, no IAM headaches.",
  },
  {
    step: 2,
    title: "run all your queries.",
    description:
      "mori creates a copy-on-write proxy layer. reads hit real data, writes are sandboxed to an ephemeral instance. prod untouched.",
  },
  {
    step: 3,
    title: "test, then tear it down.",
    description:
      "run migrations, seed data, break things. when you're done, the instance dies and nothing persists. memento mori.",
  },
];

export function UnderTheHood() {
  return (
    <section className="py-24 px-6">
      <div className="max-w-6xl mx-auto">
        <FadeIn>
          <h2 className="font-[family-name:var(--font-manrope)] font-bold text-3xl md:text-4xl text-header-white tracking-[-0.05em] mb-12">
            under the hood
          </h2>
        </FadeIn>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {steps.map((item, i) => (
            <FadeIn key={item.step} delay={i * 0.1}>
              <div className="bg-card-gray border border-white/10 rounded-xl p-6 h-full">
                <FiDatabase className="w-6 h-6 text-brand-purple-light mb-4" />
                <span className="font-[family-name:var(--font-dm-mono)] text-brand-purple text-xs tracking-[-0.02em] uppercase">
                  Step {item.step}
                </span>
                <h3 className="font-[family-name:var(--font-manrope)] font-bold text-xl text-header-white mt-2 mb-3 tracking-[-0.03em]">
                  {item.title}
                </h3>
                <p className="font-[family-name:var(--font-inter)] text-card-white/70 text-sm leading-relaxed tracking-[-0.01em]">
                  {item.description}
                </p>
              </div>
            </FadeIn>
          ))}
        </div>
      </div>
    </section>
  );
}

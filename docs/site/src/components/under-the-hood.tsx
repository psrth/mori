"use client";

import { DatabaseCylinder } from "./database-cylinder";
import { FadeIn } from "./fade-in";

const steps = [
  {
    step: 1,
    title: "Connect to prod.",
    description:
      "point mori at your production database. it authenticates with your existing provider credentials — no config, no IAM headaches.",
  },
  {
    step: 2,
    title: "Run all your queries.",
    description:
      "mori creates a copy-on-write proxy layer. reads hit real data, writes are sandboxed to an ephemeral instance. prod untouched.",
  },
  {
    step: 3,
    title: "Test, then tear it down.",
    description:
      "run migrations, seed data, break things. when you're done, the instance dies and nothing persists. memento mori.",
  },
];

export function UnderTheHood() {
  return (
    <section className="py-24 px-6">
      <div className="max-w-6xl mx-auto">
        <FadeIn>
          <h2 className="font-[family-name:var(--font-manrope)] font-bold text-3xl md:text-4xl text-header-white tracking-[-0.05em] mb-12 text-center">
            under the hood
          </h2>
        </FadeIn>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {steps.map((item, i) => (
            <FadeIn key={item.step} delay={i * 0.1}>
              <div className="bg-card-gray border border-white/10 rounded-xl p-6 h-full flex flex-col min-h-[320px] md:min-h-[420px]">
                <span className="font-[family-name:var(--font-dm-mono)] text-card-white/40 text-xs tracking-[0.15em] uppercase">
                  STEP&nbsp;&nbsp;{item.step}
                </span>

                <div className="flex-1 flex items-center justify-center py-6">
                  <DatabaseCylinder className="w-28 h-40 md:w-36 md:h-52" />
                </div>

                <div>
                  <h3 className="font-[family-name:var(--font-manrope)] font-bold text-xl md:text-2xl text-header-white tracking-[-0.03em] mb-2">
                    {item.title}
                  </h3>
                  <p className="font-[family-name:var(--font-inter)] text-card-white/70 text-sm leading-relaxed tracking-[-0.01em]">
                    {item.description}
                  </p>
                </div>
              </div>
            </FadeIn>
          ))}
        </div>
      </div>
    </section>
  );
}

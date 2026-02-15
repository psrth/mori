"use client";

import { FadeIn } from "./fade-in";

export function CliDemo() {
  return (
    <section className="py-12 px-6">
      <div className="max-w-4xl mx-auto">
        <FadeIn>
          <div className="bg-card-gray border border-white/10 rounded-xl p-6 overflow-x-auto">
            <div className="flex items-center gap-2 font-[family-name:var(--font-dm-mono)] text-sm tracking-[-0.02em]">
              <span className="text-card-white/50">$</span>
              <span className="text-card-white">mori start</span>
              <span className="text-card-white/50">--provider</span>
              <span className="bg-brand-purple/20 text-brand-purple-light border border-brand-purple/30 rounded-full px-3 py-0.5 text-xs">
                Cloud SQL
              </span>
              <span className="text-card-white/50">--db</span>
              <span className="bg-brand-purple/20 text-brand-purple-light border border-brand-purple/30 rounded-full px-3 py-0.5 text-xs">
                PostgreSQL
              </span>
              <span className="text-brand-purple-light animate-blink">|</span>
            </div>
          </div>
        </FadeIn>
      </div>
    </section>
  );
}

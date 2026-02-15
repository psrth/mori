"use client";

import Image from "next/image";
import { FiTerminal } from "react-icons/fi";
import { FadeIn } from "./fade-in";

export function Hero() {
  return (
    <section className="pt-32 pb-16 px-6">
      <div className="max-w-6xl mx-auto text-center">
        <FadeIn delay={0}>
          <h1 className="font-[family-name:var(--font-manrope)] font-bold text-4xl md:text-5xl lg:text-6xl text-header-white tracking-[-0.05em] leading-[1.1]">
            test on prod.
            <br />
            break nothing.
          </h1>
        </FadeIn>

        <FadeIn delay={0.1}>
          <p className="mt-6 text-lg text-card-white/70 max-w-3xl mx-auto font-[family-name:var(--font-inter)] leading-[1.5] tracking-[-0.01em]">
            mori creates copy-on-write proxy layers over your production
            database. reads hit real data, writes are sandboxed to an ephemeral
            instance. test migrations, seed data, break things — then tear it
            all down. memento mori.
          </p>
        </FadeIn>

        <FadeIn delay={0.2}>
          <div className="mt-8 flex flex-col sm:flex-row items-center justify-center gap-3">
            <button className="flex items-center gap-2 bg-brand-purple hover:bg-brand-purple/90 text-white px-6 py-3 rounded-lg font-[family-name:var(--font-mono)] text-sm transition-colors w-full sm:w-auto justify-center">
              <FiTerminal className="w-4 h-4" />
              brew install mori
            </button>
            <button className="flex items-center gap-2 bg-card-gray border border-card-white/30 hover:border-card-white/50 text-card-white px-6 py-3 rounded-lg font-[family-name:var(--font-inter)] text-sm transition-colors w-full sm:w-auto justify-center">
              or let your AI agent set it up
            </button>
          </div>
        </FadeIn>

        <FadeIn delay={0.3}>
          <div className="mt-16 relative">
            <Image
              src="/images/hero-img.png"
              alt="mori dashboard preview"
              width={1200}
              height={675}
              priority
              className="w-full rounded-xl"
            />
          </div>
        </FadeIn>
      </div>
    </section>
  );
}

"use client";

import Image from "next/image";
import { FadeIn } from "./fade-in";

export function CliDemo() {
  return (
    <section className="py-12 px-6">
      <div className="max-w-4xl mx-auto">
        <FadeIn>
          <div className="bg-card-gray border border-white/10 rounded-xl p-6 overflow-x-auto">
            <div className="flex items-center gap-2 font-[family-name:var(--font-dm-mono)] text-sm tracking-[-0.02em]">
              <span className="text-card-white/50">$</span>
              <span className="text-card-white font-medium">mori</span>
              <span className="text-card-white">start</span>
              <span className="text-card-white/50 ml-2">--provider</span>
              <span className="inline-flex items-center gap-1.5">
                <Image
                  src="/images/cloud-sql-logo.svg"
                  alt="Cloud SQL"
                  width={20}
                  height={20}
                  className="inline-block"
                />
                <span className="text-card-white">Cloud SQL</span>
              </span>
              <span className="text-card-white/50 ml-2">--db</span>
              <span className="inline-flex items-center gap-1.5">
                <Image
                  src="/images/postgresql-logo.svg"
                  alt="PostgreSQL"
                  width={20}
                  height={20}
                  className="inline-block"
                />
                <span className="text-card-white">SQL</span>
              </span>
              <span className="text-brand-purple-light animate-blink">|</span>
            </div>
          </div>
        </FadeIn>
      </div>
    </section>
  );
}

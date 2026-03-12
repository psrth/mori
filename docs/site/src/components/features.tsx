"use client";

import { useState } from "react";
import Image from "next/image";
import { AnimatePresence, motion } from "framer-motion";
import { FadeIn } from "./fade-in";

const easeOutQuint: [number, number, number, number] = [0.23, 1, 0.32, 1];

const features = [
  {
    number: "01",
    title: "zero-config provider auth",
    description:
      "auto-detects credentials for AWS RDS, GCP Cloud SQL, Supabase, Neon, PlanetScale, etc — or bring your own connection string.",
    gif: "/features/01-provider-auth.gif",
  },
  {
    number: "02",
    title: "copy-on-write isolation",
    description:
      "reads from prod, writes to ephemeral sandbox. you and your agents get live production data, and the confidence that your changes will never modify your database.",
    gif: "/features/02-copy-on-write.gif",
  },
  {
    number: "03",
    title: "migration testing",
    description:
      "validate schema changes by running your migrations against a copy of real data before deploying. let that intern drop the unique constraint on emails — who cares?",
    gif: "/features/03-migration-testing.gif",
  },
  {
    number: "04",
    title: "model context protocol",
    description:
      'mori comes as an mcp server out-of-the-box. drop it into claude code or cursor, letting your agent actually test the code before you "carefully review" and merge into main.',
    gif: "/features/04-mcp-server.gif",
  },
  {
    number: "05",
    title: "nice pretty tui dashboard",
    description:
      "monitor all queries the strategy used to resolve them with real-time streaming, latency charts, delta tracking. you probably don't need it, but you'll leave it open anyway.",
    gif: "/features/05-tui-dashboard.gif",
  },
  {
    number: "06",
    title: "start over",
    description:
      'just run mori reset. your instance drops all local modifications and reverts back to prod state. infinitely repeatable, so that "brute force vibe coding" is now a viable career option.',
    gif: "/features/06-reset.gif",
  },
];

export function Features() {
  const [activeIndex, setActiveIndex] = useState(0);

  return (
    <section className="py-24">
      <div className="max-w-[1200px] mx-auto px-6">
        <FadeIn>
          <h2 className="font-(family-name:--font-manrope) font-bold text-[26px] text-header-white tracking-[-0.05em] mb-6 ml-2">
            features
          </h2>
        </FadeIn>

        <FadeIn delay={0.1}>
          <div className="flex flex-col lg:flex-row gap-6">
            {/* Left: Accordion — 45% */}
            <div className="lg:w-[45%] flex flex-col gap-2">
              {features.map((feature, i) => (
                <button
                  key={feature.number}
                  onClick={() => setActiveIndex(i)}
                  className={`text-left rounded-xl p-5 transition-colors duration-200 ${
                    activeIndex === i
                      ? "bg-card-gray"
                      : "bg-card-gray/50 hover:bg-card-gray/80"
                  }`}
                >
                  <div className="flex items-start gap-4">
                    <span className="font-(family-name:--font-dm-mono) text-brand-purple text-sm tracking-[-0.02em] mt-1">
                      {feature.number}
                    </span>
                    <div className="overflow-hidden">
                      <h3 className="font-(family-name:--font-manrope) font-bold text-header-white text-[18px] tracking-[-0.03em]">
                        {feature.title}
                      </h3>
                      <AnimatePresence initial={false}>
                        {activeIndex === i && (
                          <motion.div
                            initial={{ height: 0, opacity: 0 }}
                            animate={{ height: "auto", opacity: 1 }}
                            exit={{ height: 0, opacity: 0 }}
                            transition={{
                              height: { duration: 0.25, ease: easeOutQuint },
                              opacity: { duration: 0.2, ease: easeOutQuint },
                            }}
                            className="overflow-hidden"
                          >
                            <p className="font-(family-name:--font-inter) text-card-white/70 text-[14px] max-w-[98%] leading-relaxed tracking-[-0.01em] mt-2">
                              {feature.description}
                            </p>
                          </motion.div>
                        )}
                      </AnimatePresence>
                    </div>
                  </div>
                </button>
              ))}
            </div>

            {/* Right: Image — 55% */}
            <div className="lg:w-[55%] relative rounded-xl overflow-hidden min-h-[400px]">
              <Image
                src="/features/features-bg.png"
                alt=""
                fill
                className="object-cover"
              />
              <div className="absolute inset-0 flex items-center justify-center p-8">
                <AnimatePresence mode="wait">
                  <motion.div
                    key={activeIndex}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.2, ease: easeOutQuint }}
                    className="w-full"
                  >
                    <Image
                      src={features[activeIndex].gif}
                      alt={features[activeIndex].title}
                      width={600}
                      height={400}
                      className="rounded-lg w-full h-auto relative z-10"
                      unoptimized
                    />
                  </motion.div>
                </AnimatePresence>
              </div>
            </div>
          </div>
        </FadeIn>
      </div>
    </section>
  );
}

"use client";

import { FiServer, FiCodesandbox, FiRewind } from "react-icons/fi";
import { FadeIn } from "./fade-in";
import { type ComponentType } from "react";

const steps: {
  step: number;
  title: string;
  description: string;
  icon: ComponentType<{ className?: string; strokeWidth?: number }>;
}[] = [
  {
    step: 1,
    title: "Connect to prod.",
    description:
      "point mori at your production db. it authenticates with your existing provider credentials — no extra IAM headache.",
    icon: FiServer,
  },
  {
    step: 2,
    title: "Run all your queries.",
    description:
      "mori creates a lightweight sandbox to track drifts. data changes stay local, and reads feel like you're  testing in prod.",
    icon: FiCodesandbox,
  },
  {
    step: 3,
    title: "Test, then tear it down.",
    description:
      "run queries, apply migrations, modify data, and break things. when you're done, just reset or kill the instance.",
    icon: FiRewind,
  },
];

export function UnderTheHood() {
  return (
    <section className="py-24">
      <div className="max-w-[1200px] mx-auto px-6">
        <FadeIn>
          <h2 className="font-(family-name:--font-manrope) font-bold text-[26px] text-header-white tracking-[-0.05em] mb-6 ml-2">
            getting started
          </h2>
        </FadeIn>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {steps.map((item, i) => (
            <FadeIn key={item.step} delay={i * 0.1}>
              <div className="bg-[#191816] rounded-xl p-8 h-full flex flex-col min-h-[320px] md:min-h-[420px] hover:-translate-y-0.5 transition-transform duration-200">
                <span className="font-(family-name:--font-dm-mono) text-header-white text-[14px] uppercase">
                  STEP&nbsp;{item.step}
                </span>

                <div className="flex-1 flex items-center justify-center py-10">
                  <item.icon
                    className="w-[250px] h-[250px] text-[#454545]"
                    strokeWidth={0.9}
                  />
                </div>

                <div>
                  <h3 className="font-(family-name:--font-manrope) font-bold text-[18px] text-header-white tracking-[-0.03em] mb-2">
                    {item.title}
                  </h3>
                  <p className="font-(family-name:--font-inter) text-card-white/70 text-[16px] leading-relaxed tracking-[-0.01em]">
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

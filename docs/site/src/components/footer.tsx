"use client";

import { FiExternalLink, FiGithub, FiMail } from "react-icons/fi";
import { FiAtSign } from "react-icons/fi";
import { FadeIn } from "./fade-in";

const links = [
  {
    label: "psrth.sh",
    href: "https://psrth.sh",
    icon: FiExternalLink,
  },
  {
    label: "GitHub",
    href: "https://github.com/psr-th/mori",
    icon: FiGithub,
  },
  {
    label: "Email",
    href: "mailto:hello@psrth.sh",
    icon: FiMail,
  },
  {
    label: "X/Twitter",
    href: "https://x.com/psrth_",
    icon: FiAtSign,
  },
];

export function Footer() {
  return (
    <footer className="relative py-24 px-6 overflow-hidden">
      {/* Decorative sphere on right */}
      <div
        className="absolute -right-20 top-1/2 -translate-y-1/2 w-[300px] h-[300px] md:w-[500px] md:h-[500px] rounded-full pointer-events-none"
        style={{
          background:
            "radial-gradient(circle, rgba(50,45,35,0.8) 0%, rgba(30,28,22,0.5) 40%, transparent 70%)",
        }}
      />

      <div className="max-w-6xl mx-auto relative z-10">
        <FadeIn>
          <div className="flex flex-col items-start text-left gap-4">
            <h3 className="font-[family-name:var(--font-manrope)] font-bold text-2xl text-header-white tracking-[-0.05em]">
              moridb.sh
            </h3>
            <p className="font-[family-name:var(--font-inter)] text-card-white/40 text-sm">
              built with love in los angeles, california
            </p>

            <div className="flex items-center gap-6 mt-2">
              {links.map((link) => (
                <a
                  key={link.label}
                  href={link.href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1.5 text-sm text-card-white/50 hover:text-header-white transition-colors font-[family-name:var(--font-inter)]"
                >
                  <link.icon className="w-4 h-4" />
                  <span>{link.label}</span>
                </a>
              ))}
            </div>
          </div>
        </FadeIn>
      </div>
    </footer>
  );
}

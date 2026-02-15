"use client";

import { FiExternalLink, FiGithub, FiMail, FiTwitter } from "react-icons/fi";
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
    label: "Twitter",
    href: "https://x.com/psrth_",
    icon: FiTwitter,
  },
];

export function Footer() {
  return (
    <footer className="relative py-24 px-6 overflow-hidden">
      {/* Decorative background */}
      <div
        className="absolute inset-0 opacity-[0.03] pointer-events-none"
        style={{
          backgroundImage: "url(/images/footer.png)",
          backgroundSize: "cover",
          backgroundPosition: "center",
        }}
      />

      <div className="max-w-6xl mx-auto relative z-10">
        <FadeIn>
          <div className="flex flex-col items-center text-center gap-4">
            <h3 className="font-[family-name:var(--font-manrope)] font-bold text-2xl text-header-white tracking-[-0.05em]">
              moridb.sh
            </h3>
            <p className="font-[family-name:var(--font-inter)] italic text-card-white/50 text-sm">
              built with love in los angeles, california
            </p>

            <div className="flex items-center gap-6 mt-4">
              {links.map((link) => (
                <a
                  key={link.label}
                  href={link.href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1.5 text-sm text-card-white/50 hover:text-header-white transition-colors font-[family-name:var(--font-inter)]"
                >
                  <link.icon className="w-4 h-4" />
                  <span className="hidden sm:inline">{link.label}</span>
                </a>
              ))}
            </div>
          </div>
        </FadeIn>
      </div>
    </footer>
  );
}

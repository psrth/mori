"use client";

import Image from "next/image";
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
    href: "https://github.com/psrth/mori",
    icon: FiGithub,
  },
  {
    label: "Email",
    href: "mailto:mori@psrth.sh",
    icon: FiMail,
  },
  {
    label: "X/Twitter",
    href: "https://x.com/psrthsharma",
    icon: FiAtSign,
  },
];

export function Footer() {
  return (
    <footer className="relative py-24 overflow-hidden">
      <Image
        src="/footer-mori.png"
        alt=""
        width={900}
        height={900}
        className="absolute right-0 bottom-0 w-[400px] md:w-[750px] opacity-80 pointer-events-none object-contain"
      />

      <div className="max-w-[1200px] mx-auto px-6 relative z-10">
        <FadeIn>
          <div className="flex flex-col items-start text-left">
            <h3 className="font-(family-name:--font-manrope) font-bold text-[22px] tracking-tight text-header-white ml-2">
              moridb.sh
            </h3>
            <p className="font-(family-name:--font-inter) text-card-white/40 text-[22px] ml-2 tracking-tight">
              built with love in los angeles, california
            </p>

            <div className="flex items-center gap-3 mt-5 ">
              {links.map((link) => (
                <a
                  key={link.label}
                  href={link.href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-2 text-[16px] text-card-white/70 hover:text-header-white active:scale-[0.97] transition-all duration-150 font-(family-name:--font-inter) bg-card-gray rounded-full px-5 py-2.5"
                >
                  {link.label === "psrth.sh" ? (
                    <>
                      <span>{link.label}</span>
                      <link.icon className="w-4 h-4" />
                    </>
                  ) : (
                    <>
                      <link.icon className="w-4 h-4" />
                      <span>{link.label}</span>
                    </>
                  )}
                </a>
              ))}
            </div>
          </div>
        </FadeIn>
      </div>
    </footer>
  );
}

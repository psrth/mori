"use client";

import Image from "next/image";
import Link from "next/link";
import { FiExternalLink } from "react-icons/fi";

export function Navbar() {
  return (
    <nav className="fixed top-0 left-0 right-0 z-50 backdrop-blur-md bg-background/80 border-b border-white/5">
      <div className="max-w-6xl mx-auto px-6 h-16 flex items-center justify-between">
        <Link href="/" className="flex items-center gap-2.5">
          <Image
            src="/mori-logo.png"
            alt="mori logo"
            width={28}
            height={28}
            className="rounded-sm"
          />
          <span className="font-[family-name:var(--font-manrope)] font-bold text-lg text-header-white tracking-[-0.05em]">
            mori
          </span>
        </Link>

        <div className="flex items-center gap-4">
          <Link
            href="/docs"
            className="text-sm text-card-white/70 hover:text-header-white transition-colors font-[family-name:var(--font-inter)]"
          >
            Documentation
          </Link>
          <a
            href="https://github.com/psr-th/mori"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1.5 text-sm text-card-white/70 hover:text-header-white transition-colors border border-card-white/20 rounded-lg px-3 py-1.5 hover:border-card-white/40 font-[family-name:var(--font-inter)]"
          >
            GitHub
            <FiExternalLink className="w-3.5 h-3.5" />
          </a>
        </div>
      </div>
    </nav>
  );
}

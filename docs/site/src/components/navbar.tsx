"use client";

import Image from "next/image";
import Link from "next/link";
import { useEffect, useState } from "react";
import { FiExternalLink, FiMenu, FiX } from "react-icons/fi";

export function Navbar() {
  const [scrolled, setScrolled] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 20);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  // Close menu on resize to desktop
  useEffect(() => {
    const onResize = () => {
      if (window.innerWidth >= 768) setMenuOpen(false);
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  return (
    <nav
      className={`fixed top-0 left-0 right-0 z-50 backdrop-blur-md bg-background/80 transition-[border-color] duration-200 border-b ${
        scrolled ? "border-white/5" : "border-transparent"
      }`}
    >
      <div className="max-w-[1200px] mx-auto px-6 mt-8 mb-4 h-16 flex items-center justify-between">
        <Link href="/" className="flex items-center gap-2.5">
          <Image
            src="/mori-icon.png"
            alt="mori logo"
            width={28}
            height={28}
            className="rounded-sm"
          />
          <span className="font-(family-name:--font-manrope) font-bold text-lg text-header-white tracking-[-0.05em]">
            mori
          </span>
        </Link>

        {/* Desktop nav */}
        <div className="hidden md:flex items-center gap-6">
          <Link
            href="/docs"
            className="text-sm text-card-white/70 hover:text-header-white active:scale-[0.97] transition-all duration-150 font-(family-name:--font-inter)"
          >
            Documentation
          </Link>
          <Link
            href="/demo"
            className="text-sm text-card-white/70 hover:text-header-white active:scale-[0.97] transition-all duration-150 font-(family-name:--font-inter)"
          >
            Demo
          </Link>
          <a
            href="https://github.com/psrth/mori"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1.5 text-sm text-black bg-white hover:bg-white/90 active:scale-[0.97] rounded-full px-4 py-1.5 font-(family-name:--font-inter) transition-all duration-150"
          >
            GitHub
            <FiExternalLink className="w-3.5 h-3.5" />
          </a>
        </div>

        {/* Mobile hamburger */}
        <button
          onClick={() => setMenuOpen(!menuOpen)}
          className="md:hidden text-card-white/70 hover:text-header-white transition-colors duration-150"
        >
          {menuOpen ? <FiX className="w-6 h-6" /> : <FiMenu className="w-6 h-6" />}
        </button>
      </div>

      {/* Mobile menu */}
      {menuOpen && (
        <div className="md:hidden border-t border-white/5 bg-background/95 backdrop-blur-md px-6 pb-6 pt-4 flex flex-col gap-4">
          <Link
            href="/docs"
            onClick={() => setMenuOpen(false)}
            className="text-sm text-card-white/70 hover:text-header-white transition-all duration-150 font-(family-name:--font-inter)"
          >
            Documentation
          </Link>
          <Link
            href="/demo"
            onClick={() => setMenuOpen(false)}
            className="text-sm text-card-white/70 hover:text-header-white transition-all duration-150 font-(family-name:--font-inter)"
          >
            Demo
          </Link>
          <a
            href="https://github.com/psrth/mori"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1.5 text-sm text-black bg-white hover:bg-white/90 rounded-full px-4 py-1.5 font-(family-name:--font-inter) transition-all duration-150 w-fit"
          >
            GitHub
            <FiExternalLink className="w-3.5 h-3.5" />
          </a>
        </div>
      )}
    </nav>
  );
}

"use client";

import Image from "next/image";
import { useState, useEffect } from "react";
import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { FadeIn } from "./fade-in";

const providers = [
  { src: "/assets/provider_awsrds.png", name: "Amazon RDS" },
  { src: "/assets/provider_gcp.png", name: "Cloud SQL" },
  { src: "/assets/provider_neon.png", name: "Neon" },
  { src: "/assets/provider_supabase.png", name: "Supabase" },
  { src: "/assets/provider_planetscale.png", name: "PlanetScale" },
  { src: "/assets/provider_railway.png", name: "Railway" },
  { src: "/assets/provider_vercel.png", name: "Vercel" },
  { src: "/assets/provider_azure.png", name: "Azure" },
  { src: "/assets/provider_digitalocean.png", name: "DigitalOcean" },
  { src: "/assets/provider_cloudflare.png", name: "Cloudflare" },
  { src: "/assets/provider_firebase.png", name: "Firebase" },
  { src: "/assets/provider_upstash.png", name: "Upstash" },
];

const engines = [
  { src: "/assets/engine_postgresql.png", name: "PostgreSQL" },
  { src: "/assets/engine_mysql.png", name: "MySQL" },
  { src: "/assets/engine_redis.png", name: "Redis" },
  { src: "/assets/engine_sqlite.png", name: "SQLite" },
  { src: "/assets/engine_mariadb.png", name: "MariaDB" },
  { src: "/assets/engine_cockroachdb.png", name: "CockroachDB" },
  { src: "/assets/engine_mssqlserver.png", name: "SQL Server" },
  { src: "/assets/engine_duckdb.png", name: "DuckDB" },
  { src: "/assets/engine_firestore.png", name: "Firestore" },
];

export function CliDemo() {
  const [providerIndex, setProviderIndex] = useState(0);
  const [engineIndex, setEngineIndex] = useState(0);
  const shouldReduceMotion = useReducedMotion();

  useEffect(() => {
    if (shouldReduceMotion) return;
    const providerInterval = setInterval(() => {
      setProviderIndex((i) => (i + 1) % providers.length);
    }, 2000);
    const engineTimeout = setTimeout(() => {
      setEngineIndex((i) => (i + 1) % engines.length);
      const engineInterval = setInterval(() => {
        setEngineIndex((i) => (i + 1) % engines.length);
      }, 2000);
      cleanupRef.current = engineInterval;
    }, 1000);
    const cleanupRef = {
      current: null as ReturnType<typeof setInterval> | null,
    };
    return () => {
      clearInterval(providerInterval);
      clearTimeout(engineTimeout);
      if (cleanupRef.current) clearInterval(cleanupRef.current);
    };
  }, []);

  return (
    <section>
      <FadeIn>
        <div className="flex items-center justify-center gap-4 font-(family-name:--font-dm-mono) text-lg md:text-xl tracking-[-0.02em]">
          <span className="text-card-white/30">&gt;</span>
          <span className="text-brand-purple-light/80 font-medium">mori</span>
          <span className="text-card-white/55">init</span>
          <span className="text-card-white/30 ml-2">--db</span>

          <span className="relative inline-flex items-center justify-center w-[160px] h-[160px] mx-2">
            <AnimatePresence mode="popLayout">
              <motion.span
                key={engineIndex}
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -8 }}
                transition={{ duration: 0.3, ease: [0.23, 1, 0.32, 1] }}
                className="absolute inset-0 flex items-center justify-center"
              >
                <Image
                  src={engines[engineIndex].src}
                  alt={engines[engineIndex].name}
                  width={160}
                  height={160}
                  className="object-contain"
                />
              </motion.span>
            </AnimatePresence>
          </span>

          <span className="text-card-white/30 ml-2">--provider</span>

          <span className="relative inline-flex items-center justify-center w-[160px] h-[160px] mx-2">
            <AnimatePresence mode="popLayout">
              <motion.span
                key={providerIndex}
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -8 }}
                transition={{ duration: 0.3, ease: [0.23, 1, 0.32, 1] }}
                className="absolute inset-0 flex items-center justify-center"
              >
                <Image
                  src={providers[providerIndex].src}
                  alt={providers[providerIndex].name}
                  width={160}
                  height={160}
                  className="object-contain"
                />
              </motion.span>
            </AnimatePresence>
          </span>
        </div>
      </FadeIn>
    </section>
  );
}

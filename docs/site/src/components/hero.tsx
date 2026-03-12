"use client";

import Image from "next/image";
import { useState } from "react";
import { motion, useReducedMotion } from "framer-motion";
import { FiCopy, FiCheck, FiHelpCircle } from "react-icons/fi";
import { FadeIn } from "./fade-in";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";

const DEFAULT_PROMPT = `Hey! I came across this open-source tool called "mori" online, can you evaluate whether this would be a good fit for our development workflow? Go through our codebase, specifically focusing on the database, provider, ORM, and the common queries we use. Then, pull information from mori's website ->
- GitHub repository: https://github.com/psrth/mori
- Documentation: https://moridb.sh/docs/

## Overview
Mori is an open-source CLI tool that lets you develop against your production database without risk. It sits as a transparent proxy between your app and the database — reads hit real prod data, writes are captured in a local shadow copy. Production is never modified. Think of it as copy-on-write for your database.
**Supported engines:** PostgreSQL, MySQL, MariaDB, CockroachDB, MSSQL, SQLite, DuckDB, Redis, Firestore
**Supported providers:** Direct/self-hosted, AWS RDS/Aurora, GCP Cloud SQL, Azure Database, Neon, Supabase, PlanetScale, Vercel, Postgres, DigitalOcean, Railway, Upstash, Cloudflare D1/KV, Firebase

## 1. Database compatibility
Scan the codebase for database connection strings, ORM configs, driver imports, docker-compose services, and environment variables. What database engine(s) does this project use? Are they in Mori's supported list above?

## 2. Provider compatibility
Check for cloud provider references — hosting configs, deployment files, IaC, (Terraform/Pulumi), or environment patterns that indicate where the database is hosted. Does the provider match one of Mori's supported providers?

## 3. Would this project benefit from Mori? Consider:
- **Data realism**: Does the app have behavior that depends on real data (search, recommendations, dashboards, reports, edge cases in business logic)? Synthetic seeds often miss these.
- **Safe mutation testing**: Would it be useful to test writes (migrations, backfills, bulk updates, deletes) against real data without consequences?
- **AI agent workflows**: Is this project using or planning to use AI coding agents? Mori has a built-in MCP server (mori start --mcp) that gives agents safe read/write database access.
- **Onboarding**: Would new devs benefit from instant access to a realistic database without maintaining seed scripts?
- **Schema complexity**: Does the project have enough tables/relations that synthetic data is hard to keep realistic?
- **Known Limitations:** for our engine, are any of the known limitations blockers for local testing?

## 4. Final Recommendation
If Mori is a good fit, explain in detail (a) why is it a good fit, (b) what would be the impact of using this tool on a day-to-day basis, (c) show what getting started would look like: The mori init --from "CONNECTION_STRING" command using the project's actual connection string format and how to swap the app's database config to point at the Mori proxy.

Keep the assessment honest. If the project uses an unsupported database, doesn't add value, or is simple enough that local seeds work fine, say so.
`;

export function Hero() {
  const [prompt, setPrompt] = useState(DEFAULT_PROMPT);
  const [copied, setCopied] = useState(false);
  const [promptCopied, setPromptCopied] = useState(false);
  const [dialogOpen, setDialogOpen] = useState(false);
  const shouldReduceMotion = useReducedMotion();

  return (
    <section className="pt-46 pb-0">
      <div className="max-w-[1200px] mx-auto px-6 text-left">
        <FadeIn delay={0}>
          <h1 className="font-(family-name:--font-manrope) font-bold text-[28px] md:text-[36px] text-header-white tracking-[-0.05em] leading-[1.1]">
            test on prod. break nothing.
          </h1>
        </FadeIn>

        <FadeIn delay={0.1}>
          <p className="mt-3 text-[16px] md:text-[18px] text-card-white/70 max-w-3xl font-(family-name:--font-inter) leading-[1.6] tracking-[-0.01em]">
            mori is an ephemeral proxy to your production database. writes are
            sandboxed locally. reads get live data from prod, resolved with all
            your local changes in-flight.
          </p>
        </FadeIn>

        <FadeIn delay={0.2}>
          <div className="mt-12 flex flex-col items-start gap-3">
            <div className="-ml-2 flex items-center bg-card-gray border border-card-white/10 hover:border-card-white/20 rounded-full px-4 md:px-6 py-3 max-w-xl w-full transition-colors duration-150 overflow-hidden">
              <code className="font-mono text-[13px] md:text-[16px] flex-1 truncate">
                <span className="text-[#b56cce]">curl</span>
                <span className="text-card-white/80">
                  {" "}
                  -fsSL https://moridb.sh/install.sh | sh
                </span>
              </code>
              <button
                onClick={() => {
                  navigator.clipboard.writeText(
                    "curl -fsSL https://moridb.sh/install.sh | sh",
                  );
                  setCopied(true);
                  setTimeout(() => setCopied(false), 2000);
                }}
                className="ml-4 text-card-white/40 hover:text-card-white/70 active:scale-[0.97] transition-all duration-150 cursor-pointer"
              >
                {copied ? (
                  <FiCheck className="w-4 h-4 text-green-400" />
                ) : (
                  <FiCopy className="w-4 h-4" />
                )}
              </button>
            </div>

            <AlertDialog open={dialogOpen} onOpenChange={setDialogOpen}>
              <AlertDialogTrigger asChild>
                <button className="flex items-center gap-1.5 text-card-white/40 text-[16px] font-(family-name:--font-inter) ml-4 cursor-pointer hover:text-card-white/60 transition-colors duration-150 group">
                  <FiHelpCircle className="w-3.5 h-3.5" />
                  <span className="underline decoration-card-white/20 group-hover:decoration-card-white/40 underline-offset-[3px] transition-colors duration-150">
                    or ask your coding agent what it thinks
                  </span>
                </button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>
                    Ask your coding agent to evaluate mori
                  </AlertDialogTitle>
                  <AlertDialogDescription>
                    Paste this into wherever you use coding agents (Claude Code,
                    Cursor, Codex, etc.)
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <textarea
                  value={prompt}
                  onChange={(e) => setPrompt(e.target.value)}
                  className="w-full min-h-[120px] bg-card-gray border border-card-white/10 rounded-lg p-4 text-sm text-card-white/80 font-mono resize-y focus:outline-none focus:border-card-white/20"
                />
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancel</AlertDialogCancel>
                  <AlertDialogAction
                    onClick={(e) => {
                      e.preventDefault();
                      navigator.clipboard.writeText(prompt);
                      setPromptCopied(true);
                      setTimeout(() => {
                        setPromptCopied(false);
                        setDialogOpen(false);
                      }, 1000);
                    }}
                  >
                    {promptCopied ? "Copied!" : "Copy prompt"}
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        </FadeIn>
      </div>

      <FadeIn delay={0.3} duration={0.5}>
        <motion.div
          initial={shouldReduceMotion ? {} : { scale: 0.98 }}
          whileInView={{ scale: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6, delay: 0.3, ease: [0.23, 1, 0.32, 1] }}
          className="mt-20 max-w-[1340px] mx-auto px-6"
        >
          <Image
            src="/images/hero-img.png"
            alt="mori dashboard preview"
            width={1340}
            height={754}
            priority
            className="w-full rounded-xl"
          />
        </motion.div>
      </FadeIn>
    </section>
  );
}

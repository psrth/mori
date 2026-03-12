import type { Metadata } from "next";
import { Manrope, Inter, DM_Mono } from "next/font/google";
import Script from "next/script";
import "./globals.css";

const manrope = Manrope({
  variable: "--font-manrope",
  subsets: ["latin"],
  weight: ["400", "500", "600", "700", "800"],
});

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
  weight: ["400", "500", "600"],
});

const dmMono = DM_Mono({
  variable: "--font-dm-mono",
  subsets: ["latin"],
  weight: ["400"],
});

export const metadata: Metadata = {
  title: "mori — test on prod. break nothing.",
  description:
    "mori creates copy-on-write proxy layers over your production database. reads hit real data, writes are sandboxed. test migrations, seed data, break things — then tear it all down.",
  openGraph: {
    title: "mori — test on prod. break nothing.",
    description:
      "mori creates copy-on-write proxy layers over your production database. reads hit real data, writes are sandboxed.",
    url: "https://moridb.sh",
    siteName: "mori",
    type: "website",
  },
  twitter: {
    card: "summary_large_image",
    title: "mori — test on prod. break nothing.",
    description:
      "copy-on-write proxy layers over your production database. reads hit real data, writes are sandboxed.",
  },
  icons: {
    icon: "/mori-logo.svg",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <head>
        <Script
          src={`https://www.googletagmanager.com/gtag/js?id=${process.env.NEXT_PUBLIC_GA_ID}`}
          strategy="afterInteractive"
        />
        <Script id="google-analytics" strategy="afterInteractive">
          {`
            window.dataLayer = window.dataLayer || [];
            function gtag(){dataLayer.push(arguments);}
            gtag('js', new Date());
            gtag('config', '${process.env.NEXT_PUBLIC_GA_ID}');
          `}
        </Script>
      </head>
      <body
        className={`${manrope.variable} ${inter.variable} ${dmMono.variable} antialiased`}
      >
        {children}
      </body>
    </html>
  );
}

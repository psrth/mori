import { Navbar } from "@/components/navbar";
import { Hero } from "@/components/hero";
import { CliDemo } from "@/components/cli-demo";
import { UnderTheHood } from "@/components/under-the-hood";
import { Features } from "@/components/features";
import { Footer } from "@/components/footer";

export default function Home() {
  return (
    <>
      <Navbar />
      <main>
        <Hero />
        <CliDemo />
        <UnderTheHood />
        <Features />
      </main>
      <Footer />
    </>
  );
}

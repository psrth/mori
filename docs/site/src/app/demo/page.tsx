import { Navbar } from "@/components/navbar";
import { Footer } from "@/components/footer";

export default function Demo() {
  return (
    <>
      <Navbar />
      <main className="min-h-screen pt-36 md:pt-40 pb-20 px-4 md:px-6">
        <div className="max-w-[900px] mx-auto">
          <h1 className="font-(family-name:--font-manrope) font-bold text-3xl md:text-4xl text-header-white tracking-[-0.05em] mb-4">
            Demo
          </h1>
          <p className="text-card-white/70 font-(family-name:--font-inter) text-sm md:text-base mb-8 md:mb-10">
            Watch mori in action — see how copy-on-write proxy layers work over
            a production database.
          </p>
          <div
            style={{
              position: "relative",
              paddingBottom: "64.92335437330928%",
              height: 0,
            }}
          >
            <iframe
              src="https://www.loom.com/embed/cf373afb741a49e0820895405524472d"
              frameBorder="0"
              allowFullScreen
              style={{
                position: "absolute",
                top: 0,
                left: 0,
                width: "100%",
                height: "100%",
                borderRadius: "12px",
              }}
            />
          </div>
        </div>
      </main>
      <Footer />
    </>
  );
}

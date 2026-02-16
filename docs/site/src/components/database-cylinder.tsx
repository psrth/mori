export function DatabaseCylinder({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 180 260"
      className={className}
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      {/* Top ellipse */}
      <ellipse
        cx="90"
        cy="50"
        rx="70"
        ry="28"
        stroke="rgba(255,255,255,0.15)"
        strokeWidth="1.5"
      />
      {/* Left side */}
      <line
        x1="20"
        y1="50"
        x2="20"
        y2="210"
        stroke="rgba(255,255,255,0.15)"
        strokeWidth="1.5"
      />
      {/* Right side */}
      <line
        x1="160"
        y1="50"
        x2="160"
        y2="210"
        stroke="rgba(255,255,255,0.15)"
        strokeWidth="1.5"
      />
      {/* Bottom ellipse */}
      <ellipse
        cx="90"
        cy="210"
        rx="70"
        ry="28"
        stroke="rgba(255,255,255,0.15)"
        strokeWidth="1.5"
      />
      {/* Middle banding line */}
      <ellipse
        cx="90"
        cy="130"
        rx="70"
        ry="28"
        stroke="rgba(255,255,255,0.07)"
        strokeWidth="1"
      />
    </svg>
  );
}

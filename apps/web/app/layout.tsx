import type { Metadata } from "next";
import type { ReactNode } from "react";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "Historical Steam Delivery Performance",
    template: "%s | Historical Steam Delivery Performance",
  },
  description:
    "Investigate governed steam-delivery commitments, measurements, SLA outcomes and corrections across industrial sites.",
};

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en-GB">
      <body>
        <a className="skip-link" href="#main-content">Skip to main content</a>
        {children}
      </body>
    </html>
  );
}

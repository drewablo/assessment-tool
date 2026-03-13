import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Ministry Assessment Tool",
  description: "Market assessment for schools, housing, and elder care ministries",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen">{children}</body>
    </html>
  );
}

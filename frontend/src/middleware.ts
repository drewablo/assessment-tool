export { default } from "next-auth/middleware";

export const config = {
  // Protect everything except the auth API routes, login page, and Next.js internals.
  matcher: ["/((?!api/auth|api/health|login|_next/static|_next/image|favicon.ico).*)"],
};

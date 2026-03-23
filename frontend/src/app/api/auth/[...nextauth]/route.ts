export const dynamic = "force-dynamic";

import NextAuth from "next-auth";
import CredentialsProvider from "next-auth/providers/credentials";
import { compareSync } from "bcryptjs";

// Parse AUTH_USERS="alice:pass1,bob:pass2" into a lookup map.
// Supports both plain-text and bcrypt-hashed passwords.
// To use hashed passwords: AUTH_USERS="alice:$2b$10$hash...,bob:$2b$10$hash..."
// Generate hashes with: node -e "console.log(require('bcryptjs').hashSync('mypass', 10))"
const userMap = new Map<string, string>(
  (process.env.AUTH_USERS || "")
    .split(",")
    .filter(Boolean)
    .map((entry) => {
      const colonIdx = entry.indexOf(":");
      const username = entry.slice(0, colonIdx).trim();
      const password = entry.slice(colonIdx + 1).trim();
      return [username, password];
    })
);

function isBcryptHash(value: string): boolean {
  return /^\$2[aby]\$\d{2}\$.{53}$/.test(value);
}

const handler = NextAuth({
  providers: [
    CredentialsProvider({
      name: "Credentials",
      credentials: {
        username: { label: "Username", type: "text" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        const { username = "", password = "" } = credentials ?? {};
        const stored = userMap.get(username);
        if (!stored) return null;

        const match = isBcryptHash(stored)
          ? compareSync(password, stored)
          : stored === password;

        if (match) {
          return { id: username, name: username };
        }
        return null;
      },
    }),
  ],
  pages: { signIn: "/login" },
  session: { strategy: "jwt" },
  secret: process.env.NEXTAUTH_SECRET,
});

export { handler as GET, handler as POST };

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
    NEXT_PUBLIC_API_KEY: process.env.NEXT_PUBLIC_API_KEY || "",
  },
  webpack: (config) => {
    // We only use CredentialsProvider (no OAuth/OpenID flows), so alias
    // openid-client away entirely. Using `false` avoids runtime filesystem
    // references to a local stub file that may not exist in standalone images.
    config.resolve.alias["openid-client"] = false;
    return config;
  },
};

module.exports = nextConfig;

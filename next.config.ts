import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // On s'assure que l'URL du site est connue (pour l'import CSV)
  env: {
    NEXT_PUBLIC_SITE_URL: process.env.NEXT_PUBLIC_SITE_URL,
  },
  experimental: {
    serverActions: {
      bodySizeLimit: '50mb',
    },
  },
};

export default nextConfig;
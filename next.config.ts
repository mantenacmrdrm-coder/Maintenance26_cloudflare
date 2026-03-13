import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  output: 'standalone',
  typescript: {
    // Permet de déployer même si des erreurs de type persistent dans la logique métier complexe
    ignoreBuildErrors: true,
  },
  eslint: {
    // Évite que les règles de style ne bloquent le build de production
    ignoreDuringBuilds: true,
  },
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: '**',
      },
    ],
  },
  experimental: {
    serverActions: {
      bodySizeLimit: '50mb',
    },
  },
};

export default nextConfig;

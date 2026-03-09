import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  // Pour les variables publiques (ex: ton SITE_URL pour imports CSV ou API calls)
  env: {
    NEXT_PUBLIC_SITE_URL: process.env.NEXT_PUBLIC_SITE_URL,
  },

  // Important pour Cloudflare Pages + Edge runtime
  output: 'standalone',  // ou 'export' si tu veux full static (mais pas pour SSR)

  // Images : configure pour éviter des erreurs avec next/image sur Edge
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: '**',  // ou liste tes domaines autorisés (ex: 'images.unsplash.com')
      },
    ],
    // Si tu as beaucoup d'images locales, ajoute : unoptimized: true (mais attention perf)
  },

  // Experimental features (tu as déjà serverActions, c'est bien)
  experimental: {
    serverActions: {
      bodySizeLimit: '50mb',  // OK pour uploads lourds
    },
    // Si tu as des middleware ou features récentes
    // middlewarePrefetch: 'flexible',  // optionnel, teste si prefetch bugge
  },

  // Tweaks webpack pour éviter des crashes build sur CF (cache, plugins inutiles)
  webpack: (config, { isServer }) => {
    if (!isServer) {
      // Évite certains modules Node.js côté client
      config.resolve.fallback = {
        ...config.resolve.fallback,
        fs: false,
        path: false,
        os: false,
      };
    }

    // Désactive cache webpack si build instable sur CF
    config.cache = false;

    // Filtre plugins de cache problématiques (souvent mentionné)
    config.plugins = config.plugins?.filter((plugin: any) => {
      const name = plugin.constructor.name;
      return !name.includes('Cache') && !name.includes('MiniCssExtract');
    });

    return config;
  },

  // Optionnel mais utile : désactive dev indicators en prod
  devIndicators: {
    buildActivity: false,
  },
};

export default nextConfig;
import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  typescript: {
    ignoreBuildErrors: true,
    
  },

  // ✅ Option racine pour le développement (Next.js 14.2+)
  allowedDevOrigins: [
    'legendary-adventure-wrp7x9pjprpvfgvw7-3000.app.github.dev',
    '*.app.github.dev',
    '*.github.dev',
    'localhost:3000',
    '127.0.0.1:3000',
  ],

  // ✅ Option spécifique aux Server Actions (dans experimental)
  experimental: {
    serverActions: {
      allowedOrigins: [
        'legendary-adventure-wrp7x9pjprpvfgvw7-3000.app.github.dev',
        '*.app.github.dev',
        '*.github.dev',
        'localhost:3000',
        '127.0.0.1:3000',
      ],
      bodySizeLimit: '50mb',
    },
  },

  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'placehold.co',
        port: '',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'images.unsplash.com',
        port: '',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'picsum.photos',
        port: '',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'i.imgur.com',
        port: '',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'lh3.googleusercontent.com',
        port: '',
        pathname: '/**',
      },
    ],
  },

  // ✅ AJOUT IMPORTANT POUR CLOUDFLARE D1
  // Cela empêche Next.js d'essayer de "bundler" la librairie native D1
  webpack: (config, { isServer }) => {
    if (isServer) {
      config.externals = config.externals || [];
      config.externals.push('@libsql/client');
    }
    return config;
  },
};

export default nextConfig;
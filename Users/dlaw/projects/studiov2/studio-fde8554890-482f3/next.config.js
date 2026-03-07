/** @type {import('next').NextConfig} */
const nextConfig = {
  typescript: {
    ignoreBuildErrors: true,
  },

  // ✅ Add allowed origins for development
  allowedDevOrigins: [
    "3000-firebase-studio-1765109651160.cluster-lu4mup47g5gm4rtyvhzpwbfadi.cloudworkstations.dev",
    '*.cloudworkstations.dev', // A more general wildcard
    'localhost:3000',
    '127.0.0.1:3000',
  ],

  // ✅ Add allowed origins for Server Actions
  experimental: {
    serverActions: {
      allowedOrigins: [
        "3000-firebase-studio-1765109651160.cluster-lu4mup47g5gm4rtyvhzpwbfadi.cloudworkstations.dev",
        '*.cloudworkstations.dev',
        'localhost:3000',
        '127.0.0.1:3000',
      ],
      bodySizeLimit: '2mb',
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
};

module.exports = nextConfig;

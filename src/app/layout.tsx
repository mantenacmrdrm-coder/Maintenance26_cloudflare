import type { Metadata } from 'next';
import { PT_Sans } from 'next/font/google';
import { AppShell } from '@/components/app-shell';
import { Toaster } from '@/components/ui/toaster';
import './globals.css';
import { FirebaseClientProvider } from '@/firebase';
import { cn } from '@/lib/utils';

const ptSans = PT_Sans({
  subsets: ['latin'],
  weight: ['400', '700'],
  style: ['normal', 'italic'],
  display: 'swap',
  variable: '--font-pt-sans',
});

export const metadata: Metadata = {
  title: 'Maintenance Hub',
  description: 'Gestion de la maintenance des équipements',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="fr" suppressHydrationWarning>
      <body className={cn('font-body antialiased', ptSans.variable)}>
        <FirebaseClientProvider>
          <AppShell>{children}</AppShell>
        </FirebaseClientProvider>
        <Toaster />
      </body>
    </html>
  );
}

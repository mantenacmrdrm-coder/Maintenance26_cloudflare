'use client';

import React from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { cn } from '@/lib/utils';
import { Button } from './ui/button';
import {
  LayoutDashboard,
  Wrench,
  ClipboardList,
  CalendarClock,
  Bell,
  History,
  Database,
  CalendarCheck2,
  Cog,
  FolderKanban,
  Package,
  Droplet,
  LogIn,
} from 'lucide-react';

const navItems = [
  {
    href: '/',
    label: 'Tableau de bord',
    icon: LayoutDashboard,
  },
  {
    href: '/equipment',
    label: 'Équipements',
    icon: Wrench,
  },
  {
    href: '/operations',
    label: 'Opérations',
    icon: ClipboardList,
  },
  {
    href: '/planning',
    label: 'Planification',
    icon: CalendarClock,
  },
  {
    href: '/suivi',
    label: 'Suivi',
    icon: CalendarCheck2,
  },
  {
    href: '/stock',
    label: 'Saisie Consommations',
    icon: Package,
  },
   {
    href: '/stock-entries',
    label: 'Entrées de Stock',
    icon: LogIn,
  },
  {
    href: '/consommations',
    label: 'Consommations',
    icon: Droplet,
  },
  {
    href: '/alerts',
    label: 'Alertes',
    icon: Bell,
  },
   {
    href: '/documents',
    label: 'Documents',
    icon: FolderKanban,
  },
  {
    href: '/history',
    label: 'Historique',
    icon: History,
  },
  {
    href: '/parameters',
    label: 'Paramètres',
    icon: Cog,
  },
  {
    href: '/init-db',
    label: 'Initialisation',
    icon: Database,
  }
];

export function MainNav() {
  const pathname = usePathname();

  return (
    <nav className="flex flex-col gap-2">
      {navItems.map(item => {
        const isActive =
          item.href === '/' ? pathname === '/' : pathname.startsWith(item.href);
        return (
          <Button
            key={item.href}
            asChild
            variant={isActive ? 'default' : 'ghost'}
            className="justify-start"
          >
            <Link href={item.href}>
              <item.icon className="mr-2 h-4 w-4" />
              {item.label}
            </Link>
          </Button>
        );
      })}
    </nav>
  );
}

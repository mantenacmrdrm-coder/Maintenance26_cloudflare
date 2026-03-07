'use client';

import React from 'react';
import {
  Sidebar,
  SidebarContent,
  SidebarHeader,
  SidebarInset,
  SidebarProvider,
  SidebarTrigger,
  SidebarFooter,
} from '@/components/ui/sidebar';
import { MainNav } from './main-nav';
import { Button } from './ui/button';
import { Construction, LogOut } from 'lucide-react';
import Link from 'next/link';
import { useUser } from '@/firebase/auth/use-user';
import { signOut } from '@/firebase/auth/auth';
import { Avatar, AvatarFallback, AvatarImage } from './ui/avatar';
import { Skeleton } from './ui/skeleton';


export function AppShell({ children }: { children: React.ReactNode }) {
  const user = useUser();

  return (
    <SidebarProvider>
      <Sidebar>
        <SidebarHeader className="p-4">
            <Link href="/" className="flex items-center gap-2">
                <Button variant="ghost" size="icon" className="bg-primary hover:bg-primary/90 text-primary-foreground rounded-lg">
                    <Construction className="h-5 w-5" />
                </Button>
                <h1 className="text-xl font-bold text-primary">Maintenance Hub</h1>
            </Link>
        </SidebarHeader>
        <SidebarContent className="p-4">
          <MainNav />
        </SidebarContent>
        <SidebarFooter className="p-4">
            {user ? (
              <div className="flex items-center gap-3">
                <Avatar className="h-9 w-9">
                  <AvatarImage src={user.photoURL || ''} alt={user.displayName || ''} />
                  <AvatarFallback>{user.displayName?.charAt(0) || 'U'}</AvatarFallback>
                </Avatar>
                <div className="flex-1 overflow-hidden">
                  <p className="truncate text-sm font-semibold">{user.displayName}</p>
                  <p className="truncate text-xs text-muted-foreground">{user.email}</p>
                </div>
                <Button variant="ghost" size="icon" onClick={signOut}>
                  <LogOut className="h-4 w-4" />
                </Button>
              </div>
            ) : (
              <div className="flex items-center gap-3">
                 <Avatar className="h-9 w-9">
                  <AvatarFallback>I</AvatarFallback>
                </Avatar>
                <div className="flex-1 overflow-hidden">
                  <p className="truncate text-sm font-semibold">Invité</p>
                  <p className="truncate text-xs text-muted-foreground">Non connecté</p>
                </div>
              </div>
            )}
        </SidebarFooter>
      </Sidebar>
      <SidebarInset className="max-w-full overflow-x-hidden p-4 lg:p-8">
        <div className='flex justify-end mb-4 -mt-2 print-hide'>
          <SidebarTrigger />
        </div>
        {children}
      </SidebarInset>
    </SidebarProvider>
  );
}

import { Button } from '@/components/ui/button';
import Link from 'next/link';
import { Bell, CalendarClock, History } from 'lucide-react';

export function QuickActions() {
  return (
    <div className='flex flex-col gap-4'>
        <Button asChild variant='default' size="lg">
            <Link href="/history">
                <History className="mr-2 h-4 w-4" />
                Voir l'Historique
            </Link>
        </Button>
        <Button asChild variant='secondary' size="lg">
            <Link href="/planning">
                <CalendarClock className="mr-2 h-4 w-4" />
                Aller à la Planification
            </Link>
        </Button>
            <Button asChild variant='secondary' size="lg">
            <Link href="/alerts">
                <Bell className="mr-2 h-4 w-4" />
                Générer les alertes
            </Link>
        </Button>
    </div>
  );
}

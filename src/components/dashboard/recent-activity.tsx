import type { Operation } from '@/lib/types';
import { Wrench } from 'lucide-react';
import Link from 'next/link';
import { Button } from '../ui/button';
import { CardDescription } from '../ui/card';

type Props = {
  data: Operation[];
};

export function RecentActivity({ data }: Props) {
  if (!data || data.length === 0) {
    return <CardDescription>Aucune activité récente.</CardDescription>;
  }

  return (
    <ul className="space-y-4">
      {data.map(op => (
        <li key={op.id} className="flex items-center gap-4">
          <div className="icon-container icon-container-destructive">
            <Wrench className="h-6 w-6" />
          </div>
          <div className="flex-1">
            <p className="font-semibold truncate">{op.operation}</p>
            <p className="text-sm text-muted-foreground">{op.matricule}</p>
          </div>
          <Button asChild variant="ghost" size="sm">
            <Link href={`/equipment/${op.matricule}`}>Voir</Link>
          </Button>
        </li>
      ))}
    </ul>
  );
}

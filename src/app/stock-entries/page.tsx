import Link from 'next/link';

export const dynamic = 'force-dynamic';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { PlusCircle } from 'lucide-react';
import { getStockEntriesAction } from '@/lib/actions/maintenance-actions';
import { EntriesListTable } from './entries-list-table';

export default async function StockEntriesPage() {
  const entries = await getStockEntriesAction();

  return (
    <div className="flex flex-col gap-8">
      <header>
        <div className="flex justify-between items-center flex-wrap gap-4">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">
              Entrées de Stock
            </h1>
            <p className="text-muted-foreground">
              Gérez et consultez les entrées de stock de lubrifiants.
            </p>
          </div>
          <Button asChild>
            <Link href="/stock-entries/new">
                <PlusCircle className="mr-2 h-4 w-4" />
                Nouvelle Entrée
            </Link>
          </Button>
        </div>
      </header>
      <main>
        <Card>
          <CardHeader>
            <CardTitle>Historique des Entrées</CardTitle>
            <CardDescription>
              Voici la liste de toutes les entrées de stock que vous avez enregistrées.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <EntriesListTable entries={entries} />
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

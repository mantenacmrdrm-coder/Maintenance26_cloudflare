import Link from 'next/link';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { FilePlus2 } from 'lucide-react';
import { getBonsDeSortieListAction } from '@/lib/actions/maintenance-actions';
import { BonsListTable } from './bons-list-table';


export const dynamic = 'force-dynamic';
export default async function BonsDeSortieListPage() {
  const bons = await getBonsDeSortieListAction();

  return (
    <div className="flex flex-col gap-8">
      <header>
        <div className="flex justify-between items-center flex-wrap gap-4">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Bons de Sortie</h1>
            <p className="text-muted-foreground">
              Gérez, modifiez et consultez les bons de sortie existants.
            </p>
          </div>
          <Button asChild>
            <Link href="/bons-de-sortie/new">
                <FilePlus2 className="mr-2 h-4 w-4" />
                Nouveau Bon de Sortie
            </Link>
          </Button>
        </div>
      </header>
      <main>
        <Card>
          <CardHeader>
            <CardTitle>Bons de Sortie Sauvegardés</CardTitle>
            <CardDescription>
              Voici la liste de tous les bons de sortie que vous avez générés.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <BonsListTable bons={bons} />
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

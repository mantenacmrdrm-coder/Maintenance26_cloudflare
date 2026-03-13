import Link from 'next/link';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { FilePlus2 } from 'lucide-react';
import { getDeclarationsListAction } from '@/lib/actions/maintenance-actions';
import { DeclarationsListTable } from './declarations-list-table';
import type { DeclarationListItem } from '@/lib/types';

export const dynamic = 'force-dynamic';

export default async function DeclarationsListPage() {
  const declarations: DeclarationListItem[] = await getDeclarationsListAction() as DeclarationListItem[];

  return (
    <div className="flex flex-col gap-8">
      <header>
        <div className="flex justify-between items-center flex-wrap gap-4">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">
              Déclarations de Panne
            </h1>
            <p className="text-muted-foreground">
              Gérez, modifiez et consultez les déclarations de panne existantes.
            </p>
          </div>
          <Button asChild>
            <Link href="/declarations/select-operation">
                <FilePlus2 className="mr-2 h-4 w-4" />
                Générer une nouvelle déclaration
            </Link>
          </Button>
        </div>
      </header>
      <main>
        <Card>
          <CardHeader>
            <CardTitle>Déclarations Sauvegardées</CardTitle>
            <CardDescription>
              Voici la liste de toutes les déclarations que vous avez générées.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <DeclarationsListTable declarations={declarations} />
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

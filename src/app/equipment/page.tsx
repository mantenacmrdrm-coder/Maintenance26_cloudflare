import { getAllEquipment } from '@/lib/data';
import { EquipmentDataTable } from './data-table';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { AlertTriangle, PlusCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import Link from 'next/link';


export const dynamic = 'force-dynamic';
export default async function EquipmentPage() {
  const equipments = await getAllEquipment();

  return (
    <div className="flex flex-col gap-8">
      <header>
        <div className="flex justify-between items-center flex-wrap gap-4">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Équipements</h1>
            <p className="text-muted-foreground">
              Gérez et consultez la liste de vos équipements.
            </p>
          </div>
          <Button asChild>
            <Link href="/equipment/new">
                <PlusCircle className="mr-2 h-4 w-4" />
                Nouvel équipement
            </Link>
          </Button>
        </div>
      </header>
       <main>
        {equipments.length > 0 ? (
          <EquipmentDataTable data={equipments} />
        ) : (
          <Alert variant="default" className="bg-amber-50 border-amber-200">
            <AlertTriangle className="h-4 w-4 !text-amber-600" />
            <AlertTitle className="text-amber-800 font-semibold">Aucune donnée trouvée</AlertTitle>
            <AlertDescription className="text-amber-700">
              La base de données n'a pas encore été initialisée ou la table `matrice` est vide.
              Allez au <a href="/" className="font-bold underline">Tableau de bord</a> pour initialiser la base de données.
            </AlertDescription>
          </Alert>
        )}
      </main>
    </div>
  );
}

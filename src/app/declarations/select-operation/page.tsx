import { getAllOperations } from '@/lib/data';
import { OperationsForDeclarationTable } from './operations-for-declaration-table';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { AlertTriangle } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import Link from 'next/link';


export const dynamic = 'force-dynamic';
export default async function SelectOperationForDeclarationPage() {
  const operations = await getAllOperations();

  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Générer une Déclaration de Panne</h1>
        <p className="text-muted-foreground">
          Sélectionnez une opération curative ci-dessous pour commencer à générer une "Déclaration de Panne".
        </p>
        <Button asChild variant="outline" className="mt-4">
            <Link href="/declarations">Retour à la liste des déclarations</Link>
        </Button>
      </header>
      <main>
        <Card>
            <CardHeader>
                <CardTitle>Étape 1: Liste des Opérations Curatives</CardTitle>
                <CardDescription>Sélectionnez une opération pour générer une déclaration.</CardDescription>
            </CardHeader>
            <CardContent>
                {operations.length > 0 ? (
                <OperationsForDeclarationTable data={operations} />
                ) : (
                <Alert variant="default" className="bg-amber-50 border-amber-200">
                    <AlertTriangle className="h-4 w-4 !text-amber-600" />
                    <AlertTitle className="text-amber-800 font-semibold">Aucune opération curative trouvée</AlertTitle>
                    <AlertDescription className="text-amber-700">
                    Vous devez d'abord enregistrer une opération curative pour pouvoir générer une déclaration.
                    </AlertDescription>
                </Alert>
                )}
            </CardContent>
        </Card>
      </main>
    </div>
  );
}

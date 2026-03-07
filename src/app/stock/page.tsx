import { StockForm } from './stock-form';
import { getEquipmentForConsumption } from '@/lib/data';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';


export const dynamic = 'force-dynamic';
export default async function StockPage() {
  const equipments = await getEquipmentForConsumption();

  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Saisie des Consommations</h1>
        <p className="text-muted-foreground">
          Saisissez ici les consommations journalières d'huiles et de graisses.
        </p>
      </header>
      <main>
        <Card>
            <CardHeader>
                <CardTitle>Formulaire de Saisie</CardTitle>
                <CardDescription>
                    Sélectionnez une date, puis ajoutez les consommations pour chaque équipement. Le type d'entretien sera calculé automatiquement.
                </CardDescription>
            </CardHeader>
            <CardContent>
                <StockForm equipments={equipments} />
            </CardContent>
        </Card>
      </main>
    </div>
  );
}

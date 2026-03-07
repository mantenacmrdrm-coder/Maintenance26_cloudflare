import { ConsumptionReportView } from './consumption-report-view';


export const dynamic = 'force-dynamic';
export default function ConsumptionReportsPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">État de Consommation Détaillé</h1>
        <p className="text-muted-foreground">
          Générez un rapport de consommation journalier pour un mois et un lubrifiant spécifiques.
        </p>
      </header>
      <main>
        <ConsumptionReportView />
      </main>
    </div>
  );
}

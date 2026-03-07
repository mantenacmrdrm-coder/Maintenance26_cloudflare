import { MonthlyReportsView } from './monthly-reports-view';


export const dynamic = 'force-dynamic';
export default function MonthlyReportsPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Rapports Mensuels de Stock</h1>
        <p className="text-muted-foreground">
          Générez un rapport de stock mensuel pour un lubrifiant sur une période donnée.
        </p>
      </header>
      <main>
        <MonthlyReportsView />
      </main>
    </div>
  );
}

import { AlertsView } from "./alerts-view";


export const dynamic = 'force-dynamic';
export default function AlertsPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Alertes de Maintenance</h1>
        <p className="text-muted-foreground">
          Générez des alertes préventives intelligentes grâce à l'IA.
        </p>
      </header>
      <main>
        <AlertsView />
      </main>
    </div>
  );
}

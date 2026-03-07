import { ConsumptionsView } from './consumptions-view';


export const dynamic = 'force-dynamic';
export default function ConsommationsPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Analyse des Consommations</h1>
        <p className="text-muted-foreground">
          Consultez, analysez et gérez vos stocks de lubrifiants.
        </p>
      </header>
      <main>
        <ConsumptionsView />
      </main>
    </div>
  );
}

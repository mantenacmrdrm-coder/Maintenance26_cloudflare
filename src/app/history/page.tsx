import { HistoryView } from './history-view';


export const dynamic = 'force-dynamic';
export default function HistoryPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Génération de l'Historique</h1>
        <p className="text-muted-foreground">
          Consolidez les données de maintenance dans une table d'historique unifiée.
        </p>
      </header>
       <main>
          <HistoryView />
      </main>
    </div>
  );
}

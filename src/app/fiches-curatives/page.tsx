import { FichesCurativesView } from "./fiches-curatives-view";


export const dynamic = 'force-dynamic';
export default function FichesCurativesPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Fiches de Maintenance Curative</h1>
        <p className="text-muted-foreground">
          Générez des fiches d'intervention curative pour une période ou un équipement spécifique.
        </p>
      </header>
      <main>
        <FichesCurativesView />
      </main>
    </div>
  );
}

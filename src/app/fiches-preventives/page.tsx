import { FichesPreventivesView } from "./fiches-preventives-view";


export const dynamic = 'force-dynamic';
export default function FichesPreventivesPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Fiches d'Entretien Préventif</h1>
        <p className="text-muted-foreground">
          Générez des fiches d'entretien préventif pour une période ou un équipement spécifique.
        </p>
      </header>
      <main>
        <FichesPreventivesView />
      </main>
    </div>
  );
}

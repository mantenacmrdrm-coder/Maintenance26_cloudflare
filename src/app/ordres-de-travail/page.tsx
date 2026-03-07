import { OrdresDeTravailView } from "./ordres-de-travail-view";


export const dynamic = 'force-dynamic';
export default function OrdresDeTravailPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Ordres de Travail</h1>
        <p className="text-muted-foreground">
          Générez des ordres de travail pour une période ou un équipement spécifique.
        </p>
      </header>
      <main>
        <OrdresDeTravailView />
      </main>
    </div>
  );
}

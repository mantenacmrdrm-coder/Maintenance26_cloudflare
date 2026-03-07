import { PlanningView } from '../planning/planning-view';


export const dynamic = 'force-dynamic';
export default function SuiviPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Suivi du Planning</h1>
        <p className="text-muted-foreground">
          Comparez le planning prévisionnel avec les interventions réalisées.
        </p>
      </header>
      <main>
        <PlanningView mode="suivi" />
      </main>
    </div>
  );
}

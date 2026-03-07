import { PlanningView } from './planning-view';

export default function PlanningPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Matrice de Planification</h1>
        <p className="text-muted-foreground">
          Générez et consultez les plannings de maintenance préventive.
        </p>
      </header>
      <main>
        <PlanningView mode="planning" />
      </main>
    </div>
  );
}

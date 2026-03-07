import { BonForm } from '../bon-form';


export const dynamic = 'force-dynamic';
export default function NewBonPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Nouveau Bon de Sortie</h1>
        <p className="text-muted-foreground">
          Remplissez les informations pour créer un nouveau bon de sortie.
        </p>
      </header>
      <main>
        <BonForm />
      </main>
    </div>
  );
}

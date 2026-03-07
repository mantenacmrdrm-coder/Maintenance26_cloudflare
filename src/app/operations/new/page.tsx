import { NewOperationForm } from './new-operation-form';


export const dynamic = 'force-dynamic';
export default function NewOperationPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Nouvelle Opération Curative</h1>
        <p className="text-muted-foreground">
          Enregistrez une nouvelle panne ou réparation non planifiée.
        </p>
      </header>
      <main>
        <NewOperationForm />
      </main>
    </div>
  );
}

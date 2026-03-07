import { NewEntryForm } from './new-entry-form';


export const dynamic = 'force-dynamic';
export default function NewStockEntryPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Nouvelle Entrée de Stock</h1>
        <p className="text-muted-foreground">
          Enregistrez une nouvelle livraison ou un ajout de lubrifiant au stock.
        </p>
      </header>
      <main>
        <NewEntryForm />
      </main>
    </div>
  );
}

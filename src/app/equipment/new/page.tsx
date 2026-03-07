import { EquipmentForm } from '../equipment-form';


export const dynamic = 'force-dynamic';
export default function NewEquipmentPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Nouvel Équipement</h1>
        <p className="text-muted-foreground">
          Ajoutez un nouvel équipement à la flotte.
        </p>
      </header>
      <main>
        <EquipmentForm />
      </main>
    </div>
  );
}

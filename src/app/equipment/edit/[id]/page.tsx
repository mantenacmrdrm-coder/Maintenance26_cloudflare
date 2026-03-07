import { getEquipmentByIdAction } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { EquipmentForm } from "../../equipment-form";
import type { Equipment } from "@/lib/types";


export const dynamic = 'force-dynamic';
export default async function EditEquipmentPage({ params }: { params: Promise<{ id: string }> }) {
    const resolvedParams = await params;
    const equipmentId = parseInt(resolvedParams.id, 10);
    if (isNaN(equipmentId)) {
        notFound();
    }

    const equipment = await getEquipmentByIdAction(equipmentId);

    if (!equipment) {
        notFound();
    }

    return (
        <div className="flex flex-col gap-8">
            <header>
                <h1 className="text-3xl font-bold tracking-tight">Modifier l'Équipement</h1>
                <p className="text-muted-foreground">
                    Modification de l'équipement <span className="font-bold text-primary">{equipment.matricule}</span>.
                </p>
            </header>
            <main>
                <EquipmentForm initialData={equipment as Equipment} />
            </main>
        </div>
    );
}

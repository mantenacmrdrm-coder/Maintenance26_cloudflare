import { getConsumptionByIdAction, getEquipmentForConsumptionAction } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { EditConsumptionForm } from "./edit-consumption-form";


export const dynamic = 'force-dynamic';
export default async function EditConsumptionPage({ params }: { params: Promise<{ id: string } }) {
    const consumptionId = parseInt(params.id, 10);
    if (isNaN(consumptionId)) {
        notFound();
    }

    const [consumption, equipments] = await Promise.all([
        getConsumptionByIdAction(consumptionId),
        getEquipmentForConsumptionAction()
    ]);
    
    if (!consumption) {
        notFound();
    }

    return (
        <div className="flex flex-col gap-8">
            <header>
                <h1 className="text-3xl font-bold tracking-tight">Modifier une Consommation</h1>
                <p className="text-muted-foreground">
                    Modification de la consommation du {consumption.date} pour l'équipement <span className="font-bold text-primary">{consumption.matricule}</span>.
                </p>
            </header>
            <main>
                <EditConsumptionForm consumption={consumption} equipments={equipments} />
            </main>
        </div>
    );
}

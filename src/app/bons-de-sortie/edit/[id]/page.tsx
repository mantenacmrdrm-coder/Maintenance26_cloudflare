import { getBonDeSortieAction } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { BonForm } from "../../bon-form";
import type { BonDeSortie } from "@/lib/types";


export const dynamic = 'force-dynamic';
export default async function EditBonPage({ params }: { params: { id: string } }) {
    const resolvedParams = await params;
    const bonId = parseInt(resolvedParams.id, 10);
    if (isNaN(bonId)) {
        notFound();
    }

    const bon = await getBonDeSortieAction(bonId);

    if (!bon) {
        notFound();
    }

    return (
        <div className="flex flex-col gap-8">
            <header>
                <h1 className="text-3xl font-bold tracking-tight">Modifier le Bon de Sortie</h1>
                <p className="text-muted-foreground">
                    Modification du bon N° <span className="font-bold text-primary">{bon.id}</span>.
                </p>
            </header>
            <main>
                <BonForm initialData={bon as BonDeSortie} />
            </main>
        </div>
    );
}

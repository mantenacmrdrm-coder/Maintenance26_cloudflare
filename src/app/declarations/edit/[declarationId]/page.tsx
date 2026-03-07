import { getDeclaration } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { EditDeclarationForm } from "./edit-form";
import type { DeclarationPanne } from "@/lib/types";


export const dynamic = 'force-dynamic';
export default async function EditDeclarationPage({ params }: { params: { declarationId: string } }) {
    const resolvedParams = await params;
    const declarationId = parseInt(resolvedParams.declarationId, 10);
    if (isNaN(declarationId)) {
        notFound();
    }

    const declaration = await getDeclaration(declarationId);

    if (!declaration) {
        notFound();
    }

    return (
        <div className="flex flex-col gap-8">
            <header>
                <h1 className="text-3xl font-bold tracking-tight">Modifier une Déclaration de Panne</h1>
                <p className="text-muted-foreground">
                    Modification de la déclaration pour l'équipement <span className="font-bold text-primary">{declaration.equipment.matricule}</span>.
                </p>
            </header>
            <main>
                <EditDeclarationForm declaration={declaration as DeclarationPanne} />
            </main>
        </div>
    );
}

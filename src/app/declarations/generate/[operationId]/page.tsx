import { getOperationForDeclaration } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { DeclarationForm } from "./form";
import type { Operation } from "@/lib/types";


export const dynamic = 'force-dynamic';
export default async function GenerateDeclarationPage({ params }: { params: Promise<{ operationId: string }> }) {
    const resolvedParams = await params;
    const operationId = parseInt(resolvedParams.operationId, 10);
    if (isNaN(operationId)) {
        notFound();
    }

    const operation = await getOperationForDeclaration(operationId);

    if (!operation) {
        notFound();
    }

    return (
        <div className="flex flex-col gap-8">
            <header>
                <h1 className="text-3xl font-bold tracking-tight">Générer une Déclaration de Panne</h1>
                <p className="text-muted-foreground">
                    Complétez les informations pour la déclaration de panne de l'équipement <span className="font-bold text-primary">{operation.matricule}</span>.
                </p>
            </header>
            <main>
                <DeclarationForm operation={operation as Operation} />
            </main>
        </div>
    );
}

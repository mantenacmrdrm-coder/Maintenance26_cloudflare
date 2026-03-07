
import { getOperationByIdAction } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { EditOperationForm } from "./edit-operation-form";
import type { Operation } from "@/lib/types";


export const dynamic = 'force-dynamic';
export default async function EditOperationPage({ params }: { params: { id: string } }) {
    const resolvedParams = await params;
    const operationId = parseInt(resolvedParams.id, 10);
    if (isNaN(operationId)) {
        notFound();
    }

    const operation = await getOperationByIdAction(operationId);

    if (!operation) {
        notFound();
    }

    return (
        <div className="flex flex-col gap-8">
            <header>
                <h1 className="text-3xl font-bold tracking-tight">Modifier l'Opération Curative</h1>
                <p className="text-muted-foreground">
                    Modification de l'opération pour <span className="font-bold text-primary">{operation.matricule}</span>.
                </p>
            </header>
            <main>
                <EditOperationForm operation={operation as Operation} />
            </main>
        </div>
    );
}

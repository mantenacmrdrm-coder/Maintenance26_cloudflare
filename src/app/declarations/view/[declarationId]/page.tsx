import { getDeclaration } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { DeclarationView } from "./view";


export const dynamic = 'force-dynamic';
export default async function ViewDeclarationPage({ params }: { params: Promise<{ declarationId: string }> }) {
    const resolvedParams = await params;
    const declarationId = parseInt(resolvedParams.declarationId, 10);
    if (isNaN(declarationId)) {
        notFound();
    }

    const declaration = await getDeclaration(declarationId);

    if (!declaration) {
        notFound();
    }

    return <DeclarationView declaration={declaration} />;
}

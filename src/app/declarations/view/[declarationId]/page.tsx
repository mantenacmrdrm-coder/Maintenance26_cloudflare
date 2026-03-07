import { getDeclaration } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { DeclarationView } from "./view";

export const dynamic = 'force-dynamic';

// 1. Mise à jour du type : params est maintenant un Promise
export default async function ViewDeclarationPage({ 
  params 
}: { 
  params: Promise<{ declarationId: string }> 
}) {
  // 2. Il faut faire un await sur params avant de le déstructurer
  const { declarationId } = await params;
  
  const id = parseInt(declarationId, 10);

  if (isNaN(id)) {
    notFound();
  }

  const declaration = await getDeclaration(id);

  if (!declaration) {
    notFound();
  }

  return <DeclarationView declaration={declaration} />;
}
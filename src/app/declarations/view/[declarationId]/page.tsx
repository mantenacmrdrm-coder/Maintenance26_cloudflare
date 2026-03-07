import { getDeclaration } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { DeclarationView } from "./view";

export const dynamic = 'force-dynamic';

export default async function ViewDeclarationPage({ params }: { params: { declarationId: string } }) {
  const { declarationId } = params;
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
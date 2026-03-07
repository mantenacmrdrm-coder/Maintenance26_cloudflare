import { getBonDeSortieAction } from "@/lib/actions/maintenance-actions";
import { notFound } from "next/navigation";
import { BonDeSortieView } from "./bon-view";


export const dynamic = 'force-dynamic';
export default async function ViewBonPage({ params }: { params: Promise<{ id: string }> }) {
    const resolvedParams = await params;
    const bonId = parseInt(resolvedParams.id, 10);
    if (isNaN(bonId)) {
        notFound();
    }

    const bon = await getBonDeSortieAction(bonId);

    if (!bon) {
        notFound();
    }

    return <BonDeSortieView bon={bon} />;
}

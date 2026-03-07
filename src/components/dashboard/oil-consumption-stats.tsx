'use client';

import { Droplet } from 'lucide-react';
import type { MonthlyPreventativeStats } from '@/lib/types';
import { ScrollArea } from '@/components/ui/scroll-area';
import { useMemo } from 'react';

type Props = {
    data: MonthlyPreventativeStats;
};

export function OilConsumptionStats({ data }: Props) {
    const { oilByType } = data;

    const { totalHuile, graisseQty, sortedHuiles } = useMemo(() => {
        let totalHuile = 0;
        let graisseQty = 0;
        const huiles: [string, number][] = [];

        for (const [type, quantity] of Object.entries(oilByType)) {
            if (type === 'graisse') {
                graisseQty = quantity;
            } else {
                totalHuile += quantity;
                huiles.push([type, quantity]);
            }
        }
        
        const sortedHuiles = huiles.sort(([, a], [, b]) => b - a);

        return { totalHuile, graisseQty, sortedHuiles };
    }, [oilByType]);
    
    return (
        <div className="h-full flex flex-col">
            <div className="flex items-start gap-4 mb-4">
                <div className="icon-container icon-container-primary">
                    <Droplet className="h-6 w-6" />
                </div>
                <div>
                    <p className="stat-label">Total Huiles Consommées</p>
                    <p className="stat-value">{totalHuile.toLocaleString('fr-FR', { minimumFractionDigits: 1, maximumFractionDigits: 1 })} L</p>
                </div>
            </div>

            <p className="text-sm font-medium mb-2 text-muted-foreground">Consommation par type :</p>
            <ScrollArea className="flex-1 pr-4 h-48">
                <div className="space-y-2">
                    {sortedHuiles.map(([type, quantity]) => (
                         <div key={type} className="flex justify-between items-center text-sm">
                            <span className="text-muted-foreground capitalize">{type.replace(/_/g, ' ').replace('v', 'V')}</span>
                            <span className="font-bold">{quantity.toLocaleString('fr-FR', { minimumFractionDigits: 1, maximumFractionDigits: 1 })} L</span>
                        </div>
                    ))}
                    {graisseQty > 0 && (
                        <div key="graisse" className="flex justify-between items-center text-sm pt-2 border-t mt-2">
                            <span className="text-muted-foreground capitalize">Graisse</span>
                            <span className="font-bold">{graisseQty.toLocaleString('fr-FR', { minimumFractionDigits: 1, maximumFractionDigits: 1 })} Kg</span>
                        </div>
                    )}
                    {sortedHuiles.length === 0 && graisseQty === 0 && (
                        <p className="text-sm text-muted-foreground text-center pt-8">Aucune consommation enregistrée pour cette période.</p>
                    )}
                </div>
            </ScrollArea>
        </div>
    );
}

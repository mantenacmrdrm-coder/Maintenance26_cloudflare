'use client';

import { useState, useTransition } from 'react';
import { useRouter } from 'next/navigation';
import { Button } from '@/components/ui/button';
import { FilePlus2, Loader2 } from 'lucide-react';
import { generateWeeklyReportAction } from '@/lib/actions/maintenance-actions';
import { useToast } from '@/hooks/use-toast';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';

export function GenerateReportButton() {
    const [isPending, startTransition] = useTransition();
    const { toast } = useToast();
    const router = useRouter();
    const [date, setDate] = useState<Date | undefined>(new Date());
    const [popoverOpen, setPopoverOpen] = useState(false);

    const handleClick = () => {
        if (!date) {
            toast({
                variant: 'destructive',
                title: 'Date non sélectionnée',
                description: 'Veuillez sélectionner une date pour générer le rapport.',
            });
            return;
        }

        startTransition(async () => {
            const result = await generateWeeklyReportAction(date as Date);
            if (result.success && result.reportId) {
                toast({
                    title: 'Rapport généré',
                    description: 'Le rapport hebdomadaire a été créé avec succès.',
                });
                setPopoverOpen(false);
                router.push(`/reports/${result.reportId}`);
                router.refresh();
            } else {
                toast({
                    variant: 'destructive',
                    title: 'Erreur de génération',
                    description: result.message || 'Une erreur est survenue lors de la génération du rapport.',
                });
            }
        });
    };

    return (
        <Popover open={popoverOpen} onOpenChange={setPopoverOpen}>
            <PopoverTrigger asChild>
                <Button variant="default">
                    <FilePlus2 className="mr-2 h-4 w-4" />
                    Nouveau Rapport Hebdomadaire
                </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="end">
                <div className="p-4 pt-6 space-y-4">
                     <div className="space-y-1.5 text-center">
                        <h4 className="font-medium leading-none">Choisir une date</h4>
                        <p className="text-sm text-muted-foreground">
                            Le rapport sera généré pour la semaine contenant la date choisie.
                        </p>
                    </div>
                    <Calendar
                        mode="single"
                        selected={date}
                        onSelect={setDate}
                        initialFocus
                    />
                    <Button onClick={handleClick} disabled={isPending || !date} className="w-full">
                        {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        Générer le rapport
                    </Button>
                </div>
            </PopoverContent>
        </Popover>
    );
}

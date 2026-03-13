'use client';

import { useState, useTransition } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Search, Loader2, CalendarIcon, Droplet } from 'lucide-react';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { useToast } from '@/hooks/use-toast';
import { getConsumptionsByDateRange } from '@/lib/actions/maintenance-actions';
import { ConsommationsDataTable } from './data-table';
import { StockSummaryTable } from './stock-summary-table';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { cn } from '@/lib/utils';

dayjs.locale('fr');

export function ConsumptionsView() {
    const [isPending, startTransition] = useTransition();
    const { toast } = useToast();
    const [dateRange, setDateRange] = useState<{from: Date | undefined, to: Date | undefined}>({
        from: dayjs().subtract(30, 'day').toDate(),
        to: new Date()
    });
    const [data, setData] = useState<any | null>(null);

    const handleSearch = () => {
        if (!dateRange.from || !dateRange.to) {
            toast({ variant: 'destructive', title: 'Erreur', description: 'Veuillez sélectionner une plage de dates complète.' });
            return;
        }

        startTransition(async () => {
            const result = await getConsumptionsByDateRange(dateRange.from as Date, dateRange.to as Date);
            if (result.success && result.data) {
                setData(result.data);
                toast({ title: 'Recherche terminée', description: `${result.data.consumptions?.length || 0} enregistrements trouvés.`});
            } else {
                toast({ variant: 'destructive', title: 'Erreur', description: result.message || "Erreur de chargement" });
                setData(null);
            }
        });
    };

    return (
        <div className="space-y-6">
            <Card className="print-hide">
                <CardHeader>
                    <CardTitle>Sélectionner une période</CardTitle>
                    <CardDescription>Choisissez une plage de dates pour analyser les consommations et gérer les stocks.</CardDescription>
                </CardHeader>
                <CardContent className="flex flex-wrap items-end gap-4">
                    <div className="grid gap-2">
                        <label className="text-sm font-medium">Date de début</label>
                         <Popover>
                            <PopoverTrigger asChild>
                                <Button
                                  variant={"outline"}
                                  className={cn(
                                    "w-[240px] justify-start text-left font-normal",
                                    !dateRange.from && "text-muted-foreground"
                                  )}
                                >
                                  <CalendarIcon className="mr-2 h-4 w-4" />
                                  {dateRange.from ? dayjs(dateRange.from).format("DD/MM/YYYY") : <span>Choisir une date</span>}
                                </Button>
                            </PopoverTrigger>
                            <PopoverContent className="w-auto p-0" align="start">
                                <Calendar
                                    mode="single"
                                    selected={dateRange.from}
                                    onSelect={(date) => setDateRange(prev => ({...prev, from: date || undefined}))}
                                    initialFocus
                                />
                            </PopoverContent>
                        </Popover>
                    </div>
                    <div className="grid gap-2">
                        <label className="text-sm font-medium">Date de fin</label>
                        <Popover>
                            <PopoverTrigger asChild>
                                <Button
                                  variant={"outline"}
                                  className={cn(
                                    "w-[240px] justify-start text-left font-normal",
                                    !dateRange.to && "text-muted-foreground"
                                  )}
                                >
                                  <CalendarIcon className="mr-2 h-4 w-4" />
                                  {dateRange.to ? dayjs(dateRange.to).format("DD/MM/YYYY") : <span>Choisir une date</span>}
                                </Button>
                            </PopoverTrigger>
                            <PopoverContent className="w-auto p-0" align="start">
                                <Calendar
                                    mode="single"
                                    selected={dateRange.to}
                                    onSelect={(date) => setDateRange(prev => ({...prev, to: date || undefined}))}
                                    initialFocus
                                />
                            </PopoverContent>
                        </Popover>
                    </div>
                     <Button onClick={handleSearch} disabled={isPending}>
                        {isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Search className="mr-2 h-4 w-4" />}
                        Rechercher
                    </Button>
                </CardContent>
            </Card>

            {isPending && (
                 <div className="flex min-h-[30vh] w-full items-center justify-center rounded-lg bg-muted/50">
                    <div className='text-center space-y-2'>
                        <Loader2 className="h-8 w-8 animate-spin text-primary mx-auto" />
                        <p className='font-medium'>Analyse des données...</p>
                    </div>
                </div>
            )}
            
            {data && (
                <div className="space-y-6">
                    <StockSummaryTable summary={data.summary} initialStock={data.initialStockSummary} />
                    
                    <Card>
                    <CardHeader>
                        <CardTitle>Détail des Consommations</CardTitle>
                        <CardDescription>
                        Voici la liste de toutes les consommations enregistrées pour la période sélectionnée.
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        {data.consumptions && data.consumptions.length > 0 ? (
                        <ConsommationsDataTable data={data.consumptions} />
                        ) : (
                        <Alert variant="default" className="bg-amber-50 border-amber-200">
                            <Droplet className="h-4 w-4 !text-amber-600" />
                            <AlertTitle className="text-amber-800 font-semibold">Aucune consommation trouvée</AlertTitle>
                            <AlertDescription className="text-amber-700">
                                Aucune donnée de consommation n'a été trouvée pour cette période.
                            </AlertDescription>
                        </Alert>
                        )}
                    </CardContent>
                    </Card>
                </div>
            )}
        </div>
    );
}

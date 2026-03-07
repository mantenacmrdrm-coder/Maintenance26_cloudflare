'use client';

import { useState, useTransition } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Loader2, Printer, Search, Download, CalendarIcon } from 'lucide-react';
import { addDays, format } from 'date-fns';
import { useToast } from '@/hooks/use-toast';
import { getOrdresDeTravailAction } from '@/lib/actions/maintenance-actions';
import type { OrdreTravailData } from '@/lib/types';
import { OrdreView } from './ordre-view';
import * as XLSX from 'xlsx';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { cn } from '@/lib/utils';

export function OrdresDeTravailView() {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const [matricule, setMatricule] = useState('');
  const [startDate, setStartDate] = useState<Date | undefined>(addDays(new Date(), -30));
  const [endDate, setEndDate] = useState<Date | undefined>(new Date());
  const [ordres, setOrdres] = useState<OrdreTravailData[] | null>(null);

  const handleGenerate = () => {
    if (!startDate || !endDate) {
      toast({
        variant: 'destructive',
        title: 'Erreur',
        description: 'Veuillez sélectionner une plage de dates valide.',
      });
      return;
    }
    
    startTransition(async () => {
      setOrdres(null);

      const startDateString = format(startDate as Date, 'yyyy-MM-dd');
      const endDateString = format(endDate as Date, 'yyyy-MM-dd');

      const result = await getOrdresDeTravailAction({
        startDate: startDateString,
        endDate: endDateString,
        matricule: matricule || undefined,
      });

      if (result.success) {
        setOrdres(result.data || []);
        toast({
          title: 'Génération terminée',
          description: `${result.data?.length || 0} ordre(s) de travail trouvé(s).`,
        });
      } else {
        toast({
          variant: 'destructive',
          title: 'Erreur',
          description: result.message || 'Impossible de générer les ordres de travail.',
        });
      }
    });
  };

  const handlePrint = () => {
    window.print();
  };

  const handleExportExcel = async () => {
    if (!ordres || ordres.length === 0) {
      toast({ variant: 'destructive', title: 'Aucune donnée', description: 'Aucun ordre à exporter.' });
      return;
    }
    
    const dataToExport = ordres.map(ordre => ({
      "ID": ordre.id,
      "Date de Panne": ordre.date_entree,
      "Affectation": ordre.affectation,
      "Désignation": ordre.designation,
      "Matricule": ordre.matricule,
      "Marque": ordre.marque,
      "Type de Panne": ordre.type_de_panne,
      "Nature de Panne": ordre.panne_declaree,
    }));

    const worksheet = XLSX.utils.json_to_sheet(dataToExport);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Ordres de Travail");
    XLSX.writeFile(workbook, `Ordres_De_Travail_${format(new Date(), 'yyyy-MM-dd')}.xlsx`);
  };

  return (
    <div className="space-y-6">
      <Card className="print-hide">
        <CardHeader>
          <CardTitle>Filtres de recherche</CardTitle>
          <CardDescription>
            Sélectionnez une plage de dates et/ou un matricule pour trouver les interventions.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-4 items-end">
           <div className="grid gap-2">
            <label className="text-sm font-medium">Date de début</label>
            <Popover>
              <PopoverTrigger asChild>
                <Button variant={"outline"} className={cn("w-[240px] justify-start text-left font-normal", !startDate && "text-muted-foreground")}>
                  <CalendarIcon className="mr-2 h-4 w-4" />
                  {startDate ? format(startDate, "PPP") : <span>Choisir une date</span>}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0" align="start">
                <Calendar mode="single" selected={startDate} onSelect={setStartDate} initialFocus />
              </PopoverContent>
            </Popover>
          </div>
          <div className="grid gap-2">
            <label className="text-sm font-medium">Date de fin</label>
            <Popover>
              <PopoverTrigger asChild>
                <Button variant={"outline"} className={cn("w-[240px] justify-start text-left font-normal", !endDate && "text-muted-foreground")}>
                  <CalendarIcon className="mr-2 h-4 w-4" />
                  {endDate ? format(endDate, "PPP") : <span>Choisir une date</span>}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0" align="start">
                <Calendar mode="single" selected={endDate} onSelect={setEndDate} initialFocus />
              </PopoverContent>
            </Popover>
          </div>
          <div className="grid gap-2">
            <label htmlFor="matricule" className="text-sm font-medium">Matricule (Optionnel)</label>
            <Input
              id="matricule"
              placeholder="Filtrer par matricule..."
              value={matricule}
              onChange={(e) => setMatricule(e.target.value)}
              className="w-full md:w-[250px]"
            />
          </div>
          <Button onClick={handleGenerate} disabled={isPending}>
            {isPending ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Search className="mr-2 h-4 w-4" />
            )}
            Générer les ordres
          </Button>
        </CardContent>
      </Card>
      
      {isPending && (
        <div className="flex min-h-[40vh] w-full items-center justify-center rounded-lg bg-muted/50">
          <div className='text-center space-y-2'>
            <Loader2 className="h-8 w-8 animate-spin text-primary mx-auto" />
            <p className='font-medium'>Recherche des interventions...</p>
          </div>
        </div>
      )}

      {ordres !== null && (
        <>
          <div className="flex justify-between items-center print-hide">
            <p className="text-sm text-muted-foreground">{ordres.length} ordre(s) de travail à imprimer</p>
            <div className="flex gap-2">
                <Button onClick={handleExportExcel} variant="outline" disabled={ordres.length === 0}>
                    <Download className="mr-2 h-4 w-4" />
                    Exporter (Excel)
                </Button>
                <Button onClick={handlePrint} variant="outline" disabled={ordres.length === 0}>
                <Printer className="mr-2 h-4 w-4" />
                Imprimer Tout
                </Button>
            </div>
          </div>
          <div id="print-section" className="space-y-8">
            {ordres.length > 0 ? (
              ordres.map(ordre => (
                <div key={ordre.id} className="ordre-travail-card">
                  <OrdreView data={ordre} />
                </div>
              ))
            ) : (
              <Card>
                <CardContent className="pt-6">
                  <p className="text-center text-muted-foreground">
                    Aucun ordre de travail trouvé pour les critères sélectionnés.
                  </p>
                </CardContent>
              </Card>
            )}
          </div>
        </>
      )}

      <style jsx global>{`
        @media print {
          body * { visibility: hidden; }
          .print-hide { display: none !important; }
          #print-section, #print-section * { visibility: visible; }
          #print-section {
            /* Removed absolute positioning to allow normal document flow for printing */
            margin: 0;
            padding: 0;
            border: none;
            box-shadow: none;
          }
          .ordre-travail-card {
            page-break-after: always;
            page-break-inside: avoid; /* Add this to prevent a single card from breaking */
            box-shadow: none !important;
            border: none !important;
          }
           .ordre-travail-card:last-child {
            page-break-after: auto;
           }
          @page { 
            size: A4 portrait; 
            margin: 20mm; 
          }
        }
      `}</style>
    </div>
  );
}

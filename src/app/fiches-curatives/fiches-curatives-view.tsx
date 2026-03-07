
'use client';

import { useState, useTransition } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Loader2, Printer, Search, Download, CalendarIcon } from 'lucide-react';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { useToast } from '@/hooks/use-toast';
import { getCurativeFichesAction } from '@/lib/actions/maintenance-actions';
import type { CurativeFicheData } from '@/lib/types';
import { FicheView } from './fiche-view';
import * as XLSX from 'xlsx';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { cn } from '@/lib/utils';

dayjs.locale('fr');

export function FichesCurativesView() {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const [matricule, setMatricule] = useState('');
  const [startDate, setStartDate] = useState<Date | undefined>(dayjs().subtract(30, 'day').toDate());
  const [endDate, setEndDate] = useState<Date | undefined>(new Date());
  const [fiches, setFiches] = useState<CurativeFicheData[] | null>(null);

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
      setFiches(null);

      const startDateString = dayjs(startDate).format('YYYY-MM-DD');
      const endDateString = dayjs(endDate).format('YYYY-MM-DD');

      const result = await getCurativeFichesAction({
        startDate: startDateString,
        endDate: endDateString,
        matricule: matricule || undefined,
      });

      if (result.success) {
        setFiches(result.data || []);
        toast({
          title: 'Génération terminée',
          description: `${result.data?.length || 0} fiche(s) trouvée(s).`,
        });
      } else {
        toast({
          variant: 'destructive',
          title: 'Erreur',
          description: result.message || 'Impossible de générer les fiches.',
        });
      }
    });
  };

  const handlePrint = () => {
    window.print();
  };

  const handleExportExcel = async () => {
    if (!fiches || fiches.length === 0) {
      toast({ variant: 'destructive', title: 'Aucune donnée', description: 'Aucune fiche à exporter.' });
      return;
    }
    
    const dataToExport = fiches.map(fiche => ({
      "N°": fiche.id,
      "Date": fiche.date_entree,
      "Affectation": fiche.affectation,
      "Désignation": fiche.designation,
      "Matricule": fiche.matricule,
      "Marque": fiche.marque,
      "Catégorie": fiche.categorie,
      "Type de Panne": fiche.type_de_panne,
      "Anomalie Constatée": fiche.panne_declaree,
      "Travail à exécuter (Pièces)": fiche.pieces?.join(', '),
      "Intervenant": fiche.intervenant,
      "Situation": fiche.sitactuelle,
      "Indisponibilité (jours)": fiche.nbr_indisponibilite,
    }));

    const worksheet = XLSX.utils.json_to_sheet(dataToExport);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Fiches Curatives");
    XLSX.writeFile(workbook, `Fiches_Curatives_${dayjs().format('YYYY-MM-DD')}.xlsx`);
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
                  {startDate ? dayjs(startDate).format("DD/MM/YYYY") : <span>Choisir une date</span>}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0" align="start">
                <Calendar mode="single" selected={startDate} onSelect={setStartDate} />
              </PopoverContent>
            </Popover>
          </div>
          <div className="grid gap-2">
            <label className="text-sm font-medium">Date de fin</label>
            <Popover>
              <PopoverTrigger asChild>
                <Button variant={"outline"} className={cn("w-[240px] justify-start text-left font-normal", !endDate && "text-muted-foreground")}>
                  <CalendarIcon className="mr-2 h-4 w-4" />
                  {endDate ? dayjs(endDate).format("DD/MM/YYYY") : <span>Choisir une date</span>}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0" align="start">
                <Calendar mode="single" selected={endDate} onSelect={setEndDate} />
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
            Générer les fiches
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

      {fiches !== null && (
        <>
          <div className="flex justify-between items-center print-hide">
            <p className="text-sm text-muted-foreground">{fiches.length} fiche(s) à imprimer</p>
            <div className='flex gap-2'>
                <Button onClick={handleExportExcel} variant="outline" disabled={fiches.length === 0}>
                    <Download className="mr-2 h-4 w-4" />
                    Exporter (Excel)
                </Button>
                <Button onClick={handlePrint} variant="outline" disabled={fiches.length === 0}>
                <Printer className="mr-2 h-4 w-4" />
                Imprimer Tout
                </Button>
            </div>
          </div>
          <div id="print-section" className="space-y-8">
            {fiches.length > 0 ? (
              fiches.map(fiche => 
                <div key={fiche.id} className="fiche-curative-card">
                  <FicheView data={fiche} />
                </div>
              )
            ) : (
              <Card>
                <CardContent className="pt-6">
                  <p className="text-center text-muted-foreground">
                    Aucune fiche de maintenance curative trouvée pour les critères sélectionnés.
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
          .fiche-curative-card {
            page-break-after: always;
            page-break-inside: avoid; /* Add this to prevent a single card from breaking */
            box-shadow: none !important;
            border: none !important;
          }
           .fiche-curative-card:last-child {
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

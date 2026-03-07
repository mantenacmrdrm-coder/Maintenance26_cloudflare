'use client';

import { useState, useTransition } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Loader2, Printer, Search, Download } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { getDailyConsumptionReportAction } from '@/lib/actions/maintenance-actions';
import { DailyConsumptionReport } from './daily-consumption-report';
import type { DailyReportData } from '@/lib/types';
import { Input } from '@/components/ui/input';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';

dayjs.locale('fr');

const LUBRICANT_TYPES = [
    't32', '15w40_4400', '10w', '15w40', '90', '15w40_v', 
    'hvol', 'tvol', 't30', 'graisse', 't46', '15w40_quartz'
] as const;

export function ConsumptionReportView() {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const [year, setYear] = useState<string>(new Date().getFullYear().toString());
  const [month, setMonth] = useState<string>((new Date().getMonth() + 1).toString());
  const [lubricant, setLubricant] = useState<string | undefined>(undefined);
  const [reportData, setReportData] = useState<DailyReportData | null>(null);

  const handleGenerate = () => {
    if (!lubricant) {
        toast({
            variant: 'destructive',
            title: 'Erreur',
            description: 'Veuillez sélectionner un type de lubrifiant.',
        });
        return;
    }
    startTransition(async () => {
      setReportData(null);
      const result = await getDailyConsumptionReportAction({
        year: parseInt(year, 10),
        month: parseInt(month, 10),
        lubricantType: lubricant,
      });

      if (result.success) {
        setReportData(result.data as DailyReportData);
        toast({
          title: 'Rapport généré',
          description: 'Le rapport de consommation a été généré avec succès.',
        });
      } else {
        toast({
          variant: 'destructive',
          title: 'Erreur',
          description: result.message || 'Impossible de générer le rapport.',
        });
      }
    });
  };

  const handlePrint = () => {
    window.print();
  };

  const handleExportXLSX = async () => {
    if (!reportData) {
      toast({
        variant: 'destructive',
        title: 'Aucune donnée',
        description: "Générez d'abord un rapport avant de l'exporter.",
      });
      return;
    }

    toast({
      title: "Préparation de l'exportation...",
      description: 'Veuillez patienter pendant la création du fichier.',
    });

    try {
      const XLSX = await import('xlsx');
      const { initialStock, dailyData, totalEntrees, totalSorties, finalStock, lubricantType, month, year } = reportData;

      const monthName = new Date(year, month - 1).toLocaleString('fr', { month: 'long' });

      // Data rows for the sheet
      const data = [
        [`ETAT DE CONSOMMATION MENSUELLE DE LUBRIFIANT - ${monthName.toUpperCase()} ${year}`],
        [`Type de lubrifiant: ${lubricantType.replace(/_/g, ' ').toUpperCase()}`],
        [], // Spacer row
        ['DÉSIGNATION', 'ENTRÉES', null, 'SORTIES', null, 'RESTE', null],
        ['', 'Qte', null, 'Qte', null, 'Qte', null],
        ['Stock Début de Période', null, null, null, null, initialStock, null]
      ];

      let runningStock = initialStock;
      dailyData.forEach(day => {
        runningStock += (day.entree || 0) - (day.sortie || 0);
        data.push([
          dayjs(day.date).format('DD/MM/YYYY'),
          day.entree > 0 ? day.entree : null,
          null, // Montant Entree
          day.sortie > 0 ? day.sortie : null,
          null, // Montant Sortie
          runningStock,
          null // Montant Reste
        ]);
      });

      data.push(['Stock Fin de Période', null, null, null, null, finalStock, null]);
      data.push(['Totaux', totalEntrees > 0 ? totalEntrees : null, null, totalSorties > 0 ? totalSorties : null, null, finalStock, null]);

      const worksheet = XLSX.utils.aoa_to_sheet(data);

      // Define merges
      worksheet['!merges'] = [
        { s: { r: 0, c: 0 }, e: { r: 0, c: 6 } }, // Title
        { s: { r: 1, c: 0 }, e: { r: 1, c: 6 } }, // Lubricant Type
        { s: { r: 3, c: 1 }, e: { r: 3, c: 2 } }, // Header 'ENTRÉES'
        { s: { r: 3, c: 3 }, e: { r: 3, c: 4 } }, // Header 'SORTIES'
        { s: { r: 3, c: 5 }, e: { r: 3, c: 6 } }, // Header 'RESTE'
      ];
      
      // Define column widths
      worksheet['!cols'] = [
        { wch: 25 }, // Désignation
        { wch: 12 }, // Entrée Qte
        { wch: 12 }, // Entrée Montant
        { wch: 12 }, // Sortie Qte
        { wch: 12 }, // Sortie Montant
        { wch: 15 }, // Reste Qte
        { wch: 15 }, // Reste Montant
      ];

      const workbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(workbook, worksheet, 'Consommation');
      XLSX.writeFile(workbook, `Rapport_Consommation_${lubricantType}_${month}_${year}.xlsx`);

    } catch (error) {
      console.error("Erreur lors de l'exportation XLSX:", error);
      toast({
        variant: 'destructive',
        title: "Erreur d'exportation",
        description: "Une erreur est survenue lors de la création du fichier XLSX.",
      });
    }
  };


  const months = Array.from({ length: 12 }, (_, i) => ({
    value: (i + 1).toString(),
    label: new Date(0, i).toLocaleString('fr-FR', { month: 'long' }),
  }));

  const years = Array.from({ length: 7 }, (_, i) => (new Date().getFullYear() - 3 + i).toString());

  return (
    <div className="space-y-6">
      <Card className="print-hide">
        <CardHeader>
          <CardTitle>Sélection de la Période et du Lubrifiant</CardTitle>
          <CardDescription>
            Choisissez le mois, l'année et le lubrifiant pour générer le rapport. Le stock initial est calculé automatiquement.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-4 items-end">
          <div className="grid gap-2">
            <label className="text-sm font-medium">Mois</label>
            <Select value={month} onValueChange={setMonth}>
              <SelectTrigger className="w-[180px]">
                <SelectValue placeholder="Mois" />
              </SelectTrigger>
              <SelectContent>
                {months.map(m => (
                  <SelectItem key={m.value} value={m.value}>
                    {m.label.charAt(0).toUpperCase() + m.label.slice(1)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="grid gap-2">
            <label className="text-sm font-medium">Année</label>
            <Select value={year} onValueChange={setYear}>
              <SelectTrigger className="w-[120px]">
                <SelectValue placeholder="Année" />
              </SelectTrigger>
              <SelectContent>
                {years.map(y => (
                  <SelectItem key={y} value={y}>{y}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="grid gap-2">
            <label className="text-sm font-medium">Lubrifiant</label>
             <Select value={lubricant} onValueChange={setLubricant}>
                <SelectTrigger className="w-[220px]">
                    <SelectValue placeholder="Sélectionner un type" />
                </SelectTrigger>
                <SelectContent>
                {LUBRICANT_TYPES.map(type => (
                    <SelectItem key={type} value={type}>{type.replace(/_/g, ' ').toUpperCase()}</SelectItem>
                ))}
                </SelectContent>
            </Select>
          </div>
          <Button onClick={handleGenerate} disabled={isPending || !lubricant}>
            {isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Search className="mr-2 h-4 w-4" />}
            Générer
          </Button>
        </CardContent>
      </Card>

      {isPending && (
        <div className="flex min-h-[40vh] w-full items-center justify-center rounded-lg bg-muted/50">
          <div className='text-center space-y-2'>
            <Loader2 className="h-8 w-8 animate-spin text-primary mx-auto" />
            <p className='font-medium'>Génération du rapport...</p>
          </div>
        </div>
      )}

      {reportData && (
        <>
          <div className="flex justify-end items-center gap-2 print-hide">
            <Button onClick={handleExportXLSX} variant="outline">
              <Download className="mr-2 h-4 w-4" />
              Exporter (XLSX)
            </Button>
            <Button onClick={handlePrint} variant="outline">
              <Printer className="mr-2 h-4 w-4" />
              Imprimer le rapport
            </Button>
          </div>
          <div id="print-section">
            <DailyConsumptionReport
              data={reportData}
            />
          </div>
        </>
      )}

      <style jsx global>{`
        @media print {
          body * { visibility: hidden; }
          .print-hide { display: none !important; }
          #print-section, #print-section * { visibility: visible; }
          #print-section {
            position: absolute;
            left: 0;
            top: 0;
            width: 100%;
            margin: 0;
            padding: 0;
            border: none;
            box-shadow: none;
          }
          @page {
            size: A4 landscape;
            margin: 15mm;
          }
        }
      `}</style>
    </div>
  );
}

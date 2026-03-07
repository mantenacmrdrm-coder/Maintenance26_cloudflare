
'use client';

import { useState, useTransition, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Loader2, Printer, Search, Info, Download } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { getMonthlyStockReportAction, getLastEntryDateAction } from '@/lib/actions/maintenance-actions';
import type { MonthlyStockReportData } from '@/lib/types';
import { LUBRICANT_TYPES } from '@/lib/constants';
import { MonthlyReportDocument } from './monthly-report-document';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import * as XLSX from 'xlsx';

dayjs.locale('fr');

const generateYears = () => {
    const currentYear = new Date().getFullYear();
    return Array.from({ length: 7 }, (_, i) => (currentYear - 3 + i).toString());
};

const generateMonths = () => {
    return Array.from({ length: 12 }, (_, i) => ({
        value: (i + 1).toString(),
        label: dayjs().month(i).format('MMMM'),
    }));
};

export function MonthlyReportsView() {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  
  const currentYear = new Date().getFullYear().toString();
  const [startYear, setStartYear] = useState<string>(currentYear);
  const [startMonth, setStartMonth] = useState<string>('1');
  const [endYear, setEndYear] = useState<string>(currentYear);
  const [endMonth, setEndMonth] = useState<string>((new Date().getMonth() + 1).toString());

  const [lubricant, setLubricant] = useState<string | undefined>(undefined);
  const [reportData, setReportData] = useState<MonthlyStockReportData | null>(null);
  const [lastEntry, setLastEntry] = useState<{ date: string | null; loading: boolean }>({ date: null, loading: false });


  const years = generateYears();
  const months = generateMonths();

  useEffect(() => {
    if (lubricant) {
      setLastEntry({ date: null, loading: true });
      startTransition(async () => {
        const result = await getLastEntryDateAction(lubricant);
        if (result.success) {
          setLastEntry({ date: result.date, loading: false });
        } else {
          setLastEntry({ date: null, loading: false });
        }
      });
    } else {
      setLastEntry({ date: null, loading: false });
    }
  }, [lubricant]);

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
      const params = {
        startYear: parseInt(startYear, 10),
        startMonth: parseInt(startMonth, 10),
        endYear: parseInt(endYear, 10),
        endMonth: parseInt(endMonth, 10),
        lubricantType: lubricant,
      };

      const result = await getMonthlyStockReportAction(params);

      if (result.success && result.data) {
        setReportData(result.data);
        toast({
          title: 'Rapport généré',
          description: `Le rapport de stock pour ${result.data.reportData.length} mois a été généré.`,
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
      const { reportData: monthlyData, totalEntries, totalSorties, overallInitialStock, overallFinalStock, lubricantType, period } = reportData;

      const data = [
        [`ETAT DE CONSOMMATION MENSUELLE DE LUBRIFIANT - ${period}`],
        [`Type de lubrifiant: ${lubricantType.replace(/_/g, ' ').toUpperCase()}`],
        [],
        ['DÉSIGNATION', 'ENTRÉES', null, 'SORTIES', null, 'RESTE', null],
        ['', 'Qte', null, 'Qte', null, 'Qte', null],
        ['Stock Début de Période', null, null, null, null, overallInitialStock, null]
      ];

      monthlyData.forEach(month => {
        data.push([
          month.month,
          month.entries > 0 ? month.entries : null,
          null,
          month.sorties > 0 ? month.sorties : null,
          null,
          month.finalStock,
          null
        ]);
      });

      data.push(['Stock Fin de Période', null, null, null, null, overallFinalStock, null]);
      data.push(['Totaux', totalEntries > 0 ? totalEntries : null, null, totalSorties > 0 ? totalSorties : null, null, overallFinalStock, null]);

      const worksheet = XLSX.utils.aoa_to_sheet(data);

      worksheet['!merges'] = [
        { s: { r: 0, c: 0 }, e: { r: 0, c: 6 } },
        { s: { r: 1, c: 0 }, e: { r: 1, c: 6 } },
        { s: { r: 3, c: 1 }, e: { r: 3, c: 2 } },
        { s: { r: 3, c: 3 }, e: { r: 3, c: 4 } },
        { s: { r: 3, c: 5 }, e: { r: 3, c: 6 } },
      ];
      
      worksheet['!cols'] = [
        { wch: 25 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 15 }, { wch: 15 },
      ];

      const workbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(workbook, worksheet, 'Stock Mensuel');
      XLSX.writeFile(workbook, `Rapport_Stock_${lubricantType}_${reportData.period.replace(/\s/g, '_')}.xlsx`);

    } catch (error) {
      console.error("Erreur lors de l'exportation XLSX:", error);
      toast({
        variant: 'destructive',
        title: "Erreur d'exportation",
        description: "Une erreur est survenue lors de la création du fichier XLSX.",
      });
    }
  };


  return (
    <div className="space-y-6">
      <Card className="print-hide">
        <CardHeader>
          <CardTitle>Sélection de la Période et du Lubrifiant</CardTitle>
          <CardDescription>
            Choisissez la plage de dates et le lubrifiant pour générer le rapport de stock mensuel.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-4 items-end">
                 <div className="grid gap-2 col-span-2 md:col-span-4 lg:col-span-1">
                    <label className="text-sm font-medium">Lubrifiant</label>
                    <Select value={lubricant} onValueChange={setLubricant}>
                        <SelectTrigger><SelectValue placeholder="Sélectionner un type" /></SelectTrigger>
                        <SelectContent>
                            {LUBRICANT_TYPES.map(type => (
                                <SelectItem key={type} value={type}>{type.replace(/_/g, ' ').toUpperCase()}</SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </div>
                <div className="grid gap-2">
                    <label className="text-sm font-medium">Mois de début</label>
                    <Select value={startMonth} onValueChange={setStartMonth}>
                        <SelectTrigger><SelectValue placeholder="Mois" /></SelectTrigger>
                        <SelectContent>{months.map(m => <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>)}</SelectContent>
                    </Select>
                </div>
                <div className="grid gap-2">
                    <label className="text-sm font-medium">Année de début</label>
                    <Select value={startYear} onValueChange={setStartYear}>
                        <SelectTrigger><SelectValue placeholder="Année" /></SelectTrigger>
                        <SelectContent>{years.map(y => <SelectItem key={y} value={y}>{y}</SelectItem>)}</SelectContent>
                    </Select>
                </div>
                 <div className="grid gap-2">
                    <label className="text-sm font-medium">Mois de fin</label>
                    <Select value={endMonth} onValueChange={setEndMonth}>
                        <SelectTrigger><SelectValue placeholder="Mois" /></SelectTrigger>
                        <SelectContent>{months.map(m => <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>)}</SelectContent>
                    </Select>
                </div>
                <div className="grid gap-2">
                    <label className="text-sm font-medium">Année de fin</label>
                    <Select value={endYear} onValueChange={setEndYear}>
                        <SelectTrigger><SelectValue placeholder="Année" /></SelectTrigger>
                        <SelectContent>{years.map(y => <SelectItem key={y} value={y}>{y}</SelectItem>)}</SelectContent>
                    </Select>
                </div>
            </div>
             <div className='space-y-4'>
                {lastEntry.loading && (
                    <p className="text-sm text-muted-foreground animate-pulse mt-2">Recherche de la dernière entrée...</p>
                )}
                {lubricant && !lastEntry.loading && (
                    <Alert variant="default" className="bg-blue-50 border-blue-200 mt-2">
                        <Info className="h-4 w-4 !text-blue-600" />
                        <AlertTitle className="text-blue-800 font-semibold">Information</AlertTitle>
                        <AlertDescription className="text-blue-700">
                            {lastEntry.date
                            ? <>La dernière entrée pour <span className="font-bold">{lubricant.replace(/_/g, ' ').toUpperCase()}</span> date du <span className="font-bold">{lastEntry.date}</span>.</>
                            : "Aucune entrée de stock n'a été trouvée pour ce lubrifiant."}
                        </AlertDescription>
                    </Alert>
                )}

                <Button onClick={handleGenerate} disabled={isPending || !lubricant}>
                    {isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Search className="mr-2 h-4 w-4" />}
                    Générer le rapport
                </Button>
            </div>
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
            <MonthlyReportDocument data={reportData} />
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
            size: A4 portrait;
            margin: 15mm;
          }
        }
      `}</style>
    </div>
  );
}



'use client';

import React, { useState, useTransition, useMemo, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Loader2, Download } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { runHistoryGeneration, getHistory } from '@/lib/actions/maintenance-actions';
import dayjs from 'dayjs';
import { cn } from '@/lib/utils';

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

type HistoryMatrix = {
    headers: readonly string[];
    rows: (string | null)[][];
    counts?: Record<string, number>;
} | null;

const ITEMS_PER_PAGE = 50;

export function HistoryView() {
  const [history, setHistory] = useState<HistoryMatrix>(null);
  const [isGenerating, startGeneration] = useTransition();
  const [isLoading, startLoading] = useTransition();
  const { toast } = useToast();

  const [filter, setFilter] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  
  const isPending = isGenerating || isLoading;

  useEffect(() => {
    startLoading(async () => {
        try {
            const initialHistory = await getHistory();
             if (initialHistory && initialHistory.rows.length > 0) {
                setHistory(initialHistory as any);
            }
        } catch (error) {
            // No toast here, as it might just mean the DB isn't initialized
            console.warn("Could not load initial history.", error);
        }
    });
  }, []);

  const handleGenerateHistory = () => {
    startGeneration(async () => {
      try {
        const result = await runHistoryGeneration();
        setHistory(result as any);
        setFilter('');
        setCurrentPage(1);
        if (result.rows.length > 0) {
           toast({
            title: 'Succès',
            description: `${result.rows.length.toLocaleString('fr-FR')} lignes d'historique ont été traitées.`,
          });
        } else {
             toast({
                variant: 'default',
                title: 'Information',
                description: "Aucun historique trouvé pour les opérations spécifiées.",
             });
        }
      } catch (error: any) {
        console.error(error);
        toast({
          variant: 'destructive',
          title: 'Erreur',
          description: error.message || "Impossible de générer l'historique.",
        });
      }
    });
  };

  const filteredRows = useMemo(() => {
    if (!history || !history.rows) return [];
    if (!filter) return history.rows;
    
    const lowercasedFilter = filter.toLowerCase();
    
    return history.rows.filter(row => 
      row.some(cell => 
        cell?.toString().toLowerCase().includes(lowercasedFilter)
      )
    );
  }, [history, filter]);

  const paginatedRows = useMemo(() => {
    const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
    return filteredRows.slice(startIndex, startIndex + ITEMS_PER_PAGE);
  }, [filteredRows, currentPage]);
  
  const totalPages = Math.ceil(filteredRows.length / ITEMS_PER_PAGE);

  const handleExport = () => {
    if (!history || !history.rows) return;

    const rowsToExport = filter ? filteredRows : history.rows;
    const headers = history.headers;
    const csvContent = [
      headers.join(';'),
      ...rowsToExport.map(row => row.map(cell => `"${cell || ''}"`).join(';'))
    ].join('\n');

    const blob = new Blob([`\uFEFF${csvContent}`], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `matrice_historique_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    toast({
        title: 'Exportation réussie',
        description: `${rowsToExport.length} lignes exportées.`,
    });
  };

  const getDateCellClass = (dateString: string | null): string => {
    if (!dateString) return '';
    // Basic check to avoid trying to parse matricules etc. as dates
    if (!/^\d{2}\/\d{2}\/\d{4}$/.test(dateString)) return '';

    const date = dayjs(dateString, 'DD/MM/YYYY');
    if (!date.isValid()) return '';

    const today = dayjs();
    const diffInMonths = today.diff(date, 'month');

    if (diffInMonths < 1) return 'bg-primary/25';   // Moins d'un mois (plus récent)
    if (diffInMonths < 3) return 'bg-primary/20';
    if (diffInMonths < 6) return 'bg-primary/15';
    if (diffInMonths < 12) return 'bg-primary/10';
    if (diffInMonths < 24) return 'bg-primary/[.05]'; // Plus ancien
    return ''; // Très ancien
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Génération de l'historique</CardTitle>
          <CardDescription>
            Consolidez les données de maintenance des fichiers `vidange`, `consolide` et `suivi_curatif` dans une vue d'historique unifiée. Cliquez pour actualiser les données.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Button onClick={handleGenerateHistory} disabled={isPending}>
            {isGenerating ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : null}
            {isGenerating ? 'Génération...' : "Lancer la génération de l'historique"}
          </Button>
        </CardContent>
      </Card>

      {isPending && (
        <div className="flex min-h-[30vh] w-full items-center justify-center rounded-lg bg-muted/50">
           <div className='text-center space-y-2'>
               <Loader2 className="h-8 w-8 animate-spin text-primary mx-auto" />
               <p className='font-medium'>Analyse et consolidation des données...</p>
               <p className='text-sm text-muted-foreground'>Cela peut prendre quelques instants.</p>
           </div>
       </div>
      )}

      {!isPending && history && (
        <>
            {history.rows.length === 0 ? (
                 <Card>
                    <CardContent className='pt-6'>
                        <div className="flex flex-col items-center justify-center h-48 text-center bg-muted/50 rounded-lg">
                            <p className="text-lg font-medium">Aucun résultat</p>
                            <p className="text-muted-foreground">La génération n'a produit aucune intervention d'historique. Essayez de lancer la génération.</p>
                        </div>
                    </CardContent>
                </Card>
            ) : (
                <div className='space-y-6'>
                     <Card>
                        <CardHeader>
                            <div className='flex flex-wrap gap-4 justify-between items-center'>
                                <div>
                                    <CardTitle>Matrice d'Historique</CardTitle>
                                    <CardDescription>
                                        {filteredRows.length.toLocaleString('fr-FR')} / {history.rows.length.toLocaleString('fr-FR')} lignes trouvées.
                                    </CardDescription>
                                </div>
                                <div className='flex gap-2 items-center'>
                                    <Input
                                    placeholder="Filtrer les résultats..."
                                    value={filter}
                                    onChange={(e) => { setFilter(e.target.value); setCurrentPage(1);}}
                                    className="max-w-sm"
                                    />
                                    <Button onClick={handleExport} variant="outline">
                                        <Download className="mr-2 h-4 w-4" />
                                        Exporter
                                    </Button>
                                </div>
                            </div>
                        </CardHeader>
                        <CardContent>
                            <div className="rounded-md border w-full overflow-auto">
                                <Table>
                                    <TableHeader>
                                        <TableRow>
                                            {history && history.headers && history.headers.map((header, index) => (
                                                <TableHead key={index} className={`text-xs whitespace-nowrap ${index === 0 ? 'whitespace-nowrap' : ''}`}>
                                                    {header}
                                                    {history.counts && history.counts[header] > 0 && (
                                                        <span className="font-normal text-muted-foreground ml-1">({history.counts[header]})</span>
                                                    )}
                                                </TableHead>
                                            ))}
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                    {paginatedRows.length > 0 ? paginatedRows.map((row, rowIndex) => (
                                        <TableRow key={rowIndex}>
                                           {row.map((cell, cellIndex) => (
                                                <TableCell 
                                                    key={cellIndex} 
                                                    className={cn(
                                                        'text-xs whitespace-nowrap transition-colors duration-300', 
                                                        cellIndex === 0 ? 'font-bold' : 'text-center',
                                                        cellIndex > 0 && getDateCellClass(cell)
                                                    )}
                                                >
                                                    {cell}
                                                </TableCell>
                                           ))}
                                        </TableRow>
                                    )) : (
                                        <TableRow>
                                            <TableCell colSpan={history.headers.length} className="h-24 text-center">Aucun résultat pour votre filtre.</TableCell>
                                        </TableRow>
                                    )}
                                    </TableBody>
                                </Table>
                            </div>
                            <div className="flex items-center justify-end space-x-2 py-4">
                                <span className="text-sm text-muted-foreground">
                                    Page {currentPage} sur {totalPages}
                                </span>
                                <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                                    disabled={currentPage === 1}
                                >
                                    Précédent
                                </Button>
                                <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                                    disabled={currentPage === totalPages}
                                >
                                    Suivant
                                </Button>
                            </div>
                        </CardContent>
                    </Card>
                </div>
            )}
        </>
      )}

      {!isPending && !history && (
         <Card>
            <CardContent className='pt-6'>
                <div className="flex flex-col items-center justify-center h-48 text-center bg-muted/50 rounded-lg">
                    <p className="text-lg font-medium">Prêt à consolider.</p>
                    <p className="text-muted-foreground">Lancez la génération pour afficher l'historique de maintenance.</p>
                </div>
            </CardContent>
        </Card>
      )}
    </div>
  );
}

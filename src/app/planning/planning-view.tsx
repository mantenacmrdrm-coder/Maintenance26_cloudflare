



'use client';

import React, { useState, useTransition, useCallback, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Download, Loader2, CalendarClock, Search, CheckCircle2, Layers3, Wrench } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { generatePlanning, getPlanningPage, getFollowUpPage, getAllPlanningForExport, getAllFollowUpForExport, getFollowUpStatistics } from '@/lib/actions/maintenance-actions';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import { OFFICIAL_ENTRETIENS } from '@/lib/constants';
import type { FollowUpStats } from '@/lib/types';
import { Progress } from '@/components/ui/progress';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Skeleton } from '@/components/ui/skeleton';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';

dayjs.locale('fr');

type Intervention = {
    date_programmee: string;
    niveau: string;
    realise?: boolean;
    date_realisation?: string;
    inBreakdown?: boolean;
};

type PlanningMatrix = {
  headers: readonly string[];
  rows: (string | Intervention[] | null)[][];
  total: number;
} | null;

const EQUIPMENTS_PER_PAGE = 1;

type PlanningViewProps = {
    mode: 'planning' | 'suivi';
};


function StatsDisplay({ stats, loading, year }: { stats: FollowUpStats | null, loading: boolean, year: string }) {
    if (loading) {
        return <div className="grid gap-4 md:grid-cols-3 mb-6">
            <Card><CardHeader><Skeleton className="h-6 w-2/3" /></CardHeader><CardContent><Skeleton className="h-10 w-1/2" /><Skeleton className="h-4 w-full mt-2" /></CardContent></Card>
            <Card><CardHeader><Skeleton className="h-6 w-2/3" /></CardHeader><CardContent><Skeleton className="h-16 w-full" /></CardContent></Card>
            <Card><CardHeader><Skeleton className="h-6 w-2/3" /></CardHeader><CardContent><Skeleton className="h-16 w-full" /></CardContent></Card>
        </div>
    }
    if (!stats || stats.totalPlanifie === 0) {
        return null;
    }
    
    const tauxRealisation = stats.totalPlanifie > 0 ? (stats.totalRealise / stats.totalPlanifie) * 100 : 0;

    return (
        <div className="mb-6 space-y-4">
            <h2 className="text-2xl font-bold tracking-tight">Statistiques pour {year}</h2>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Taux de Réalisation</CardTitle>
                        <CheckCircle2 className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{stats.totalRealise.toLocaleString('fr-FR')} / {stats.totalPlanifie.toLocaleString('fr-FR')}</div>
                        <p className="text-xs text-muted-foreground">({tauxRealisation.toFixed(1)}% des interventions planifiées)</p>
                        <Progress value={tauxRealisation} className="mt-2 h-2" />
                    </CardContent>
                </Card>
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Répartition par Niveau</CardTitle>
                        <Layers3 className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-1 text-sm">
                            <div className="flex justify-between"><span>Niveau C (Contrôle)</span> <span className="font-bold">{stats.realiseByNiveau.C || 0} / {stats.planifieByNiveau.C || 0}</span></div>
                            <div className="flex justify-between"><span>Niveau N (Nettoyage)</span> <span className="font-bold">{stats.realiseByNiveau.N || 0} / {stats.planifieByNiveau.N || 0}</span></div>
                            <div className="flex justify-between"><span>Niveau CH (Changement)</span> <span className="font-bold">{stats.realiseByNiveau.CH || 0} / {stats.planifieByNiveau.CH || 0}</span></div>
                        </div>
                    </CardContent>
                </Card>
                <Card className="md:col-span-2 lg:col-span-1">
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Top 5 des Réalisations</CardTitle>
                        <Wrench className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                         {Object.keys(stats.realiseByEntretien).length > 0 ? (
                            <div className="space-y-1 text-sm pr-4">
                                {Object.entries(stats.realiseByEntretien).sort(([, a], [, b]) => b - a).slice(0, 5).map(([name, count]) => (
                                    <div key={name} className="flex justify-between">
                                        <span className="truncate pr-2">{name}</span>
                                        <span className="font-bold">{count.toLocaleString('fr-FR')}</span>
                                    </div>
                                ))}
                            </div>
                         ) : (
                             <p className="text-sm text-muted-foreground text-center pt-8">Aucune intervention réalisée pour cette période.</p>
                         )}
                    </CardContent>
                </Card>
            </div>
             <Card className="md:col-span-2 lg:col-span-3">
                <CardHeader>
                    <CardTitle className="text-sm font-medium">Détail des Réalisations vs. Planification</CardTitle>
                </CardHeader>
                <CardContent>
                    <ScrollArea className="h-64">
                        <Table>
                            <TableHeader>
                                <TableRow>
                                    <TableHead>Entretien</TableHead>
                                    <TableHead className="text-right">Planifié</TableHead>
                                    <TableHead className="text-right">Réalisé</TableHead>
                                    <TableHead className="text-right">Taux</TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {OFFICIAL_ENTRETIENS.map(entretien => {
                                    const planifie = stats.planifieByEntretien[entretien] || 0;
                                    const realise = stats.realiseByEntretien[entretien] || 0;
                                    const taux = planifie > 0 ? (realise / planifie) * 100 : 0;
                                    if (planifie === 0 && realise === 0) return null;

                                    return (
                                        <TableRow key={entretien}>
                                            <TableCell className="font-medium truncate">{entretien}</TableCell>
                                            <TableCell className="text-right">{planifie}</TableCell>
                                            <TableCell className="text-right font-bold">{realise}</TableCell>
                                            <TableCell className="text-right">
                                                <Badge variant={taux >= 80 ? 'default' : taux >= 50 ? 'secondary' : 'destructive'} className={taux >= 80 ? 'bg-green-600' : ''}>
                                                    {taux.toFixed(0)}%
                                                </Badge>
                                            </TableCell>
                                        </TableRow>
                                    )
                                })}
                            </TableBody>
                        </Table>
                    </ScrollArea>
                </CardContent>
            </Card>
        </div>
    )
}


export function PlanningView({ mode }: PlanningViewProps) {
  const [year, setYear] = useState<string>(new Date().getFullYear().toString());
  const [planningData, setPlanningData] = useState<PlanningMatrix>(null);
  const [isGenerating, startGeneration] = useTransition();
  const [isLoading, startLoading] = useTransition();
  const { toast } = useToast();

  const [filter, setFilter] = useState('');
  const [searchTerm, setSearchTerm] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const [stats, setStats] = useState<FollowUpStats | null>(null);
  const [statsLoading, setStatsLoading] = useState(false);

  const isPending = isGenerating || isLoading;

  const fetchPage = useCallback(async (year: number, page: number, currentFilter: string) => {
    startLoading(async () => {
      try {
        let result;
        if (mode === 'suivi') {
            result = await getFollowUpPage(year, page, EQUIPMENTS_PER_PAGE, currentFilter);
        } else {
            result = await getPlanningPage(year, page, EQUIPMENTS_PER_PAGE, currentFilter);
        }
        setPlanningData(result as any);
      } catch (error: any) {
        toast({
          variant: 'destructive',
          title: 'Erreur de chargement',
          description: "Impossible de charger les données."
        });
        setPlanningData(null);
      }
    });
  }, [toast, startLoading, mode]);


  useEffect(() => {
    if (year) {
      fetchPage(parseInt(year, 10), currentPage, filter);
    }
  }, [year, currentPage, filter, fetchPage]);

  useEffect(() => {
    if (mode === 'suivi' && year) {
        setStatsLoading(true);
        setStats(null);
        getFollowUpStatistics(parseInt(year, 10)).then(data => {
            setStats(data as any);
        }).catch(err => {
            console.error(err);
            toast({ variant: 'destructive', title: 'Erreur', description: "Impossible de charger les statistiques."});
            setStats(null);
        }).finally(() => {
            setStatsLoading(false);
        });
    }
}, [year, mode, toast]);

  const handleSearch = () => {
    setCurrentPage(1);
    setFilter(searchTerm);
  };
  
  const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      handleSearch();
    }
  };

  const handleGenerate = () => {
    startGeneration(async () => {
      setPlanningData(null);
      setFilter('');
      setSearchTerm('');
      setCurrentPage(1);
      try {
        const result = await generatePlanning(parseInt(year, 10));
        if (result.count > 0) {
          toast({
            title: 'Succès',
            description: `${result.count.toLocaleString('fr-FR')} interventions programmées pour ${year}.`
          });
        } else {
          toast({
            title: 'Information',
            description: `Aucun planning généré pour ${year}.`
          });
        }
      } catch (error: any) {
        console.error(error);
        toast({
          variant: 'destructive',
          title: 'Erreur de Génération',
          description: error.message || "Impossible de générer le planning. Vérifiez la console pour les erreurs."
        });
      }
    });
  };

  const handleYearChange = (newYear: string) => {
      setYear(newYear);
      setPlanningData(null);
      setFilter('');
      setSearchTerm('');
      setCurrentPage(1);
  };


  const totalEquipments = planningData?.total || 0;
  const totalPages = Math.ceil(totalEquipments / EQUIPMENTS_PER_PAGE);

  const handleExport = async () => {
    toast({ title: "Préparation de l'export...", description: "Cela peut prendre quelques instants." });
    try {
      const dataToExport = mode === 'suivi'
          ? await getAllFollowUpForExport(parseInt(year, 10))
          : await getAllPlanningForExport(parseInt(year, 10));

      if (!dataToExport || !dataToExport.rows || dataToExport.rows.length === 0) {
        toast({ variant: 'destructive', title: "Erreur", description: "Aucune donnée à exporter." });
        return;
      }

      const { headers, rows } = dataToExport;
      const csvContent = [
        headers.join(';'),
        ...rows.map(row => {
          return row.map((cell: any) => {
            if (cell && typeof cell === 'object' && !Array.isArray(cell)) {
                const intervention: Intervention = cell as Intervention;
                if (intervention.realise) {
                    return `"${intervention.date_realisation} (R)"`;
                }
                if (intervention.inBreakdown) {
                    return `"${intervention.date_programmee} (P)"`;
                }
                return `"${intervention.date_programmee} (${intervention.niveau})"`;
            }
            return `"${cell || ''}"`;
          }).join(';');
        })
      ].join('\n');

      const blob = new Blob([`\uFEFF${csvContent}`], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      link.href = URL.createObjectURL(blob);
      link.download = `${mode}_${year}_${new Date().toISOString().split('T')[0]}.csv`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      toast({ title: "Exportation réussie", description: `${rows.length} lignes exportées.` });
    } catch (error: any) {
      toast({ variant: 'destructive', title: "Erreur d'exportation", description: error.message });
    }
  };
  
 const getCellClass = (intervention: Intervention) => {
    if (!intervention) return '';
    if (intervention.realise) {
        return 'bg-white text-black font-bold border border-black';
    }
    if (intervention.inBreakdown) {
        return 'bg-black text-white font-bold';
    }
    const n = intervention.niveau?.toUpperCase();
    switch (n) {
      case 'CH':
        return 'bg-red-600 text-white font-bold';
      case 'C':
        return 'bg-yellow-400 text-black font-bold';
      case 'N':
        return 'bg-green-700 text-white font-bold';
      default:
        return 'bg-gray-200';
    }
  };
  
  const currentPageNumber = totalPages > 0 ? Math.min(currentPage, totalPages) : 1;
  
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex flex-col sm:flex-row gap-4 justify-between items-start sm:items-center">
            <div>
              <CardTitle>{mode === 'planning' ? 'Générateur de Planning' : 'Suivi des Interventions'}</CardTitle>
              <CardDescription>
                {mode === 'planning' 
                    ? "Générez le planning préventif annuel. Les données affichées proviennent du dernier planning généré."
                    : "Comparez les interventions planifiées avec celles réellement effectuées."
                }
              </CardDescription>
            </div>
            <div className="flex gap-2 items-center">
              <Select value={year} onValueChange={handleYearChange} disabled={isPending}>
                <SelectTrigger className="w-[120px]">
                  <SelectValue placeholder="Année" />
                </SelectTrigger>
                <SelectContent>
                  {Array.from({ length: 7 }, (_, i) => new Date().getFullYear() - 3 + i).map(y => (
                    <SelectItem key={y} value={y.toString()}>{y}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
               {mode === 'planning' && (
                <Button onClick={handleGenerate} disabled={isPending}>
                    {isGenerating ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <CalendarClock className='mr-2 h-4 w-4' />}
                    {isGenerating ? 'Génération...' : 'Générer'}
                </Button>
               )}
            </div>
          </div>
        </CardHeader>
        {isPending && (
          <CardContent>
            <div className="flex justify-center items-center h-64">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
              <p className='ml-4 text-muted-foreground'>{isGenerating ? 'Génération du planning en cours...' : 'Chargement...'}</p>
            </div>
          </CardContent>
        )}
        {!isPending && planningData && (
          <CardContent>
             {mode === 'suivi' && <StatsDisplay stats={stats} loading={statsLoading} year={year} />}
            {planningData.total > 0 ? (
                <div className='space-y-6'>
                    <Card>
                        <CardHeader>
                            <div className='flex flex-wrap gap-4 justify-between items-center'>
                                <div>
                                <CardTitle>Matrice de {mode === 'planning' ? 'Planning' : 'Suivi'} pour {year}</CardTitle>
                                <CardDescription>
                                    {totalEquipments.toLocaleString('fr-FR')} équipement(s) trouvé(s). Affichage de la page {currentPageNumber} sur {totalPages}.
                                </CardDescription>
                                </div>
                                <div className='flex gap-2 items-center flex-wrap'>
                                <div className="relative">
                                    <Input
                                        placeholder="Filtrer par matricule..."
                                        value={searchTerm}
                                        onChange={(e) => setSearchTerm(e.target.value)}
                                        onKeyDown={handleKeyDown}
                                        className="max-w-xs pr-10"
                                    />
                                    <Button onClick={handleSearch} disabled={isLoading} size="icon" variant="ghost" className="absolute right-1 top-1/2 -translate-y-1/2 h-8 w-8">
                                        {isLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className='h-4 w-4' />}
                                    </Button>
                                    </div>
                                <Button onClick={handleExport} variant="outline" disabled={isPending || totalEquipments === 0}>
                                    {isLoading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                                    <Download className="mr-2 h-4 w-4" />
                                    Exporter Tout
                                </Button>
                                </div>
                            </div>
                        </CardHeader>
                        <CardContent>
                        <div className="rounded-md border w-full overflow-auto">
                            <Table>
                            <TableHeader>
                                <TableRow>
                                {planningData.headers.map((header, index) => (
                                    <TableHead key={index} className={`text-xs whitespace-nowrap sticky top-0 bg-card z-10 ${index <= 1 ? 'whitespace-nowrap font-bold text-center' : 'text-center'}`}>{header}</TableHead>
                                ))}
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {planningData.rows.length > 0 ? planningData.rows.map((row, rowIndex) => (
                                <TableRow key={rowIndex} className={cn(row[1]?.toString().toLowerCase() === 'décembre' ? 'border-b-4 border-b-primary/50' : '')}>
                                    {row.map((cell, cellIndex) => (
                                    <TableCell key={cellIndex} className={cn('text-xs p-0 text-center', cellIndex > 1 ? 'align-top' : 'align-middle p-2')}>
                                        {(() => {
                                        if (cellIndex === 0) {
                                            return <Badge variant="secondary" className="whitespace-nowrap">{cell as string}</Badge>
                                        }
                                        if (cellIndex === 1) {
                                            return <span className='font-semibold capitalize'>{cell as string}</span>
                                        }
                                        if (cell && typeof cell === 'object' && !Array.isArray(cell)) {
                                            const intervention = cell as Intervention;
                                            return (
                                                <div className={cn('w-full h-full', getCellClass(intervention))}>
                                                    <div className='p-2'>
                                                        {intervention.realise ? intervention.date_realisation : intervention.date_programmee}
                                                    </div>
                                                </div>
                                            );
                                        }
                                        return <div className="h-9">&nbsp;</div>;
                                        })()}
                                    </TableCell>
                                    ))}
                                </TableRow>
                                )) : (
                                <TableRow>
                                    <TableCell colSpan={planningData.headers.length} className="h-24 text-center">
                                    Aucun résultat pour votre filtre.
                                    </TableCell>                          
                                </TableRow>
                                )}
                            </TableBody>
                            </Table>
                        </div>
                        <div className="flex items-center justify-end space-x-2 py-4">
                            <span className="text-sm text-muted-foreground">
                            Page {currentPageNumber} sur {totalPages}
                            </span>
                            <Button
                            variant="outline"
                            size="sm"
                            onClick={() => { setCurrentPage(p => Math.max(1, p - 1)); }}
                            disabled={currentPage === 1 || isPending}
                            >
                            Précédent
                            </Button>
                            <Button
                            variant="outline"
                            size="sm"
                            onClick={() => { setCurrentPage(p => p + 1); }}
                            disabled={(currentPageNumber >= totalPages) || isPending}
                            >
                            Suivant
                            </Button>
                        </div>
                        </CardContent>
                    </Card>
                </div>
            ) : (
                 <CardContent>
                    <div className="flex flex-col items-center justify-center h-64 text-center bg-muted/50 rounded-lg">
                        <CalendarClock className="h-12 w-12 text-muted-foreground mb-4" />
                        <p className="text-lg font-medium">Aucun planning trouvé pour {year}.</p>
                        <p className="text-muted-foreground">
                            {mode === 'planning' 
                                ? 'Sélectionnez une année et cliquez sur "Générer" pour créer un planning de maintenance.'
                                : "Aucune donnée de planning n'a été trouvée pour cette année. Veuillez d'abord la générer dans l'onglet 'Planification'."
                            }
                        </p>
                    </div>
                </CardContent>
            )}
          </CardContent>
        )}
      </Card>
    </div>
  );
}


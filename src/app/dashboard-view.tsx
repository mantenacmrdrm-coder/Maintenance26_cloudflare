'use client';

import {
  Database,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { StatCards } from '@/components/dashboard/stat-cards';
import { QuickActions } from '@/components/dashboard/quick-actions';
import { Button } from '@/components/ui/button';
import Link from 'next/link';
import { YearSelector, PrintButton, MonthSelector } from '@/components/dashboard/dashboard-controls';
import { OilConsumptionStats } from '@/components/dashboard/oil-consumption-stats';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { RecentActivity } from '@/components/dashboard/recent-activity';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import dynamic from 'next/dynamic';
import { Skeleton } from '@/components/ui/skeleton';
import type { DashboardData } from '@/lib/types';

dayjs.locale('fr');

const OverviewChart = dynamic(
  () => import('@/components/dashboard/overview-chart').then(mod => mod.OverviewChart),
  {
    loading: () => <Skeleton className="h-[350px] w-full" />,
    ssr: false,
  }
);
const CompletionChart = dynamic(
  () => import('@/components/dashboard/completion-chart').then(mod => mod.CompletionChart),
  {
    loading: () => <Skeleton className="h-64 w-full" />,
    ssr: false,
  }
);
const PreventativeOverviewChart = dynamic(
  () => import('@/components/dashboard/preventative-overview-chart').then(mod => mod.PreventativeOverviewChart),
  {
    loading: () => <Skeleton className="h-[350px] w-full" />,
    ssr: false,
  }
);


type DashboardViewProps = {
    data: DashboardData;
}

export function DashboardView({ data }: DashboardViewProps) {
    const { year, month } = data;
    const selectedMonthName = month ? dayjs().month(month - 1).format('MMMM') : null;
    const descriptionPeriod = selectedMonthName ? `pour ${selectedMonthName.charAt(0).toUpperCase() + selectedMonthName.slice(1)} ${year}` : `pour l'année ${year}`;

    return (
        <div className="space-y-6">
        <header className="print-hide">
            <div className="flex flex-col sm:flex-row gap-4 justify-between items-start sm:items-center">
                <div>
                <h1 className="text-3xl font-bold tracking-tight">Tableau de bord</h1>
                <p className="text-muted-foreground">Vue d'ensemble de l'activité de maintenance pour l'année {data.year}.</p>
                </div>
                <div className="flex gap-2 items-center">
                    <MonthSelector currentMonth={month?.toString() ?? 'all'} />
                    <YearSelector currentYear={data.year.toString()} />
                    <PrintButton />
                </div>
            </div>
        </header>
        
        <main id="dashboard-content" className="grid gap-6">
            <StatCards data={data} />
            
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <Card className="dashboard-card lg:col-span-2">
                    <Tabs defaultValue="curative">
                        <CardHeader>
                            <div className="flex flex-wrap gap-4 justify-between items-start">
                                <div className="space-y-1">
                                    <CardTitle>Aperçu des Interventions ({data.year})</CardTitle>
                                    <TabsContent value="curative" className="p-0 m-0 border-0 !mt-0 print:block">
                                        <CardDescription>Interventions de réparation et pannes par mois.</CardDescription>
                                    </TabsContent>
                                    <TabsContent value="preventive" className="p-0 m-0 border-0 !mt-0 print:block">
                                        <CardDescription>Interventions de maintenance planifiée par mois.</CardDescription>
                                    </TabsContent>
                                </div>
                                <TabsList className="print-hide">
                                    <TabsTrigger value="curative">Curatives</TabsTrigger>
                                    <TabsTrigger value="preventive">Préventives</TabsTrigger>
                                </TabsList>
                            </div>
                        </CardHeader>
                        <CardContent className="pl-2">
                            <TabsContent value="curative" className="print:block">
                                <OverviewChart data={data.monthlyCounts} />
                            </TabsContent>
                            <TabsContent value="preventive" className="print:block">
                                <PreventativeOverviewChart data={data.preventativeStats.monthlyData} />
                            </TabsContent>
                        </CardContent>
                    </Tabs>
                </Card>

                <div className="space-y-6">
                    <Card className="dashboard-card">
                        <CardHeader>
                            <CardTitle>Consommation d'Huiles & Graisses</CardTitle>
                            <CardDescription>Total {descriptionPeriod}.</CardDescription>
                        </CardHeader>
                        <CardContent>
                            <OilConsumptionStats data={data.preventativeStats} />
                        </CardContent>
                    </Card>
                    <Card className="dashboard-card">
                        <CardHeader>
                            <CardTitle>Activité Curative Récente</CardTitle>
                            <CardDescription>Les 5 dernières pannes enregistrées.</CardDescription>
                        </CardHeader>
                        <CardContent>
                            <RecentActivity data={data.recentOperations} />
                        </CardContent>
                    </Card>
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                <Card className="dashboard-card print-hide">
                <CardHeader>
                    <CardTitle>Actions Rapides</CardTitle>
                </CardHeader>
                <CardContent>
                    <QuickActions />
                </CardContent>
                </Card>
                <Card className="dashboard-card print-hide">
                <CardHeader>
                    <CardTitle>Initialisation</CardTitle>
                    <CardDescription>Action unique pour démarrer l'application.</CardDescription>
                </CardHeader>
                <CardContent>
                    <p className='text-sm text-muted-foreground mb-4'>
                        Si vous ne voyez aucune donnée, vous devez peut-être initialiser la base de données.
                    </p>
                    <Button asChild variant="outline">
                        <Link href="/init-db">
                            <Database className="mr-2 h-4 w-4" />
                            Page d'Initialisation
                        </Link>
                    </Button>
                </CardContent>
                </Card>
                <Card className="dashboard-card">
                    <CardHeader>
                    <CardTitle>Taux de Réalisation</CardTitle>
                    <CardDescription>Préventif {data.year}</CardDescription>
                    </CardHeader>
                    <CardContent>
                    <CompletionChart data={data.followUpStats} />
                    </CardContent>
                </Card>
            </div>
        </main>
        </div>
    );
}

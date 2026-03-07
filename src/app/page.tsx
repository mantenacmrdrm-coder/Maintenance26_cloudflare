import { getDashboardData } from '@/lib/actions/maintenance-actions';
import { DashboardView } from './dashboard-view';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { AlertTriangle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import Link from 'next/link';


export const dynamic = 'force-dynamic';
export default async function DashboardPage({ searchParams }: { searchParams: Promise<{ year?: string; month?: string }> }) {
  const resolvedSearchParams = await searchParams;
  const year = resolvedSearchParams.year ? parseInt(resolvedSearchParams.year, 10) : undefined;
  const month = resolvedSearchParams.month ? parseInt(resolvedSearchParams.month, 10) : undefined;
  const data = await getDashboardData(year, month);

  if (data.error) {
    return (
      <div className="flex flex-col gap-8">
        <header>
          <h1 className="text-3xl font-bold tracking-tight">Tableau de bord</h1>
          <p className="text-muted-foreground">Application de Maintenance GMAO</p>
        </header>
         <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertTitle>Erreur de chargement des données</AlertTitle>
            <AlertDescription>
              {data.error}
              <br/>
              Assurez-vous que la base de données a été initialisée.
               <Button asChild variant="link" className="p-0 h-auto ml-2">
                  <Link href="/init-db">Aller à la page d'initialisation</Link>
                </Button>
            </AlertDescription>
          </Alert>
      </div>
    )
  }

  return <DashboardView data={data} />;
}

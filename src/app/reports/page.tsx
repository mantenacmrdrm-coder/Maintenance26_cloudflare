import Link from 'next/link';

export const dynamic = 'force-dynamic';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { Eye } from 'lucide-react';
import { getWeeklyReports } from '@/lib/actions/maintenance-actions';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { GenerateReportButton } from './generate-report-button';
import { DeleteReportButton } from './delete-report-button';

dayjs.locale('fr');

export default async function ReportsPage() {
  const reports = await getWeeklyReports();

  return (
    <div className="flex flex-col gap-8">
      <header>
        <div className="flex justify-between items-center flex-wrap gap-4">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">
              Rapports
            </h1>
            <p className="text-muted-foreground">
              Générez et consultez les rapports hebdomadaires.
            </p>
          </div>
          <GenerateReportButton />
        </div>
      </header>
      <main>
        <Card>
          <CardHeader>
            <CardTitle>Rapports Hebdomadaires Sauvegardés</CardTitle>
            <CardDescription>
              Voici la liste des rapports que vous avez générés.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Période du Rapport</TableHead>
                    <TableHead>Date de Génération</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {reports && reports.length > 0 ? (
                    reports.map((report) => (
                      <TableRow key={report.id}>
                        <TableCell className="font-medium">
                          Rapport du {dayjs(report.start_date).format('DD/MM/YYYY')} au {dayjs(report.end_date).format('DD/MM/YYYY')}
                        </TableCell>
                        <TableCell>
                           {dayjs(report.generated_at).format('DD/MM/YYYY HH:mm')}
                        </TableCell>
                        <TableCell className="text-right">
                          <div className="flex justify-end gap-2">
                            <Button asChild variant="ghost" size="icon">
                              <Link href={`/reports/${report.id}`}>
                                <Eye className="h-4 w-4" />
                              </Link>
                            </Button>
                            <DeleteReportButton reportId={report.id} />
                          </div>
                        </TableCell>
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell colSpan={3} className="h-24 text-center">
                        Aucun rapport généré pour le moment.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

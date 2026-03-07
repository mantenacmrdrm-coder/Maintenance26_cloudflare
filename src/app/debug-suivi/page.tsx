import { getSuiviCuratifRaw } from '@/lib/data';

export const dynamic = 'force-dynamic';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import Link from 'next/link';

export default async function SuiviCuratifDebugPage() {
  const data = await getSuiviCuratifRaw();
  const headers = data.length > 0 ? Object.keys(data[0]) : [];

  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Données brutes : suivi_curatif</h1>
        <p className="text-muted-foreground">
          Ceci est une vue directe des 50 derniers enregistrements de la table `suivi_curatif`.
        </p>
        <Button asChild variant="link" className="p-0 h-auto">
          <Link href="/operations">Retour à la liste des opérations</Link>
        </Button>
      </header>
      <main>
        <Card>
            <CardHeader>
                <CardTitle>Contenu de la table</CardTitle>
                <CardDescription>{data.length} lignes trouvées. Affichage par ordre décroissant (les plus récentes en premier).</CardDescription>
            </CardHeader>
            <CardContent>
                {data.length > 0 ? (
                     <div className="rounded-md border w-full overflow-auto">
                        <Table>
                            <TableHeader>
                                <TableRow>
                                    {headers.map(header => (
                                        <TableHead key={header} className="whitespace-nowrap text-xs">{header.replace(/_/g, ' ')}</TableHead>
                                    ))}
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {data.map((row: any) => (
                                    <TableRow key={row.id}>
                                        {headers.map(header => (
                                            <TableCell key={header} className="whitespace-nowrap text-xs p-2">
                                                {row[header]}
                                            </TableCell>
                                        ))}
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </div>
                ) : (
                    <p>Aucune donnée dans la table `suivi_curatif`. Vous pouvez en ajouter depuis le <Link href="/operations/new" className="underline">formulaire de nouvelle opération</Link>.</p>
                )}
            </CardContent>
        </Card>
      </main>
    </div>
  );
}

import Link from 'next/link';
import { Card, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { FileSpreadsheet, FileText, Wrench, ClipboardCheck, ClipboardPlus, BarChart3, Truck, FileClock } from 'lucide-react';


export const dynamic = 'force-dynamic';
export default function DocumentsPage() {
  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Documents</h1>
        <p className="text-muted-foreground">
          Générez et consultez les différents documents de maintenance.
        </p>
      </header>
      <main className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
        <Link href="/reports" className="group">
          <Card className="h-full transition-all hover:border-primary/50 hover:shadow-md">
            <CardHeader className="flex flex-row items-center gap-4">
                <div className="icon-container icon-container-primary">
                    <FileSpreadsheet className="h-6 w-6" />
                </div>
                <div className='space-y-1'>
                    <CardTitle className="text-lg">Rapports Hebdomadaires</CardTitle>
                    <CardDescription>Consultez les états des pannes.</CardDescription>
                </div>
            </CardHeader>
          </Card>
        </Link>
        <Link href="/declarations" className="group">
          <Card className="h-full transition-all hover:border-primary/50 hover:shadow-md">
             <CardHeader className="flex flex-row items-center gap-4">
                <div className="icon-container icon-container-primary">
                    <FileText className="h-6 w-6" />
                </div>
                <div className='space-y-1'>
                    <CardTitle className="text-lg">Déclarations de Panne</CardTitle>
                    <CardDescription>Créez les déclarations détaillées.</CardDescription>
                </div>
            </CardHeader>
          </Card>
        </Link>
        <Link href="/bons-de-sortie" className="group">
          <Card className="h-full transition-all hover:border-primary/50 hover:shadow-md">
             <CardHeader className="flex flex-row items-center gap-4">
                <div className="icon-container icon-container-primary">
                    <Truck className="h-6 w-6" />
                </div>
                <div className='space-y-1'>
                    <CardTitle className="text-lg">Bons de Sortie</CardTitle>
                    <CardDescription>Générez les bons de sortie matériel.</CardDescription>
                </div>
            </CardHeader>
          </Card>
        </Link>
        <Link href="/fiches-curatives" className="group">
          <Card className="h-full transition-all hover:border-primary/50 hover:shadow-md">
             <CardHeader className="flex flex-row items-center gap-4">
                <div className="icon-container icon-container-primary">
                    <Wrench className="h-6 w-6" />
                </div>
                <div className='space-y-1'>
                    <CardTitle className="text-lg">Fiches Curatives</CardTitle>
                    <CardDescription>Générez les fiches de maintenance curative.</CardDescription>
                </div>
            </CardHeader>
          </Card>
        </Link>
        <Link href="/fiches-preventives" className="group">
          <Card className="h-full transition-all hover:border-primary/50 hover:shadow-md">
             <CardHeader className="flex flex-row items-center gap-4">
                <div className="icon-container icon-container-primary">
                    <ClipboardPlus className="h-6 w-6" />
                </div>
                <div className='space-y-1'>
                    <CardTitle className="text-lg">Fiches Préventives</CardTitle>
                    <CardDescription>Générez les fiches d'entretien préventif.</CardDescription>
                </div>
            </CardHeader>
          </Card>
        </Link>
        <Link href="/ordres-de-travail" className="group">
          <Card className="h-full transition-all hover:border-primary/50 hover:shadow-md">
             <CardHeader className="flex flex-row items-center gap-4">
                <div className="icon-container icon-container-primary">
                    <ClipboardCheck className="h-6 w-6" />
                </div>
                <div className='space-y-1'>
                    <CardTitle className="text-lg">Ordres de Travail</CardTitle>
                    <CardDescription>Générez les ordres de travail curatifs.</CardDescription>
                </div>
            </CardHeader>
          </Card>
        </Link>
        <Link href="/consumption-reports" className="group">
          <Card className="h-full transition-all hover:border-primary/50 hover:shadow-md">
             <CardHeader className="flex flex-row items-center gap-4">
                <div className="icon-container icon-container-primary">
                    <BarChart3 className="h-6 w-6" />
                </div>
                <div className='space-y-1'>
                    <CardTitle className="text-lg">Rapports de Consommation</CardTitle>
                    <CardDescription>Générez les états journaliers des stocks.</CardDescription>
                </div>
            </CardHeader>
          </Card>
        </Link>
        <Link href="/monthly-reports" className="group">
          <Card className="h-full transition-all hover:border-primary/50 hover:shadow-md">
             <CardHeader className="flex flex-row items-center gap-4">
                <div className="icon-container icon-container-primary">
                    <FileClock className="h-6 w-6" />
                </div>
                <div className='space-y-1'>
                    <CardTitle className="text-lg">Rapports Mensuels de Stock</CardTitle>
                    <CardDescription>Suivi mensuel des entrées/sorties.</CardDescription>
                </div>
            </CardHeader>
          </Card>
        </Link>
      </main>
    </div>
  );
}

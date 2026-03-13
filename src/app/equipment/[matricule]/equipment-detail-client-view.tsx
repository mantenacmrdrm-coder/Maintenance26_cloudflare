
'use client';

import { useState, useRef } from 'react';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle, 
} from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import Image from 'next/image';
import { PlaceHolderImages } from '@/lib/placeholder-images';
import { EquipmentImages } from '@/lib/equipment-images';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { PreventativeMaintenanceHistory } from '@/app/equipment/[matricule]/preventative-maintenance-history';
import { CurativeMaintenanceHistory } from './curative-maintenance-history';
import { Wrench, Zap, SlidersHorizontal, GaugeCircle, Droplet, Filter, Power, PowerOff, Upload, Printer } from 'lucide-react';
import { Separator } from '@/components/ui/separator';
import { cn } from '@/lib/utils';
import { useToast } from '@/hooks/use-toast';
import type { Equipment, Operation, PreventativeMaintenanceEntry, CurativeMaintenanceEntry } from '@/lib/types';
import { parse } from 'date-fns';
import { Skeleton } from '@/components/ui/skeleton';
import dynamic from 'next/dynamic';
import { Button } from '@/components/ui/button';
import { OperationsHistoryTable } from './operations-history-table';
import { Input } from '@/components/ui/input';

const EquipmentStatusChart = dynamic(
  () => import('./equipment-status-chart').then(mod => mod.EquipmentStatusChart),
  { 
    ssr: false,
    loading: () => (
        <div className="flex flex-col items-center justify-center h-[300px] w-full">
            <CardDescription>Chargement du graphique...</CardDescription>
        </div>
    )
  }
);

type ClientViewProps = {
    equipment: Equipment;
    operations: Operation[];
    preventativeHistory: Record<string, PreventativeMaintenanceEntry[]>;
    curativeHistory: CurativeMaintenanceEntry[];
    dynamicStatus: 'En Marche' | 'En Panne' | 'Actif';
}

export function EquipmentDetailClientView({ equipment, operations, preventativeHistory, curativeHistory, dynamicStatus }: ClientViewProps) {
  const { toast } = useToast();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const { matricule } = equipment;
  const [newCounter, setNewCounter] = useState('');

  const getInitialImage = () => {
    const specificImage = EquipmentImages.find(img => img.matricule === matricule);
    const getPlaceholderImage = (category: string | undefined) => {
        const cat = category?.toLowerCase() || '';
        if (cat.includes('camion') || cat.includes('truck') || cat.includes('benne') || cat.includes('transmarchandise')) {
            return PlaceHolderImages.find(img => img.id === 'camion');
        }
        if (cat.includes('voiture') || cat.includes('vehicule') || cat.includes('leger') || cat.includes('symbol') || cat.includes('transpersonnel')) {
            return PlaceHolderImages.find(img => img.id === 'voiture');
        }
        if (cat.includes('pelle') || cat.includes('engin') || cat.includes('niveleuse') || cat.includes('chargeur') || cat.includes('manutention')) {
            return PlaceHolderImages.find(img => img.id === 'engin');
        }
        if (cat.includes('groupe') || cat.includes('generateur') || cat.includes('geg')) {
            return PlaceHolderImages.find(img => img.id === 'generateur');
        }
        if (cat.includes('chariot') || cat.includes('forklift')) {
            return PlaceHolderImages.find(img => img.id === 'chariot');
        }
        return PlaceHolderImages.find(img => img.id === 'equipment-generic');
    };
    return specificImage || getPlaceholderImage(equipment.categorie || undefined) || PlaceHolderImages.find(img => img.id === 'equipment-generic');
  }

  const [currentImage, setCurrentImage] = useState(getInitialImage());
  
  const handleImageClick = () => {
    fileInputRef.current?.click();
  };

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const newImageUrl = e.target?.result as string;
        setCurrentImage(prev => prev ? { ...prev, imageUrl: newImageUrl } : { id: 'local-preview', imageUrl: newImageUrl, description: 'Aperçu local', imageHint: '' });
        toast({
          title: "Aperçu de l'image mis à jour",
          description: "Cette modification est temporaire. Pour la sauvegarder de manière permanente, veuillez mettre à jour le fichier `equipment-images.json`.",
        });
      };
      reader.readAsDataURL(file);
    }
  };

  const handlePrint = () => {
    window.print();
  };

  const statusConfig = {
    'En Marche': {
      label: 'En Marche',
      icon: Power,
      color: 'bg-green-600 text-green-50',
    },
    'En Panne': {
      label: 'En Panne',
      icon: PowerOff,
      color: 'bg-destructive text-destructive-foreground',
    },
    'Actif': {
      label: 'Actif',
      icon: Wrench,
      color: 'bg-blue-500 text-blue-50',
    }
  }
  const currentStatus = statusConfig[dynamicStatus] || statusConfig['Actif'];


  // Preventative Stats Calculation
  const preventativeStats = {
      total: Object.values(preventativeHistory).reduce((sum, arr) => sum + arr.length, 0),
      vidanges: preventativeHistory['Vidanger le carter moteur']?.length || 0,
      graissages: preventativeHistory['Graissage général']?.length || 0,
      filtres: (preventativeHistory['Filtre à huile']?.length || 0) + 
               (preventativeHistory['Filtre carburant']?.length || 0) +
               (preventativeHistory['Filtre à air']?.length || 0) +
               (preventativeHistory['Filtre hydraulique']?.length || 0),
  };
  
  const findLatestDate = (entries: { date: string, details: string[] }[] | undefined) => {
      if (!entries || entries.length === 0) return null;
      return entries[0]; // Already sorted descending by date
  };

  const findLatestCounterAndDate = (history: Record<string, PreventativeMaintenanceEntry[]>): { counter: string | null; date: string | null } => {
      let latestEntry: PreventativeMaintenanceEntry | null = null;
      let latestDateObj: Date | null = null;

      for (const entry of Object.values(history).flat()) {
          const entryDate = parse(entry.date, 'dd/MM/yyyy', new Date());
          if (!isNaN(entryDate.getTime())) {
            if (!latestDateObj || entryDate > latestDateObj) {
                latestDateObj = entryDate;
                latestEntry = entry;
            }
          }
      }
      
      if (latestEntry) {
        const counterDetail = latestEntry.details.find(d => d.toLowerCase().includes('relevé compteur'));
        if (counterDetail) {
          const counterValue = counterDetail.split(':')[1]?.trim() || null;
          return { counter: counterValue, date: latestEntry.date };
        }
      }
      return { counter: null, date: null };
  };

  const lastVidange = findLatestDate(preventativeHistory['Vidanger le carter moteur']);
  const lastGraissage = findLatestDate(preventativeHistory['Graissage général']);
  const lastFiltreHuile = findLatestDate(preventativeHistory['Filtre à huile']);
  const lastFiltreCarburant = findLatestDate(preventativeHistory['Filtre carburant']);
  const lastFiltreAir = findLatestDate(preventativeHistory['Filtre à air']);
  const lastFiltreHydraulique = findLatestDate(preventativeHistory['Filtre hydraulique']);
  const { counter: lastCounter, date: lastCounterDate } = findLatestCounterAndDate(preventativeHistory);

  const lastCounterValue = lastCounter ? parseFloat(String(lastCounter).replace(/[^0-9.]/g, '')) : 0;
  const newCounterValue = newCounter ? parseFloat(newCounter) : 0;
  const counterDifference = (newCounterValue && lastCounterValue && newCounterValue > lastCounterValue)
      ? newCounterValue - lastCounterValue
      : null;


  // Curative Stats Calculation
  const curativeStats = curativeHistory.reduce((acc, entry) => {
      acc.totalPannes += 1;
      // Force conversion to number to avoid string concatenation
      const daysValue = typeof entry.dureeIntervention === 'number' 
        ? entry.dureeIntervention 
        : parseFloat(String(entry.dureeIntervention || '0').replace(',', '.'));
      acc.totalJoursIndisponibilite += isNaN(daysValue) ? 0 : daysValue;
      
      if (entry.typePanne) {
        // Use lowercase trim keys for consistency with UI lookup
        const typeKey = String(entry.typePanne).toLowerCase().trim();
        acc.types[typeKey] = (acc.types[typeKey] || 0) + 1;
      }
      return acc;
  }, {
      totalPannes: 0,
      totalJoursIndisponibilite: 0,
      types: {} as Record<string, number>,
  });

  return (
    <div className="space-y-6">
      <header className="print-hide">
        <div className="flex flex-col sm:flex-row gap-4 justify-between items-start sm:items-center">
            <div>
              <h1 className="text-3xl font-bold tracking-tight flex items-center gap-2">
                <Badge variant="default" className="text-xl px-3 py-1">{equipment.matricule}</Badge>
                {equipment.designation}
              </h1>
              <p className="text-muted-foreground">
                Détails et historique de maintenance de l'équipement.
              </p>
            </div>
            <Button onClick={handlePrint} variant="outline">
              <Printer className="mr-2 h-4 w-4" />
              Imprimer
            </Button>
        </div>
      </header>

      <header className="hidden print:block">
        <h1 className="text-3xl font-bold tracking-tight flex items-center gap-2">
          <Badge variant="default" className="text-xl px-3 py-1">{equipment.matricule}</Badge>
          {equipment.designation}
        </h1>
        <p className="text-muted-foreground">
          Dossier de maintenance de l'équipement.
        </p>
      </header>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card className="lg:col-span-2 dashboard-card">
          <CardHeader>
            <CardTitle>Informations Générales</CardTitle>
          </CardHeader>
          <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="relative aspect-video w-full rounded-lg overflow-hidden group cursor-pointer" onClick={handleImageClick}>
                {currentImage && (
                    <Image
                        src={currentImage.imageUrl}
                        alt={equipment.designation}
                        fill
                        className="object-cover"
                        data-ai-hint={currentImage.imageHint}
                    />
                )}
                 <div className="absolute inset-0 bg-black/50 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity">
                  <Upload className="h-8 w-8 text-white" />
                </div>
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileChange}
                  className="hidden"
                  accept="image/*"
                />
            </div>
            <div className="space-y-4 text-sm">
                <div><strong>Marque:</strong> {equipment.marque}</div>
                <div><strong>Catégorie:</strong> {equipment.categorie}</div>
                <div><strong>Année de mise en service:</strong> {equipment.annee || '-'}</div>
                 {lastCounter && <div><strong>Dernier Relevé:</strong> <span className='font-bold'>{lastCounter}</span> ({lastCounterDate})</div>}
                <div className='flex items-center gap-2'>
                  <strong>Statut:</strong> 
                  <Badge variant='default' className={cn('flex items-center gap-1.5', currentStatus.color)}>
                    <currentStatus.icon className="h-3.5 w-3.5" />
                    {currentStatus.label}
                  </Badge>
                </div>
                <Separator />
                <div>
                    <h4 className="font-semibold text-base mb-2">Calculateur de Compteur</h4>
                    <div className="space-y-3">
                        <div className="flex items-center gap-2">
                            <label htmlFor="new-counter" className="text-sm min-w-fit font-medium">Nouveau Relevé:</label>
                            <Input
                                id="new-counter"
                                type="number"
                                value={newCounter}
                                onChange={(e) => setNewCounter(e.target.value)}
                                placeholder="Saisir la valeur..."
                                className="h-8"
                            />
                        </div>
                        {counterDifference !== null && (
                            <div className="pt-1">
                                <p className="text-xs text-muted-foreground">Parcouru / Travaillé depuis dernier relevé :</p>
                                <p className="text-xl font-bold text-primary">
                                    {counterDifference.toLocaleString('fr-FR')} {equipment.categorie === 'Camion' ? 'km' : 'heures'}
                                </p>
                            </div>
                        )}
                         {newCounterValue > 0 && lastCounterValue > 0 && newCounterValue <= lastCounterValue && (
                            <p className="text-xs text-destructive">Le nouveau relevé doit être supérieur au dernier.</p>
                        )}
                    </div>
                </div>
            </div>
          </CardContent>
        </Card>
        
        <div className="space-y-4 lg:col-span-2 grid grid-cols-1 sm:grid-cols-2 gap-4">
          <Card className="dashboard-card">
            <CardHeader>
              <CardTitle>Statistiques Préventives</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              <div className="flex justify-between items-center"><span className="flex items-center gap-2"><Wrench />Total Interventions:</span><span className="font-bold text-lg">{preventativeStats.total}</span></div>
              <Separator/>
              <div className="flex justify-between items-center"><span className='flex items-center gap-2'><Droplet/>Dernière Vidange:</span><span className="font-bold">{lastVidange?.date || '-'}</span></div>
              <div className="flex justify-between items-center"><span className='flex items-center gap-2'><GaugeCircle/>Dernier Graissage:</span><span className="font-bold">{lastGraissage?.date || '-'}</span></div>
              <div className="flex justify-between items-center"><span className='flex items-center gap-2'><Filter/>Derniers Filtres:</span>
                <div className='text-right'>
                    {lastFiltreHuile && <div className='font-bold'>Huile: {lastFiltreHuile.date}</div>}
                    {lastFiltreCarburant && <div className='font-bold'>Carburant: {lastFiltreCarburant.date}</div>}
                    {lastFiltreAir && <div className='font-bold'>Air: {lastFiltreAir.date}</div>}
                    {lastFiltreHydraulique && <div className='font-bold'>Hydraulique: {lastFiltreHydraulique.date}</div>}
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="dashboard-card">
            <CardHeader>
              <CardTitle>Statistiques Curatives</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              <div className="flex justify-between items-center"><span>Total Pannes:</span><span className="font-bold text-lg text-destructive">{curativeStats.totalPannes}</span></div>
              <div className="flex justify-between items-center"><span>Jours d'indisponibilité:</span><span className="font-bold">{curativeStats.totalJoursIndisponibilite} jour(s)</span></div>
              <Separator />
              <div className="flex justify-around items-center pt-1">
                <div className='text-center'><Wrench className='mx-auto h-4 w-4 mb-1' /><span className='font-bold'>{curativeStats.types['mécanique'] || 0}</span><p className='text-xs text-muted-foreground'>Mécanique</p></div>
                <div className='text-center'><Zap className='mx-auto h-4 w-4 mb-1' /><span className='font-bold'>{curativeStats.types['électrique'] || 0}</span><p className='text-xs text-muted-foreground'>Électrique</p></div>
                <div className='text-center'><SlidersHorizontal className='mx-auto h-4 w-4 mb-1' /><span className='font-bold'>{curativeStats.types['autres'] || 0}</span><p className='text-xs text-muted-foreground'>Autres</p></div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
      
      <Card className="dashboard-card">
        <CardHeader>
          <CardTitle>Répartition des interventions</CardTitle>
           <CardDescription>
            Aperçu visuel de toutes les interventions enregistrées pour cet équipement.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <EquipmentStatusChart operations={operations} />
        </CardContent>
      </Card>
      
      <Card className="dashboard-card">
        <CardHeader>
          <CardTitle>Détail des Interventions</CardTitle>
        </CardHeader>
        <CardContent>
            <Tabs defaultValue="preventive">
                <TabsList className="grid w-full grid-cols-2 print-hide">
                    <TabsTrigger value="preventive">Maintenance Préventive</TabsTrigger>
                    <TabsTrigger value="curative">Maintenance Curative</TabsTrigger>
                </TabsList>
                <TabsContent value="preventive" className="mt-6 print:block">
                    <PreventativeMaintenanceHistory history={preventativeHistory} />
                </TabsContent>
                <TabsContent value="curative" className="mt-6 print:block">
                   <CurativeMaintenanceHistory history={curativeHistory} />
                </TabsContent>
            </Tabs>
        </CardContent>
      </Card>

      <Card className="dashboard-card">
        <CardHeader>
          <CardTitle>Historique Global des Interventions</CardTitle>
          <CardDescription>Recherchez et filtrez toutes les opérations enregistrées pour cet équipement.</CardDescription>
        </CardHeader>
        <CardContent>
          <OperationsHistoryTable operations={operations} />
        </CardContent>
      </Card>
    </div>
  );
}

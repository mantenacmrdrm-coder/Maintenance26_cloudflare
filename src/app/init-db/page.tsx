'use client';

import { Card, CardHeader, CardTitle, CardContent, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Database, Loader2, DownloadCloud, UploadCloud, AlertTriangle } from 'lucide-react';
import { useState, useTransition, useRef } from 'react';
import { useToast } from '@/hooks/use-toast';
import { initializeDatabase, restoreDatabase } from '@/lib/actions/maintenance-actions';
import { UploadCard } from './upload-card';

export const dynamic = 'force-dynamic';

export default function InitDbPage() {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleInitDb = () => {
    startTransition(async () => {
      const result = await initializeDatabase();
      if (result.success) {
        toast({ title: 'Succès', description: result.message });
      } else {
        toast({ variant: 'destructive', title: 'Erreur', description: result.message });
      }
    });
  };

  const handleBackupDb = () => {
    startTransition(async () => {
      window.open('/api/export-db', '_blank');
      toast({ title: 'Téléchargement lancé', description: 'Si le fichier existe, le téléchargement devrait commencer.' });
    });
  };

  // --- GESTION DE L'IMPORT ---
  const handleImportClick = () => {
    // Ouvre la boîte de dialogue de sélection de fichier
    fileInputRef.current?.click();
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    // Confirmation de sécurité
    const confirmRestore = confirm("ATTENTION : Vous allez remplacer TOUTE la base de données actuelle par celle de ce fichier. Cette action est irréversible. Continuer ?");
    
    if (!confirmRestore) return;

    const formData = new FormData();
    formData.append('file', file);

    startTransition(async () => {
      const result = await restoreDatabase(formData);
      if (result.success) {
        toast({ 
          title: 'Restauration Réussie', 
          description: result.message,
          variant: "default"
        });
      } else {
        toast({ 
          variant: 'destructive', 
          title: 'Échec de la Restauration', 
          description: result.message 
        });
      }
      // Reset input
      if (fileInputRef.current) fileInputRef.current.value = '';
    });
  };

  const fileUploads = [
    { tableName: 'matrice', title: 'Matrice des équipements', description: 'Le fichier principal contenant la liste de tous vos équipements.' },
    { tableName: 'consolide', title: 'Consolidé des consommations', description: 'L\'historique des consommations de lubrifiants.' },
    { tableName: 'suivi_curatif', title: 'Suivi Curatif', description: 'L\'historique des pannes et interventions curatives.' },
    { tableName: 'vidange', title: 'Vidanges', description: 'Ancien historique des vidanges.' },
    { tableName: 'Param', title: 'Paramètres de maintenance', description: 'Intervalles et niveaux pour la maintenance préventive.' },
  ];

  return (
     <div className="flex flex-col gap-8">
        <header>
            <h1 className="text-3xl font-bold tracking-tight">Initialisation & Données</h1>
            <p className="text-muted-foreground">Gérez la structure et les données de base de votre application.</p>
        </header>

        <main className="space-y-8">
             <Card>
                <CardHeader>
                <CardTitle>Initialisation de la Structure</CardTitle>
                 <CardDescription>
                    Actions globales sur la base de données.
                </CardDescription>
                </CardHeader>
                <CardContent className='flex flex-wrap gap-4'>
                    <Button onClick={handleInitDb} disabled={isPending}>
                    {isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Database className="mr-2 h-4 w-4" />}
                    Vérifier et Initialiser
                    </Button>

                    <Button onClick={handleBackupDb} variant="secondary" disabled={isPending}>
                        {isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <DownloadCloud className="mr-2 h-4 w-4" />}
                        Télécharger la BD (Sauvegarde)
                    </Button>

                    {/* Input caché pour le fichier */}
                    <input 
                        type="file" 
                        ref={fileInputRef} 
                        onChange={handleFileChange} 
                        accept=".db" 
                        className="hidden" 
                    />

                    {/* Bouton Import (Dangereux) */}
                    <Button onClick={handleImportClick} variant="destructive" disabled={isPending}>
                        {isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <UploadCloud className="mr-2 h-4 w-4" />}
                        Importer une BD (Restauration)
                    </Button>
                </CardContent>
                <CardContent className="text-xs text-muted-foreground pt-0">
                    <p className="flex items-center gap-2">
                        <AlertTriangle className="h-4 w-4 text-orange-500" />
                        <span><strong>Attention :</strong> L'importation écrase toutes les données actuelles. Une sauvegarde de sécurité (.backup) sera automatiquement créée avant l'opération.</span>
                    </p>
                </CardContent>
            </Card>

            <Card>
                <CardHeader>
                    <CardTitle>Mise à jour des données via CSV</CardTitle>
                    <CardDescription>
                        Chargez un nouveau fichier CSV pour une table spécifique. <span className="font-bold text-destructive">Attention :</span> cette action remplacera entièrement les données de la table correspondante.
                    </CardDescription>
                </CardHeader>
                <CardContent className='grid md:grid-cols-2 lg:grid-cols-3 gap-6'>
                    {fileUploads.map(upload => (
                        <UploadCard
                            key={upload.tableName}
                            tableName={upload.tableName}
                            title={upload.title}
                            description={upload.description}
                        />
                    ))}
                </CardContent>
            </Card>
        </main>
    </div>
  );
}



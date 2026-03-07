'use client';

import { useState, useTransition, useRef } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { useToast } from '@/hooks/use-toast';
import { Loader2, UploadCloud } from 'lucide-react';
import { handleFileUploadAction } from '@/lib/actions/maintenance-actions';

type UploadCardProps = {
  tableName: string;
  title: string;
  description: string;
};

export function UploadCard({ tableName, title, description }: UploadCardProps) {
  const [file, setFile] = useState<File | null>(null);
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const inputRef = useRef<HTMLInputElement>(null);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      setFile(e.target.files[0]);
    } else {
      setFile(null);
    }
  };

  const handleUpload = () => {
    if (!file) {
      toast({
        variant: 'destructive',
        title: 'Aucun fichier sélectionné',
        description: 'Veuillez choisir un fichier CSV à charger.',
      });
      return;
    }

    startTransition(async () => {
      const reader = new FileReader();
      reader.onload = async (event) => {
        if (event.target?.result) {
          const fileContent = event.target.result as string;
          const result = await handleFileUploadAction(tableName, fileContent);
          
          if (result.success) {
            toast({
              title: 'Succès',
              description: result.message,
            });
            if (inputRef.current) {
              inputRef.current.value = '';
            }
            setFile(null);
          } else {
            toast({
              variant: 'destructive',
              title: 'Erreur',
              description: result.message,
            });
          }
        }
      };
      reader.onerror = () => {
          toast({
              variant: 'destructive',
              title: 'Erreur de lecture',
              description: 'Impossible de lire le fichier sélectionné.',
          });
      };
      reader.readAsText(file, 'utf-8'); // Specify encoding
    });
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">{title}</CardTitle>
        <CardDescription className="text-xs">{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          <Input
            ref={inputRef}
            type="file"
            accept=".csv"
            onChange={handleFileChange}
            disabled={isPending}
            className="text-xs"
          />
          <Button onClick={handleUpload} disabled={isPending || !file} className="w-full">
            {isPending ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <UploadCloud className="mr-2 h-4 w-4" />
            )}
            Mettre à jour
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

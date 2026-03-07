'use client';

import { useTransition } from 'react';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog';
import { Button } from '@/components/ui/button';
import { Loader2, Trash2 } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { deleteBonDeSortieAction } from '@/lib/actions/maintenance-actions';

export function DeleteBonButton({ bonId }: { bonId: number }) {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();

  const handleDelete = () => {
    startTransition(async () => {
      const result = await deleteBonDeSortieAction(bonId);
      if (result.success) {
        toast({
          title: 'Bon de sortie supprimé',
          description: 'Le bon a été supprimé avec succès.',
        });
      } else {
        toast({
          variant: 'destructive',
          title: 'Erreur',
          description: result.message || 'Impossible de supprimer le bon.',
        });
      }
    });
  };

  return (
    <AlertDialog>
      <AlertDialogTrigger asChild>
        <Button variant="destructive" size="icon">
          <Trash2 className="h-4 w-4" />
        </Button>
      </AlertDialogTrigger>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Êtes-vous sûr de vouloir supprimer ce bon de sortie ?</AlertDialogTitle>
          <AlertDialogDescription>
            Cette action est irréversible. Le bon sera définitivement supprimé de la base de données.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Annuler</AlertDialogCancel>
          <AlertDialogAction onClick={handleDelete} disabled={isPending}>
            {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Supprimer
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}

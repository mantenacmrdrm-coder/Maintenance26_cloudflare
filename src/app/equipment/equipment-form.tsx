'use client';

import { zodResolver } from '@hookform/resolvers/zod';
import { useForm } from 'react-hook-form';
import { z } from 'zod';
import { Button } from '@/components/ui/button';
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { useTransition } from 'react';
import { useToast } from '@/hooks/use-toast';
import { useRouter } from 'next/navigation';
import { addEquipmentAction, updateEquipmentAction } from '@/lib/actions/maintenance-actions';
import { Loader2 } from 'lucide-react';
import type { Equipment } from '@/lib/types';


const equipmentSchema = z.object({
  matricule: z.string().min(1, "Le matricule est obligatoire."),
  designation: z.string().min(1, "La désignation est obligatoire."),
  marque: z.string().optional().transform(v => v === '' ? null : v),
  categorie: z.string().optional().transform(v => v === '' ? null : v),
  annee: z.string().optional().transform(v => v === '' ? null : v),
  qte_vidange: z.coerce.number().min(0).optional().transform(v => v || null),
  code_barre: z.string().optional().transform(v => v === '' ? null : v),
  pneumatique: z.string().optional().transform(v => v === '' ? null : v),
});

type EquipmentFormValues = z.infer<typeof equipmentSchema>;

type Props = {
    initialData?: Equipment;
}

export function EquipmentForm({ initialData }: Props) {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const router = useRouter();

  const isEditMode = !!initialData;

  const form = useForm<EquipmentFormValues>({
    resolver: zodResolver(equipmentSchema),
    defaultValues: initialData ? {
        ...initialData,
        marque: initialData.marque || '',
        categorie: initialData.categorie || '',
        annee: initialData.annee || '',
        qte_vidange: initialData.qte_vidange || 0,
        code_barre: initialData.code_barre || '',
        pneumatique: initialData.pneumatique || '',
    } : {
      matricule: '',
      designation: '',
      marque: '',
      categorie: '',
      annee: '',
      qte_vidange: 0,
      code_barre: '',
      pneumatique: '',
    },
  });

  function onSubmit(values: EquipmentFormValues) {
    startTransition(async () => {
      if (isEditMode) {
        const result = await updateEquipmentAction(initialData.id, values);
        if (result.success) {
          toast({
            title: 'Succès',
            description: result.message || "L'équipement a été mis à jour avec succès.",
            duration: result.matriculeChanged ? 8000 : 5000,
          });
          router.push('/equipment');
          router.refresh();
        } else {
          toast({
            variant: 'destructive',
            title: 'Erreur',
            description: result.message || 'Impossible de mettre à jour l\'équipement.',
          });
        }
      } else {
        const result = await addEquipmentAction(values);
        if (result.success) {
          toast({
            title: 'Succès',
            description: "L'équipement a été ajouté avec succès.",
          });
          router.push('/equipment');
          router.refresh();
        } else {
          toast({
            variant: 'destructive',
            title: 'Erreur',
            description: result.message || 'Impossible d\'ajouter l\'équipement.',
          });
        }
      }
    });
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>{isEditMode ? "Modifier l'équipement" : "Ajouter un nouvel équipement"}</CardTitle>
        <CardDescription>
          Remplissez les détails ci-dessous. Tous les champs sont importants pour la logique de l'application.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
            <div className="grid md:grid-cols-2 gap-8">
              <FormField control={form.control} name="matricule" render={({ field }) => (
                <FormItem>
                  <FormLabel>Matricule</FormLabel>
                  <FormControl><Input {...field} /></FormControl>
                  <FormMessage />
                </FormItem>
              )}/>
               <FormField control={form.control} name="designation" render={({ field }) => (
                <FormItem>
                  <FormLabel>Désignation</FormLabel>
                  <FormControl><Input {...field} /></FormControl>
                  <FormMessage />
                </FormItem>
              )}/>
               <FormField control={form.control} name="marque" render={({ field }) => (
                <FormItem>
                  <FormLabel>Marque</FormLabel>
                  <FormControl><Input {...field} /></FormControl>
                  <FormMessage />
                </FormItem>
              )}/>
               <FormField control={form.control} name="categorie" render={({ field }) => (
                <FormItem>
                  <FormLabel>Catégorie</FormLabel>
                  <FormControl><Input {...field} /></FormControl>
                  <FormMessage />
                </FormItem>
              )}/>
               <FormField control={form.control} name="annee" render={({ field }) => (
                <FormItem>
                  <FormLabel>Année de mise en service</FormLabel>
                  <FormControl><Input {...field} /></FormControl>
                  <FormMessage />
                </FormItem>
              )}/>
              <FormField control={form.control} name="qte_vidange" render={({ field }) => (
                <FormItem>
                    <FormLabel>Qté Vidange (L)</FormLabel>
                    <FormControl><Input type="number" {...field} /></FormControl>
                    <FormDescription>Quantité d'huile pour une vidange complète.</FormDescription>
                    <FormMessage />
                </FormItem>
              )}/>
              <FormField control={form.control} name="pneumatique" render={({ field }) => (
                <FormItem>
                    <FormLabel>Pneumatique</FormLabel>
                    <FormControl><Input {...field} /></FormControl>
                    <FormDescription>Type ou dimensions des pneus.</FormDescription>
                    <FormMessage />
                </FormItem>
              )}/>
              <FormField control={form.control} name="code_barre" render={({ field }) => (
                <FormItem>
                    <FormLabel>Code Barre</FormLabel>
                    <FormControl><Input {...field} /></FormControl>
                    <FormDescription>Code barre ou autre identifiant interne.</FormDescription>
                    <FormMessage />
                </FormItem>
              )}/>
            </div>
            <Button type="submit" disabled={isPending}>
              {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              {isEditMode ? 'Enregistrer les modifications' : 'Ajouter l\'équipement'}
            </Button>
          </form>
        </Form>
      </CardContent>
    </Card>
  );
}

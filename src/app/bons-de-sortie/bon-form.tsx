'use client';

import { zodResolver } from '@hookform/resolvers/zod';
import { useForm, useFieldArray } from 'react-hook-form';
import { z } from 'zod';
import { Button } from '@/components/ui/button';
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Loader2, PlusCircle, Trash2, Save, CalendarIcon } from 'lucide-react';
import { useTransition } from 'react';
import { useToast } from '@/hooks/use-toast';
import { useRouter } from 'next/navigation';
import { saveBonDeSortieAction, updateBonDeSortieAction } from '@/lib/actions/maintenance-actions';
import type { BonDeSortie } from '@/lib/types';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { cn } from '@/lib/utils';

dayjs.locale('fr');

const itemSchema = z.object({
  code: z.string().optional(),
  designation: z.string().min(1, "La désignation est requise."),
  unite: z.string().min(1, "L'unité est requise."),
  quantite: z.coerce.number().min(1, "La quantité doit être au moins 1."),
});

const formSchema = z.object({
  date: z.date({ required_error: "La date est requise." }),
  destinataire_chantier: z.string().min(1, "Le chantier destinataire est requis."),
  destinataire_code: z.string().optional(),
  transporteur_nom: z.string().optional(),
  transporteur_immatriculation: z.string().optional(),
  items: z.array(itemSchema).min(1, "Au moins un article est requis."),
});

type BonFormValues = z.infer<typeof formSchema>;

type Props = {
    initialData?: BonDeSortie;
}

export function BonForm({ initialData }: Props) {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const router = useRouter();
  const isEditMode = !!initialData;

  const form = useForm<BonFormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: isEditMode ? {
        ...initialData,
        date: dayjs(initialData.date, 'DD/MM/YYYY').toDate(),
    } : {
      date: new Date(),
      items: [{ code: '', designation: '', unite: 'L', quantite: 1 }],
    },
  });

  const { fields, append, remove } = useFieldArray({
    control: form.control,
    name: "items"
  });

  function onSubmit(values: BonFormValues) {
    startTransition(async () => {
      const action = isEditMode
        ? updateBonDeSortieAction(initialData.id, values)
        : saveBonDeSortieAction(values);

      const result = await action;
      
      if (result.success && result.bonId) {
        toast({
          title: 'Succès',
          description: `Le bon de sortie a été ${isEditMode ? 'mis à jour' : 'enregistré'}.`,
        });
        router.push(`/bons-de-sortie/view/${result.bonId}`);
      } else {
        toast({
          variant: 'destructive',
          title: 'Erreur',
          description: result.message || "Une erreur est survenue.",
        });
      }
    });
  }

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
        <Card>
          <CardHeader>
            <CardTitle>Informations Générales</CardTitle>
            <CardDescription>Renseignez les détails du bon de sortie.</CardDescription>
          </CardHeader>
          <CardContent className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
            <FormField
              control={form.control}
              name="date"
              render={({ field }) => (
                <FormItem className="flex flex-col">
                  <FormLabel>Date</FormLabel>
                   <Popover>
                    <PopoverTrigger asChild>
                      <FormControl>
                        <Button
                          variant={"outline"}
                          className={cn(
                            "w-full pl-3 text-left font-normal",
                            !field.value && "text-muted-foreground"
                          )}
                        >
                          {field.value ? (
                            dayjs(field.value).format('DD/MM/YYYY')
                          ) : (
                            <span>Choisir une date</span>
                          )}
                          <CalendarIcon className="ml-auto h-4 w-4 opacity-50" />
                        </Button>
                      </FormControl>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0" align="start">
                      <Calendar
                        mode="single"
                        selected={field.value}
                        onSelect={field.onChange}
                        initialFocus
                      />
                    </PopoverContent>
                  </Popover>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField control={form.control} name="destinataire_chantier" render={({ field }) => (
                <FormItem>
                    <FormLabel>Destinataire (Chantier)</FormLabel>
                    <FormControl><Input placeholder="Nom du chantier" {...field} /></FormControl>
                    <FormMessage />
                </FormItem>
            )}/>
             <FormField control={form.control} name="destinataire_code" render={({ field }) => (
                <FormItem>
                    <FormLabel>Code Chantier</FormLabel>
                    <FormControl><Input placeholder="Code" {...field} /></FormControl>
                    <FormMessage />
                </FormItem>
            )}/>
            <FormField control={form.control} name="transporteur_nom" render={({ field }) => (
                <FormItem>
                    <FormLabel>Nom et prénom Transporteur</FormLabel>
                    <FormControl><Input placeholder="Nom du transporteur" {...field} /></FormControl>
                    <FormMessage />
                </FormItem>
            )}/>
             <FormField control={form.control} name="transporteur_immatriculation" render={({ field }) => (
                <FormItem>
                    <FormLabel>Immatriculation</FormLabel>
                    <FormControl><Input placeholder="N° immatriculation" {...field} /></FormControl>
                    <FormMessage />
                </FormItem>
            )}/>
          </CardContent>
        </Card>

        <Card>
            <CardHeader>
                <CardTitle>Articles</CardTitle>
                <CardDescription>Liste des articles à sortir du stock.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
                {fields.map((field, index) => (
                    <div key={field.id} className="grid grid-cols-12 gap-x-4 gap-y-2 items-start border p-4 rounded-md relative">
                        <FormField control={form.control} name={`items.${index}.code`} render={({ field }) => (<FormItem className="col-span-12 sm:col-span-2"><FormLabel>Code</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>)} />
                        <FormField control={form.control} name={`items.${index}.designation`} render={({ field }) => (<FormItem className="col-span-12 sm:col-span-5"><FormLabel>Désignation</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>)} />
                        <FormField control={form.control} name={`items.${index}.unite`} render={({ field }) => (<FormItem className="col-span-6 sm:col-span-2"><FormLabel>Unité</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>)} />
                        <FormField control={form.control} name={`items.${index}.quantite`} render={({ field }) => (<FormItem className="col-span-6 sm:col-span-2"><FormLabel>Quantité</FormLabel><FormControl><Input type="number" {...field} /></FormControl><FormMessage /></FormItem>)} />
                        <div className="col-span-12 sm:col-span-1 flex items-end h-full">
                            <Button type="button" variant="destructive" size="icon" onClick={() => remove(index)}><Trash2 className="h-4 w-4" /></Button>
                        </div>
                    </div>
                ))}
                <Button type="button" variant="outline" size="sm" onClick={() => append({ code: '', designation: '', unite: 'L', quantite: 1 })}>
                    <PlusCircle className="mr-2 h-4 w-4" /> Ajouter un article
                </Button>
            </CardContent>
        </Card>
        
        <Button type="submit" size="lg" disabled={isPending}>
          {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          <Save className="mr-2 h-4 w-4" />
          {isEditMode ? 'Enregistrer les modifications' : 'Enregistrer le Bon'}
        </Button>
      </form>
    </Form>
  );
}

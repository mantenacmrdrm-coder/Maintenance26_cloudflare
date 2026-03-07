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
import { Textarea } from '@/components/ui/textarea';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Loader2, CalendarIcon } from 'lucide-react';
import { useTransition } from 'react';
import { useToast } from '@/hooks/use-toast';
import { useRouter } from 'next/navigation';
import { updateOperationAction } from '@/lib/actions/maintenance-actions';
import { Card, CardContent } from '@/components/ui/card';
import type { Operation } from '@/lib/types';
import dayjs from 'dayjs';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { cn } from '@/lib/utils';

const formSchema = z.object({
  matricule: z.string().min(1, { message: 'Le matricule est obligatoire.' }),
  dateEntree: z.string().min(1, { message: "La date d'entrée est obligatoire." }),
  panneDeclaree: z.string().min(1, { message: 'La déclaration de panne est obligatoire.' }),
  sitActuelle: z.enum(['En Cours', 'Réparée', 'Dépanné'], {
    required_error: 'Le statut actuel est obligatoire.',
  }),
  pieces: z.string().optional(),
  dateSortie: z.string().optional(),
  intervenant: z.string().optional(),
  affectation: z.string().optional(),
}).refine(data => {
    if (data.sitActuelle === 'Réparée' && (!data.dateSortie || data.dateSortie.trim() === '')) {
        return false;
    }
    return true;
}, {
    message: 'La date de sortie est obligatoire si le statut est "Réparée".',
    path: ['dateSortie'],
});

const parseDateToString = (dateString: string | null | undefined): string | undefined => {
    if (!dateString || dateString.toLowerCase().includes('cour')) return undefined;
    const date = dayjs(dateString.trim(), 'DD/MM/YYYY');
    return date.isValid() ? date.format('DD/MM/YYYY') : undefined;
}

export function EditOperationForm({ operation }: { operation: Operation }) {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const router = useRouter();

  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      matricule: operation.matricule,
      dateEntree: parseDateToString(operation.date_entree),
      panneDeclaree: operation.panne_declaree,
      sitActuelle: operation.sitactuelle as any,
      pieces: operation.pieces || '',
      dateSortie: parseDateToString(operation.date_sortie),
      intervenant: operation.intervenant || '',
      affectation: operation.affectation || '',
    },
  });
  
  const sitActuelleValue = form.watch('sitActuelle');

  function onSubmit(values: z.infer<typeof formSchema>) {
    startTransition(async () => {
      const result = await updateOperationAction(operation.id, values);
      if (result.success) {
        toast({
          title: 'Succès',
          description: 'L\'opération curative a été mise à jour.',
        });
        router.push('/operations');
        router.refresh();
      } else {
        toast({
          variant: 'destructive',
          title: 'Erreur',
          description: result.message || "Une erreur est survenue lors de la mise à jour.",
        });
      }
    });
  }

  return (
    <Card>
      <CardContent className="pt-6">
        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
            <div className="grid md:grid-cols-2 gap-8">
              {/* Indispensable Fields */}
              <div className="space-y-4">
                <FormField
                  control={form.control}
                  name="matricule"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Matricule</FormLabel>
                      <FormControl>
                        <Input placeholder="ex: 041-01468-40" {...field} />
                      </FormControl>
                      <FormDescription>L'identifiant unique de l'équipement.</FormDescription>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name="dateEntree"
                  render={({ field }) => (
                    <FormItem className="flex flex-col">
                      <FormLabel>Date d'entrée en atelier</FormLabel>
                        <Popover>
                            <PopoverTrigger asChild>
                                <FormControl>
                                    <Button variant={"outline"} className={cn("w-full pl-3 text-left font-normal", !field.value && "text-muted-foreground")}>
                                        {field.value ? field.value : <span>Choisir une date</span>}
                                        <CalendarIcon className="ml-auto h-4 w-4 opacity-50" />
                                    </Button>
                                </FormControl>
                            </PopoverTrigger>
                            <PopoverContent className="w-auto p-0" align="start">
                                <Calendar 
                                  mode="single" 
                                  selected={field.value ? dayjs(field.value, "DD/MM/YYYY").toDate() : undefined} 
                                  onSelect={(date) => field.onChange(date ? dayjs(date).format('DD/MM/YYYY') : "")} 
                                  initialFocus 
                                />
                            </PopoverContent>
                        </Popover>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name="panneDeclaree"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Panne déclarée</FormLabel>
                      <FormControl>
                        <Textarea placeholder="Description de la panne observée..." {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                 <FormField
                  control={form.control}
                  name="sitActuelle"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Statut actuel de l'intervention</FormLabel>
                      <Select onValueChange={field.onChange} defaultValue={field.value}>
                        <FormControl>
                          <SelectTrigger>
                            <SelectValue placeholder="Sélectionner un statut" />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          <SelectItem value="En Cours">En Cours</SelectItem>
                          <SelectItem value="Réparée">Réparée</SelectItem>
                          <SelectItem value="Dépanné">Dépanné (temporaire)</SelectItem>
                        </SelectContent>
                      </Select>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </div>

              {/* Optional Fields */}
              <div className="space-y-4">
                <FormField
                  control={form.control}
                  name="dateSortie"
                  render={({ field }) => (
                    <FormItem className="flex flex-col">
                      <FormLabel>Date de sortie de l'atelier</FormLabel>
                        <Popover>
                            <PopoverTrigger asChild>
                                <FormControl>
                                    <Button variant={"outline"} className={cn("w-full pl-3 text-left font-normal", !field.value && "text-muted-foreground")} disabled={sitActuelleValue !== 'Réparée'}>
                                        {field.value ? field.value : <span>Choisir une date</span>}
                                        <CalendarIcon className="ml-auto h-4 w-4 opacity-50" />
                                    </Button>
                                </FormControl>
                            </PopoverTrigger>
                            <PopoverContent className="w-auto p-0" align="start">
                                <Calendar 
                                  mode="single" 
                                  selected={field.value ? dayjs(field.value, "DD/MM/YYYY").toDate() : undefined} 
                                  onSelect={(date) => field.onChange(date ? dayjs(date).format('DD/MM/YYYY') : '')} 
                                  initialFocus 
                                />
                            </PopoverContent>
                        </Popover>
                      <FormDescription>
                        {sitActuelleValue === 'Réparée' ? 'Obligatoire si le statut est "Réparée".' : 'Sera "En Cours" si le statut est "En Cours" ou "Dépanné"'}
                      </FormDescription>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name="pieces"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Pièces remplacées</FormLabel>
                      <FormControl>
                        <Textarea placeholder="Lister les pièces, séparées par des tirets (-)" {...field} />
                      </FormControl>
                      <FormDescription>Ex: filtre a huile - bougie - pneu avant</FormDescription>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name="intervenant"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Intervenant</FormLabel>
                      <FormControl>
                        <Input placeholder="Nom du mécanicien ou de l'équipe" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name="affectation"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Affectation</FormLabel>
                      <FormControl>
                        <Input placeholder="Lieu/chantier où se trouvait l'équipement" {...field} />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </div>
            </div>
            <Button type="submit" disabled={isPending}>
              {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Enregistrer les modifications
            </Button>
          </form>
        </Form>
      </CardContent>
    </Card>
  );
}

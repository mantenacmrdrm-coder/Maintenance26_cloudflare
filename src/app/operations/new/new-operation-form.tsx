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
import { Loader2, CalendarIcon, BrainCircuit } from 'lucide-react';
import { useTransition, useState } from 'react';
import { useToast } from '@/hooks/use-toast';
import { useRouter } from 'next/navigation';
import { addCurativeOperation } from '@/lib/actions/maintenance-actions';
import { getCurativeDiagnosisAction } from '@/lib/actions/ai-actions';
import { Card, CardContent } from '@/components/ui/card';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { cn } from '@/lib/utils';
import dayjs from 'dayjs';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';

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

export function NewOperationForm() {
  const [isPending, startTransition] = useTransition();
  const [isDiagnosing, startDiagnosis] = useTransition();
  const [diagnosis, setDiagnosis] = useState<any | null>(null);
  const { toast } = useToast();
  const router = useRouter();

  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      matricule: '',
      dateEntree: dayjs().format('DD/MM/YYYY'),
      panneDeclaree: '',
      pieces: '',
      intervenant: '',
      affectation: '',
    },
  });
  
  const sitActuelleValue = form.watch('sitActuelle');
  const panneDeclareeValue = form.watch('panneDeclaree');
  const matriculeValue = form.watch('matricule');

  const handleAiDiagnosis = () => {
      if (!matriculeValue || !panneDeclareeValue) {
          toast({
              variant: 'destructive',
              title: 'Informations manquantes',
              description: 'Veuillez saisir le matricule et la description de la panne avant de lancer le diagnostic.'
          });
          return;
      }
      setDiagnosis(null);
      startDiagnosis(async () => {
          const result = await getCurativeDiagnosisAction(matriculeValue, panneDeclareeValue);
          if (result.success) {
              setDiagnosis(result.data);
              toast({ title: 'Diagnostic IA terminé.' });
          } else {
              toast({ variant: 'destructive', title: 'Erreur de diagnostic', description: result.message });
          }
      });
  };

  function onSubmit(values: z.infer<typeof formSchema>) {
    startTransition(async () => {
      const result = await addCurativeOperation(values);
      if (result.success) {
        toast({
          title: 'Succès',
          description: 'La nouvelle opération curative a été enregistrée.',
        });
        router.push('/operations');
        router.refresh();
      } else {
        toast({
          variant: 'destructive',
          title: 'Erreur',
          description: result.message || "Une erreur est survenue lors de l'enregistrement.",
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
                                  selected={field.value ? dayjs(field.value, 'DD/MM/YYYY').toDate() : undefined} 
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
                 <Button type="button" variant="outline" onClick={handleAiDiagnosis} disabled={isDiagnosing || !panneDeclareeValue || !matriculeValue}>
                    {isDiagnosing ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <BrainCircuit className="mr-2 h-4 w-4" />}
                    Analyser avec l'IA
                </Button>
                {isDiagnosing && <p className="text-sm text-muted-foreground animate-pulse">Analyse en cours, veuillez patienter...</p>}
                
                {diagnosis && (
                    <Alert>
                        <BrainCircuit className="h-4 w-4" />
                        <AlertTitle>Diagnostic de l'Assistant IA</AlertTitle>
                        <AlertDescription className="space-y-3">
                            <p><strong>Type de Panne Suggéré:</strong> <Badge>{diagnosis.typeDePanne}</Badge></p>
                            <div>
                                <strong className="block">Causes Possibles:</strong>
                                <ul className="list-disc list-inside ml-4">
                                    {diagnosis.causesPossibles.map((cause: string, i: number) => <li key={i}>{cause}</li>)}
                                </ul>
                            </div>
                            <div>
                                <strong className="block">Pièces à Vérifier:</strong>
                                <ul className="list-disc list-inside ml-4">
                                    {diagnosis.piecesSuggerees.map((piece: string, i: number) => <li key={i}>{piece}</li>)}
                                </ul>
                            </div>
                             <Button
                                type="button"
                                size="sm"
                                variant="secondary"
                                className='mt-2'
                                onClick={() => form.setValue('pieces', diagnosis.piecesSuggerees.join(' - '))}
                            >
                                Utiliser ces pièces
                            </Button>
                        </AlertDescription>
                    </Alert>
                )}
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
                                  selected={field.value ? dayjs(field.value, 'DD/MM/YYYY').toDate() : undefined} 
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
              Enregistrer l'opération
            </Button>
          </form>
        </Form>
      </CardContent>
    </Card>
  );
}

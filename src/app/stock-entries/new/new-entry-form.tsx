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
import { addStockEntryAction } from '@/lib/actions/maintenance-actions';
import { Loader2, CalendarIcon } from 'lucide-react';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { cn } from '@/lib/utils';

dayjs.locale('fr');

const LUBRICANT_TYPES = [
    't32', '15w40_4400', '10w', '15w40', '90', '15w40_v', 
    'hvol', 'tvol', 't30', 'graisse', 't46', '15w40_quartz'
] as const;

const formSchema = z.object({
  date: z.date({ required_error: 'La date est obligatoire.' }),
  lubricant_type: z.enum(LUBRICANT_TYPES, {
    required_error: 'Le type de lubrifiant est obligatoire.',
  }),
  quantity: z.coerce.number().gt(0, "La quantité doit être supérieure à 0."),
  reference: z.string().optional(),
});

type FormValues = z.infer<typeof formSchema>;

export function NewEntryForm() {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const router = useRouter();

  const form = useForm<FormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      date: new Date(),
      quantity: 0,
      reference: '',
    },
  });

  function onSubmit(values: FormValues) {
    startTransition(async () => {
      const result = await addStockEntryAction(values);
      if (result.success) {
        toast({
          title: 'Succès',
          description: "L'entrée de stock a été enregistrée.",
        });
        router.push('/stock-entries');
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
      <CardHeader>
        <CardTitle>Détails de l'Entrée</CardTitle>
        <CardDescription>
          Remplissez les informations ci-dessous.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
            <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
              <FormField
                control={form.control}
                name="date"
                render={({ field }) => (
                  <FormItem className="flex flex-col">
                    <FormLabel>Date de l'entrée</FormLabel>
                    <Popover>
                        <PopoverTrigger asChild>
                            <FormControl>
                                <Button variant={"outline"} className={cn("w-full pl-3 text-left font-normal", !field.value && "text-muted-foreground")}>
                                    {field.value ? dayjs(field.value).format('DD/MM/YYYY') : <span>Choisir une date</span>}
                                    <CalendarIcon className="ml-auto h-4 w-4 opacity-50" />
                                </Button>
                            </FormControl>
                        </PopoverTrigger>
                        <PopoverContent className="w-auto p-0" align="start">
                            <Calendar mode="single" selected={field.value} onSelect={field.onChange} initialFocus />
                        </PopoverContent>
                    </Popover>
                    <FormMessage />
                  </FormItem>
                )}
              />
               <FormField
                control={form.control}
                name="lubricant_type"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Type de Lubrifiant</FormLabel>
                    <Select onValueChange={field.onChange} defaultValue={field.value}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Sélectionner un type" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {LUBRICANT_TYPES.map(type => (
                            <SelectItem key={type} value={type}>{type.replace(/_/g, ' ').toUpperCase()}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
               <FormField
                control={form.control}
                name="quantity"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Quantité (en litres/unités)</FormLabel>
                    <FormControl>
                      <Input type="number" step="0.1" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
               <FormField
                control={form.control}
                name="reference"
                render={({ field }) => (
                  <FormItem className="md:col-span-2 lg:col-span-3">
                    <FormLabel>Référence (Optionnel)</FormLabel>
                    <FormControl>
                      <Input placeholder="Ex: Bon de livraison n°12345" {...field} />
                    </FormControl>
                    <FormDescription>Un numéro de bon de livraison, de facture, ou une autre référence.</FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>
            <Button type="submit" disabled={isPending}>
              {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Enregistrer l'Entrée
            </Button>
          </form>
        </Form>
      </CardContent>
    </Card>
  );
}

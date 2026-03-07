'use client';

import React, { useEffect } from 'react';
import { zodResolver } from '@hookform/resolvers/zod';
import { useForm, useFieldArray } from 'react-hook-form';
import { z } from 'zod';
import { Button } from '@/components/ui/button';
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { cn } from '@/lib/utils';
import { CalendarIcon, PlusCircle, Trash2, Loader2, Save } from 'lucide-react';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { useToast } from '@/hooks/use-toast';
import { updateConsumptionAction } from '@/lib/actions/maintenance-actions';
import { useRouter } from 'next/navigation';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { useTransition } from 'react';

dayjs.locale('fr');

type EquipmentData = {
    matricule: string;
    designation: string;
    qte_vidange: number | null;
}

const lubricantTypes = [
    't32', '15w40_4400', '10w', '15w40', '90', '15w40_v', 
    'hvol', 'tvol', 't30', 'graisse', 't46', '15w40_quartz'
] as const;

const lubricantSchema = z.object({
    type: z.enum(lubricantTypes),
    quantity: z.coerce.number().min(0, "La quantité doit être >= 0"),
});

const formSchema = z.object({
    date: z.coerce.date({ required_error: 'La date est obligatoire.' }),
    matricule: z.string().min(1, "Le matricule est requis."),
    designation: z.string().optional(),
    qte_vidange: z.coerce.number().nullable().optional(),
    lubricants: z.array(lubricantSchema).min(1, "Au moins un lubrifiant est requis.").or(z.array(lubricantSchema).length(0)),
    obs: z.string().optional(),
    entretien: z.string().optional(),
});

type FormValues = z.infer<typeof formSchema>;

const calculateEntretien = (lubricants: {type: string; quantity: number}[], qteVidange: number | null | undefined): string => {
    const getQty = (type: string): number => lubricants.find(c => c.type === type)?.quantity || 0;

    const engineOils = ['15w40', '15w40_v', '15w40_quartz', '15w40_4400'];
    const hydraulicOils = ['10w', 't32', 'hvol', 't46'];
    const transmissionOils = ['90', 'tvol', 't30'];
    const grease = ['graisse'];

    const totalEngineOil = engineOils.reduce((sum, type) => sum + getQty(type), 0);
    const totalHydraulicOil = hydraulicOils.reduce((sum, type) => sum + getQty(type), 0);
    const totalTransmissionOil = transmissionOils.reduce((sum, type) => sum + getQty(type), 0);
    const totalGrease = grease.reduce((sum, type) => sum + getQty(type), 0);

    const hasAnyConsumption = totalEngineOil > 0 || totalHydraulicOil > 0 || totalTransmissionOil > 0 || totalGrease > 0;

    if (!hasAnyConsumption) return "";

    if (qteVidange && qteVidange > 0 && totalEngineOil >= qteVidange) return "VIDANGE,M";
    if (totalGrease > 0) return "GR";
    if (totalHydraulicOil > 0) return "HYDRAULIQUE";
    if (totalTransmissionOil > 0) return "TRANSMISSION";
    if (totalEngineOil > 0) return "NIVEAU HUILE";

    return "";
};

export function EditConsumptionForm({ consumption, equipments }: { consumption: any, equipments: EquipmentData[] }) {
    const [isPending, startTransition] = useTransition();
    const router = useRouter();
    const { toast } = useToast();

    const transformToFormData = (data: any) => {
        const lubricants = lubricantTypes.map(type => ({
            type: type,
            quantity: parseFloat(data[type]?.toString().replace(',', '.')) || 0
        })).filter(l => l.quantity > 0);

        return {
            date: dayjs(data.date, 'DD/MM/YYYY').toDate(),
            matricule: data.matricule,
            designation: data.designation,
            qte_vidange: data.v,
            obs: data.obs || '',
            entretien: data.entretien || '',
            lubricants: lubricants.length > 0 ? lubricants : [],
        };
    };

    const form = useForm<FormValues>({
        resolver: zodResolver(formSchema),
        defaultValues: transformToFormData(consumption),
    });

    const { fields, append, remove } = useFieldArray({
        control: form.control,
        name: "lubricants",
    });

    const watchedLubricants = form.watch("lubricants");
    const watchedQteVidange = form.watch("qte_vidange");

    useEffect(() => {
        const newEntretien = calculateEntretien(watchedLubricants, watchedQteVidange);
        if (form.getValues('entretien') !== newEntretien) {
            form.setValue('entretien', newEntretien);
        }
    }, [watchedLubricants, watchedQteVidange, form]);

    const handleMatriculeChange = (value: string) => {
        form.setValue(`matricule`, value);
        const equipment = equipments.find(e => e.matricule === value);
        if (equipment) {
            form.setValue(`designation`, equipment.designation);
            form.setValue(`qte_vidange`, equipment.qte_vidange);
        } else {
            form.setValue(`designation`, 'Matricule non trouvé');
            form.setValue(`qte_vidange`, null);
        }
    };

    function onSubmit(values: FormValues) {
        startTransition(async () => {
            const result = await updateConsumptionAction(consumption.id, values);
            if (result.success) {
                toast({ title: "Succès", description: "Consommation mise à jour avec succès." });
                router.push('/consommations');
                router.refresh();
            } else {
                toast({ variant: "destructive", title: "Erreur", description: result.message || "La mise à jour a échoué." });
            }
        });
    }

    const selectedLubricants = form.watch('lubricants', []).map((l: any) => l.type);
    const availableLubricants = lubricantTypes.filter(type => !selectedLubricants.includes(type));
    const matriculeValue = form.watch(`matricule`);
    const designation = form.watch(`designation`);
    const qteVidange = form.watch(`qte_vidange`);
    const entretien = form.watch(`entretien`);

    return (
        <Card>
            <CardContent className="pt-6">
                <Form {...form}>
                    <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6">
                        <div className="grid lg:grid-cols-3 gap-6 items-start">
                            <div className="lg:col-span-1 space-y-4">
                                <FormField control={form.control} name="date" render={({ field }) => (
                                    <FormItem className="flex flex-col">
                                        <FormLabel>Date de consommation</FormLabel>
                                        <Popover>
                                            <PopoverTrigger asChild><FormControl><Button variant="outline" className={cn("pl-3 text-left font-normal", !field.value && "text-muted-foreground")}>{field.value ? dayjs(field.value).format('D MMMM YYYY') : <span>Choisir une date</span>}<CalendarIcon className="ml-auto h-4 w-4 opacity-50" /></Button></FormControl></PopoverTrigger>
                                            <PopoverContent className="w-auto p-0" align="start"><Calendar mode="single" selected={field.value} onSelect={field.onChange} fromYear={2015} toYear={new Date().getFullYear() + 2} initialFocus /></PopoverContent>
                                        </Popover>
                                        <FormMessage />
                                    </FormItem>
                                )} />
                                <FormField control={form.control} name="matricule" render={({ field }) => (
                                    <FormItem>
                                        <FormLabel>Matricule</FormLabel>
                                        <FormControl>
                                            <div>
                                                <Input {...field} list={`equipments-list`} onChange={(e) => handleMatriculeChange(e.target.value)} placeholder="Saisir un matricule..." />
                                                <datalist id={`equipments-list`}>{equipments.map((eq: EquipmentData) => <option key={eq.matricule} value={eq.matricule} />)}</datalist>
                                            </div>
                                        </FormControl>
                                        <FormMessage />
                                    </FormItem>
                                )} />
                                {matriculeValue && (<div className="text-sm space-y-1 rounded-md bg-muted p-3"><p><strong>Désignation:</strong> {designation || '...'}</p><p><strong>Qte Vidange:</strong> {qteVidange ? `${qteVidange} L` : 'N/A'}</p></div>)}
                                {entretien && (<div className="text-sm space-y-1"><p className="font-medium">Entretien Calculé:</p><Badge>{entretien}</Badge></div>)}
                            </div>

                            <div className="lg:col-span-2 space-y-4">
                                <div className="space-y-2">
                                    <FormLabel>Lubrifiants Consommés</FormLabel>
                                    <FormField control={form.control} name="lubricants" render={() => (<FormMessage className="text-xs" />)} />
                                    {fields.map((lubField, lubIndex) => (
                                        <div key={lubField.id} className="flex items-end gap-2">
                                            <FormField control={form.control} name={`lubricants.${lubIndex}.type`} render={({ field }) => (
                                                <FormItem className="flex-1">
                                                    <Select onValueChange={field.onChange} defaultValue={field.value}>
                                                        <FormControl><SelectTrigger><SelectValue placeholder="Type" /></SelectTrigger></FormControl>
                                                        <SelectContent>
                                                            {field.value && <SelectItem value={field.value}>{field.value.replace(/_/g, ' ').toUpperCase()}</SelectItem>}
                                                            {availableLubricants.map(t => <SelectItem key={t} value={t}>{t.replace(/_/g, ' ').toUpperCase()}</SelectItem>)}
                                                        </SelectContent>
                                                    </Select>
                                                </FormItem>
                                            )} />
                                            <FormField control={form.control} name={`lubricants.${lubIndex}.quantity`} render={({ field }) => (
                                                <FormItem><FormControl><Input type="number" step="0.1" placeholder="Qté" {...field} className="w-24" /></FormControl><FormMessage className="text-xs" /></FormItem>
                                            )} />
                                            <Button type="button" variant="ghost" size="icon" onClick={() => remove(lubIndex)}><Trash2 className="h-4 w-4 text-destructive" /></Button>
                                        </div>
                                    ))}
                                    <Button type="button" size="sm" variant="outline" onClick={() => append({ type: availableLubricants[0] || '15w40', quantity: 0 })}>
                                        <PlusCircle className="mr-2 h-4 w-4" /> Ajouter Lubrifiant
                                    </Button>
                                </div>
                                <FormField control={form.control} name="obs" render={({ field }) => (
                                    <FormItem>
                                        <FormLabel>Observations</FormLabel>
                                        <FormControl><Textarea placeholder="Ex: FH + FAIR COMPTEUR 12345" {...field} /></FormControl>
                                        <FormMessage />
                                    </FormItem>
                                )} />
                            </div>
                        </div>
                        <Button type="submit" disabled={isPending} className="mt-6">
                            {isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Save className="mr-2 h-4 w-4" />}
                            Enregistrer les modifications
                        </Button>
                    </form>
                </Form>
            </CardContent>
        </Card>
    );
}

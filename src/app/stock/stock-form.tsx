
'use client';

import React, { useState, useTransition, useEffect, useCallback } from 'react';
import { zodResolver } from '@hookform/resolvers/zod';
import { useForm, useFieldArray, Controller } from 'react-hook-form';
import { z } from 'zod';
import { Button } from '@/components/ui/button';
import { Form, FormControl, FormDescription, FormField, FormItem, FormLabel, FormMessage } from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { PlusCircle, Trash2, Loader2, Save, CalendarIcon } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { addConsumptionsAction } from '@/lib/actions/maintenance-actions';
import { Separator } from '@/components/ui/separator';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent } from '@/components/ui/card';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { cn } from '@/lib/utils';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';

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
    quantity: z.coerce.number().min(0, "La quantité ne peut pas être négative."),
});

const consumptionSchema = z.object({
    matricule: z.string().min(1, "Le matricule est requis."),
    designation: z.string().optional(),
    qte_vidange: z.coerce.number().nullable().optional(),
    lubricants: z.array(lubricantSchema).transform(val => val.filter(l => l.quantity > 0)),
    obs: z.string().optional(),
    entretien: z.string().optional(),
}).refine(data => data.lubricants.length > 0 || (!!data.obs && data.obs.trim() !== ''), {
    message: "Au moins un lubrifiant (quantité > 0) ou une observation est requis(e).",
    path: ["lubricants"],
});

const formSchema = z.object({
    date: z.coerce.date({ required_error: 'La date est obligatoire.' }),
    entries: z.array(consumptionSchema),
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

export function StockForm({ equipments }: { equipments: EquipmentData[] }) {
    const [isPending, startTransition] = useTransition();
    const { toast } = useToast();
    const form = useForm<FormValues>({
        resolver: zodResolver(formSchema),
        defaultValues: { date: new Date(), entries: [] },
    });

    const { fields, append, remove } = useFieldArray({ control: form.control, name: "entries" });
    const watchedEntries = form.watch("entries");
    
    useEffect(() => {
        watchedEntries.forEach((entry, index) => {
            const newEntretien = calculateEntretien(entry.lubricants, entry.qte_vidange);
            if (entry.entretien !== newEntretien) {
                form.setValue(`entries.${index}.entretien`, newEntretien);
            }
        });
    }, [watchedEntries, form]);

    const handleMatriculeChange = (value: string, index: number) => {
        form.setValue(`entries.${index}.matricule`, value);
        const equipment = equipments.find(e => e.matricule === value);
        if (equipment) {
            form.setValue(`entries.${index}.designation`, equipment.designation);
            form.setValue(`entries.${index}.qte_vidange`, equipment.qte_vidange);
        } else {
            form.setValue(`entries.${index}.designation`, 'Matricule non trouvé');
            form.setValue(`entries.${index}.qte_vidange`, null);
        }
    };
    
    function onSubmit(values: FormValues) {
        const payload = {
            ...values,
            entries: values.entries.map(e => ({
                ...e,
                lubricants: lubricantTypes.reduce((acc, l_type) => {
                    const consumed = e.lubricants.find(l => l.type === l_type);
                    acc[l_type] = consumed ? consumed.quantity : 0;
                    return acc;
                }, {} as Record<string, number>)
            }))
        };
        
        startTransition(async () => {
            const result = await addConsumptionsAction(payload);
            if(result.success) {
                toast({ title: "Succès", description: result.message });
                form.reset({ date: values.date, entries: [] });
            } else {
                toast({ variant: "destructive", title: "Erreur", description: result.message });
            }
        });
    }

    return (
        <Form {...form}>
            <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6">
                <FormField
                    control={form.control}
                    name="date"
                    render={({ field }) => (
                        <FormItem className="flex flex-col">
                            <FormLabel>Date de consommation</FormLabel>
                            <Popover>
                                <PopoverTrigger asChild>
                                    <FormControl>
                                        <Button variant={"outline"} className={cn("w-[240px] pl-3 text-left font-normal", !field.value && "text-muted-foreground")}>
                                            {field.value ? dayjs(field.value).format('DD/MM/YYYY') : <span>Choisir une date</span>}
                                            <CalendarIcon className="ml-auto h-4 w-4 opacity-50" />
                                        </Button>
                                    </FormControl>
                                </PopoverTrigger>
                                <PopoverContent className="w-auto p-0" align="start">
                                    <Calendar mode="single" selected={field.value} onSelect={field.onChange} />
                                </PopoverContent>
                            </Popover>
                            <FormMessage />
                        </FormItem>
                    )}
                />
                
                <Separator />

                <div className="space-y-4">
                    {fields.map((field, index) => (
                        <ConsumptionRow key={field.id} index={index} form={form} equipments={equipments} remove={remove} handleMatriculeChange={handleMatriculeChange} />
                    ))}
                </div>

                <div className="flex items-center gap-4">
                    <Button type="button" variant="outline" onClick={() => append({ matricule: '', lubricants: [], obs: '' })}>
                        <PlusCircle className="mr-2 h-4 w-4" /> Ajouter une consommation
                    </Button>
                     <Button type="submit" disabled={isPending || fields.length === 0}>
                        {isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Save className="mr-2 h-4 w-4" />}
                        Valider et Enregistrer
                    </Button>
                </div>
            </form>
        </Form>
    );
}

function ConsumptionRow({ index, form, equipments, remove, handleMatriculeChange }: any) {
    const { fields, append, remove: removeLubricant } = useFieldArray({
        control: form.control,
        name: `entries.${index}.lubricants`
    });
    
    const matriculeValue = form.watch(`entries.${index}.matricule`);
    const designation = form.watch(`entries.${index}.designation`);
    const qteVidange = form.watch(`entries.${index}.qte_vidange`);
    const entretien = form.watch(`entries.${index}.entretien`);
    const selectedLubricants = form.watch(`entries.${index}.lubricants`, []).map((l:any) => l.type);

    const availableLubricants = lubricantTypes.filter(type => !selectedLubricants.includes(type));

    return (
        <Card>
            <CardContent className="pt-6">
                <div className="grid lg:grid-cols-3 gap-6 items-start">
                    <div className="lg:col-span-1 space-y-4">
                        <FormField
                            control={form.control}
                            name={`entries.${index}.matricule`}
                            render={({ field }) => (
                                <FormItem>
                                    <FormLabel>Matricule</FormLabel>
                                    <FormControl>
                                        <div>
                                            <Input {...field} list={`equipments-list-${index}`} onChange={(e) => handleMatriculeChange(e.target.value, index)} placeholder="Saisir un matricule..."/>
                                            <datalist id={`equipments-list-${index}`}>
                                                {equipments.map((eq: EquipmentData) => <option key={eq.matricule} value={eq.matricule} />)}
                                            </datalist>
                                        </div>
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />
                         {matriculeValue && (
                            <div className="text-sm space-y-1 rounded-md bg-muted p-3">
                                <p><strong>Désignation:</strong> {designation || '...'}</p>
                                <p><strong>Qte Vidange:</strong> {qteVidange ? `${qteVidange} L` : 'N/A'}</p>
                            </div>
                         )}
                         {entretien && (
                            <div className="text-sm space-y-1">
                                <p className="font-medium">Entretien Calculé:</p>
                                <Badge>{entretien}</Badge>
                            </div>
                         )}
                    </div>
                    
                    <div className="lg:col-span-2 space-y-4">
                        <div className="grid sm:grid-cols-2 gap-4">
                            <div className="space-y-2">
                                <FormLabel>Lubrifiants Consommés</FormLabel>
                                <FormField
                                    control={form.control}
                                    name={`entries.${index}.lubricants`}
                                    render={() => (
                                        <FormMessage className="text-xs" />
                                    )}
                                />
                                {fields.map((lubField, lubIndex) => (
                                    <div key={lubField.id} className="flex items-end gap-2">
                                        <FormField
                                            control={form.control}
                                            name={`entries.${index}.lubricants.${lubIndex}.type`}
                                            render={({ field }) => (
                                                <FormItem className="flex-1">
                                                    <Select onValueChange={field.onChange} defaultValue={field.value}>
                                                        <FormControl><SelectTrigger><SelectValue placeholder="Type" /></SelectTrigger></FormControl>
                                                        <SelectContent>
                                                            {/* Current value should always be in the list */}
                                                            {field.value && <SelectItem value={field.value}>{field.value.replace(/_/g, ' ').toUpperCase()}</SelectItem>}
                                                            {availableLubricants.map(t => <SelectItem key={t} value={t}>{t.replace(/_/g, ' ').toUpperCase()}</SelectItem>)}
                                                        </SelectContent>
                                                    </Select>
                                                </FormItem>
                                            )}
                                        />
                                        <FormField
                                            control={form.control}
                                            name={`entries.${index}.lubricants.${lubIndex}.quantity`}
                                            render={({ field }) => (
                                                <FormItem>
                                                    <FormControl><Input type="number" step="0.1" placeholder="Qté" {...field} className="w-24"/></FormControl>
                                                    <FormMessage className="text-xs" />
                                                </FormItem>
                                            )}
                                        />
                                        <Button type="button" variant="ghost" size="icon" onClick={() => removeLubricant(lubIndex)}><Trash2 className="h-4 w-4 text-destructive" /></Button>
                                    </div>
                                ))}
                                <Button type="button" size="sm" variant="outline" onClick={() => append({ type: availableLubricants[0] || '15w40', quantity: 0 })}>
                                    <PlusCircle className="mr-2 h-4 w-4" /> Ajouter Lubrifiant
                                </Button>
                            </div>
                            <FormField
                                control={form.control}
                                name={`entries.${index}.obs`}
                                render={({ field }) => (
                                    <FormItem>
                                        <FormLabel>Observations</FormLabel>
                                        <FormControl><Textarea placeholder="Ex: FH + FAIR COMPTEUR 12345" {...field} /></FormControl>
                                        <FormMessage />
                                    </FormItem>
                                )}
                            />
                        </div>
                    </div>
                </div>
                 <Button type="button" variant="destructive" size="sm" onClick={() => remove(index)} className="mt-4">
                    Supprimer cette ligne
                </Button>
            </CardContent>
        </Card>
    );
}

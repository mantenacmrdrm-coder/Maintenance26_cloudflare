'use client';

import { zodResolver } from '@hookform/resolvers/zod';
import { useForm, useFieldArray } from 'react-hook-form';
import { z } from 'zod';
import { Button } from '@/components/ui/button';
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Loader2, PlusCircle, Trash2 } from 'lucide-react';
import { useTransition } from 'react';
import { useToast } from '@/hooks/use-toast';
import { useRouter } from 'next/navigation';
import { updateDeclarationAction } from '@/lib/actions/maintenance-actions';
import type { DeclarationPanne } from '@/lib/types';
import { Separator } from '@/components/ui/separator';

const pieceSchema = z.object({
  designation: z.string().min(1, "La désignation est requise."),
  reference: z.string().optional(),
  quantite: z.coerce.number().min(1, "La quantité doit être au moins 1."),
  montant: z.coerce.number().min(0, "Le montant ne peut être négatif."),
});

const intervenantSchema = z.object({
    type: z.string().min(1, "Le type est requis."),
    description: z.string().min(1, "La description est requise."),
});

const formSchema = z.object({
  chauffeur_conducteur: z.string().optional(),
  diagnostique_intervenant: z.string().optional(),
  causes: z.string().optional(),
  pieces: z.array(pieceSchema),
  intervenants: z.array(intervenantSchema),
  montant_main_oeuvre: z.coerce.number().min(0).optional(),
  obs_reserves: z.string().optional(),
});

type DeclarationFormValues = z.infer<typeof formSchema>;


export function EditDeclarationForm({ declaration }: { declaration: DeclarationPanne }) {
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();
  const router = useRouter();

  const form = useForm<DeclarationFormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      chauffeur_conducteur: declaration.chauffeur_conducteur || '',
      diagnostique_intervenant: declaration.diagnostique_intervenant || '',
      causes: declaration.causes || '',
      pieces: declaration.pieces || [],
      intervenants: declaration.intervenants || [],
      montant_main_oeuvre: declaration.montant_main_oeuvre || 0,
      obs_reserves: declaration.obs_reserves || '',
    },
  });

  const { fields: pieceFields, append: appendPiece, remove: removePiece } = useFieldArray({ control: form.control, name: "pieces" });
  const { fields: intervenantFields, append: appendIntervenant, remove: removeIntervenant } = useFieldArray({ control: form.control, name: "intervenants" });

  const pieces = form.watch('pieces');
  const montantMainOeuvre = form.watch('montant_main_oeuvre') || 0;
  const totalPieces = pieces.reduce((acc, piece) => acc + (piece.montant || 0), 0);
  const totalGlobal = totalPieces + montantMainOeuvre;

  function onSubmit(values: DeclarationFormValues) {
    startTransition(async () => {
      const result = await updateDeclarationAction(declaration.id, values);
      if (result.success) {
        toast({
          title: 'Succès',
          description: 'La déclaration de panne a été mise à jour.',
        });
        router.push(`/declarations/view/${result.declarationId}`);
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
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
        <div className="grid lg:grid-cols-3 gap-8">
            {/* Left Column */}
            <div className="lg:col-span-2 space-y-8">
                <Card>
                    <CardHeader>
                        <CardTitle>Informations Générales</CardTitle>
                        <CardDescription>Détails sur la panne et le matériel.</CardDescription>
                    </CardHeader>
                    <CardContent className="grid md:grid-cols-2 gap-6">
                        <p><strong>Date de panne:</strong> {declaration.operation.date_entree}</p>
                        <p><strong>Type de matériel:</strong> {declaration.equipment.marque} {declaration.equipment.designation}</p>
                        <p><strong>Immatriculation:</strong> <span className='font-mono'>{declaration.operation.matricule}</span></p>
                        <p><strong>Affectation:</strong> {declaration.operation.affectation || 'N/A'}</p>
                        <div className="md:col-span-2">
                           <p><strong>Déclaration de l'utilisateur:</strong></p>
                           <p className="text-muted-foreground p-2 bg-muted rounded-md">{declaration.operation.panne_declaree}</p>
                        </div>
                        <FormField control={form.control} name="chauffeur_conducteur" render={({ field }) => (
                            <FormItem>
                                <FormLabel>Chauffeur / Conducteur</FormLabel>
                                <FormControl><Input placeholder="Nom du conducteur" {...field} /></FormControl>
                                <FormMessage />
                            </FormItem>
                        )}/>
                        <FormField control={form.control} name="causes" render={({ field }) => (
                            <FormItem>
                                <FormLabel>Causes de la panne</FormLabel>
                                <FormControl><Input placeholder="Ex: Usure normale" {...field} /></FormControl>
                                <FormMessage />
                            </FormItem>
                        )}/>
                        <div className="md:col-span-2">
                            <FormField control={form.control} name="diagnostique_intervenant" render={({ field }) => (
                                <FormItem>
                                    <FormLabel>Diagnostique de l'intervenant</FormLabel>
                                    <FormControl><Textarea placeholder="Diagnostic technique..." {...field} /></FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}/>
                        </div>
                    </CardContent>
                </Card>

                 <Card>
                    <CardHeader>
                        <CardTitle>Pièces Endommagées à Changer</CardTitle>
                        <CardDescription>Liste des pièces, quantités et coûts.</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="space-y-4">
                            {pieceFields.map((field, index) => (
                            <div key={field.id} className="grid grid-cols-12 gap-2 items-start border p-3 rounded-md">
                                <FormField control={form.control} name={`pieces.${index}.designation`} render={({ field }) => (
                                    <FormItem className="col-span-12 md:col-span-5"><FormLabel className="text-xs">Désignation</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>
                                )}/>
                                <FormField control={form.control} name={`pieces.${index}.reference`} render={({ field }) => (
                                    <FormItem className="col-span-4 md:col-span-2"><FormLabel className="text-xs">Référence</FormLabel><FormControl><Input {...field} /></FormControl><FormMessage /></FormItem>
                                )}/>
                                 <FormField control={form.control} name={`pieces.${index}.quantite`} render={({ field }) => (
                                    <FormItem className="col-span-2 md:col-span-1"><FormLabel className="text-xs">Qté</FormLabel><FormControl><Input type="number" {...field} /></FormControl><FormMessage /></FormItem>
                                )}/>
                                 <FormField control={form.control} name={`pieces.${index}.montant`} render={({ field }) => (
                                    <FormItem className="col-span-4 md:col-span-3"><FormLabel className="text-xs">Montant</FormLabel><FormControl><Input type="number" {...field} /></FormControl><FormMessage /></FormItem>
                                )}/>
                                <div className="col-span-2 md:col-span-1 flex items-end h-full">
                                    <Button type="button" variant="destructive" size="icon" onClick={() => removePiece(index)}><Trash2 className="h-4 w-4" /></Button>
                                </div>
                            </div>
                            ))}
                        </div>
                        <Button type="button" variant="outline" size="sm" onClick={() => appendPiece({ designation: '', reference: '', quantite: 1, montant: 0 })}>
                            <PlusCircle className="mr-2 h-4 w-4" /> Ajouter une pièce
                        </Button>
                    </CardContent>
                </Card>
            </div>

            {/* Right Column */}
            <div className="lg:col-span-1 space-y-8">
                 <Card>
                    <CardHeader><CardTitle>Intervenants</CardTitle></CardHeader>
                    <CardContent className="space-y-4">
                        {intervenantFields.map((field, index) => (
                           <div key={field.id} className="grid grid-cols-12 gap-2 items-end border p-3 rounded-md">
                                <FormField control={form.control} name={`intervenants.${index}.type`} render={({ field }) => (
                                    <FormItem className="col-span-5"><FormLabel className="text-xs">Type</FormLabel><FormControl><Input placeholder="Externe" {...field} /></FormControl><FormMessage /></FormItem>
                                )}/>
                                <FormField control={form.control} name={`intervenants.${index}.description`} render={({ field }) => (
                                    <FormItem className="col-span-6"><FormLabel className="text-xs">Description</FormLabel><FormControl><Input placeholder="Nom, commerce..." {...field} /></FormControl><FormMessage /></FormItem>
                                )}/>
                                <Button type="button" variant="ghost" size="icon" onClick={() => removeIntervenant(index)}><Trash2 className="h-4 w-4" /></Button>
                           </div>
                        ))}
                         <Button type="button" variant="outline" size="sm" onClick={() => appendIntervenant({ type: '', description: '' })}>
                            <PlusCircle className="mr-2 h-4 w-4" /> Ajouter un intervenant
                        </Button>
                    </CardContent>
                 </Card>

                 <Card>
                    <CardHeader><CardTitle>Détails & Coûts</CardTitle></CardHeader>
                     <CardContent className="space-y-4">
                        <FormField control={form.control} name="montant_main_oeuvre" render={({ field }) => (
                            <FormItem>
                                <FormLabel>Montant de la main d'œuvre (2)</FormLabel>
                                <FormControl><Input type="number" {...field} /></FormControl>
                                <FormMessage />
                            </FormItem>
                        )}/>
                         <FormField control={form.control} name="obs_reserves" render={({ field }) => (
                            <FormItem>
                                <FormLabel>Obs / Réserves</FormLabel>
                                <FormControl><Textarea {...field} /></FormControl>
                                <FormMessage />
                            </FormItem>
                        )}/>
                        <Separator />
                        <div className="space-y-2 text-sm">
                            <div className="flex justify-between">
                                <span>Montant total des pièces (1):</span>
                                <span className="font-bold">{totalPieces.toLocaleString('fr-FR', {style: 'currency', currency: 'DZD'})}</span>
                            </div>
                             <div className="flex justify-between">
                                <span>Montant Global (1) + (2):</span>
                                <span className="font-bold text-lg">{totalGlobal.toLocaleString('fr-FR', {style: 'currency', currency: 'DZD'})}</span>
                            </div>
                        </div>
                     </CardContent>
                 </Card>
            </div>
        </div>

        <Button type="submit" disabled={isPending} size="lg">
          {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          Enregistrer les modifications
        </Button>
      </form>
    </Form>
  );
}

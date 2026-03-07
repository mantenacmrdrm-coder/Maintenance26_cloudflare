'use client';
import { useState, useTransition } from 'react';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
  DropdownMenuLabel
} from '@/components/ui/dropdown-menu';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { useToast } from '@/hooks/use-toast';
import { updateCategoryEntretiens } from '@/lib/actions/maintenance-actions';
import { OFFICIAL_ENTRETIENS } from '@/lib/constants';
import { ChevronDown, Loader2 } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';

type Props = {
    categories: string[];
    initialData: Record<string, Record<string, boolean>>;
};

export function CategoryParametersTable({ categories, initialData }: Props) {
    const [data, setData] = useState(initialData);
    const [isPending, startTransition] = useTransition();
    const { toast } = useToast();

    const handleUpdate = (category: string, entretien: string, isActive: boolean) => {
        startTransition(async () => {
            try {
                // Optimistic update
                setData(prevData => ({
                    ...prevData,
                    [category]: {
                        ...prevData[category],
                        [entretien]: isActive,
                    },
                }));

                await updateCategoryEntretiens(category, entretien, isActive);
                
                toast({
                    title: 'Succès',
                    description: `Paramètre mis à jour pour ${category}. Le planning doit être regénéré.`,
                });
            } catch (error: any) {
                toast({
                    variant: 'destructive',
                    title: 'Erreur',
                    description: `Impossible de mettre à jour le paramètre : ${error.message}`,
                });
                // Revert on error
                setData(initialData);
            }
        });
    };

    return (
        <Card>
            <CardHeader>
                <div className="flex justify-between items-start">
                    <div>
                        <CardTitle>Paramètres par Catégorie d'Équipement</CardTitle>
                        <CardDescription>
                            Gérez les entretiens à inclure pour chaque catégorie via le menu déroulant.
                        </CardDescription>
                    </div>
                    {isPending && <Loader2 className="h-5 w-5 animate-spin" />}
                </div>
            </CardHeader>
            <CardContent>
                <div className="rounded-md border">
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead className="font-bold">Catégorie</TableHead>
                                <TableHead className="font-bold">Entretiens Actifs</TableHead>
                                <TableHead className="text-right font-bold">Actions</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {categories.map(category => {
                                const activeCount = Object.values(data[category] ?? {}).filter(Boolean).length;
                                return (
                                    <TableRow key={category}>
                                        <TableCell className="font-medium">{category}</TableCell>
                                        <TableCell>
                                            <span className="font-mono text-sm">
                                                {activeCount} / {OFFICIAL_ENTRETIENS.length}
                                            </span>
                                        </TableCell>
                                        <TableCell className="text-right">
                                            <DropdownMenu>
                                                <DropdownMenuTrigger asChild>
                                                    <Button variant="outline" size="sm" disabled={isPending}>
                                                        Gérer les entretiens
                                                        <ChevronDown className="ml-2 h-4 w-4" />
                                                    </Button>
                                                </DropdownMenuTrigger>
                                                <DropdownMenuContent className="w-64 max-h-96 overflow-y-auto">
                                                    <DropdownMenuLabel>Entretiens pour {category}</DropdownMenuLabel>
                                                    <DropdownMenuSeparator />
                                                    {OFFICIAL_ENTRETIENS.map(entretien => (
                                                        <DropdownMenuCheckboxItem
                                                            key={entretien}
                                                            checked={data[category]?.[entretien] ?? false}
                                                            onCheckedChange={(checked) => handleUpdate(category, entretien, !!checked)}
                                                            disabled={isPending}
                                                        >
                                                            {entretien}
                                                        </DropdownMenuCheckboxItem>
                                                    ))}
                                                </DropdownMenuContent>
                                            </DropdownMenu>
                                        </TableCell>
                                    </TableRow>
                                );
                            })}
                        </TableBody>
                    </Table>
                </div>
            </CardContent>
        </Card>
    );
}

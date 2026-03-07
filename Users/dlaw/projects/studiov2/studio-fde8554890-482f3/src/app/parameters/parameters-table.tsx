
'use client';
import { useState, useTransition, useMemo } from 'react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { updateParam } from '@/lib/actions/maintenance-actions';
import { useToast } from '@/hooks/use-toast';
import { Loader2 } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';

type Param = {
    id: number;
    [key: string]: any;
};

type Props = {
    data: Param[];
    headers: string[]; // This prop is kept for potential future use but isn't critical for opCol anymore
};

export function ParametersTable({ data, headers }: Props) {
    const [tableData, setTableData] = useState(data);
    const [filter, setFilter] = useState('');
    const [isPending, startTransition] = useTransition();
    const { toast } = useToast();

    // The operation column is now standardized to 'operation' in the database.
    const opCol = 'operation';
    const intervalCols = useMemo(() => ['interval_7', 'interval_30', 'interval_90', 'interval_180', 'interval_360'], []);

    const handleUpdate = (id: number, column: string, value: string | null) => {
        startTransition(async () => {
            try {
                // Optimistic update
                const newTableData = tableData.map(row =>
                    row.id === id ? { ...row, [column]: value } : row
                );
                setTableData(newTableData);

                await updateParam(id, column, value);
                
                toast({
                    title: 'Succès',
                    description: 'Paramètre mis à jour. Le planning doit être regénéré.',
                });
            } catch (error: any) {
                toast({
                    variant: 'destructive',
                    title: 'Erreur',
                    description: `Impossible de mettre à jour le paramètre : ${error.message}`,
                });
                 // Revert optimistic update on error
                 setTableData(data);
            }
        });
    };
    
    const filteredData = useMemo(() => {
        if (!filter) return tableData;
        const lowercasedFilter = filter.toLowerCase();
        // Ensure row[opCol] exists before calling toString()
        return tableData.filter(row => 
            row[opCol]?.toString().toLowerCase().includes(lowercasedFilter)
        );
    }, [tableData, filter, opCol]);


    return (
        <Card>
            <CardHeader>
                <div className="flex items-center justify-between">
                    <div>
                        <CardTitle>Paramètres Généraux des Intervalles</CardTitle>
                        <CardDescription>Définissez le niveau de maintenance pour chaque opération et intervalle.</CardDescription>
                    </div>
                     {isPending && <Loader2 className="h-5 w-5 animate-spin" />}
                </div>
                 <Input
                    placeholder="Filtrer par opération..."
                    value={filter}
                    onChange={e => setFilter(e.target.value)}
                    className="max-w-sm mt-4"
                />
            </CardHeader>
            <CardContent>
                <div className="rounded-md border">
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead className="font-bold">Opération</TableHead>
                                {intervalCols.map(header => (
                                    <TableHead key={header} className="text-center font-bold">{header.split('_')[1]} jours</TableHead>
                                ))}
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {filteredData.map(row => (
                                <TableRow key={row.id}>
                                    <TableCell className="font-medium whitespace-nowrap">{row[opCol]}</TableCell>
                                    {intervalCols.map(col => (
                                        <TableCell key={col}>
                                            <Select
                                                value={row[col] ?? 'none'}
                                                onValueChange={(value) => handleUpdate(row.id, col, value === 'none' ? null : value)}
                                                disabled={isPending}
                                            >
                                                <SelectTrigger className="w-28 mx-auto">
                                                    <SelectValue placeholder="-" />
                                                </SelectTrigger>
                                                <SelectContent>
                                                    <SelectItem value="none">-</SelectItem>
                                                    <SelectItem value="C">C - Contrôle</SelectItem>
                                                    <SelectItem value="N">N - Nettoyage</SelectItem>
                                                    <SelectItem value="CH">CH - Changement</SelectItem>
                                                </SelectContent>
                                            </Select>
                                        </TableCell>
                                    ))}
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </div>
            </CardContent>
        </Card>
    );
}

'use client';
import { useState, useTransition, useMemo } from 'react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Checkbox } from '@/components/ui/checkbox';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { updateParam } from '@/lib/actions/maintenance-actions';
import { useToast } from '@/hooks/use-toast';
import { Loader2 } from 'lucide-react';
import { Input } from '@/components/ui/input';

type Param = {
    id: number;
    [key: string]: any;
};

type Props = {
    data: Param[];
    headers: string[];
};

export function ParametersTable({ data, headers }: Props) {
    const [tableData, setTableData] = useState(data);
    const [filter, setFilter] = useState('');
    const [isPending, startTransition] = useTransition();
    const { toast } = useToast();

    const { opCol, intervalCols, levelCols, visibleHeaders } = useMemo(() => {
        const intervalCols = ['7', '30', '90', '180', '360'];
        
        const levelMapping: { keyword: string; level: 'C' | 'N' | 'CH' }[] = [
            { keyword: 'contrôler', level: 'C' },
            { keyword: 'nettoyage', level: 'N' },
            { keyword: 'changement', level: 'CH' },
        ];

        const levelCols: { name: string; level: 'C' | 'N' | 'CH' }[] = [];
        const levelColNames: string[] = [];

        for (const mapping of levelMapping) {
            const foundHeader = headers.find(h => h.toLowerCase().includes(mapping.keyword));
            if (foundHeader) {
                levelCols.push({ name: foundHeader, level: mapping.level });
                levelColNames.push(foundHeader);
            }
        }

        const knownCols = new Set(['id', ...intervalCols, ...levelColNames]);
        const opCol = headers.find(h => !knownCols.has(h)) || 'id';

        const visibleHeaders = [opCol, ...intervalCols, ...levelColNames];
        
        return { opCol, intervalCols, levelCols, visibleHeaders };
    }, [headers]);


    const handleUpdate = (id: number, column: string, value: string | null) => {
        startTransition(async () => {
            try {
                await updateParam(id, column, value);
                // Update local state for immediate feedback
                setTableData(prevData =>
                    prevData.map(row =>
                        row.id === id ? { ...row, [column]: value } : row
                    )
                );
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
        return tableData.filter(row => 
            row[opCol]?.toString().toLowerCase().includes(lowercasedFilter)
        );
    }, [tableData, filter, opCol]);


    return (
        <div className="space-y-4">
             <div className="flex items-center justify-between">
                <Input
                    placeholder="Filtrer par opération..."
                    value={filter}
                    onChange={e => setFilter(e.target.value)}
                    className="max-w-sm"
                />
                {isPending && <Loader2 className="h-5 w-5 animate-spin" />}
            </div>
            <div className="rounded-md border">
                <Table>
                    <TableHeader>
                        <TableRow>
                            {visibleHeaders.map(header => (
                                <TableHead key={header}>{header}</TableHead>
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
                                            <SelectTrigger className="w-20">
                                                <SelectValue placeholder="-" />
                                            </SelectTrigger>
                                            <SelectContent>
                                                <SelectItem value="none">-</SelectItem>
                                                <SelectItem value="*">*</SelectItem>
                                                <SelectItem value="**">**</SelectItem>
                                            </SelectContent>
                                        </Select>
                                    </TableCell>
                                ))}
                                {levelCols.map(colInfo => (
                                    <TableCell key={colInfo.name} className="text-center">
                                         <Checkbox
                                            checked={row[colInfo.name] === colInfo.level}
                                            onCheckedChange={(checked) => handleUpdate(row.id, colInfo.name, checked ? colInfo.level : null)}
                                            disabled={isPending}
                                            aria-label={`Niveau ${colInfo.level}`}
                                        />
                                    </TableCell>
                                ))}
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </div>
        </div>
    );
}

'use client';

import React, { useState, useMemo } from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Download, Pencil } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { Badge } from '@/components/ui/badge';
import Link from 'next/link';
import { DeleteConsumptionButton } from './delete-consumption-button';

// This must match the order from data.ts
const CONSOLIDE_COLUMNS_ORDER = [
    'v', 'n', 'date', 'designation', 'matricule', 't32', '15w40_4400', 
    '10w', '15w40', '90', '15w40_v', 'hvol', 'tvol', 't30', 'graisse', 
    't46', '15w40_quartz', 'obs', 'entretien'
] as const;

const CONSUMPTION_COLUMNS = [
    'v', 't32', '15w40_4400', '10w', '15w40', '90', '15w40_v', 'hvol', 
    'tvol', 't30', 'graisse', 't46', '15w40_quartz'
];


export function ConsommationsDataTable({ data }: { data: any[] }) {
  const [filter, setFilter] = useState('');
  const { toast } = useToast();

  const tableHeaders = ['id', ...CONSOLIDE_COLUMNS_ORDER, 'Actions'];

  const filteredData = useMemo(() => {
    if (!filter) return data;
    const lowercasedFilter = filter.toLowerCase();
    return data.filter(item =>
      Object.values(item).some(val =>
        String(val).toLowerCase().includes(lowercasedFilter)
      )
    );
  }, [data, filter]);

  const handleExport = () => {
    if (filteredData.length === 0) {
      toast({
        variant: 'destructive',
        title: 'Aucune donnée',
        description: "Il n'y a aucune consommation à exporter.",
      });
      return;
    }

    const exportHeaders = ['id', ...CONSOLIDE_COLUMNS_ORDER];

    const csvContent = [
      exportHeaders.join(';'),
      ...filteredData.map(item =>
        exportHeaders.map(header => `"${item[header.toLowerCase()] || ''}"`).join(';')
      )
    ].join('\n');

    const blob = new Blob([`\uFEFF${csvContent}`], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `consommations_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    toast({
      title: 'Exportation réussie',
      description: `${filteredData.length} lignes ont été exportées.`,
    });
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <Input
          placeholder="Filtrer les consommations..."
          value={filter}
          onChange={e => setFilter(e.target.value)}
          className="max-w-sm"
        />
        <Button onClick={handleExport} variant="outline">
          <Download className="mr-2 h-4 w-4" />
          Exporter la vue
        </Button>
      </div>
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              {tableHeaders.map(header => (
                <TableHead key={header} className={`whitespace-nowrap ${CONSUMPTION_COLUMNS.includes(header.toLowerCase()) ? 'text-center' : ''}`}>{header.toUpperCase()}</TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredData.length > 0 ? (
              filteredData.map(item => (
                <TableRow key={item.id}>
                  {tableHeaders.map(header => {
                    if (header === 'Actions') {
                      return (
                        <TableCell key="actions">
                          <div className="flex justify-end gap-2">
                              <Button asChild variant="ghost" size="icon">
                                <Link href={`/consommations/edit/${item.id}`}>
                                  <Pencil className="h-4 w-4" />
                                </Link>
                              </Button>
                              <DeleteConsumptionButton consumptionId={item.id} />
                            </div>
                        </TableCell>
                      )
                    }
                    const key = header.toLowerCase();
                    const value = item[key];

                    const isConsumptionCol = CONSUMPTION_COLUMNS.includes(key);
                    const isPositive = isConsumptionCol && value && parseFloat(String(value).replace(',', '.')) > 0;
                    
                    return (
                      <TableCell key={key} className={`whitespace-nowrap ${isPositive ? 'bg-amber-50 font-bold' : ''} ${isConsumptionCol ? 'text-center' : ''}`}>
                        {key === 'matricule' || key === 'entretien' ? (
                          <Badge variant={key === 'entretien' ? 'default' : 'secondary'}>{value}</Badge>
                        ) : (
                          value
                        )}
                      </TableCell>
                    )
                  })}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={tableHeaders.length} className="h-24 text-center">
                  Aucun résultat.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

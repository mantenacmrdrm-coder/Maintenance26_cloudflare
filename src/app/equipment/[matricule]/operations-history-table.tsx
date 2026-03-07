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
import { Badge } from '@/components/ui/badge';
import type { Operation } from '@/lib/types';
import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat';

dayjs.extend(customParseFormat);

const formatDate = (dateString: string | null | undefined) => {
  if (!dateString) return '-';
  try {
    const date = dayjs(dateString, 'DD/MM/YYYY');
    if (!date.isValid()) {
        return dateString;
    }
    return date.format('DD/MM/YYYY');
  } catch (e) {
    return dateString;
  }
};

const statusVariant: { [key: string]: 'default' | 'secondary' | 'destructive' | 'outline' } = {
    'Réalisé': 'default',
    'Programmé': 'secondary',
    'En retard': 'destructive',
    'Hors planning': 'outline',
    'En Panne': 'destructive',
};

type OperationsHistoryTableProps = {
  operations: Operation[];
};

export function OperationsHistoryTable({ operations }: OperationsHistoryTableProps) {
  const [filter, setFilter] = useState('');

  const filteredData = useMemo(() => {
    if (!filter) return operations;
    const lowercasedFilter = filter.toLowerCase();
    
    return operations.filter(op => {
      return (
        op.operation.toLowerCase().includes(lowercasedFilter) ||
        (op.date_programmee && formatDate(op.date_programmee).toLowerCase().includes(lowercasedFilter)) ||
        (op.date_realisation && formatDate(op.date_realisation).toLowerCase().includes(lowercasedFilter)) ||
        op.nature.toLowerCase().includes(lowercasedFilter)
      );
    });
  }, [operations, filter]);

  return (
    <div className="space-y-4">
        <Input
          placeholder="Filtrer l'historique..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="max-w-sm"
        />
        <div className="rounded-md border">
            <Table>
                <TableHeader>
                <TableRow>
                    <TableHead>Opération</TableHead>
                    <TableHead>Date Programmée</TableHead>
                    <TableHead>Date Réalisation</TableHead>
                    <TableHead>Nature</TableHead>
                </TableRow>
                </TableHeader>
                <TableBody>
                {filteredData.length > 0 ? (
                    filteredData.map(op => (
                    <TableRow key={op.id}>
                        <TableCell className="font-medium">{op.operation}</TableCell>
                        <TableCell>{formatDate(op.date_programmee)}</TableCell>
                        <TableCell>{formatDate(op.date_realisation)}</TableCell>
                        <TableCell><Badge variant={statusVariant[op.nature] || 'outline'}>{op.nature}</Badge></TableCell>
                    </TableRow>
                    ))
                ) : (
                    <TableRow>
                    <TableCell colSpan={4} className="h-24 text-center">Aucune opération ne correspond à votre filtre.</TableCell>
                    </TableRow>
                )}
                </TableBody>
            </Table>
        </div>
    </div>
  );
}

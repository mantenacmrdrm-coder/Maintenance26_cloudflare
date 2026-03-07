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
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { Badge } from '@/components/ui/badge';
import type { StockEntry } from '@/lib/types';
import { DeleteEntryButton } from './delete-entry-button';

dayjs.locale('fr');

export function EntriesListTable({ entries }: { entries: StockEntry[] }) {
  const [filter, setFilter] = useState('');

  const filteredEntries = useMemo(() => {
    if (!filter) return entries;
    const lowercasedFilter = filter.toLowerCase();

    return entries.filter(entry => 
      (entry.lubricant_type && entry.lubricant_type.toLowerCase().includes(lowercasedFilter)) ||
      (entry.reference && entry.reference.toLowerCase().includes(lowercasedFilter)) ||
      (entry.date && dayjs(entry.date).format('DD/MM/YYYY').includes(lowercasedFilter))
    );
  }, [entries, filter]);

  return (
    <div className="space-y-4">
      <Input
        placeholder="Filtrer les entrées..."
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        className="max-w-sm"
      />
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Date</TableHead>
              <TableHead>Type de Lubrifiant</TableHead>
              <TableHead>Référence</TableHead>
              <TableHead className="text-right">Quantité (L)</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredEntries.length > 0 ? (
              filteredEntries.map((entry) => (
                <TableRow key={entry.id}>
                  <TableCell>
                    {dayjs(entry.date).format('DD/MM/YYYY')}
                  </TableCell>
                  <TableCell>
                      <Badge variant="secondary">{entry.lubricant_type.replace(/_/g, ' ').toUpperCase()}</Badge>
                  </TableCell>
                  <TableCell className="font-medium">
                    {entry.reference}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {entry.quantity.toLocaleString('fr-FR')}
                  </TableCell>
                  <TableCell className="text-right">
                    <DeleteEntryButton entryId={entry.id} />
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={5} className="h-24 text-center">
                  {entries.length === 0 ? "Aucune entrée de stock pour le moment." : "Aucune entrée ne correspond à votre filtre."}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

'use client';
import React, { useState, useMemo } from 'react';
import Link from 'next/link';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { FileText } from 'lucide-react';
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


export function OperationsForDeclarationTable({ data }: { data: Operation[] }) {
  const [filter, setFilter] = useState('');

  const filteredData = useMemo(() => {
    if (!filter) return data;
    const lowercasedFilter = filter.toLowerCase();
    
    return data.filter(op => 
        Object.values(op).some(val => 
            String(val).toLowerCase().includes(lowercasedFilter)
        )
    );
  }, [data, filter]);

  return (
    <div className="space-y-4">
      <Input
        placeholder="Filtrer les opérations..."
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        className="max-w-sm"
      />
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Date d'Entrée</TableHead>
              <TableHead>Matricule</TableHead>
              <TableHead>Opération (Panne Déclarée)</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredData.length > 0 ? (
              filteredData.map(op => (
                <TableRow key={op.id}>
                  <TableCell>{formatDate(op.date_entree)}</TableCell>
                  <TableCell><Badge>{op.matricule}</Badge></TableCell>
                  <TableCell className="font-medium">{op.operation}</TableCell>
                  <TableCell className="text-right">
                    <Button asChild>
                      <Link href={`/declarations/generate/${op.id}`}>
                        <FileText className="mr-2 h-4 w-4" />
                        Générer
                      </Link>
                    </Button>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={4} className="h-24 text-center">
                  Aucune opération ne correspond à votre filtre.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

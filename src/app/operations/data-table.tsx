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
import { Button } from '@/components/ui/button';
import { Download, Pencil } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import Link from 'next/link';
import { DeleteOperationButton } from './delete-operation-button';

dayjs.extend(customParseFormat);

export function OperationsDataTable({ data }: { data: Operation[] }) {
  const [filter, setFilter] = useState('');
  const { toast } = useToast();

  const filteredData = useMemo(() => {
    return data.filter(item =>
      Object.values(item).some(val =>
        String(val).toLowerCase().includes(filter.toLowerCase())
      )
    );
  }, [data, filter]);

  const statusVariant: { [key: string]: 'default' | 'secondary' | 'destructive' | 'outline' } = {
    'Réalisé': 'default',
    'Programmé': 'secondary',
    'En retard': 'destructive',
    'Hors planning': 'outline',
    'En Panne': 'destructive',
    'mécanique': 'secondary',
    'électrique': 'destructive',
    'autres': 'outline',
    'non spécifié': 'destructive',
  };
  
  const formatDate = (dateString: string | null | undefined) => {
    if (!dateString || dateString.toLowerCase().includes('cour')) return 'En Cours';
    try {
      const date = dayjs(dateString.trim(), 'DD/MM/YYYY');
      if (date.isValid()) {
          return date.format('DD/MM/YYYY');
      }
      return dateString; // Return original string if parsing fails
    } catch (e) {
      return dateString; // Return original on error
    }
  };

  const handleExport = () => {
    if (filteredData.length === 0) {
      toast({
        variant: 'destructive',
        title: 'Aucune donnée',
        description: "Il n'y a aucune opération à exporter.",
      });
      return;
    }

    const headers = [
        'MATRICULE', 
        'DATE ENTREE', 
        'PANNE DECLAREE', 
        'SIT.ACTUELLE', 
        'PIECES', 
        'DATE SORTIE', 
        'INTERVENANT', 
        'AFFECTATION', 
        'TYPE DE PANNE'
    ];

    const csvContent = [
      headers.join(';'),
      ...filteredData.map(op => [
        `"${op.matricule || ''}"`,
        `"${formatDate(op.date_entree)}"`,
        `"${(op.panne_declaree || '').replace(/"/g, '""')}"`,
        `"${op.sitactuelle || ''}"`,
        `"${(op.pieces || '').replace(/"/g, '""')}"`,
        `"${formatDate(op.date_sortie)}"`,
        `"${op.intervenant || ''}"`,
        `"${op.affectation || ''}"`,
        `"${op.type_de_panne || ''}"`,
      ].join(';'))
    ].join('\n');

    const blob = new Blob([`\uFEFF${csvContent}`], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `operations_maintenance_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    toast({
      title: 'Exportation réussie',
      description: `${filteredData.length} opérations (filtrées) ont été exportées.`,
    });
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <Input
          placeholder="Filtrer les opérations..."
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
              <TableHead className="whitespace-nowrap">Matricule</TableHead>
              <TableHead>Opération</TableHead>
              <TableHead>Pièces</TableHead>
              <TableHead>Date Programmée</TableHead>
              <TableHead>Date Réalisation</TableHead>
              <TableHead>Nature</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredData.length > 0 ? (
              filteredData.map(op => (
                <TableRow key={op.id}>
                   <TableCell className="whitespace-nowrap">
                    <Badge>{op.matricule}</Badge>
                  </TableCell>
                  <TableCell className="font-medium">{op.operation}</TableCell>
                  <TableCell>{op.pieces}</TableCell>
                  <TableCell>{formatDate(op.date_programmee)}</TableCell>
                  <TableCell>
                    {op.date_realisation === 'En Cours' ? (
                        <span className="inline-flex items-center rounded-full border-transparent bg-yellow-200 text-red-700 px-2.5 py-0.5 text-xs font-semibold">
                            En Cours
                        </span>
                    ) : (
                        formatDate(op.date_realisation)
                    )}
                  </TableCell>
                  <TableCell>
                    <Badge variant={statusVariant[op.nature] || 'outline'}>{op.nature}</Badge>
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-2">
                      <Button asChild variant="ghost" size="icon">
                        <Link href={`/operations/edit/${op.id}`}>
                          <Pencil className="h-4 w-4" />
                        </Link>
                      </Button>
                      <DeleteOperationButton operationId={op.id} />
                    </div>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={7} className="h-24 text-center">
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

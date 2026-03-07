'use client';

import React, { useState, useMemo } from 'react';
import Link from 'next/link';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { Eye, Pencil } from 'lucide-react';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { DeleteBonButton } from './delete-bon-button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';

dayjs.locale('fr');

type BonListItem = {
    id: number;
    generated_at: string;
    date: string;
    destinataire_chantier: string;
}

export function BonsListTable({ bons }: { bons: BonListItem[] }) {
  const [filter, setFilter] = useState('');

  const filteredBons = useMemo(() => {
    if (!filter) return bons;
    const lowercasedFilter = filter.toLowerCase();

    return bons.filter(bon => 
      (bon.destinataire_chantier && bon.destinataire_chantier.toLowerCase().includes(lowercasedFilter))
    );
  }, [bons, filter]);

  return (
    <div className="space-y-4">
      <Input
        placeholder="Filtrer par destinataire..."
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        className="max-w-sm"
      />
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>N° Bon</TableHead>
              <TableHead>Destinataire (Chantier)</TableHead>
              <TableHead>Date du Bon</TableHead>
              <TableHead>Date de Génération</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredBons.length > 0 ? (
              filteredBons.map((bon) => (
                <TableRow key={bon.id}>
                  <TableCell>
                      <Badge variant="secondary">{bon.id}</Badge>
                  </TableCell>
                  <TableCell className="font-medium">
                    {bon.destinataire_chantier}
                  </TableCell>
                   <TableCell>
                    {dayjs(bon.date).format('DD/MM/YYYY')}
                  </TableCell>
                  <TableCell>
                    {dayjs(bon.generated_at).format('DD/MM/YYYY HH:mm')}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-2">
                      <Button asChild variant="ghost" size="icon">
                        <Link href={`/bons-de-sortie/view/${bon.id}`}>
                          <Eye className="h-4 w-4" />
                        </Link>
                      </Button>
                      <Button asChild variant="ghost" size="icon">
                        <Link href={`/bons-de-sortie/edit/${bon.id}`}>
                          <Pencil className="h-4 w-4" />
                        </Link>
                      </Button>
                      <DeleteBonButton bonId={bon.id} />
                    </div>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={5} className="h-24 text-center">
                  {bons.length === 0 ? "Aucun bon de sortie généré pour le moment." : "Aucun bon ne correspond à votre filtre."}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

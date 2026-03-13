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
import { Button } from '@/components/ui/button';
import { Eye, Pencil } from 'lucide-react';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { DeleteDeclarationButton } from './delete-declaration-button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import type { DeclarationListItem } from '@/lib/types';

dayjs.locale('fr');

export function DeclarationsListTable({ declarations }: { declarations: DeclarationListItem[] }) {
  const [filter, setFilter] = useState('');

  const filteredDeclarations = useMemo(() => {
    if (!filter) return declarations;
    const lowercasedFilter = filter.toLowerCase();

    return declarations.filter(declaration => 
      (declaration.matricule && declaration.matricule.toLowerCase().includes(lowercasedFilter)) ||
      (declaration.designation && declaration.designation.toLowerCase().includes(lowercasedFilter)) ||
      (declaration.panne_declaree && declaration.panne_declaree.toLowerCase().includes(lowercasedFilter))
    );
  }, [declarations, filter]);

  return (
    <div className="space-y-4">
      <Input
        placeholder="Filtrer les déclarations..."
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        className="max-w-sm"
      />
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Matricule</TableHead>
              <TableHead>Désignation</TableHead>
              <TableHead>Panne Déclarée</TableHead>
              <TableHead>Date de Génération</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredDeclarations.length > 0 ? (
              filteredDeclarations.map((declaration) => (
                <TableRow key={declaration.id}>
                  <TableCell>
                      <Badge variant="secondary">{declaration.matricule}</Badge>
                  </TableCell>
                  <TableCell className="font-medium">
                    {declaration.designation}
                  </TableCell>
                  <TableCell>
                    {declaration.panne_declaree}
                  </TableCell>
                  <TableCell>
                    {dayjs(declaration.generated_at).format('DD/MM/YYYY HH:mm')}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-2">
                      <Button asChild variant="ghost" size="icon">
                        <Link href={`/declarations/view/${declaration.id}`}>
                          <Eye className="h-4 w-4" />
                        </Link>
                      </Button>
                      <Button asChild variant="ghost" size="icon">
                        <Link href={`/declarations/edit/${declaration.id}`}>
                          <Pencil className="h-4 w-4" />
                        </Link>
                      </Button>
                      <DeleteDeclarationButton declarationId={declaration.id} />
                    </div>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={5} className="h-24 text-center">
                  {declarations.length === 0 ? "Aucune déclaration générée pour le moment." : "Aucune déclaration ne correspond à votre filtre."}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

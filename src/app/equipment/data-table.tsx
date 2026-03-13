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
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Eye, ListFilter, Pencil } from 'lucide-react';
import type { Equipment } from '@/lib/types';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { DeleteEquipmentButton } from './delete-equipment-button';

export function EquipmentDataTable({ data }: { data: Equipment[] }) {
  const [filter, setFilter] = useState('');
  const [categoryFilter, setCategoryFilter] = useState('Toutes');
  const [brandFilter, setBrandFilter] = useState('Toutes');

  const categories = useMemo(() => {
    const cats = new Set(data.map(item => item.categorie).filter((c): c is string => !!c));
    return ['Toutes', ...Array.from(cats).sort()];
  }, [data]);

  const marques = useMemo(() => {
    const brands = new Set(data.map(item => item.marque).filter((b): b is string => !!b));
    return ['Toutes', ...Array.from(brands).sort()];
  }, [data]);

  const filteredData = useMemo(() => {
    return data.filter(item => {
      const textMatch = Object.values(item).some(val =>
        String(val).toLowerCase().includes(filter.toLowerCase())
      );
      const categoryMatch = categoryFilter === 'Toutes' || item.categorie === categoryFilter;
      const brandMatch = brandFilter === 'Toutes' || item.marque === brandFilter;

      return textMatch && categoryMatch && brandMatch;
    });
  }, [data, filter, categoryFilter, brandFilter]);

  return (
    <div className="space-y-4">
      <div className="flex flex-col md:flex-row items-center gap-4">
        <Input
          placeholder="Filtrer par matricule, désignation..."
          value={filter}
          onChange={e => setFilter(e.target.value)}
          className="max-w-sm"
        />
        <div className='flex gap-2 flex-wrap'>
          <Select value={categoryFilter} onValueChange={setCategoryFilter}>
            <SelectTrigger className="w-full md:w-[180px]">
               <ListFilter className="h-4 w-4 mr-2" />
              <SelectValue placeholder="Catégorie" />
            </SelectTrigger>
            <SelectContent>
              {categories.map(cat => (
                <SelectItem key={cat} value={cat}>
                  {cat}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={brandFilter} onValueChange={setBrandFilter}>
            <SelectTrigger className="w-full md:w-[180px]">
              <ListFilter className="h-4 w-4 mr-2" />
              <SelectValue placeholder="Marque" />
            </SelectTrigger>
            <SelectContent>
              {marques.map(brand => (
                <SelectItem key={brand} value={brand}>
                  {brand}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="whitespace-nowrap">Matricule</TableHead>
              <TableHead>Désignation</TableHead>
              <TableHead>Marque</TableHead>
              <TableHead>Catégorie</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredData.length > 0 ? (
              filteredData.map(equip => (
                <TableRow key={equip.id}>
                  <TableCell className="whitespace-nowrap">
                    <Badge>{equip.matricule}</Badge>
                  </TableCell>
                  <TableCell className="font-medium">{equip.designation}</TableCell>
                  <TableCell>{equip.marque}</TableCell>
                  <TableCell>{equip.categorie}</TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-2">
                      <Button asChild variant="ghost" size="icon">
                        <Link href={`/equipment/${equip.matricule}`}>
                          <Eye className="h-4 w-4" />
                          <span className="sr-only">Voir détails</span>
                        </Link>
                      </Button>
                      <Button asChild variant="ghost" size="icon">
                        <Link href={`/equipment/edit/${equip.id}`}>
                          <Pencil className="h-4 w-4" />
                           <span className="sr-only">Modifier</span>
                        </Link>
                      </Button>
                      <DeleteEquipmentButton equipmentId={equip.id} />
                    </div>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={5} className="h-24 text-center">
                  Aucun résultat pour les filtres sélectionnés.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

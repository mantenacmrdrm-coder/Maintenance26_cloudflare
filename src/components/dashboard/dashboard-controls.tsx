'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Button } from '@/components/ui/button';
import { Printer } from 'lucide-react';

export function YearSelector({ currentYear }: { currentYear: string }) {
  const router = useRouter();
  const searchParams = useSearchParams();

  const handleYearChange = (newYear: string) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set('year', newYear);
    router.push(`/?${params.toString()}`);
  };

  return (
    <Select value={currentYear} onValueChange={handleYearChange}>
      <SelectTrigger className="w-[120px]">
        <SelectValue placeholder="Année" />
      </SelectTrigger>
      <SelectContent>
        {Array.from({ length: 7 }, (_, i) => new Date().getFullYear() - 3 + i).map(y => (
          <SelectItem key={y} value={y.toString()}>{y}</SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

export function MonthSelector({ currentMonth }: { currentMonth: string }) {
    const router = useRouter();
    const searchParams = useSearchParams();

    const handleMonthChange = (newMonth: string) => {
        const params = new URLSearchParams(searchParams.toString());
        if (newMonth === 'all') {
            params.delete('month');
        } else {
            params.set('month', newMonth);
        }
        router.push(`/?${params.toString()}`);
    };

    const months = [
        { value: 'all', label: 'Toute l\'année' },
        ...Array.from({ length: 12 }, (_, i) => ({
            value: (i + 1).toString(),
            label: new Date(0, i).toLocaleString('fr-FR', { month: 'long' }),
        }))
    ];

    return (
        <Select value={currentMonth} onValueChange={handleMonthChange}>
            <SelectTrigger className="w-[180px]">
                <SelectValue placeholder="Mois" />
            </SelectTrigger>
            <SelectContent>
                {months.map(m => (
                    <SelectItem key={m.value} value={m.value}>
                        {m.label.charAt(0).toUpperCase() + m.label.slice(1)}
                    </SelectItem>
                ))}
            </SelectContent>
        </Select>
    );
}

export function PrintButton() {
    const handlePrint = () => {
        window.print();
    };

    return (
        <Button onClick={handlePrint} variant="outline">
            <Printer className="mr-2 h-4 w-4" />
            Imprimer
        </Button>
    );
}

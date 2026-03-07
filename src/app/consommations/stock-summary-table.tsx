'use client';

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Printer } from 'lucide-react';
import { cn } from '@/lib/utils';

const LUBRICANT_TYPES = [
    't32', '15w40_4400', '10w', '15w40', '90', '15w40_v', 
    'hvol', 'tvol', 't30', 'graisse', 't46', '15w40_quartz'
] as const;

const formatLabel = (label: string) => label.replace(/_/g, ' ').toUpperCase();

const formatNumber = (num: number) => num.toLocaleString('fr-FR', { minimumFractionDigits: 1, maximumFractionDigits: 1 });

export function StockSummaryTable({ summary, initialStock }: { summary: { consumed: Record<string, number>, entries: Record<string, number> }, initialStock: Record<string, number> }) {
    
    const handlePrint = () => {
        const printWindow = window.open('', '', 'height=600,width=800');
        if (printWindow) {
            printWindow.document.write('<html><head><title>État des Stocks</title>');
            printWindow.document.write(`
                <style>
                    body { font-family: sans-serif; }
                    table { width: 100%; border-collapse: collapse; }
                    th, td { border: 1px solid #ddd; padding: 8px; text-align: right; }
                    th { background-color: #f2f2f2; text-align: left; }
                    td:first-child { text-align: left; font-weight: bold; }
                    h1 { text-align: center; }
                    .text-destructive { color: red; }
                </style>
            `);
            printWindow.document.write('</head><body>');
            printWindow.document.write('<h1>État des Stocks de Lubrifiants</h1>');
            const printContent = document.getElementById('stock-summary-print-area')?.innerHTML;
            if (printContent) {
                printWindow.document.write(printContent);
            }
            printWindow.document.write('</body></html>');
            printWindow.document.close();
            printWindow.focus();
            setTimeout(() => { printWindow.print(); }, 500);
        }
    };

    return (
        <Card>
            <CardHeader>
                <div className="flex justify-between items-center">
                    <div>
                        <CardTitle>Gestion des Stocks</CardTitle>
                        <CardDescription>Aperçu du stock basé sur la période sélectionnée. Les entrées sont gérées depuis la page "Entrées de Stock".</CardDescription>
                    </div>
                    <Button onClick={handlePrint} variant="outline" size="sm">
                        <Printer className="mr-2 h-4 w-4" />
                        Imprimer l'état
                    </Button>
                </div>
            </CardHeader>
            <CardContent>
                <div className="rounded-md border" id="stock-summary-print-area">
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>Lubrifiant</TableHead>
                                <TableHead className="text-right">Stock Initial</TableHead>
                                <TableHead className="text-right">Entrée</TableHead>
                                <TableHead className="text-right">Total Consommé</TableHead>
                                <TableHead className="text-right">Stock Final</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {LUBRICANT_TYPES.map(type => {
                                const consumed = summary.consumed[type] || 0;
                                const entries = summary.entries[type] || 0;
                                const initial = initialStock[type] || 0;
                                const final = initial + entries - consumed;
                                const isGraisse = type === 'graisse';
                                const unit = isGraisse ? 'Kg' : 'L';

                                return (
                                    <TableRow key={type}>
                                        <TableCell className="font-medium">{formatLabel(type)}</TableCell>
                                        <TableCell className={cn("text-right font-mono", initial < 0 && "text-destructive")}>{formatNumber(initial)} {unit}</TableCell>
                                        <TableCell className="text-right font-mono">{formatNumber(entries)} {unit}</TableCell>
                                        <TableCell className="text-right font-mono">{formatNumber(consumed)} {unit}</TableCell>
                                        <TableCell className={cn("text-right font-bold font-mono", final < 0 && "text-destructive")}>
                                            {formatNumber(final)} {unit}
                                        </TableCell>
                                    </TableRow>
                                );
                            })}
                        </TableBody>
                    </Table>
                </div>
            </CardContent>
        </Card>
    );
}

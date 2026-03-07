'use client';

import { Button } from '@/components/ui/button';
import type { WeeklyReport } from '@/lib/types';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';
import { Printer, Download } from 'lucide-react';
import React from 'react';
import Image from 'next/image';
import { useToast } from '@/hooks/use-toast';
import * as XLSX from 'xlsx';

dayjs.locale('fr');

export function ReportView({ report }: { report: WeeklyReport }) {
  const { toast } = useToast();

  const handlePrint = () => {
    window.print();
  };

  const handleExportExcel = async () => {
    if (!report || report.pannes.length === 0) {
      toast({
        variant: 'destructive',
        title: 'Aucune donnée',
        description: 'Le rapport est vide et ne peut pas être exporté.',
      });
      return;
    }

    const headers = ['N°', 'DESIGNATION', 'Matricule', 'DATE DE PANNE', 'NATURE DE PANNE', 'REPARATIONS ET PIECES', 'DATE DE SORTIE', 'INTERVENANT', 'OBS'];
    
    const data = report.pannes.flatMap(panne => {
      if (panne.reparations.length === 0) {
        return [[
          panne.numero,
          panne.designation,
          panne.matricule,
          panne.date_panne,
          panne.nature_panne,
          '', // No reparation
          panne.date_sortie,
          panne.intervenant,
          panne.obs
        ]];
      }
      return panne.reparations.map((reparation, index) => {
        if (index === 0) {
          return [
            panne.numero,
            panne.designation,
            panne.matricule,
            panne.date_panne,
            panne.nature_panne,
            reparation,
            panne.date_sortie,
            panne.intervenant,
            panne.obs
          ];
        }
        // For subsequent rows of the same panne, only fill the reparation column
        return ['', '', '', '', '', reparation, '', '', ''];
      });
    });

    const worksheet = XLSX.utils.aoa_to_sheet([headers, ...data]);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, `Rapport Semaine`);
    XLSX.writeFile(workbook, `Rapport_hebdomadaire_${report.id}.xlsx`);
  };

  return (
    <>
      <style jsx global>{`
        @media print {
          body * {
            visibility: hidden;
          }
          .print\\:hidden {
            display: none;
          }
          #print-section,
          #print-section * {
            visibility: visible;
          }
          #print-section {
            position: static;
            padding: 0;
            margin: 0;
          }
          thead {
            display: table-header-group; /* Repeat headers on each page */
          }
          tr {
            page-break-inside: avoid;
          }
          @page {
            size: A4 landscape;
            margin: 15mm;
          }
        }
      `}</style>

      <div className="flex justify-end mb-4 print:hidden gap-2">
        <Button onClick={handleExportExcel} variant="outline">
          <Download className="mr-2 h-4 w-4" />
          Exporter (Excel)
        </Button>
        <Button onClick={handlePrint}>
          <Printer className="mr-2 h-4 w-4" />
          Imprimer
        </Button>
      </div>

      <div id="print-section" className="bg-white p-8 rounded-lg text-black w-full">
        {/* Header - Remplacé par une image unique */}
        <div className="w-full flex justify-center mb-6">
          <Image
            src="/templates/en-tete-officiel.png" // IMPORTANT: Remplacez par le chemin de votre image d'en-tête
            alt="En-tête officiel FOREMHYD GERHYD"
            width={1500}
            height={200}
            className="object-contain max-h-[800px]"
            priority
          />
        </div>

        <div className="text-left my-4 print:text-sm">
          <p className="font-bold text-xl md:text-sm print:text-base" style={{ fontStyle: 'italic', fontSize: '10pt' }}>DIRECTION DES RESSOURCES MATERIELLES</p>
          <p className="font-bold text-lg md:text-sm print:text-base" style={{ fontStyle: 'italic', fontSize: '10pt' }}>DEPARTEMENT MATERIEL</p>
        </div>

        {/* Title */}
        <div className="text-center my-4">
          <h1 className="text-sm font-bold underline">
            ETAT HEBDOMADAIRE DES PANNES (DU{' '}
            {dayjs(report.start_date).format('DD/MM/YYYY')} AU{' '}
            {dayjs(report.end_date).format('DD/MM/YYYY')})
          </h1>
        </div>

        {/* Table */}
        <div className="w-full overflow-x-auto">
          <table className="w-full border-collapse text-xs">
            <thead>
              <tr>
                {/* Colonnes avec largeurs personnalisées */}
                <th className="border-t border-b border-l border-black p-1 w-[50px]">N°</th>
                <th className="border-t border-b border-black p-1 w-[20%]">DESIGNATION</th>
                <th className="border-t border-b border-black p-1 whitespace-nowrap w-[100px]">Matricule</th>
                <th className="border-t border-b border-black p-1 whitespace-nowrap w-[110px]">DATE DE PANNE</th>
                <th className="border-t border-b border-black p-1 w-[15%]">NATURE DE PANNE</th>
                <th className="border-t border-b border-black p-1 w-[25%]">REPARATIONS ET PIECES</th>
                <th className="border-t border-b border-black p-1 whitespace-nowrap w-[110px]">DATE DE SORTIE</th>
                <th className="border-t border-b border-black p-1 w-[120px]">INTERVENANT</th>
                <th className="border-t border-b border-r border-black p-1 w-[100px]">OBS</th>
              </tr>
            </thead>
            <tbody>
              {report.pannes.length === 0 ? (
                <tr>
                  <td colSpan={9} className="text-left p-4 border-t border-b border-l border-r border-black">
                    Aucune panne à afficher pour cette période.
                  </td>
                </tr>
              ) : (
                report.pannes.flatMap((panne, panneIndex) =>
                  (panne.reparations.length > 0 ? panne.reparations : ['']).map((reparation, pieceIndex) => {
                    const isFirst = pieceIndex === 0;
                    const isLast = pieceIndex === panne.reparations.length - 1;

                    // Bordures conditionnelles pour la colonne "REPARATIONS ET PIECES"
                    const repBorderClasses = [
                      'p-1',
                      isFirst && 'border-t border-black',
                      isLast && 'border-b border-black',
                    ]
                      .filter(Boolean)
                      .join(' ');

                    return (
                      <tr key={`${panne.numero}-${pieceIndex}`}>
                        {/* 1ère partie : Colonnes fixes (affichées une seule fois via rowSpan) */}
                        {isFirst && (
                          <>
                            <td className="border-l border-black p-1 text-center" rowSpan={panne.reparations.length || 1}
                              style={{ borderTop: isFirst ? '1px solid black' : 'none', borderBottom: isLast ? '1px solid black' : 'none' }}>
                              {panneIndex + 1}
                            </td>
                            <td className="p-1" rowSpan={panne.reparations.length || 1}
                              style={{ borderTop: isFirst ? '1px solid black' : 'none', borderBottom: isLast ? '1px solid black' : 'none' }}>
                              {panne.designation}
                            </td>
                            <td className="p-1 whitespace-nowrap" rowSpan={panne.reparations.length || 1}
                              style={{ borderTop: isFirst ? '1px solid black' : 'none', borderBottom: isLast ? '1px solid black' : 'none' }}>
                              {panne.matricule}
                            </td>
                            <td className="p-1 text-center whitespace-nowrap" rowSpan={panne.reparations.length || 1}
                              style={{ borderTop: isFirst ? '1px solid black' : 'none', borderBottom: isLast ? '1px solid black' : 'none' }}>
                              {panne.date_panne}
                            </td>
                            <td className="p-1" rowSpan={panne.reparations.length || 1}
                              style={{ borderTop: isFirst ? '1px solid black' : 'none', borderBottom: isLast ? '1px solid black' : 'none' }}>
                              {panne.nature_panne}
                            </td>
                          </>
                        )}

                        {/* Colonne Centrale : REPARATIONS ET PIECES (affichée pour chaque ligne de réparation) */}
                        <td className={repBorderClasses}>{reparation}</td>

                        {/* 2ème partie : Colonnes finales (affichées une seule fois via rowSpan) */}
                        {isFirst && (
                          <>
                            <td className="p-1 text-center whitespace-nowrap" rowSpan={panne.reparations.length || 1}
                              style={{ borderTop: isFirst ? '1px solid black' : 'none', borderBottom: isLast ? '1px solid black' : 'none' }}>
                              {panne.date_sortie}
                            </td>
                            <td className="p-1" rowSpan={panne.reparations.length || 1}
                              style={{ borderTop: isFirst ? '1px solid black' : 'none', borderBottom: isLast ? '1px solid black' : 'none' }}>
                              {panne.intervenant}
                            </td>
                            <td className="border-r border-black p-1" rowSpan={panne.reparations.length || 1}
                              style={{ borderTop: isFirst ? '1px solid black' : 'none', borderBottom: isLast ? '1px solid black' : 'none' }}>
                              {panne.obs || ''}
                            </td>
                          </>
                        )}
                      </tr>
                    );
                  })
                )
              )}
            </tbody>
          </table>
        </div>

        {/* Footer */}
        <footer className="mt-12 text-right">
          <p className="font-bold text-xl md:text-sm print:text-base" style={{ fontStyle: 'italic', fontSize: '12pt' }}>Le Chef de Département Matériel</p>
        </footer>
      </div>
    </>
  );
}

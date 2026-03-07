
'use client';
import type { MonthlyStockReportData } from '@/lib/types';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';

dayjs.locale('fr');

type Props = {
  data: MonthlyStockReportData;
};

const formatComptable = (value: number, decimals = 1): string => {
  if (value === 0) return '-';
  return value.toLocaleString('fr-FR', { 
    minimumFractionDigits: decimals, 
    maximumFractionDigits: decimals 
  });
};

export function MonthlyReportDocument({ data }: Props) {
  const { reportData, totalEntries, totalSorties, overallInitialStock, overallFinalStock, lubricantType, period } = data;
  const minRows = 15;
  const emptyRows = Math.max(0, minRows - reportData.length);

  return (
    <div 
      className="bg-white p-1.5 rounded-none text-black w-full max-w-3xl mx-auto border-2 border-black font-sans"
      style={{ maxWidth: '18cm' }}
    >
      <div className="border-b-2 border-black mb-1">
        <img 
          src="/templates/en-tete-officiel.png" 
          alt="En-tête officiel" 
          className="w-full h-auto"
        />
      </div>

      <div className="italic text-left mb-1">
        <div className="font-bold uppercase text-[9.5px]">DIRECTION DES RESSOURCES MATERIELLES</div>
        <div className="bg-gray-900 text-white font-bold uppercase text-[9px] px-24 py-1 w-[600px] text-center mx-auto">
          ETAT DE CONSOMMATION MENSUELLE DE LUBRIFIANT
        </div>
        <div className="text-[12px] font-semibold mt-2 flex justify-center">
          <span className="text-black">{period}</span>
          <span className="ml-2 text-red-600 font-bold">{lubricantType.replace(/_/g, ' ').toUpperCase()}</span>
        </div>
      </div>

      <table 
        className="w-full border-collapse border border-black text-[11px] italic"
        style={{ fontSize: '11px' }}
      >
        <thead>
          <tr>
            <th rowSpan={2} className="border border-gray-500 p-1 bg-gray-300 font-bold text-center w-[150px]">DÉSIGNATION</th>
            <th colSpan={2} className="border border-gray-500 p-1 bg-gray-300 font-bold text-center">ENTRÉES</th>
            <th colSpan={2} className="border border-gray-500 p-1 bg-gray-300 font-bold text-center">SORTIES</th>
            <th colSpan={2} className="border border-gray-500 p-1 bg-gray-300 font-bold text-center">RESTE</th>
          </tr>
          <tr>
            <th className="border border-gray-500 p-1 bg-gray-300 font-bold text-center">Qte</th>
            <th className="border border-gray-500 p-1 bg-gray-300 font-bold text-center">Montant</th>
            <th className="border border-gray-500 p-1 bg-gray-300 font-bold text-center">Qte</th>
            <th className="border border-gray-500 p-1 bg-gray-300 font-bold text-center">Montant</th>
            <th className="border border-gray-500 p-1 bg-gray-300 font-bold text-center">Qte</th>
            <th className="border border-gray-500 p-1 bg-gray-300 font-bold text-center">Montant</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td className="border border-gray-500 p-1 bg-gray-700 text-white font-bold text-center text-[11px] italic">Stock Début de Période</td>
            <td colSpan={4} className="border border-gray-500 p-1"></td>
            <td className="border border-gray-500 p-1 text-right text-[12px] italic font-bold bg-gray-200">
              {formatComptable(overallInitialStock)}
            </td>
            <td className="border border-gray-500 p-1"></td>
          </tr>

          {reportData.map((month, index) => (
              <tr key={index}>
                <td className="border border-gray-500 p-1 font-bold text-center text-[11px] italic capitalize">
                  {month.month}
                </td>
                <td className={`border border-gray-500 p-1 text-right text-[11px] italic ${month.entries > 0 ? 'bg-gray-200' : ''}`}>
                  {formatComptable(month.entries, 1)}
                </td>
                <td className="border border-gray-500 p-1 text-[11px] italic"></td>
                <td className={`border border-gray-500 p-1 text-right text-[11px] italic ${month.sorties > 0 ? 'bg-gray-200' : ''}`}>
                  {formatComptable(month.sorties, 1)}
                </td>
                <td className="border border-gray-500 p-1 text-[11px] italic"></td>
                <td className={`border border-gray-500 p-1 text-right text-[12px] italic ${month.finalStock > 0 ? 'bg-gray-200' : ''}`}>
                  {formatComptable(month.finalStock, 1)}
                </td>
                <td className="border border-gray-500 p-1 text-[11px] italic"></td>
              </tr>
            ))}
          
          {emptyRows > 0 && Array.from({ length: emptyRows }).map((_, index) => (
              <tr key={`empty-${index}`} className="h-6">
                  <td className="border border-gray-500 p-1">&nbsp;</td>
                  <td className="border border-gray-500 p-1">&nbsp;</td>
                  <td className="border border-gray-500 p-1">&nbsp;</td>
                  <td className="border border-gray-500 p-1">&nbsp;</td>
                  <td className="border border-gray-500 p-1">&nbsp;</td>
                  <td className="border border-gray-500 p-1">&nbsp;</td>
                  <td className="border border-gray-500 p-1">&nbsp;</td>
              </tr>
          ))}

          <tr className="font-bold">
            <td className="border border-gray-500 p-1 bg-gray-700 text-white text-center text-[11px] italic">Stock Fin de Période</td>
            <td colSpan={4} className="border border-gray-500 p-1"></td>
            <td className="border border-gray-500 p-1 text-right text-[12px] italic">
              {formatComptable(overallFinalStock)}
            </td>
            <td className="border border-gray-500 p-1"></td>
          </tr>

          <tr className="bg-gray-300 font-bold">
            <td className="border border-gray-500 p-1 text-center text-[11px] italic">Totaux</td>
            <td className="border border-gray-500 p-1 text-right text-[11px] italic bg-gray-200">
              {formatComptable(totalEntries, 1)}
            </td>
            <td className="border border-gray-500 p-1 text-[11px] italic"></td>
            <td className="border border-gray-500 p-1 text-right text-[11px] italic bg-gray-200">
              {formatComptable(totalSorties, 1)}
            </td>
            <td className="border border-gray-500 p-1 text-[11px] italic"></td>
            <td className="border border-gray-500 p-1 text-right text-[12px] italic">
              {formatComptable(overallFinalStock)}
            </td>
            <td className="border border-gray-500 p-1 text-[11px] italic"></td>
          </tr>
        </tbody>
      </table>

      <footer className="grid grid-cols-3 gap-4 mt-4 text-center text-[12px] italic">
        <div className="font-bold">Le chargé de la distribution</div>
        <div className="font-bold">Le Chef de Département Matériel</div>
        <div className="font-bold">Le D.R.M</div>
      </footer>
    </div>
  );
}

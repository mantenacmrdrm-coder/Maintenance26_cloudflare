'use client';
import { DailyReportData } from '@/lib/types';
import Image from 'next/image';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';

dayjs.locale('fr');

type Props = {
  data: DailyReportData;
};

// Fonction utilitaire pour le format comptable
const formatComptable = (value: number, decimals = 1): string => {
  if (value === 0) return '-';
  return value.toLocaleString('fr-FR', { 
    minimumFractionDigits: decimals, 
    maximumFractionDigits: decimals 
  });
};

export function DailyConsumptionReport({ data }: Props) {
  const { initialStock, dailyData, totalEntrees, totalSorties, finalStock, lubricantType, month, year } = data;

  let runningStock = initialStock;
  const monthName = new Date(year, month - 1).toLocaleString('fr', { month: 'long' });

  return (
    <div 
      className="bg-white p-1.5 rounded-none text-black w-full max-w-3xl mx-auto border-2 border-black font-sans"
      style={{ maxWidth: '18cm' }}
    >
      {/* === EN-TÊTE OFFICIEL === */}
      <div className="border-b-2 border-black mb-1">
        <img 
          src="/templates/en-tete-officiel.png" 
          alt="En-tête officiel" 
          className="w-full h-auto"
        />
      </div>

      {/* === TITRE === */}
      <div className="italic text-left mb-1">
        <div className="font-bold uppercase text-[9.5px]">DIRECTION DES RESSOURCES MATERIELLES</div>
        <div className="flex justify-centre mt-1">
      </div>    
      <div className="bg-black text-white font-bold uppercase text-[9px] px-24 py-2 w-[600px] text-center mx-auto">
            ETAT DE CONSOMMATION MENSUELLE DE LUBRIFIANT
        </div>
        <div className="text-[10px] font-semibold mt-1 flex justify-center space-x-16">
          <span className="text-black">{monthName.toUpperCase()} {year}</span>
          <span className="text-red-600 font-bold">{lubricantType.replace(/_/g, ' ').toUpperCase()}</span>
        </div>
      </div>

      {/* === TABLEAU — bordures internes en gris clair, dates centrées === */}
      <table 
        className="w-full border-collapse border border-black text-[11px] italic"
        style={{ fontSize: '11px' }}
      >
        <thead>
          <tr>
            <th rowSpan={2} className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[60px]">DÉSIGNATION</th>
            <th colSpan={2} className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[60px]">ENTRÉES</th>
            <th colSpan={2} className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[60px]">SORTIES</th>
            <th colSpan={2} className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[60px]">RESTE</th>
          </tr>
          <tr>
            <th className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[30px]">Qte</th>
            <th className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[30px]">Montant</th>
            <th className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[30px]">Qte</th>
            <th className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[30px]">Montant</th>
            <th className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[30px]">Qte</th>
            <th className="border border-gray-300 p-1 bg-gray-200 font-bold text-center w-[30px]">Montant</th>
          </tr>
        </thead>
        <tbody>
          {/* Stock Début */}
          <tr>
            <td className="border border-gray-300 p-1 font-bold text-center text-[11px] italic">Stock Début de Période</td>
            <td className="border border-gray-300 p-1"></td>
            <td className="border border-gray-300 p-1"></td>
            <td className="border border-gray-300 p-1"></td>
            <td className="border border-gray-300 p-1"></td>
            <td className="border border-gray-300 p-1 text-right text-[12px] italic font-bold bg-gray-100">
              {formatComptable(initialStock)}
            </td>
            <td className="border border-gray-300 p-1"></td>
          </tr>

          {/* Jours du mois — DATES CENTRÉES */}
          {dailyData.map((day, index) => {
            runningStock = runningStock + day.entree - day.sortie;
            return (
              <tr key={index}>
                <td className="border border-gray-300 p-1 font-bold text-center text-[11px] italic">
                  {dayjs(day.date).format('DD/MM/YYYY')}
                </td>
                <td className={`border border-gray-300 p-1 text-right text-[11px] italic ${day.entree > 0 ? 'bg-gray-100' : ''}`}>
                  {formatComptable(day.entree, 1)}
                </td>
                <td className="border border-gray-300 p-1 text-[11px] italic"></td>
                <td className={`border border-gray-300 p-1 text-right text-[11px] italic ${day.sortie > 0 ? 'bg-gray-100' : ''}`}>
                  {formatComptable(day.sortie, 1)}
                </td>
                <td className="border border-gray-300 p-1 text-[11px] italic"></td>
                <td className={`border border-gray-300 p-1 text-right text-[12px] italic ${runningStock > 0 ? 'bg-gray-100' : ''}`}>
                  {formatComptable(runningStock, 1)}
                </td>
                <td className="border border-gray-300 p-1 text-[11px] italic"></td>
              </tr>
            );
          })}

          {/* Stock Fin */}
          <tr className="font-bold">
            <td className="border border-gray-300 p-1 text-center text-[11px] italic">Stock Fin de Période</td>
            <td className="border border-gray-300 p-1"></td>
            <td className="border border-gray-300 p-1"></td>
            <td className="border border-gray-300 p-1"></td>
            <td className="border border-gray-300 p-1"></td>
            <td className="border border-gray-300 p-1 text-right text-[12px] italic">
              {formatComptable(finalStock)}
            </td>
            <td className="border border-gray-300 p-1"></td>
          </tr>

          {/* Totaux */}
          <tr className="bg-gray-200 font-bold">
            <td className="border border-gray-300 p-1 text-center text-[11px] italic">Totaux</td>
            <td className="border border-gray-300 p-1 text-right text-[11px] italic bg-gray-100">
              {formatComptable(totalEntrees, 1)}
            </td>
            <td className="border border-gray-300 p-1 text-[11px] italic"></td>
            <td className="border border-gray-300 p-1 text-right text-[11px] italic bg-gray-100">
              {formatComptable(totalSorties, 1)}
            </td>
            <td className="border border-gray-300 p-1 text-[11px] italic"></td>
            <td className="border border-gray-300 p-1 text-right text-[12px] italic">
              {formatComptable(finalStock)}
            </td>
            <td className="border border-gray-300 p-1 text-[11px] italic"></td>
          </tr>
        </tbody>
      </table>

      {/* === PIED DE PAGE === */}
      <footer className="grid grid-cols-3 gap-4 mt-4 text-center text-[12px] italic">
      <div className="font-bold">Le chargé de la distribution</div>

      <div className="font-bold">Le Chef de Département Matériel</div>

      <div className="font-bold">Le D.R.M</div>
      </footer>
    </div>
  );
}

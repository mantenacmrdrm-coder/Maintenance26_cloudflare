'use client';
import Image from 'next/image';
import { cn } from '@/lib/utils';
import type { OrdreTravailData } from '@/lib/types';

type OrdreViewProps = {
  data: OrdreTravailData;
};

export function OrdreView({ data }: OrdreViewProps) {
  const typePanneMap = {
    'mécanique': 'mecanique',
    'électrique': 'electrique',
    'autres': 'autres',
    'non spécifié': 'autres'
  };
  const natureIntervention = typePanneMap[data.type_de_panne];

  return (
    <div className="bg-white p-2 rounded-none text-black w-full mx-auto font-sans">
      {/* === EN-TÊTE IMAGE === */}
      <div className="border-b-2 border-black mb-2">
        <Image
          src="/templates/en-tete-officiel.png"
          alt="En-tête Officiel"
          width={1200}
          height={720}
          className="w-full h-auto"
          priority
        />
      </div>
  {/* Texte de l’en-tête (petit et italique) */}
  <div className='text-left my-2 space-y-1'>
    <p className='text-xs italic'></p>
    <p className='font-bold text-xs italic'>DIRECTION DES RESSOURCES MATERIELLES</p>
    <p className='font-bold text-xs italic'>DEPARTEMENT MATERIEL</p>
  </div>

      <table className="w-full border-collapse text-xs">
        <tbody>
          <tr>
            <td className="p-1 font-semibold" colSpan={2}>N°........./{(data.date_entree || '').slice(-4)}</td>
            <td colSpan={4}></td>
          </tr>
          <tr><td colSpan={6} className="h-6"></td></tr>
          <tr className="h-8">
            <td className="border-y-2 border-x-2 border-gray-800 bg-gray-400 my-2 py-1 font-bold text-base tracking-wider text-center" colSpan={6}>ORDRE DE TRAVAIL</td>
          </tr>
          <tr><td colSpan={6} className="h-16"></td></tr>
          <tr>
            <td className="p-1 font-semibold w-[20%]">DATE</td>
            <td className="p-1" colSpan={5}>{new Date().toLocaleDateString('fr-FR')}</td>
          </tr>
           <tr><td colSpan={6} className="h-6"></td></tr>
          <tr className="h-8">
            <td className="p-1 font-semibold">DATE DE PANNE</td>
            <td className="p-1">{data.date_entree}</td>
            <td colSpan={4}></td>
          </tr>
          <tr className="h-8">
            <td className="p-1 font-semibold">VEHICULE (MARQUE)</td>
            <td className="p-1" colSpan={2}>{data.designation} {data.marque}</td>
            <td className="p-1 font-semibold text-right">IMMATRICULATION</td>
            <td className="p-1" colSpan={2}>{data.matricule}</td>
          </tr>
          <tr className="h-8">
            <td className="p-1 font-semibold">AFFECTATION</td>
            <td className="p-1">{data.affectation}</td>
            <td colSpan={4}></td>
          </tr>
           <tr><td colSpan={6} className="h-6"></td></tr>
           <tr>
            <td colSpan={6} className="p-1">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="font-semibold">PANNE ELECTRIQUE</span>
                  <div
                    className={cn(
                      'border border-black w-6 h-6 flex items-center justify-center font-bold',
                      natureIntervention === 'electrique' && 'bg-gray-400'
                    )}
                  >
                    {natureIntervention === 'electrique' ? 'X' : ''}
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <span className="font-semibold">PANNE MECANIQUE</span>
                  <div
                    className={cn(
                      'border border-black w-6 h-6 flex items-center justify-center font-bold',
                      natureIntervention === 'mecanique' && 'bg-gray-400'
                    )}
                  >
                    {natureIntervention === 'mecanique' ? 'X' : ''}
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <span className="font-semibold">AUTRES</span>
                  <div
                    className={cn(
                      'border border-black w-6 h-6 flex items-center justify-center font-bold',
                      natureIntervention === 'autres' && 'bg-gray-400'
                    )}
                  >
                    {natureIntervention === 'autres' ? 'X' : ''}
                  </div>
                </div>
              </div>
            </td>
          </tr>
          <tr><td colSpan={6} className="h-6"></td></tr>
          <tr>
            <td className="p-1 font-semibold" colSpan={6}>NATURE DE PANNE</td>
          </tr>
          <tr className="h-32">
            <td className="p-1 border border-black align-top" colSpan={6}>{data.panne_declaree}</td>
          </tr>
        </tbody>
      </table>
      <div className="grid grid-cols-2 gap-4 mt-20 text-center text-xs">
        <div className='pt-2 border-t-2 border-black'><strong>CHEF D'ATELIER MECANIQUE</strong></div>
        <div className='pt-2 border-t-2 border-black'><strong>VISA DU CHEF SERVICE MAINTENANCE</strong></div>
      </div>
    </div>
  );
}

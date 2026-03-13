'use client';

import type { PreventiveFicheData } from '@/lib/types';
import { cn } from '@/lib/utils';
import Image from 'next/image';

export function FichePreventiveView({ data }: { data: PreventiveFicheData }) {
  const { equipment, entretien, travaux, filtrations, observation } = data;
  
  const Td = ({ children, className = '', ...props }: { children?: React.ReactNode, className?: string } & React.TdHTMLAttributes<HTMLTableCellElement>) => (
    <td className={cn("py-0.5 px-1 border border-gray-300 text-center", className)} {...props}>{children}</td>
  );

  const getTravailColor = (travail?: string) => {
      switch (travail) {
          case 'VIDANGE': return 'bg-red-600 text-yellow-300';
          case 'NIVEAU D\'HUILE': return 'bg-yellow-400 text-red-600';
          case 'TRANSMISSION': return 'bg-yellow-400 text-black';
          case 'HYDRAULIQUE': return 'bg-gray-700 text-white';
          case 'GRAISSAGE': return 'bg-green-600 text-white';
          default: return '';
      }
  };

  const moteurTravail = travaux.find(t => t.organe === 'Moteur');
  const transmissionTravail = travaux.find(t => t.organe === 'Boite de vitesse');
  const hydrauliqueTravail = travaux.find(t => t.organe === 'Réservoir hydraulique');
  const graissageTravail = travaux.find(t => t.organe === 'Tous les graisseurs');

return (
  <div className="bg-white p-1.5 rounded-none text-black w-full mx-auto font-sans">
    {/* === EN-TÊTE IMAGE === */}
    <div className="border-b-2 border-black mb-1">
      <Image
        src="/templates/en-tete-officiel.png"
        alt="En-tête Officiel"
        width={1200}
        height={720}
        className="w-full h-auto max-h-[95px]"
        priority
      />
    </div>

    {/* === DIRECTION + DEPARTEMENT === */}
    <div className="text-left my-1 space-y-0.5">
      <p className="font-bold text-[11px] italic">Direction Ressources Matériel</p>
      <p className="font-bold text-[11px] italic">Département Matériel</p>
    </div>
    {/* === DIRECTION + DEPARTEMENT + N° FICHE === */}
    <div className="items-left text-[10px] font-bold italic">
      <div>N° : ........../{entretien.date?.slice(-4) || '2026'}</div>
    </div>

    {/* === TITRE === */}
    <div className="text-center my-1 bg-gray-300 border-y-2 border-black py-0.5">
      <h1 className="text-base font-bold uppercase">FICHE D'ENTRETIEN PREVENTIF</h1>
    </div>

    {/* === TABLEAUX PRINCIPAUX (hauteurs réduites) === */}
    <table className="w-full border-collapse border-[1.5px] border-black text-[10px] italic mb-1">
      <tbody>
        <tr>
          <td className='p-0.5 border border-gray-300 w-[20%]'>Machine :</td>
          <td className='p-0.5 border border-gray-300 font-bold'>{equipment.machine}</td>
        </tr>
      </tbody>
    </table>

    <table className="w-full border-collapse border-[1.5px] border-black text-[10px] italic mb-1">
      <tbody>
        <tr>
          <td className='p-0.5 border border-gray-300 w-[20%]'>Désignation :</td>
          <td className='p-0.5 border border-gray-300 font-bold'>{equipment.designation}</td>
          <td className='p-0.5 border border-gray-300 w-[15%]'>Code :</td>
          <td className='p-0.5 border border-gray-300 font-bold'>{equipment.code}</td>
        </tr>
        <tr>
          <td className='p-0.5 border border-gray-300'>Marque :</td>
          <td className='p-0.5 border border-gray-300 font-bold'>{equipment.marque}</td>
          <td className='p-0.5 border border-gray-300'>Matricule :</td>
          <td className='p-0.5 border border-gray-300 font-bold'>{equipment.matricule}</td>
        </tr>
      </tbody>
    </table>
    
    <table className="w-full border-collapse border-[1.5px] border-black text-[10px] italic mb-1">
      <tbody>
        <tr>
          <td className='p-0.5 border border-gray-300 w-[25%]'>Nom et prénom d'intervenant</td>
          <td className='p-0.5 border border-gray-300 font-bold w-[20%]'>{entretien.intervenant}</td>
          <td className='p-0.5 border border-gray-300'>Lieu</td>
          <td className='p-0.5 border border-gray-300 w-[15%]'></td>
          <td className='p-0.5 border border-gray-300 w-[15%]'>Date d'entretien</td>
          <td className='p-0.5 border border-gray-300 font-bold'>{entretien.date}</td>
        </tr>
        <tr>
          <td className='p-0.5 border border-gray-300' colSpan={4}></td>
          <td className='p-0.5 border border-gray-300'>Départ (h)</td>
          <td className='p-0.5 border border-gray-300'>Arrivée (h)</td>
        </tr>
      </tbody>
    </table>
    
    <table className="w-full border-collapse border-[1.5px] border-black text-[10px] italic mb-1">
      <tbody>
        <tr className='h-6'>
          <td className='p-0.5 border border-gray-300'>Lubrification par Gamme (A, B, C, D, E) :</td>
        </tr>
      </tbody>
    </table>

    <p className='text-[10px] font-bold italic mb-1'>Nature des travaux</p>
    <table className="w-full border-collapse border-[1.5px] border-black text-[10px] italic mb-1">
      <thead>
        <tr className='bg-gray-200'>
          <Td className='p-0.5 w-[25%] text-left'>Organes/Circuit</Td>
          <Td className='p-0.5 w-[15%]'>Apport d'huile</Td>
          <Td className='p-0.5'>Vidange et remplissage</Td>
          <Td className='p-0.5'>Date</Td>
          <Td className='p-0.5'>Lubrifiant</Td>
          <Td className='p-0.5'>Quantités</Td>
        </tr>
      </thead>
      <tbody>
        <tr className='h-6'>
          <Td className="p-0.5 text-left">Moteur</Td>
          <Td className="p-0.5" />
          <Td className={cn('p-0.5 font-bold', getTravailColor(moteurTravail?.travail))}>{moteurTravail?.travail || ''}</Td>
          <Td className="p-0.5 font-bold">{moteurTravail?.date || ''}</Td>
          <Td className="p-0.5 font-bold">{moteurTravail?.lubrifiant || ''}</Td>
          <Td className="p-0.5 font-bold">{moteurTravail?.quantite || ''}</Td>
        </tr>
        
        <tr className='h-6'>
          <Td className="p-0.5 text-left">Boite de vitesse</Td>
          <Td className="p-0.5" />
          {transmissionTravail ? (
            <>
              <Td rowSpan={3} className={cn('p-0.5 font-bold', getTravailColor(transmissionTravail.travail))}>{transmissionTravail.travail}</Td>
              <Td rowSpan={3} className="p-0.5 font-bold">{transmissionTravail.date}</Td>
              <Td rowSpan={3} className="p-0.5 font-bold">{transmissionTravail.lubrifiant}</Td>
              <Td rowSpan={3} className="p-0.5 font-bold">{transmissionTravail.quantite}</Td>
            </>
          ) : (
            <>
              <Td rowSpan={3} className="p-0.5" />
              <Td rowSpan={3} className="p-0.5" />
              <Td rowSpan={3} className="p-0.5" />
              <Td rowSpan={3} className="p-0.5" />
            </>
          )}
        </tr>
        <tr className='h-6'>
          <Td className="p-0.5 text-left">Différentiels (ponts)</Td>
          <Td className="p-0.5" />
        </tr>
        <tr className='h-6'>
          <Td className="p-0.5 text-left">Réducteurs des roues</Td>
          <Td className="p-0.5" />
        </tr>

        <tr className='h-6'>
          <Td className="p-0.5 text-left">Réservoir hydraulique</Td>
          <Td className="p-0.5" />
          <Td className={cn('p-0.5 font-bold', getTravailColor(hydrauliqueTravail?.travail))}>{hydrauliqueTravail?.travail || ''}</Td>
          <Td className="p-0.5 font-bold">{hydrauliqueTravail?.date || ''}</Td>
          <Td className="p-0.5 font-bold">{hydrauliqueTravail?.lubrifiant || ''}</Td>
          <Td className="p-0.5 font-bold">{hydrauliqueTravail?.quantite || ''}</Td>
        </tr>

        <tr className='h-6'>
          <Td className="p-0.5 text-left">Tous les graisseurs</Td>
          <Td className="p-0.5" />
          <Td className={cn('p-0.5 font-bold', getTravailColor(graissageTravail?.travail))}>{graissageTravail?.travail || ''}</Td>
          <Td className="p-0.5 font-bold">{graissageTravail?.date || ''}</Td>
          <Td className="p-0.5 font-bold">{graissageTravail?.lubrifiant || ''}</Td>
          <Td className="p-0.5 font-bold">{graissageTravail?.quantite || ''}</Td>
        </tr>
      </tbody>
    </table>
    
    <p className='text-[10px] font-bold italic mb-1 text-center'>Changement de filtrations :</p>
    <table className="w-full border-collapse border-[1.5px] border-black text-[10px] italic mb-1">
      <thead>
        <tr className='bg-gray-200'>
          <Td className='p-0.5 w-[25%] text-left'>Filtre</Td>
          <Td className='p-0.5'>Quantité</Td>
          <Td className='p-0.5'>Références</Td>
          <Td className='p-0.5'>DATE</Td>
          <Td className='p-0.5'>OBS</Td>
        </tr>
      </thead>
      <tbody>
        <tr className='h-6'>
          <Td className="p-0.5 text-left">Filtre à air</Td>
          <Td className="p-0.5 font-bold">{filtrations.air.active ? '1' : ''}</Td>
          <Td className="p-0.5" />
          <Td className="p-0.5 font-bold">{filtrations.air.date || ''}</Td>
          <Td className="p-0.5" />
        </tr>
        <tr className='h-6'>
          <Td className="p-0.5 text-left">Filtre à huile</Td>
          <Td className="p-0.5 font-bold">{filtrations.huile.active ? '1' : ''}</Td>
          <Td className="p-0.5" />
          <Td className="p-0.5 font-bold">{filtrations.huile.date || ''}</Td>
          <Td className="p-0.5" />
        </tr>
        <tr className='h-6'>
          <Td className="p-0.5 text-left">Filtre à gasoil</Td>
          <Td className="p-0.5 font-bold">{filtrations.gasoil.active ? '1' : ''}</Td>
          <Td className="p-0.5" />
          <Td className="p-0.5 font-bold">{filtrations.gasoil.date || ''}</Td>
          <Td className="p-0.5" />
        </tr>
        <tr className='h-6'>
          <Td className="p-0.5 text-left">Filtre à bypass</Td>
          <Td className="p-0.5 font-bold">{filtrations.bypass.active ? '1' : ''}</Td>
          <Td className="p-0.5" />
          <Td className="p-0.5 font-bold">{filtrations.bypass.date || ''}</Td>
          <Td className="p-0.5" />
        </tr>
        <tr className='h-6'>
          <Td className="p-0.5 text-left">Filtre à hydraulique</Td>
          <Td className="p-0.5 font-bold">{filtrations.hydraulique.active ? '1' : ''}</Td>
          <Td className="p-0.5" />
          <Td className="p-0.5 font-bold">{filtrations.hydraulique.date || ''}</Td>
          <Td className="p-0.5" />
        </tr>
      </tbody>
    </table>

    <p className='text-[10px] font-bold italic mb-1'>Observation :</p>
    <div className='border border-gray-300 p-0.5 h-12 text-[10px] italic font-bold'>{observation}</div>

    {/* === PIED DE PAGE === */}
    <div className="grid grid-cols-2 gap-2 mt-8 text-center text-[10px] font-bold italic">
      <div className='pt-8 border-t-2 border-black'>Visa intervenant</div>
      <div className='pt-8 border-t-2 border-black'>Visa de responsable</div>
    </div>
  </div>
);
}

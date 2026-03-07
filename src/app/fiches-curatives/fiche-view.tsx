'use client';
import Image from 'next/image';
import { cn } from '@/lib/utils';
import type { CurativeFicheData } from '@/lib/types';

type FicheViewProps = {
  data: CurativeFicheData;
};

export function FicheView({ data }: FicheViewProps) {
  const piecesToExecute = data.pieces || [];

  const typePanneMap: Record<string, 'électrique' | 'mécanique' | 'autres'> = {
    'mécanique': 'mécanique',
    'électrique': 'électrique',
    'autres': 'autres',
    'non spécifié': 'autres'
  };
  const natureIntervention = typePanneMap[data.type_de_panne] || 'autres';
  const isConcluant = data.sitactuelle === 'Réparée';

  const Checkbox = ({ checked }: { checked: boolean }) => (
    <div className={`w-4 h-4 border border-black flex items-center justify-center text-[8px] font-bold ${
      checked ? 'bg-gray-700 text-white' : 'bg-white'
    }`}>
      {checked ? 'X' : ''}
    </div>
  );

  return (
      <div className="bg-white p-2.5 rounded-none text-black w-full max-w-4xl mx-auto font-sans">
        {/* === EN-TÊTE IMAGE SEULE (pas de texte supplémentaire) === */}
        <div className="border-b-2 border-black mb-2">
          <img 
            src="/templates/en-tete-officiel.png" 
            alt="En-tête officiel - GERHYD-SPA" 
            className="w-full h-auto"
          />
        </div>

      {/* === TITRE + N° === */}
      <div className="bg-gray-300 py-1 px-2 text-center mb-2">
        <div className="font-bold text-base uppercase tracking-wider">Fiche de maintenance curative</div>
      </div>
      <div className="text-left text-sm font-bold mb-2">
        N°........../{data.date_entree.slice(-4)}
      </div>

      {/* === CORPS — taille augmentée à text-[9px], valeurs en gras === */}
      <div className="space-y-1.5 text-[9px]">
        {/* Ligne 1: Date */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Date :</div>
          <div className="flex-1 text-center font-bold">{data.date_entree}</div>
        </div>

        {/* Ligne 2: Atelier */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Atelier/ Service/ lieu d’affectation :</div>
          <div className="flex-1 text-center font-bold">{data.affectation}</div>
        </div>

        {/* Ligne 3: Désignation matériel */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Désignation matériel :</div>
          <div className="flex-1 text-center font-bold">{data.designation}</div>
        </div>

        {/* Ligne 4: Code de matériel */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Code de matériel :</div>
          <div className="flex-1 text-center font-bold">{data.matricule}</div>
        </div>

        {/* Ligne 5: Marque */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Marque :</div>
          <div className="flex-1 text-center font-bold">{data.marque}</div>
        </div>

        {/* Ligne 6: Type */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Type :</div>
          <div className="flex-1 text-center font-bold">{data.categorie}</div>
        </div>

        {/* ✅ LIGNE 7–8: Nature de l’intervention — CORRECTION FINALE (X centrés sous libellés) */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Nature de l’intervention</div>
          <div className="flex-1">
            {/* Libellés en haut — 3 colonnes égales, sans gap, bordure basse */}
            <div className="flex w-full">
              <div className="flex-1 flex flex-col items-center">
                <div className="pb-0.5 mb-1 text-center">électrique</div>
                <div className="h-5 flex items-center justify-center">
                  {natureIntervention === 'électrique' && <Checkbox checked={true} />}
                </div>
              </div>
              <div className="flex-1 flex flex-col items-center">
                <div className="pb-0.5 mb-1 text-center">mécanique</div>
                <div className="h-5 flex items-center justify-center">
                  {natureIntervention === 'mécanique' && <Checkbox checked={true} />}
                </div>
              </div>
              <div className="flex-1 flex flex-col items-center">
                <div className="pb-0.5 mb-1 text-center">autres</div>
                <div className="h-5 flex items-center justify-center">
                  {natureIntervention === 'autres' && <Checkbox checked={true} />}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Ligne 9: Diagnostic */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Diagnostic :</div>
          <div className="flex-1 text-center font-bold"></div>
        </div>

        {/* Ligne 10: Anomalies constatées — libellé + valeur sur même ligne, valeur en gras, alignée à gauche */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Anomalies constatées :</div>
          <div className="flex-1 text-left font-bold">{data.panne_declaree}</div>
        </div>

        {/* Ligne 11: Cause — libellé seul, puis 4 lignes indentées */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Cause :</div>
          <div className="flex-1"></div>
        </div>
        <div className="ml-[164px] text-[9px] space-y-0.5">
          <div>1-</div>
          <div>2-</div>
          <div>3-</div>
          <div>4-</div>
        </div>

        {/* Ligne 12: Travail à exécuter — libellé seul, puis 4 lignes indentées */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Travail à exécuter :</div>
          <div className="flex-1"></div>
        </div>
        <div className="ml-[164px] text-[9px] space-y-0.5">
          {piecesToExecute.length > 0 ? (
            piecesToExecute.map((piece, index) => (
              <div key={index}>{index + 1}- {piece}</div>
            ))
          ) : (
            <>
              <div>1-</div>
              <div>2-</div>
              <div>3-</div>
              <div>4-</div>
            </>
          )}
        </div>

        {/* Ligne 13: Intervention + Travaux réalisés */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Intervention :</div>
          <div className="flex-1"></div>
        </div>
        <div className="flex mt-0.5">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Travaux réalisés :</div>
          <div className="flex-1 space-y-0.5">
            <div>-</div>
            <div>-</div>
            <div>-</div>
          </div>
        </div>

        {/* Ligne 14: Résultat des essais — reste comme avant (grille 4 colonnes) */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Résultat des essais après réparation</div>
          <div className="flex-1">
            <div className="grid grid-cols-4 gap-2 text-center">
              <div>concluant</div>
              <div><Checkbox checked={isConcluant} /></div>
              <div>non-concluant</div>
              <div></div>
            </div>
            <div className="text-left">-</div>
            <div className="text-left">-</div>
            <div className="text-left">-</div>
          </div>
        </div>

        {/* Ligne 15: Heures d’arrêt */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Heures d’arrêt machine :</div>
          <div className="grid grid-cols-2 gap-2">
            <div className="font-bold">{data.nbr_indisponibilite} Jour(s)</div>
            <div>Jour (s)</div>
          </div>
        </div>

        {/* Ligne 16: Intervenants */}
        <div className="flex">
          <div className="w-[160px] text-left font-semibold whitespace-nowrap">Nom des intervenants :</div>
          <div className="grid grid-cols-1 gap-2"></div>
          <div className="text-center font-bold">{data.intervenant}</div>
        </div>
      </div>

      {/* === PIED DE PAGE === */}
      <div className="mt-12 pt-2 border-t-2 border-black">
          <div className="grid grid-cols-2 gap-4 text-center text-xs font-bold uppercase">
              <div>Visa du chargé de maintenance</div>
              <div>Visa du directeur</div>
          </div>
      </div>
    </div>
  );
}

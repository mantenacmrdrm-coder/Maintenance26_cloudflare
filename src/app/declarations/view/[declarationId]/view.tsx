'use client';

import React from 'react';
import type { DeclarationPanne } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Printer, Download } from 'lucide-react';
import Link from 'next/link';
import dayjs from 'dayjs';
import * as XLSX from 'xlsx';

export function DeclarationView({ declaration }: { declaration: DeclarationPanne }) {
    const handlePrint = () => window.print();

    const handleExportExcel = async () => {
        // ... (Logique Excel inchangée)
    };

    const montantTotalPieces = declaration.pieces.reduce((sum, piece) => sum + (piece.montant || 0), 0);
    const montantGlobal = montantTotalPieces + (declaration.montant_main_oeuvre || 0);

    const totalPieceRows = 12;
    const emptyPieceRows = Math.max(0, totalPieceRows - declaration.pieces.length);

    // ✅ Correction Tailwind : Utilisation des crochets [] pour les valeurs arbitraires
    const tableBorder = "border-[2.5px] border-black"; 
    const cellLabel = "border-[2.5px] border-black p-1 text-[12px] font-bold italic text-center leading-tight";
    const cellValue = "border-[2.5px] border-black p-1 text-[12px] text-center font-bold uppercase";
    
    // ✅ Classe spécifique pour les bordures verticales de 2.5px (Axe X)
    const verticalBorders = "border-x-[2.5px] border-black";

    const calculateDuration = () => {
        const { date_entree, date_sortie } = declaration.operation;
        if (date_sortie === 'En Cours' || !date_entree || !date_sortie) return '';
        try {
            const start = dayjs(date_entree, 'DD/MM/YYYY');
            const end = dayjs(date_sortie, 'DD/MM/YYYY');
            return start.isValid() && end.isValid() ? `${end.diff(start, 'day')} jour(s)` : '';
        } catch { return ''; }
    };

    return (
        <div className='space-y-4 max-w-5xl mx-auto p-4'>
            <div className="flex justify-between items-center print:hidden">
                <Button asChild variant="outline"><Link href="/declarations">Retour</Link></Button>
                 <div className="flex gap-2">
                    <Button onClick={handleExportExcel}><Download className="mr-2 h-4 w-4" /> Exporter (Excel)</Button>
                    <Button onClick={handlePrint}><Printer className="mr-2 h-4 w-4" /> Imprimer</Button>
                </div>
            </div>

            <div id="print-section" className="bg-white p-2 text-black w-full font-sans">
                <div className="w-full mb-2">
                    <img 
                        src="/templates/en-tete-officiel.png" 
                        alt="En-tête Officiel" 
                        className="w-full h-auto block"
                    />
                </div>

                <div className='text-center mb-4 space-y-0.5'>
                    <p className='font-bold text-[14px] underline uppercase tracking-tight'>Direction des Ressources Matérielles</p>
                    <h1 className='font-bold text-[15px] underline uppercase'>Déclaration de Panne</h1>
                </div>

                <table className={`w-full border-collapse ${tableBorder}`}>
                    <tbody>
                        <tr>
                            <td className={`${cellLabel} w-[15%]`}>Date de panne</td>
                            <td className={`${cellLabel} w-[30%]`}>Type de matériel</td>
                            <td className={`${cellLabel} w-[15%]`}>Immatriculation</td>
                            <td className={`${cellLabel} w-[10%]`}>Affectation</td>
                            <td className={`${cellLabel} w-[20%]`}>Chauffeur/Conducteur</td>
                        </tr>
                        <tr className="h-7">
                            <td className={cellValue}>{declaration.operation.date_entree}</td>
                            <td className={cellValue}>{declaration.equipment.marque} {declaration.equipment.designation}</td>
                            <td className={cellValue}>{declaration.operation.matricule}</td>
                            <td className={cellValue}>{declaration.operation.affectation}</td>
                            <td className={cellValue}>{declaration.chauffeur_conducteur}</td>
                        </tr>

                        <tr>
                            <td className={`${cellLabel} text-left px-2 h-12`}>Déclaration de L'utilisateur</td>
                            <td colSpan={4} className="border-[3px] border-black p-2 text-[14px] italic align-top font-medium">
                                {declaration.operation.panne_declaree}
                            </td>
                        </tr>
                        <tr>
                            <td className={`${cellLabel} text-left px-2 h-16`}>Diagnostique de L'intervenant</td>
                            <td colSpan={4} className="border-[3px] border-black p-2 text-[14px] italic align-top font-medium">
                                {declaration.diagnostique_intervenant}
                            </td>
                        </tr>
                        <tr>
                            <td className={`${cellLabel} text-left px-2`}>Causes</td>
                            <td colSpan={4} className="border-[3px] border-black p-1 text-[14px] italic font-medium">
                                {declaration.causes}
                            </td>
                        </tr>

                        {/* === SECTION PIÈCES AVEC BORDURES VERTICALES ÉPAISSES === */}
                        <tr>
                        <td
                            rowSpan={2 + declaration.pieces.length + emptyPieceRows}
                            className={`${verticalBorders} align-middle w-[140px] px-4 text-[12px] font-bold italic text-center leading-tight`}
                            style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}
                        >
                            Pièces Endommagées à changer
                        </td>
                        <td
                            className={`${verticalBorders} p-1 text-[12px] font-bold italic text-center leading-tight`}
                            style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}
                        >
                            Désignation
                        </td>
                        <td
                            className={`${verticalBorders} p-1 text-[12px] font-bold italic text-center leading-tight`}
                            style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}
                        >
                            Référence
                        </td>
                        <td
                            className={`${verticalBorders} p-1 text-[12px] font-bold italic text-center leading-tight`}
                            style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}
                        >
                            Quantité
                        </td>
                        <td
                            className={`${verticalBorders} p-1 text-[12px] font-bold italic text-center leading-tight`}
                            style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}
                        >
                            Montant
                        </td>
                        </tr>

                        {declaration.pieces.map((piece, i) => (
                        <tr key={i} className="h-7">
                            <td
                            className={`${verticalBorders} px-2 text-[14px] font-medium italic text-left`}
                            style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}
                            >
                            {piece.designation}
                            </td>
                            <td
                            className={`${verticalBorders} text-center text-[14px] font-medium italic`}
                            style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}
                            >
                            /
                            </td>
                            <td
                            className={`${verticalBorders} text-center text-[14px] font-bold italic`}
                            style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}
                            >
                            {String(piece.quantite).padStart(2, '0')}
                            </td>
                            <td
                            className={`${verticalBorders} text-right px-2 text-[14px] font-bold italic`}
                            style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}
                            >
                            {piece.montant ? piece.montant.toLocaleString('fr-FR', { minimumFractionDigits: 2 }) : ""}
                            </td>
                        </tr>
                        ))}

                        {Array.from({ length: emptyPieceRows }).map((_, i) => (
                        <tr key={`empty-${i}`} className="h-6">
                            <td className={verticalBorders} style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}></td>
                            <td className={verticalBorders} style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}></td>
                            <td className={verticalBorders} style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}></td>
                            <td className={verticalBorders} style={{ borderTop: '2px dotted #4B5563', borderBottom: '2px dotted #4B5563' }}></td>
                        </tr>
                        ))}

                        <tr>
                            <td colSpan={3} className={`${cellLabel} text-center h-7`}>Montant total des pièces à changer (1)</td>
                            <td className="border-[3px] border-black text-right px-2 text-[14px] font-bold bg-gray-50">
                                {montantTotalPieces.toLocaleString('fr-FR', { minimumFractionDigits: 2 })}
                            </td>
                        </tr>

                        <tr>
                            <td rowSpan={2} className={`${cellLabel} text-left px-2`}>Intervenants</td>
                            <td className="border-[3px] border-black p-1 text-[14px] font-bold italic">Externe</td>
                            <td colSpan={3} className="border-[3px] border-black p-1 text-[14px] text-center font-bold uppercase">
                                {declaration.intervenants.find(i => i.type === 'Externe')?.description || ""}
                            </td>
                        </tr>
                        <tr className="h-6"><td className="border-[3px] border-black" colSpan={4}></td></tr>

                        <tr>
                            <td className={cellLabel}>Date d'entrée</td>
                            <td className={cellValue}>{declaration.operation.date_entree}</td>
                            <td className={cellLabel}>Date de sortie</td>
                            <td colSpan={2} className={cellValue}>{declaration.operation.date_sortie === 'En Cours' ? '' : declaration.operation.date_sortie}</td>
                        </tr>

                        <tr>
                            <td className={cellLabel}>Durée de l'intervention</td>
                            <td className={cellLabel}>Montant de la main d'œuvre (2)</td>
                            <td colSpan={3} className={cellLabel}>Montant Global (1) + (2)</td>
                        </tr>
                        <tr className="h-8">
                            <td className="border-[3px] border-black text-center text-[0px] font-bold">{calculateDuration()}</td>
                            <td className="border-[3px] border-black text-right px-2 text-[0px] font-bold">
                                {declaration.montant_main_oeuvre?.toLocaleString('fr-FR', { minimumFractionDigits: 2 })}
                            </td>
                            <td colSpan={3} className="border-[3px] border-black text-right px-2 text-[0px] font-bold">
                                {montantGlobal.toLocaleString('fr-FR', { minimumFractionDigits: 2 })}
                            </td>
                        </tr>

                        <tr>
                            <td className={cellLabel}>Obs/Réserves</td>
                            <td colSpan={4} className="border-[3px] border-black p-1 text-[10px] italic h-10 align-top">
                                {declaration.obs_reserves}
                            </td>
                        </tr>
                    </tbody>
                </table>

                <div className="grid grid-cols-3 mt-6 text-[14px] font-bold italic">
                    <div className="underline pt-10 border-t-2 border-black">Visa du déclarant</div>
                    <div className="underline pt-10 border-t-2 border-black text-center">Visa du responsable de la maintenance</div>
                    <div className="underline pt-10 border-t-2 border-black text-right">Visa DRM</div>
                </div>
            </div>

            <style jsx global>{`
                @media print {
                    @page { size: A4; margin: 10mm; }
                    body { background: white !important; }
                    .print\\:hidden { display: none !important; }
                    #print-section { padding: 0 !important; }
                }
            `}</style>
        </div>
    );
}
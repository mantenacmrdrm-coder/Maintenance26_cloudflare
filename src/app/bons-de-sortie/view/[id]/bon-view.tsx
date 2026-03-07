'use client';

import React from 'react';
import type { BonDeSortie } from '@/lib/types';
import Image from 'next/image';
import { Button } from '@/components/ui/button';
import { Printer, Download } from 'lucide-react';
import Link from 'next/link';
import * as XLSX from 'xlsx';

export function BonDeSortieView({ bon }: { bon: BonDeSortie }) {
  const handlePrint = () => window.print();

  const handleExportExcel = async () => {
    // General Info
    const generalInfo = [
        ['Document', 'Bon de Sortie'],
        ['N° Bon', bon.id],
        ['Date', bon.date],
        ['Destinataire (Chantier)', bon.destinataire_chantier],
        ['Code Chantier', bon.destinataire_code],
        ['Transporteur', bon.transporteur_nom],
        ['Immatriculation', bon.transporteur_immatriculation],
    ];
    const ws1 = XLSX.utils.aoa_to_sheet(generalInfo);
    ws1['!cols'] = [{wch: 30}, {wch: 50}];

    // Items
    const itemsHeaders = ["Code", "Désignation", "Unité", "Quantité"];
    const itemsData = bon.items.map(item => [
        item.code,
        item.designation,
        item.unite,
        item.quantite
    ]);
    const ws2 = XLSX.utils.aoa_to_sheet([itemsHeaders, ...itemsData]);
    ws2['!cols'] = [{wch: 15}, {wch: 40}, {wch: 10}, {wch: 10}];
    
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws1, "Infos Générales");
    XLSX.utils.book_append_sheet(wb, ws2, "Articles");

    XLSX.writeFile(wb, `Bon_de_Sortie_${bon.id}.xlsx`);
  };

  const totalRows = 12;
  const emptyRows = Math.max(0, totalRows - bon.items.length);

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center print:hidden">
        <Button asChild variant="outline"><Link href="/bons-de-sortie">Retour à la liste</Link></Button>
        <div className="flex gap-2">
            <Button onClick={handleExportExcel}><Download className="mr-2 h-4 w-4" /> Exporter (Excel)</Button>
            <Button onClick={handlePrint}><Printer className="mr-2 h-4 w-4" /> Imprimer</Button>
        </div>
      </div>

      <div id="print-section" className="bg-white p-8 rounded-lg shadow-lg text-black w-full max-w-[210mm] mx-auto font-serif leading-tight">
        {/* En-tête avec logo */}
        <header className="mb-6">
          <div className="w-full flex justify-center">
            <Image
              src="/templates/en-tete-officiel.png"
              alt="GERHYD - Logo et informations"
              width={1200}
              height={720}
              className="w-full h-auto"
              priority
            />
          </div>
        </header>

        {/* Titre principal bilingue centré */}
        <div className="text-center mb-6 border-b-2 border-gray-600 pb-2">
          {/* Texte Arabe - Taille et Gras contrôlables */}
          <h1 className="mb-1 font-bold" style={{ fontFamily: 'Arial, sans-serif', fontSize: '22px' }}>وصل خروج</h1>
          {/* Texte Français - Taille et Gras contrôlables */}
          <h2 className="font-bold underline decoration-double tracking-wider" style={{ fontSize: '18px' }}>BON DE SORTIE</h2>
        </div>

        {/* Numéro et Date */}
        <div className="flex justify-between items-center mb-6">
          {/* Bloc N° à gauche */}
          <div className="flex items-center gap-2">
            <span className="font-bold" style={{ fontSize: '12px' }}>N° :</span>
            <span style={{ fontSize: '11px' }}>{String(bon.id).padStart(4, '0')}</span>
          </div>
          {/* Bloc التاريخ à droite */}
          <div className="flex items-center gap-2">
            <span className="font-bold" style={{ fontFamily: 'Arial, sans-serif', fontSize: '12px' }}>التاريـــــــــخ:</span>
            <span style={{ fontSize: '11px' }}>{bon.date}</span>
          </div>
        </div>

        {/* Tableau Emetteur / Destinataire - Bordures verticales gris clair */}
        <table className="w-full border-collapse border-2 border-gray-600 mb-6">
          <thead>
            <tr>
              <th className="border-2 border-gray-600 bg-gray-100 p-2 w-1/2" style={{ borderRightColor: '#d1d5db' }}>
                <div className="text-center">
                  {/* Texte Arabe - Taille et Gras contrôlables */}
                  <div className="font-bold mb-1" style={{ fontFamily: 'Arial, sans-serif', fontSize: '11px' }}>المرســــــــــــــل إليه</div>
                  {/* Texte Français - Taille et Gras contrôlables */}
                  <div className="font-bold tracking-wider" style={{ fontSize: '10px' }}>DESTINATAIRE</div>
                </div>
              </th>
              <th className="border-2 border-gray-600 bg-gray-100 p-2 w-1/2" style={{ borderLeftColor: '#d1d5db' }}>
                <div className="text-center">
                  {/* Texte Arabe - Taille et Gras contrôlables */}
                  <div className="font-bold mb-1" style={{ fontFamily: 'Arial, sans-serif', fontSize: '11px' }}>المرســـــــــل</div>
                  {/* Texte Français - Taille et Gras contrôlables */}
                  <div className="font-bold tracking-wider" style={{ fontSize: '10px' }}>EMETTEUR</div>
                </div>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td className="border-2 border-gray-600 p-3 align-top h-24" style={{ borderRightColor: '#d1d5db' }}>
                <div className="space-y-3">
                  {/* Ligne Chantier avec valeur au milieu */}
                  <div className="flex items-center justify-between">
                    {/* Texte Français à gauche */}
                    <span className="font-bold" style={{ fontSize: '10px', width: '25%' }}>Chantier :</span>
                    {/* Valeur au centre */}
                    <span className="text-center" style={{ fontSize: '9px', width: '50%' }}>{bon.destinataire_chantier}</span>
                    {/* Texte Arabe à droite */}
                    <span className="text-right" style={{ fontFamily: 'Arial, sans-serif', fontSize: '10px', width: '25%' }}>ورشـــــــــــة</span>
                  </div>
                  {/* Ligne Code avec valeur au milieu */}
                  <div className="flex items-center justify-between">
                    {/* Texte Français à gauche */}
                    <span className="font-bold" style={{ fontSize: '10px', width: '25%' }}>Code :</span>
                    {/* Valeur au centre */}
                    <span className="text-center" style={{ fontSize: '9px', width: '50%' }}>{bon.destinataire_code || '....................'}</span>
                    {/* Texte Arabe à droite */}
                    <span className="text-right" style={{ fontFamily: 'Arial, sans-serif', fontSize: '10px', width: '25%' }}>الرمز:</span>
                  </div>
                </div>
              </td>
              <td className="border-2 border-gray-600 p-3 align-top h-24" style={{ borderLeftColor: '#d1d5db' }}>
                <div className="space-y-3">
                  {/* Ligne Magasin avec valeur au milieu */}
                  <div className="flex items-center justify-between">
                    {/* Texte Français à gauche */}
                    <span className="font-bold" style={{ fontSize: '10px', width: '25%' }}>Magasin :</span>
                    {/* Valeur au centre */}
                    <span className="text-center tracking-wide" style={{ fontSize: '8px', width: '50%' }}>DEPARTEMENT MATERIEL</span>
                    {/* Texte Arabe à droite */}
                    <span className="text-right" style={{ fontFamily: 'Arial, sans-serif', fontSize: '10px', width: '25%' }}>مخزن :</span>
                  </div>
                  {/* Ligne Code avec valeur au milieu */}
                  <div className="flex items-center justify-between">
                    {/* Texte Français à gauche */}
                    <span className="font-bold" style={{ fontSize: '10px', width: '25%' }}>Code :</span>
                    {/* Valeur au centre */}
                    <span className="text-center" style={{ fontSize: '9px', width: '50%' }}>...................................</span>
                    {/* Texte Arabe à droite */}
                    <span className="text-right" style={{ fontFamily: 'Arial, sans-serif', fontSize: '10px', width: '25%' }}>الرمـــز:</span>
                  </div>
                </div>
              </td>
            </tr>
          </tbody>
        </table>

        {/* Tableau des articles - Bordures verticales gris clair */}
        <table className="w-full border-collapse border-2 border-gray-600 mb-6">
          <thead>
            <tr className="bg-gray-100">
              <th className="border-2 border-gray-600 p-2 w-[12%]" style={{ borderRightColor: '#d1d5db' }}>
                {/* Texte Arabe - Taille et Gras contrôlables */}
                <div className="font-bold" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>الرمــز</div>
                {/* Texte Français - Taille et Gras contrôlables */}
                <div className="font-bold mt-1" style={{ fontSize: '9px' }}>CODE</div>
              </th>
              <th className="border-2 border-gray-600 p-2 w-[35%]" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>
                {/* Texte Arabe - Taille et Gras contrôlables */}
                <div className="font-bold" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>التعييـــــــــــــــــن</div>
                {/* Texte Français - Taille et Gras contrôlables */}
                <div className="font-bold mt-1" style={{ fontSize: '9px' }}>DESIGNATION</div>
              </th>
              <th className="border-2 border-gray-600 p-2 w-[15%]" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>
                {/* Texte Arabe - Taille et Gras contrôlables */}
                <div className="font-bold" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>المرجـــــــــــــع</div>
                {/* Texte Français - Taille et Gras contrôlables */}
                <div className="font-bold mt-1" style={{ fontSize: '9px' }}>REFERENCE</div>
              </th>
              <th className="border-2 border-gray-600 p-2 w-[8%]" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>
                {/* Texte Arabe - Taille et Gras contrôlables */}
                <div className="font-bold" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>و.ق</div>
                {/* Texte Français - Taille et Gras contrôlables */}
                <div className="font-bold mt-1" style={{ fontSize: '9px' }}>U.M</div>
              </th>
              <th className="border-2 border-gray-600 p-2 w-[12%]" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>
                {/* Texte Arabe - Taille et Gras contrôlables */}
                <div className="font-bold" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>الكميـــــــــــــة</div>
                {/* Texte Français - Taille et Gras contrôlables */}
                <div className="font-bold mt-1" style={{ fontSize: '9px' }}>QUANTITE</div>
              </th>
              <th className="border-2 border-gray-600 p-2 w-[10%]" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>
                {/* Texte Arabe - Taille et Gras contrôlables */}
                <div className="font-bold" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>سعر الوحـــــــدة</div>
                {/* Texte Français - Taille et Gras contrôlables */}
                <div className="font-bold mt-1" style={{ fontSize: '9px' }}>P.U</div>
              </th>
              <th className="border-2 border-gray-600 p-2 w-[12%]" style={{ borderLeftColor: '#d1d5db' }}>
                {/* Texte Arabe - Taille et Gras contrôlables */}
                <div className="font-bold" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>المبلــــــــــــغ</div>
                {/* Texte Français - Taille et Gras contrôlables */}
                <div className="font-bold mt-1" style={{ fontSize: '9px' }}>MONTANT</div>
              </th>
            </tr>
          </thead>
          <tbody>
            {bon.items.map((item, index) => (
              <tr key={index} className="h-8">
                <td className="border-2 border-gray-600 p-1 text-center align-middle" style={{ borderRightColor: '#d1d5db', fontSize: '9px' }}>{item.code}</td>
                <td className="border-2 border-gray-600 p-1 align-middle" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db', fontSize: '9px' }}>{item.designation}</td>
                <td className="border-2 border-gray-600 p-1 align-middle" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}></td>
                <td className="border-2 border-gray-600 p-1 text-center align-middle" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db', fontSize: '9px' }}>{item.unite}</td>
                <td className="border-2 border-gray-600 p-1 text-center align-middle" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db', fontSize: '9px' }}>{item.quantite}</td>
                <td className="border-2 border-gray-600 p-1 align-middle" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}></td>
                <td className="border-2 border-gray-600 p-1 align-middle" style={{ borderLeftColor: '#d1d5db' }}></td>
              </tr>
            ))}
            {emptyRows > 0 && Array.from({ length: emptyRows }).map((_, index) => (
              <tr key={`empty-${index}`} className="h-8">
                <td className="border-2 border-gray-600 p-1" style={{ borderRightColor: '#d1d5db' }}>&nbsp;</td>
                <td className="border-2 border-gray-600 p-1" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>&nbsp;</td>
                <td className="border-2 border-gray-600 p-1" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>&nbsp;</td>
                <td className="border-2 border-gray-600 p-1" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>&nbsp;</td>
                <td className="border-2 border-gray-600 p-1" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>&nbsp;</td>
                <td className="border-2 border-gray-600 p-1" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>&nbsp;</td>
                <td className="border-2 border-gray-600 p-1" style={{ borderLeftColor: '#d1d5db' }}>&nbsp;</td>
              </tr>
            ))}
          </tbody>
        </table>

        {/* Section Transporteur - Divisée en 2 cases */}
        <table className="w-full border-collapse border-2 border-gray-600 mb-8">
          <tbody>
            <tr>
              {/* Case 1: Nom et prénom Transporteur */}
              <td className="border-2 border-gray-600 p-3 w-1/2 text-center" style={{ borderRightColor: '#d1d5db' }}>
                <div className="mb-1">
                  {/* Texte Arabe */}
                  <div className="font-bold mb-1" style={{ fontFamily: 'Arial, sans-serif', fontSize: '10px' }}>اسم ولقب السائق</div>
                  {/* Texte Français */}
                  <div className="font-bold" style={{ fontSize: '10px' }}>Nom et prénom Transporteur :</div>
                </div>
                {/* Valeur */}
                <div className="mt-2 border-b border-gray-400 pb-1" style={{ fontSize: '9px' }}>
                  {bon.transporteur_nom || '....................................................'}
                </div>
              </td>
              {/* Case 2: Immatriculation */}
              <td className="border-2 border-gray-600 p-3 w-1/2 text-center" style={{ borderLeftColor: '#d1d5db' }}>
                <div className="mb-1">
                  {/* Texte Arabe */}
                  <div className="font-bold mb-1" style={{ fontFamily: 'Arial, sans-serif', fontSize: '10px' }}>رقم التسجيــــــــــل</div>
                  {/* Texte Français */}
                  <div className="font-bold" style={{ fontSize: '10px' }}>Immatriculation :</div>
                </div>
                {/* Valeur */}
                <div className="mt-2 border-b border-gray-400 pb-1" style={{ fontSize: '9px' }}>
                  {bon.transporteur_immatriculation || '....................................................'}
                </div>
              </td>
            </tr>
          </tbody>
        </table>

        {/* Visas - 4 cases individuelles */}
        <table className="w-full border-collapse border-2 border-gray-600 mt-2">
          <thead>
            <tr>
              {/* Case 1: Visa du Magasinier */}
              <th className="border-2 border-gray-600 p-2 w-1/4" style={{ borderRightColor: '#d1d5db' }}>
                <div className="text-center">
                  {/* Texte Arabe - Taille et Gras contrôlables */}
                  <div className="font-bold mb-1" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>تأشيرة أمين المخزن</div>
                  {/* Texte Français - Taille et Gras contrôlables */}
                  <div className="font-bold" style={{ fontSize: '9px' }}>Visa du Magasinier</div>
                </div>
              </th>
              {/* Case 2: Visa du Chef d'Unité de Production */}
              <th className="border-2 border-gray-600 p-2 w-1/4" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>
                <div className="text-center">
                  {/* Texte Arabe - Taille et Gras contrôlables */}
                  <div className="font-bold mb-1" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>تأشيرة رئيس وحدة الإنتاج</div>
                  {/* Texte Français - Taille et Gras contrôlables */}
                  <div className="font-bold" style={{ fontSize: '9px' }}>Visa du Chef d'Unité de Production</div>
                </div>
              </th>
              {/* Case 3: Visa Client */}
              <th className="border-2 border-gray-600 p-2 w-1/4" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db' }}>
                <div className="text-center">
                  {/* Texte Arabe - Taille et Gras contrôlables */}
                  <div className="font-bold mb-1" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>تأشيرة الزبون</div>
                  {/* Texte Français - Taille et Gras contrôlables */}
                  <div className="font-bold" style={{ fontSize: '9px' }}>Visa Client</div>
                </div>
              </th>
              {/* Case 4: Visa du Transporteur */}
              <th className="border-2 border-gray-600 p-2 w-1/4" style={{ borderLeftColor: '#d1d5db' }}>
                <div className="text-center">
                  {/* Texte Arabe - Taille et Gras contrôlables */}
                  <div className="font-bold mb-1" style={{ fontFamily: 'Arial, sans-serif', fontSize: '9px' }}>تأشيرة السائق</div>
                  {/* Texte Français - Taille et Gras contrôlables */}
                  <div className="font-bold" style={{ fontSize: '9px' }}>Visa du Transporteur</div>
                </div>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr>
              {/* Espace pour signature 1 */}
              <td className="border-2 border-gray-600 p-2" style={{ borderRightColor: '#d1d5db', height: '80px' }}>
                &nbsp;
              </td>
              {/* Espace pour signature 2 */}
              <td className="border-2 border-gray-600 p-2" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db', height: '80px' }}>
                &nbsp;
              </td>
              {/* Espace pour signature 3 */}
              <td className="border-2 border-gray-600 p-2" style={{ borderRightColor: '#d1d5db', borderLeftColor: '#d1d5db', height: '80px' }}>
                &nbsp;
              </td>
              {/* Espace pour signature 4 */}
              <td className="border-2 border-gray-600 p-2" style={{ borderLeftColor: '#d1d5db', height: '80px' }}>
                &nbsp;
              </td>
            </tr>
          </tbody>
        </table>

      </div>

      <style jsx global>{`
        @media print {
          body * { visibility: hidden; }
          .print\\:hidden { display: none !important; }
          #print-section, #print-section * { visibility: visible; }
          #print-section { 
            position: absolute; 
            left: 0; 
            top: 0; 
            width: 210mm;
            margin: 0 auto;
            padding: 15mm;
            border: none;
            box-shadow: none;
            background: white;
          }
          @page { 
            size: A4; 
            margin: 10mm; 
          }
        }
      `}</style>
    </div>
  );
}

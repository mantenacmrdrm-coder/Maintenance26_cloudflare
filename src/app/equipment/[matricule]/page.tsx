import { notFound } from 'next/navigation';

export const dynamic = 'force-dynamic';
import {
  getEquipmentDetails,
  getPreventativeHistoryForEquipment,
  getCurativeHistoryForEquipment,
  getEquipmentDynamicStatus,
} from '@/lib/data';
import { EquipmentDetailClientView } from './equipment-detail-client-view';
import type { Equipment, Operation, PreventativeMaintenanceEntry, CurativeMaintenanceEntry } from '@/lib/types';
import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat';

dayjs.extend(customParseFormat);

type Props = {
  params: Promise<{ matricule: string }>;
};

export default async function EquipmentDetailPage({ params }: Props) {
  const resolvedParams = await params;
  const { matricule } = resolvedParams;
  
  const [equipment, preventativeHistory, curativeHistory, dynamicStatus] = await Promise.all([
    getEquipmentDetails(matricule),
    getPreventativeHistoryForEquipment(matricule),
    getCurativeHistoryForEquipment(matricule),
    getEquipmentDynamicStatus(matricule)
  ]);

  if (!equipment) {
    notFound();
  }

  // --- Construct a comprehensive `operations` list ---
  const preventativeOperations: Operation[] = Object.values(preventativeHistory)
    .flat()
    .map((entry, index) => ({
      id: -1 * (index + 1), // Generate a unique negative ID to avoid clashes with curative IDs
      matricule: matricule,
      operation: entry.operation,
      date_programmee: entry.date,
      date_realisation: entry.date,
      nature: 'Réalisé', // Preventative history is always considered 'realisé'
      niveau: 'Préventif',
    }));

  const curativeOperations: Operation[] = curativeHistory.map(entry => ({
    id: entry.id, // Use the existing unique ID
    matricule: matricule,
    operation: entry.panneDeclaree,
    date_programmee: entry.dateEntree,
    date_realisation: entry.dateSortie,
    nature: entry.typePanne,
    niveau: 'Curatif',
  }));

  const operations: Operation[] = [...preventativeOperations, ...curativeOperations].sort(
    (a, b) => dayjs(b.date_programmee, 'DD/MM/YYYY').valueOf() - dayjs(a.date_programmee, 'DD/MM/YYYY').valueOf()
  );


  return (
    <EquipmentDetailClientView
      equipment={equipment as Equipment}
      operations={operations}
      preventativeHistory={preventativeHistory as Record<string, PreventativeMaintenanceEntry[]>}
      curativeHistory={curativeHistory as CurativeMaintenanceEntry[]}
      dynamicStatus={dynamicStatus as 'En Marche' | 'En Panne' | 'Actif'}
    />
  );
}

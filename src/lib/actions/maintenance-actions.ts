
'use server';

import { 
  initializeDatabase as initDb, 
  generateHistoryMatrix, 
  generatePlanning as runPlanning,
  getPlanningPage as dataGetPlanningPage,
  getFollowUpPage as dataGetFollowUpPage,
  getPlanningMatrixForExport,
  getFollowUpMatrixForExport,
  getHistoryMatrixFromCache,
  getParams as dataGetParams,
  updateParam as dataUpdateParam,
  getAllPlanningForYear,
  getFollowUpStatistics as dataGetFollowUpStatistics,
  getAllOperations,
  getDistinctCategories as dataGetDistinctCategories,
  getCategoryEntretiens as dataGetCategoryEntretiens,
  updateCategoryEntretiens as dataUpdateCategoryEntretiens,
  addCurativeOperationToDb,
  generateAndSaveWeeklyReport as generateAndSaveWeeklyReportData,
  getWeeklyReports as getWeeklyReportsData,
  getWeeklyReport as getWeeklyReportData,
  getMonthlyCurativeCounts,
  getMonthlyPreventativeStats,
  deleteWeeklyReport as deleteWeeklyReportFromDb,
  getOperationById,
  saveDeclaration as saveDeclarationToDb,
  getDeclarationById,
  getDeclarationsList as getDeclarationsListData,
  deleteDeclarationFromDb as deleteDeclaration,
  updateDeclarationInDb as updateDeclaration,
  getEquipmentById as dataGetEquipmentById,
  addEquipmentToDb,
  updateEquipmentInDb,
  deleteEquipmentFromDb,
  getCurativeFiches as getCurativeFichesFromDb,
  getOrdresDeTravail as getOrdresDeTravailFromDb,
  getPreventiveFichesFromDb,
  addConsolideEntries,
  getConsolideByDateRange,
  addStockEntryToDb,
  getStockEntriesFromDb,
  deleteStockEntryFromDb,
  getDailyConsumptionReportData,
  getMonthlyStockReportData,
  getEquipmentDetails,
  deleteOperationFromDb,
  updateOperationInDb,
  getEquipmentForConsumption as getEquipmentForConsumptionData,
  getConsumptionById,
  updateConsumption,
  deleteConsumption,
  saveBonDeSortie as saveBonToDb,
  getBonsDeSortieList as getBonsListFromDb,
  getBonDeSortieById as getBonFromDb,
  updateBonDeSortie as updateBonInDb,
  deleteBonDeSortie as deleteBonFromDb,
  reinitializeTableFromCsv as reinitializeTable,
  getLastStockEntryDate as getLastEntryDateFromDb,
  getEquipmentCount,
  getOperationCountForYear,
  getRecentOperations,
} from '../data';
import type { Alert, BonDeSortie, CurativeFicheData, DeclarationPanne, OrdreTravailData, PreventiveFicheData, StockEntry, WeeklyReport, MonthlyStockReportData } from '../types';
import { revalidatePath } from 'next/cache';
import dayjs from 'dayjs';
import isSameOrBefore from 'dayjs/plugin/isSameOrBefore';
import isBetween from 'dayjs/plugin/isBetween';
import { z } from 'zod';
import { redirect } from 'next/navigation';
import { LUBRICANT_TYPES } from '../constants';
import 'dayjs/locale/fr';
import fs from 'fs/promises';
import path from 'node:path';


dayjs.locale('fr');
dayjs.extend(isSameOrBefore);
dayjs.extend(isBetween);


export async function getPreventativeAlerts({ startDate, endDate, entretiens, niveau, matricule }: { startDate: Date; endDate: Date; entretiens?: string[]; niveau?: string; matricule?: string; }): Promise<Alert[]> {
  try {
    const startYear = startDate.getFullYear();
    const endYear = endDate.getFullYear();
    
    // Fetch for all relevant years
    const years = Array.from(new Set([startYear, endYear]));
    let plannedOperations: any[] = [];
    for (const year of years) {
        const yearOps = await getAllPlanningForYear(year);
        plannedOperations.push(...yearOps);
    }
    
    const today = dayjs().startOf('day');
    const startDayjs = dayjs(startDate).startOf('day');
    const endDayjs = dayjs(endDate).endOf('day');

    const curativeOps = await getAllOperations();
    const breakdownIntervals = new Map<string, { start: dayjs.Dayjs, end: dayjs.Dayjs }[]>();

    for (const op of curativeOps) {
        if (!op.matricule || !op.date_entree) continue;
        const start = dayjs(op.date_entree, 'DD/MM/YYYY');
        let end: dayjs.Dayjs;
        if (op.date_sortie && op.date_sortie.toLowerCase().includes('cour')) {
            end = dayjs();
        } else {
            end = dayjs(op.date_sortie, 'DD/MM/YYYY');
        }

        if (start.isValid() && end.isValid() && start.isSameOrBefore(end)) {
            if (!breakdownIntervals.has(op.matricule)) {
                breakdownIntervals.set(op.matricule, []);
            }
            breakdownIntervals.get(op.matricule)!.push({ start, end });
        }
    }
    
    const alerts: Alert[] = [];

    for (const op of plannedOperations) {
        // Filtre par type d'entretien si spécifié
        if (entretiens && entretiens.length > 0 && !entretiens.includes(op.operation)) {
            continue;
        }

        const dueDate = dayjs(op.date_programmee, 'DD/MM/YYYY');
        
        // Ignorer les dates hors de la fenêtre sélectionnée
        if (!dueDate.isValid() || !dueDate.isBetween(startDayjs, endDayjs, null, '[]')) {
            continue;
        }
        
        // Nouveaux filtres
        if (niveau && niveau !== 'all' && op.niveau !== niveau) {
            continue;
        }
        if (matricule && op.matricule && !op.matricule.toLowerCase().includes(matricule.toLowerCase())) {
            continue;
        }

        const urgency = dueDate.isBefore(today) ? 'urgent' : 'near';

        let status: string | undefined;
        const intervals = breakdownIntervals.get(op.matricule);
        if (intervals) {
            for (const interval of intervals) {
                if (dueDate.isBetween(interval.start, interval.end, 'day', '[]')) {
                    status = 'En Panne';
                    break;
                }
            }
        }

        alerts.push({
            equipmentId: op.matricule,
            equipmentDesignation: op.designation,
            operation: op.operation,
            dueDate: op.date_programmee,
            urgency: urgency,
            niveau: op.niveau,
            status: status,
        });
    }
    
    const sortedAlerts = alerts.sort((a, b) => {
        const dateA = dayjs(a.dueDate, 'DD/MM/YYYY').unix();
        const dateB = dayjs(b.dueDate, 'DD/MM/YYYY').unix();
        return dateA - dateB;
    });

    return sortedAlerts;

  } catch (error: any) {
    console.error('Error generating preventative alerts:', error);
    throw new Error(error.message || 'Failed to generate alerts.');
  }
}



//import fs from 'fs/promises';

// ... tes autres fonctions ...

export async function restoreDatabase(formData: FormData) {
    const file = formData.get('file') as File;

    if (!file) {
        return { success: false, message: "Aucun fichier sélectionné." };
    }

    if (!file.name.endsWith('.db')) {
        return { success: false, message: "Le fichier doit être un .db" };
    }

    // --- CORRECTION ICI : Détection intelligente du chemin ---
    const isHuggingFace = process.env.HOME === '/root';
    const dbPath = isHuggingFace ? '/data/gmao_data.db' : path.join(process.cwd(), 'gmao_data.db');
    
    // Assurer que le dossier existe (utile pour Windows)
    await fs.mkdir(path.dirname(dbPath), { recursive: true });
    
    const backupPath = isHuggingFace ? '/data/gmao_data.db.backup_before_restore' : path.join(process.cwd(), 'gmao_data.db.backup');

    try {
        // 1. Sécurité : On fait une sauvegarde de l'actuel
        try {
            await fs.copyFile(dbPath, backupPath);
        } catch (e) {
            // Pas grave si le fichier n'existe pas encore
        }

        // 2. Conversion et écriture
        const arrayBuffer = await file.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        await fs.writeFile(dbPath, buffer);

        return { 
            success: true, 
            message: "Base de données restaurée avec succès. Une sauvegarde de l'ancienne version a été conservée." 
        };

    } catch (error: any) {
        console.error("Erreur restauration BD:", error);
        
        // Tentative de restauration du backup
        try {
            if (await fs.access(backupPath).then(() => true).catch(() => false)) {
                await fs.copyFile(backupPath, dbPath);
                return { success: false, message: `Échec de l'import. L'ancienne base a été restaurée. Erreur: ${error.message}` };
            }
        } catch (rollbackError) {
            // Rien à faire
        }

        return { success: false, message: `Erreur critique lors de l'import : ${error.message}` };
    }
}

export async function initializeDatabase() {
    try {
        return await initDb();
    } catch(e:any) {
        console.error("Erreur lors de l'initialisation de la base de données", e);
        return { success: false, message: e.message };
    }
}


export async function runHistoryGeneration() {
    console.log("Démarrage de la génération d'historique...");
    try {
        const result = await generateHistoryMatrix();
        return result;
    } catch(e: any) {
        console.error("Erreur lors de l'exécution de runHistoryGeneration", e);
        throw new Error("Impossible de générer l'historique : " + e.message);
    }
}

export async function getHistory() {
    try {
        const result = await getHistoryMatrixFromCache();
        return result;
    } catch (e: any) {
        console.error("Erreur lors de la récupération de l'historique depuis le cache", e);
        return { headers: [], rows: [], counts: {} };
    }
}

export async function generatePlanning(year: number) {
  const result = await runPlanning(year);
  revalidatePath('/planning');
  revalidatePath('/suivi');
  return result;
}

export async function getAllPlanningForExport(year: number) {
    return await getPlanningMatrixForExport(year);
}

export async function getAllFollowUpForExport(year: number) {
    return await getFollowUpMatrixForExport(year);
}

export async function getParams() {
    return dataGetParams();
}

export async function updateParam(id: number, column: string, value: string | null) {
    const result = await dataUpdateParam(id, column, value);
    revalidatePath('/parameters');
    revalidatePath('/planning'); // Also revalidate planning as data might have changed
    return result;
}

export async function getDashboardData(year?: number, month?: number) {
    const targetYear = year || new Date().getFullYear();
    const isCurrentYear = targetYear === new Date().getFullYear();
    try {
        const [
            equipmentCount,
            recentOperations,
            operationCountForYear,
            followUpStats,
            monthlyCounts,
            preventativeStats
        ] = await Promise.all([
            getEquipmentCount(),
            getRecentOperations(5),
            getOperationCountForYear(targetYear),
            dataGetFollowUpStatistics(targetYear),
            getMonthlyCurativeCounts(targetYear),
            getMonthlyPreventativeStats(targetYear, month)
        ]);

        let breakdownsThisMonth: number | null = null;
        if (month) {
            const monthName = dayjs().month(month - 1).format('MMM');
            breakdownsThisMonth = monthlyCounts.find(m => m.month === monthName)?.count ?? 0;
        } else if (isCurrentYear) {
            const currentMonthName = dayjs().format('MMM');
            breakdownsThisMonth = monthlyCounts.find(m => m.month === currentMonthName)?.count ?? 0;
        }

        return {
            equipmentCount: equipmentCount || 0,
            operationCount: operationCountForYear,
            followUpStats: followUpStats,
            monthlyCounts: monthlyCounts,
            preventativeStats: preventativeStats,
            recentOperations: recentOperations,
            error: null,
            breakdownsThisMonth,
            year: targetYear,
            month: month,
        };
    } catch (error: any) {
        console.error("Error fetching dashboard data:", error);
        return {
            equipmentCount: 0,
            operationCount: 0,
            followUpStats: null,
            monthlyCounts: [],
            preventativeStats: { monthlyData: [], totalOil: 0, oilByType: {} },
            recentOperations: [],
            error: "Impossible de charger les données du tableau de bord.",
            breakdownsThisMonth: null,
            year: targetYear,
            month: month,
        };
    }
}

export async function getDistinctCategories() {
    return dataGetDistinctCategories();
}

export async function getCategoryEntretiens() {
    return dataGetCategoryEntretiens();
}

export async function updateCategoryEntretiens(category: string, entretien: string, isActive: boolean) {
    const result = await dataUpdateCategoryEntretiens(category, entretien, isActive);
    revalidatePath('/parameters');
    revalidatePath('/planning');
    return result;
}


const curativeOperationSchema = z.object({
  matricule: z.string().min(1, { message: 'Le matricule est obligatoire.' }),
  dateEntree: z.string().min(1, { message: "La date d'entrée est obligatoire." }),
  panneDeclaree: z.string().min(1, { message: 'La déclaration de panne est obligatoire.' }),
  sitActuelle: z.enum(['En Cours', 'Réparée', 'Dépanné'], {
    required_error: 'Le statut actuel est obligatoire.',
  }),
  pieces: z.string().optional(),
  dateSortie: z.string().optional(),
  intervenant: z.string().optional(),
  affectation: z.string().optional(),
}).refine(data => {
    if (data.sitActuelle === 'Réparée' && (!data.dateSortie || data.dateSortie.trim() === '')) {
        return false;
    }
    return true;
}, {
    message: 'La date de sortie est obligatoire si le statut est "Réparée".',
    path: ['dateSortie'],
});

// Helper function for working days
function calculateWorkingDays(startDate: dayjs.Dayjs, endDate: dayjs.Dayjs): number {
    let count = 0;
    let currentDate = startDate.clone();
    while (currentDate.isBefore(endDate, 'day') || currentDate.isSame(endDate, 'day')) {
        const dayOfWeek = currentDate.day(); // Sunday = 0, ..., Saturday = 6
        if (dayOfWeek !== 5 && dayOfWeek !== 6) { // Not Friday or Saturday
            count++;
        }
        currentDate = currentDate.add(1, 'day');
    }
    return count;
}

// Helper for panne type
function determinePanneType(panneDeclaree: string, pieces: string): 'mécanique' | 'électrique' | 'autres' {
    const combinedText = (panneDeclaree + ' ' + pieces).toLowerCase();
    const electricKeywords = ['électrique', 'marir,r', 'batterie', 'alternateur', 'demarreur', 'faisceau'];
    
    if (electricKeywords.some(kw => combinedText.includes(kw))) {
        return 'électrique';
    }
    if (combinedText.includes('pneu')) {
        return 'autres';
    }
    return 'mécanique';
}


export async function addCurativeOperation(values: unknown) {
  const parsed = curativeOperationSchema.safeParse(values);
  if (!parsed.success) {
    return { success: false, message: 'Données du formulaire invalides.' };
  }
  const data = parsed.data;

  try {
    const dateEntree = data.dateEntree;
    let dateSortieString: string;

    if (data.sitActuelle === 'Réparée' && data.dateSortie) {
        dateSortieString = data.dateSortie;
    } else {
        dateSortieString = 'En Cours';
    }
    
    const dateEntreeDayjs = dayjs(dateEntree, 'DD/MM/YYYY');
    const dateSortieDayjs = (data.sitActuelle === 'Réparée' && data.dateSortie) 
      ? dayjs(data.dateSortie, 'DD/MM/YYYY') 
      : dayjs();
    
    // NBR INDISPONIBILITE
    const diff = dateSortieDayjs.diff(dateEntreeDayjs, 'day');
    const nbrIndisponibilite = diff >= 0 ? diff + 1 : 1;

    // JOUR OUVRABLE and RATIOs
    const monthStartDate = dateEntreeDayjs.startOf('month');
    const monthEndDate = dateEntreeDayjs.endOf('month');
    const totalWorkingDaysInMonth = calculateWorkingDays(monthStartDate, monthEndDate);
    const jourOuvrable = totalWorkingDaysInMonth > 0 ? totalWorkingDaysInMonth : 22; // fallback

    const ratio = nbrIndisponibilite / jourOuvrable;
    const jourDisponibilite = jourOuvrable - nbrIndisponibilite;
    const ratio2 = jourDisponibilite / jourOuvrable;
    
    // TYPE DE PANNE
    const typeDePanne = determinePanneType(data.panneDeclaree, data.pieces || '');

    const operationData = {
        matricule: data.matricule,
        date_entree: dateEntree,
        panne_declaree: data.panneDeclaree,
        sitactuelle: data.sitActuelle,
        pieces: data.pieces || null,
        date_sortie: dateSortieString,
        intervenant: data.intervenant || null,
        affectation: data.affectation || null,
        type_de_panne: typeDePanne,
        nbr_indisponibilite: nbrIndisponibilite,
        jour_ouvrable: jourOuvrable,
        ratio: isFinite(ratio) ? ratio : 0,
        jour_disponibilite: jourDisponibilite,
        ratio2: isFinite(ratio2) ? ratio2 : 0,
    };

    const result = await addCurativeOperationToDb(operationData);

    revalidatePath('/operations');
    revalidatePath(`/equipment/${data.matricule}`);
    revalidatePath('/declarations/select-operation');
    revalidatePath('/');
    
    return { success: true, message: 'Opération ajoutée.', data: result };

  } catch (error: any) {
    console.error('Failed to add curative operation:', error);
    return { success: false, message: error.message || 'Une erreur serveur est survenue.' };
  }
}

export async function generateWeeklyReportAction(targetDate: Date): Promise<{ success: boolean; reportId?: number; message?: string; }> {
  if (!targetDate) {
    return { success: false, message: 'La date cible est requise.' };
  }
  try {
    const reportId = await generateAndSaveWeeklyReportData(targetDate);
    revalidatePath('/reports');
    return { success: true, reportId: reportId };
  } catch (error: any) {
    console.error("Failed to generate weekly report:", error);
    return { success: false, message: error.message || 'La génération du rapport a échoué.' };
  }
}

export async function getWeeklyReports() {
    return await getWeeklyReportsData();
}

export async function getWeeklyReport(id: number) {
    return await getWeeklyReportData(id);
}

export async function deleteWeeklyReportAction(id: number) {
    try {
        await deleteWeeklyReportFromDb(id);
        revalidatePath('/reports');
        return { success: true };
    } catch (error: any) {
        console.error(`Failed to delete report action for id ${id}`, error);
        return { success: false, message: error.message || 'Impossible de supprimer le rapport.' };
    }
}


export async function getOperationForDeclaration(operationId: number) {
    const operation = await getOperationById(operationId);
    if (!operation) return null;
    return operation;
}

export async function saveDeclarationAction(operationId: number, data: any) {
    try {
        const declarationId = await saveDeclarationToDb(operationId, JSON.stringify(data));
        revalidatePath('/declarations');
        revalidatePath('/declarations/select-operation');
        return { success: true, declarationId };
    } catch (error: any) {
        console.error("Failed to save declaration:", error);
        return { success: false, message: error.message };
    }
}

export async function getDeclaration(declarationId: number): Promise<DeclarationPanne | null> {
    const declaration = await getDeclarationById(declarationId);
    return declaration;
}

export async function getDeclarationsListAction() {
    return getDeclarationsListData();
}

export async function deleteDeclarationAction(id: number) {
    try {
        await deleteDeclaration(id);
        revalidatePath('/declarations');
        revalidatePath('/declarations/select-operation');
        return { success: true };
    } catch (error: any) {
        console.error(`Failed to delete declaration with id ${id}`, error);
        return { success: false, message: error.message || 'Impossible de supprimer la déclaration.' };
    }
}

export async function updateDeclarationAction(declarationId: number, data: any) {
    try {
        await updateDeclaration(declarationId, JSON.stringify(data));
        revalidatePath('/declarations');
        revalidatePath(`/declarations/view/${declarationId}`);
        return { success: true, declarationId };
    } catch (error: any) {
        console.error("Failed to update declaration:", error);
        return { success: false, message: error.message };
    }
}

const equipmentSchema = z.object({
  matricule: z.string().min(1, "Le matricule est obligatoire."),
  designation: z.string().min(1, "La désignation est obligatoire."),
  marque: z.string().optional().transform(v => v === '' ? null : v),
  categorie: z.string().optional().transform(v => v === '' ? null : v),
  annee: z.string().optional().transform(v => v === '' ? null : v),
  qte_vidange: z.coerce.number().min(0).optional().transform(v => v || null),
  code_barre: z.string().optional().transform(v => v === '' ? null : v),
  pneumatique: z.string().optional().transform(v => v === '' ? null : v),
});

export async function getEquipmentByIdAction(id: number) {
    return dataGetEquipmentById(id);
}

export async function addEquipmentAction(values: unknown) {
  const parsed = equipmentSchema.safeParse(values);
  if (!parsed.success) {
    return { success: false, message: 'Données du formulaire invalides.' };
  }
  
  try {
    const result = await addEquipmentToDb(parsed.data);
    revalidatePath('/equipment');
    revalidatePath('/'); // For stats on dashboard
    return { success: true, equipmentId: result.id };
  } catch (error: any) {
    console.error('Failed to add equipment:', error);
    return { success: false, message: error.message || 'Une erreur serveur est survenue.' };
  }
}

export async function updateEquipmentAction(id: number, values: unknown) {
  const parsed = equipmentSchema.safeParse(values);
  if (!parsed.success) {
    return { success: false, message: 'Données du formulaire invalides.' };
  }

  try {
    const { matriculeChanged } = await updateEquipmentInDb(id, parsed.data);
    
    // Revalidate all relevant paths
    revalidatePath('/equipment');
    revalidatePath(`/equipment/${parsed.data.matricule}`);
    revalidatePath('/');
    revalidatePath('/history');
    revalidatePath('/planning');
    revalidatePath('/suivi');
    revalidatePath('/alerts');
    
    const message = matriculeChanged
      ? "L'équipement a été mis à jour. Le matricule a changé, l'historique a été migré. Veuillez regénérer l'historique et le planning."
      : "L'équipement a été mis à jour avec succès.";

    return { success: true, message, matricule: parsed.data.matricule, matriculeChanged };
  } catch (error: any) {
    console.error('Failed to update equipment:', error);
    return { success: false, message: error.message || 'Une erreur serveur est survenue.' };
  }
}

export async function deleteEquipmentAction(id: number) {
  try {
    await deleteEquipmentFromDb(id);
    revalidatePath('/equipment');
    revalidatePath('/');
    return { success: true };
  } catch (error: any) {
    console.error(`Failed to delete equipment action for id ${id}`, error);
    return { success: false, message: 'Impossible de supprimer l\'équipement.' };
  }
}

const getDocsSchema = z.object({
  startDate: z.string(),
  endDate: z.string(),
  matricule: z.string().optional(),
});

export async function getCurativeFichesAction(values: unknown): Promise<{ success: boolean; data?: CurativeFicheData[]; message?: string; }> {
  const parsed = getDocsSchema.safeParse(values);
  if (!parsed.success) {
    return { success: false, message: 'Données du formulaire invalides.' };
  }

  try {
    const data = await getCurativeFichesFromDb(parsed.data.startDate, parsed.data.endDate, parsed.data.matricule);
    return { success: true, data };
  } catch (error: any) {
    console.error("Failed to get curative fiches:", error);
    return { success: false, message: error.message || 'Une erreur serveur est survenue.' };
  }
}

export async function getOrdresDeTravailAction(values: unknown): Promise<{ success: boolean; data?: OrdreTravailData[]; message?: string; }> {
  const parsed = getDocsSchema.safeParse(values);
  if (!parsed.success) {
    return { success: false, message: 'Données du formulaire invalides.' };
  }

  try {
    const data = await getOrdresDeTravailFromDb(parsed.data.startDate, parsed.data.endDate, parsed.data.matricule);
    return { success: true, data };
  } catch (error: any) {
    console.error("Failed to get ordres de travail:", error);
    return { success: false, message: error.message || 'Une erreur serveur est survenue.' };
  }
}

export async function getPreventiveFichesAction(values: unknown): Promise<{ success: boolean; data?: PreventiveFicheData[]; message?: string; }> {
  const parsed = getDocsSchema.safeParse(values);
  if (!parsed.success) {
    return { success: false, message: 'Données du formulaire invalides.' };
  }

  try {
    const data = await getPreventiveFichesFromDb(parsed.data.startDate, parsed.data.endDate, parsed.data.matricule);
    return { success: true, data };
  } catch (error: any) {
    console.error("Failed to get preventive fiches:", error);
    return { success: false, message: error.message || 'Une erreur serveur est survenue.' };
  }
}

const consumptionEntrySchema = z.object({
    matricule: z.string(),
    designation: z.string().optional(),
    qte_vidange: z.coerce.number().nullable().optional(),
    obs: z.string().optional(),
    entretien: z.string().optional(),
    lubricants: z.record(z.number()),
});

const consumptionsSchema = z.object({
    date: z.coerce.date(),
    entries: z.array(consumptionEntrySchema),
});

export async function addConsumptionsAction(values: unknown) {
  const parsed = consumptionsSchema.safeParse(values);
  if (!parsed.success) {
    console.error('Zod parsing error:', parsed.error.flatten());
    return { success: false, message: 'Données du formulaire invalides.' };
  }

  const { date, entries } = parsed.data;

  if (entries.length === 0) {
      return { success: false, message: 'Aucune consommation à enregistrer.'};
  }

  try {
    const dateString = dayjs(date).format('DD/MM/YYYY');
    const result = await addConsolideEntries(dateString, entries);
    
    revalidatePath('/stock');
    revalidatePath('/consommations');
    revalidatePath('/history');
    revalidatePath('/'); // For dashboard stats

    return { success: true, message: `${result.count} consommation(s) enregistrée(s).` };

  } catch (error: any) {
    console.error('Failed to add consumptions:', error);
    return { success: false, message: error.message || 'Une erreur serveur est survenue.' };
  }
}
    
export async function getConsumptionsByDateRange(startDate: Date, endDate: Date) {
  try {
    const { consumptions, summary, initialStockSummary } = await getConsolideByDateRange(startDate, endDate);
    return { success: true, consumptions, summary, initialStockSummary };
  } catch (error: any) {
    console.error("Failed to get consumptions by date range:", error);
    return { success: false, message: error.message };
  }
}

const stockEntrySchema = z.object({
    date: z.date(),
    lubricant_type: z.string().min(1),
    quantity: z.coerce.number().gt(0),
    reference: z.string().optional(),
});

export async function addStockEntryAction(values: unknown) {
    const parsed = stockEntrySchema.safeParse(values);
    if (!parsed.success) {
        return { success: false, message: 'Données du formulaire invalides.' };
    }

    try {
        const result = await addStockEntryToDb({
            ...parsed.data,
            date: dayjs(parsed.data.date).format('YYYY-MM-DD')
        });
        revalidatePath('/stock-entries');
        revalidatePath('/consommations');
        return { success: true, ...result };
    } catch (e: any) {
        return { success: false, message: e.message };
    }
}

export async function getStockEntriesAction() {
    return getStockEntriesFromDb();
}

export async function deleteStockEntryAction(id: number) {
    try {
        await deleteStockEntryFromDb(id);
        revalidatePath('/stock-entries');
        revalidatePath('/consommations');
        return { success: true };
    } catch (e: any) {
        return { success: false, message: e.message };
    }
}

export async function getDailyConsumptionReportAction({ year, month, lubricantType }: { year: number, month: number, lubricantType: string }) {
    try {
        const data = await getDailyConsumptionReportData({ year, month, lubricantType });
        return { success: true, data };
    } catch (error: any) {
        console.error("Failed to get daily consumption report:", error);
        return { success: false, message: error.message };
    }
}

export async function getMonthlyStockReportAction(params: {
  startYear: number;
  startMonth: number;
  endYear: number;
  endMonth: number;
  lubricantType: string;
}): Promise<{ success: boolean; data?: MonthlyStockReportData; message?: string; }> {
    try {
        const data = await getMonthlyStockReportData(params);
        return { success: true, data };
    } catch (error: any) {
        console.error("Failed to get monthly stock report:", error);
        return { success: false, message: error.message };
    }
}

export async function deleteOperationAction(id: number) {
    try {
        await deleteOperationFromDb(id);
        revalidatePath('/operations');
        revalidatePath('/history');
        revalidatePath('/');
        return { success: true };
    } catch (error: any) {
        console.error(`Failed to delete operation action for id ${id}`, error);
        return { success: false, message: 'Impossible de supprimer l\'opération.' };
    }
}

export async function getOperationByIdAction(id: number) {
    return getOperationById(id);
}

export async function updateOperationAction(id: number, values: unknown) {
  const parsed = curativeOperationSchema.safeParse(values);
  if (!parsed.success) {
    return { success: false, message: 'Données du formulaire invalides.' };
  }
  const data = parsed.data;

  try {
    const dateEntree = data.dateEntree;
    let dateSortieString: string;

    if (data.sitActuelle === 'Réparée' && data.dateSortie) {
        dateSortieString = data.dateSortie;
    } else {
        dateSortieString = 'En Cours';
    }
    
    const dateEntreeDayjs = dayjs(dateEntree, 'DD/MM/YYYY');
    const dateSortieDayjs = (data.sitActuelle === 'Réparée' && data.dateSortie) 
        ? dayjs(data.dateSortie, 'DD/MM/YYYY')
        : dayjs();

    const diff = dateSortieDayjs.diff(dateEntreeDayjs, 'day');
    const nbrIndisponibilite = diff >= 0 ? diff + 1 : 1;

    const monthStartDate = dateEntreeDayjs.startOf('month');
    const monthEndDate = dateEntreeDayjs.endOf('month');
    const totalWorkingDaysInMonth = calculateWorkingDays(monthStartDate, monthEndDate);
    const jourOuvrable = totalWorkingDaysInMonth > 0 ? totalWorkingDaysInMonth : 22;
    const ratio = nbrIndisponibilite / jourOuvrable;
    const jourDisponibilite = jourOuvrable - nbrIndisponibilite;
    const ratio2 = jourDisponibilite / jourOuvrable;
    const typeDePanne = determinePanneType(data.panneDeclaree, data.pieces || '');
    
    const equipment = await getEquipmentDetails(data.matricule);
    if (!equipment) {
        return { success: false, message: `Équipement avec matricule ${data.matricule} non trouvé.` };
    }

    const operationData = {
        matricule: data.matricule,
        date_entree: dateEntree,
        panne_declaree: data.panneDeclaree,
        sitactuelle: data.sitActuelle,
        pieces: data.pieces || null,
        date_sortie: dateSortieString,
        intervenant: data.intervenant || null,
        affectation: data.affectation || null,
        type_de_panne: typeDePanne,
        nbr_indisponibilite: nbrIndisponibilite,
        jour_ouvrable: jourOuvrable,
        ratio: isFinite(ratio) ? ratio : 0,
        jour_disponibilite: jourDisponibilite,
        ratio2: isFinite(ratio2) ? ratio2 : 0,
        categorie: equipment.categorie,
        designation: equipment.designation
    };

    const result = await updateOperationInDb(id, operationData);

    revalidatePath('/operations');
    revalidatePath(`/equipment/${data.matricule}`);
    revalidatePath('/history');
    revalidatePath('/');
    
    return { success: true, message: 'Opération mise à jour.', data: result };

  } catch (error: any) {
    console.error('Failed to update curative operation:', error);
    return { success: false, message: error.message || 'Une erreur serveur est survenue.' };
  }
}

export async function getEquipmentForConsumptionAction() {
    return getEquipmentForConsumptionData();
}

export async function getConsumptionByIdAction(id: number) {
    return getConsumptionById(id);
}

export async function deleteConsumptionAction(id: number) {
    try {
        await deleteConsumption(id);
        revalidatePath('/consommations');
        revalidatePath('/history');
        revalidatePath('/');
        return { success: true };
    } catch (error: any) {
        return { success: false, message: error.message || 'Impossible de supprimer la consommation.' };
    }
}

const editConsumptionSchema = z.object({
    date: z.coerce.date(),
    matricule: z.string().min(1),
    obs: z.string().optional(),
    lubricants: z.array(z.object({
        type: z.string(),
        quantity: z.coerce.number().min(0)
    })),
    qte_vidange: z.coerce.number().nullable().optional(),
});


const calculateEntretienServer = (lubricants: Record<string, number | null>, qteVidange: number | null | undefined): string => {
    const getQty = (type: string): number => lubricants[type] || 0;

    const engineOils = ['15w40', '15w40_v', '15w40_quartz', '15w40_4400'];
    const hydraulicOils = ['10w', 't32', 'hvol', 't46'];
    const transmissionOils = ['90', 'tvol', 't30'];
    const grease = ['graisse'];

    const totalEngineOil = engineOils.reduce((sum, type) => sum + getQty(type), 0);
    const totalHydraulicOil = hydraulicOils.reduce((sum, type) => sum + getQty(type), 0);
    const totalTransmissionOil = transmissionOils.reduce((sum, type) => sum + getQty(type), 0);
    const totalGrease = grease.reduce((sum, type) => sum + getQty(type), 0);

    const hasAnyConsumption = totalEngineOil > 0 || totalHydraulicOil > 0 || totalTransmissionOil > 0 || totalGrease > 0;

    if (!hasAnyConsumption) return "";

    if (qteVidange && qteVidange > 0 && totalEngineOil >= qteVidange) return "VIDANGE,M";
    if (totalGrease > 0) return "GR";
    if (totalHydraulicOil > 0) return "HYDRAULIQUE";
    if (totalTransmissionOil > 0) return "TRANSMISSION";
    if (totalEngineOil > 0) return "NIVEAU HUILE";

    return "";
};

export async function updateConsumptionAction(id: number, values: unknown) {
  const parsed = editConsumptionSchema.safeParse(values);
  if (!parsed.success) {
    console.error('Zod parsing error:', parsed.error.flatten());
    return { success: false, message: 'Données du formulaire invalides.' };
  }

  const { date, lubricants, qte_vidange, ...rest } = parsed.data;

  const lubricantsObj = LUBRICANT_TYPES.reduce((acc, type) => {
    const consumed = lubricants.find(l => l.type === type);
    acc[type] = consumed && consumed.quantity > 0 ? consumed.quantity : null;
    return acc;
  }, {} as Record<string, number | null>);

  const entretien = calculateEntretienServer(lubricantsObj, qte_vidange);

  const payload = {
    date: dayjs(date).format('DD/MM/YYYY'),
    ...rest,
    v: qte_vidange,
    entretien,
    ...lubricantsObj
  };
  
  try {
    await updateConsumption(id, payload);
    revalidatePath('/consommations');
    revalidatePath('/history');
    revalidatePath('/');
    return { success: true, message: 'Consommation mise à jour.' };
  } catch (error: any) {
    return { success: false, message: error.message };
  }
}

export async function saveBonDeSortieAction(data: any) {
  try {
    const { id } = await saveBonToDb(data);

    const { date, items, destinataire_chantier } = data;
    
    const normalize = (str: string) => str.toLowerCase().replace(/[^a-z0-9]/g, '');

    const lubricantItems = items.map((item: any) => {
        const normalizedDesignation = normalize(item.designation);
        const matchedLube = LUBRICANT_TYPES.find(lube => normalizedDesignation.includes(normalize(lube.replace(/_/g, ''))));
        return matchedLube ? { ...item, lubricant_type: matchedLube } : null;
    }).filter(Boolean);

    if (lubricantItems.length > 0) {
        const lubricantsPayload = lubricantItems.reduce((acc: Record<string, number>, item: any) => {
            if (item) {
                acc[item.lubricant_type] = (acc[item.lubricant_type] || 0) + item.quantite;
            }
            return acc;
        }, {} as Record<string, number>);

        const consumptionPayload = {
            date: date,
            entries: [{
                matricule: `Chantier: ${destinataire_chantier}`,
                designation: `Bon de Sortie #${id}`,
                obs: `Sortie de stock via Bon de Sortie #${id}`,
                qte_vidange: null,
                lubricants: lubricantsPayload
            }]
        };
        await addConsumptionsAction(consumptionPayload);
    }
    
    revalidatePath('/bons-de-sortie');
    return { success: true, bonId: id };
  } catch (error: any) {
    return { success: false, message: error.message };
  }
}

export async function getBonsDeSortieListAction() {
    try {
        return await getBonsListFromDb();
    } catch (e: any) {
        return [];
    }
}

export async function getBonDeSortieAction(id: number): Promise<BonDeSortie | null> {
    try {
        return await getBonFromDb(id);
    } catch (e: any) {
        return null;
    }
}

export async function updateBonDeSortieAction(id: number, data: any) {
    try {
        await updateBonInDb(id, data);
        revalidatePath('/bons-de-sortie');
        revalidatePath(`/bons-de-sortie/view/${id}`);
        revalidatePath(`/bons-de-sortie/edit/${id}`);
        // Note: This does not update the stock consumption.
        // A more robust solution would require linking consumption entries to bons.
        return { success: true, bonId: id };
    } catch (error: any) {
        return { success: false, message: error.message };
    }
}

export async function deleteBonDeSortieAction(id: number) {
    try {
        await deleteBonFromDb(id);
        revalidatePath('/bons-de-sortie');
         // Note: This does not delete the associated stock consumption.
        return { success: true };
    } catch (error: any) {
        return { success: false, message: error.message };
    }
}

export async function handleFileUploadAction(tableName: string, fileContent: string) {
  try {
    const result = await reinitializeTable(tableName, fileContent);
    
    // Revalidate paths that might be affected by data changes
    revalidatePath('/init-db');
    revalidatePath('/', 'layout'); // Revalidate all pages

    return { success: true, message: result.message };
  } catch (error: any) {
    console.error(`Failed to handle file upload for table ${tableName}:`, error);
    return { success: false, message: error.message || 'Une erreur serveur est survenue.' };
  }
}


export async function getLastEntryDateAction(lubricantType: string): Promise<{ success: boolean; date: string | null; message?: string; }> {
    try {
        const date = await getLastEntryDateFromDb(lubricantType);
        return { success: true, date };
    } catch (error: any) {
        return { success: false, date: null, message: error.message };
    }
}
    

export async function getPlanningPage(
    year: number,
    page = 1,
    pageSize = 1,
    filter = ''
) {
    return dataGetPlanningPage(year, page, pageSize, filter);
}

export async function getFollowUpPage(
    year: number,
    page = 1,
    pageSize = 1,
    filter = ''
) {
    return dataGetFollowUpPage(year, page, pageSize, filter);
}

export async function getFollowUpStatistics(year: number) {
    return dataGetFollowUpStatistics(year);
}
//import fs from 'fs/promises';

// ... tes autres imports et fonctions ...

export async function testEcritureDisque() {
    const testFilePath = '/data/test_ecriture.txt';
    const timestamp = new Date().toISOString();
    
    try {
        // 1. Écrire
        await fs.writeFile(testFilePath, `Test écriture à ${timestamp}`);
        
        // 2. Lire pour vérifier
        const content = await fs.readFile(testFilePath, 'utf-8');
        
        // 3. Lister les fichiers présents
        const files = await fs.readdir('/data');
        
        return { 
            success: true, 
            message: `✅ DISQUE OK. Contenu lu: "${content}". Fichiers dans /data: ${JSON.stringify(files)}` 
        };
    } catch (e: any) {
        return { success: false, error: `❌ ERREUR: ${e.message}` };
    }
}
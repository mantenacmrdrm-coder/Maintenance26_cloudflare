'use server';

import * as Data from '../data';
import type { 
  Alert, 
  BonDeSortie, 
  CurativeFicheData, 
  DeclarationPanne, 
  OrdreTravailData, 
  PreventiveFicheData, 
  StockEntry, 
  MonthlyStockReportData,
  BonListItem,
  DeclarationListItem,
  ActionResponse,
  ConsumptionQueryResult,
  FollowUpStats
} from '../types';
import { revalidatePath } from 'next/cache';
import dayjs from 'dayjs';
import 'dayjs/locale/fr';

dayjs.locale('fr');

const getData = () => Data as any;

export async function restoreDatabase(formData: FormData): Promise<ActionResponse<string>> {
    return getData().restoreDatabase(formData);
}

export async function getPreventativeAlerts({ startDate, endDate, entretiens, niveau, matricule }: { startDate: Date; endDate: Date; entretiens?: string[]; niveau?: string; matricule?: string; }): Promise<Alert[]> {
  try {
    const plannedOperations = await getData().getAllPlanningForYear(startDate.getFullYear());
    const today = dayjs().startOf('day');
    const startDayjs = dayjs(startDate).startOf('day');
    const endDayjs = dayjs(endDate).endOf('day');
    const alerts: Alert[] = [];
    for (const op of plannedOperations) {
        const dueDate = dayjs(op.date_programmee, ['DD/MM/YYYY', 'YYYY-MM-DD']);
        if (!dueDate.isValid() || !dueDate.isBetween(startDayjs, endDayjs, null, '[]')) continue;
        if (entretiens?.length && !entretiens.includes(op.operation)) continue;
        if (niveau && niveau !== 'all' && op.niveau !== niveau) continue;
        if (matricule && op.matricule && !op.matricule.toLowerCase().includes(matricule.toLowerCase())) continue;
        alerts.push({ 
            equipmentId: op.matricule, 
            equipmentDesignation: op.designation, 
            operation: op.operation, 
            dueDate: dueDate.format('DD/MM/YYYY'), 
            urgency: dueDate.isBefore(today) ? 'urgent' : 'near', 
            niveau: op.niveau
        });
    }
    return alerts.sort((a, b) => dayjs(a.dueDate, 'DD/MM/YYYY').unix() - dayjs(b.dueDate, 'DD/MM/YYYY').unix());
  } catch (error: any) { throw new Error('Failed to generate alerts.'); }
}

export async function initializeDatabase(): Promise<ActionResponse> {
    try { return await getData().initializeDatabase(); } catch(e:any) { return { success: false, message: e.message }; }
}

export async function runHistoryGeneration(): Promise<any> {
    try { return await getData().generateHistoryMatrix(); } catch(e: any) { throw new Error(e.message); }
}

export async function getHistory(): Promise<any> {
    try { return await getData().getHistoryMatrixFromCache(); } catch (e: any) { return { headers: [], rows: [], counts: {} }; }
}

export async function generatePlanning(year: number): Promise<{ count: number }> {
  const result = await getData().generatePlanning(year);
  revalidatePath('/planning'); revalidatePath('/suivi');
  return result;
}

export async function getAllPlanningForExport(year: number): Promise<any> { return await getData().getPlanningMatrixForExport(year); }
export async function getAllFollowUpForExport(year: number): Promise<any> { return await getData().getFollowUpMatrixForExport(year); }
export async function getParams(): Promise<any[]> { return getData().getParams(); }
export async function updateParam(id: number, column: string, value: string | null): Promise<ActionResponse> {
    try { await getData().updateParam(id, column, value); revalidatePath('/parameters'); return { success: true, data: null }; } catch(e:any) { return { success: false, message: e.message }; }
}

export async function getDashboardData(year?: number, month?: number): Promise<any> {
    const targetYear = year || new Date().getFullYear();
    try {
        const [equipmentCount, recentOperations, operationCountForYear, followUpStats, monthlyCounts, preventativeStats] = await Promise.all([
            getData().getEquipmentCount(), getData().getRecentOperations(5), getData().getOperationCountForYear(targetYear), getData().getFollowUpStatistics(targetYear), getData().getMonthlyCurativeCounts(targetYear), getData().getMonthlyPreventativeStats(targetYear, month)
        ]);
        return { equipmentCount, operationCount: operationCountForYear, followUpStats, monthlyCounts, preventativeStats, recentOperations, error: null, year: targetYear, month };
    } catch (error: any) {
        return { equipmentCount: 0, operationCount: 0, followUpStats: null, monthlyCounts: [], preventativeStats: { monthlyData: [], totalOil: 0, oilByType: {} }, recentOperations: [], error: error.message, year: targetYear, month };
    }
}

export async function getDistinctCategories(): Promise<string[]> { return getData().getDistinctCategories(); }
export async function getCategoryEntretiens(): Promise<Record<string, Record<string, boolean>>> { return getData().getCategoryEntretiens(); }
export async function updateCategoryEntretiens(category: string, entretien: string, isActive: boolean): Promise<ActionResponse> {
    try { await getData().updateCategoryEntretiens(category, entretien, isActive); revalidatePath('/parameters'); return { success: true, data: null }; } catch(e:any) { return { success: false, message: e.message }; }
}

export async function addCurativeOperation(values: any): Promise<ActionResponse> {
  try {
    const res = await getData().addCurativeOperationToDb(values);
    revalidatePath('/operations'); revalidatePath('/');
    return { success: true, data: res };
  } catch (e: any) { return { success: false, message: e.message }; }
}

export async function generateWeeklyReportAction(targetDate: Date): Promise<ActionResponse<{ reportId: number }>> {
  try {
    const reportId = await getData().generateAndSaveWeeklyReport(targetDate);
    revalidatePath('/reports');
    return { success: true, data: { reportId } };
  } catch (error: any) { return { success: false, message: error.message }; }
}

export async function getWeeklyReports(): Promise<any[]> { return await getData().getWeeklyReports(); }
export async function getWeeklyReport(id: number): Promise<any> { return await getData().getWeeklyReport(id); }
export async function deleteWeeklyReportAction(id: number): Promise<ActionResponse> {
    try { await getData().deleteWeeklyReport(id); revalidatePath('/reports'); return { success: true, data: null }; } catch (error: any) { return { success: false, message: error.message }; }
}

export async function getOperationForDeclaration(id: number): Promise<any> { return getData().getOperationById(id); }
export async function saveDeclarationAction(operationId: number, data: any): Promise<ActionResponse<{ declarationId: number }>> {
    try { const id = await getData().saveDeclaration({ ...data, operation_id: operationId }); revalidatePath('/declarations'); return { success: true, data: { declarationId: id } }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function getDeclaration(id: number): Promise<any> { return getData().getDeclarationById(id); }
export async function getDeclarationsListAction(): Promise<DeclarationListItem[]> { return await getData().getDeclarationsList() as DeclarationListItem[]; }
export async function deleteDeclarationAction(id: number): Promise<ActionResponse> {
    try { await getData().deleteDeclarationFromDb(id); revalidatePath('/declarations'); return { success: true, data: null }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function updateDeclarationAction(id: number, data: any): Promise<ActionResponse<{ declarationId: number }>> {
    try { await getData().updateDeclarationInDb(id, data); revalidatePath('/declarations'); return { success: true, data: { declarationId: id } }; } catch (error: any) { return { success: false, message: error.message }; }
}

export async function getEquipmentByIdAction(id: number): Promise<any> { return getData().getEquipmentById(id); }
export async function addEquipmentAction(values: any): Promise<ActionResponse<{ equipmentId: number }>> {
  try { const id = await getData().addEquipmentToDb(values); revalidatePath('/equipment'); return { success: true, data: { equipmentId: id } }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function updateEquipmentAction(id: number, values: any): Promise<ActionResponse> {
  try { await getData().updateEquipmentInDb(id, values); revalidatePath('/equipment'); return { success: true, data: null }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function deleteEquipmentAction(id: number): Promise<ActionResponse> {
  try { await getData().deleteEquipmentFromDb(id); revalidatePath('/equipment'); return { success: true, data: null }; } catch (error: any) { return { success: false, message: error.message }; }
}

export async function getCurativeFichesAction(values: any): Promise<ActionResponse<CurativeFicheData[]>> {
  try { const data = await getData().getCurativeFiches(values.startDate, values.endDate, values.matricule); return { success: true, data }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function getOrdresDeTravailAction(values: any): Promise<ActionResponse<OrdreTravailData[]>> {
  try { const data = await getData().getOrdresDeTravail(values.startDate, values.endDate, values.matricule); return { success: true, data }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function getPreventiveFichesAction(values: any): Promise<ActionResponse<PreventiveFicheData[]>> {
  try { const data = await getData().getPreventiveFichesFromDb(values.startDate, values.endDate, values.matricule); return { success: true, data }; } catch (error: any) { return { success: false, message: error.message }; }
}

export async function addConsumptionsAction(values: any): Promise<ActionResponse> {
  try {
    const result = await getData().addConsolideEntries(dayjs(values.date).format('YYYY-MM-DD'), values.entries);
    revalidatePath('/stock'); revalidatePath('/consommations');
    return { success: true, data: result };
  } catch (error: any) { return { success: false, message: error.message }; }
}
    
export async function getConsumptionsByDateRange(startDate: Date, endDate: Date): Promise<ActionResponse<ConsumptionQueryResult>> {
  try { 
    const res = await getData().getConsolideByDateRange(dayjs(startDate).format('YYYY-MM-DD'), dayjs(endDate).format('YYYY-MM-DD')); 
    return { success: true, data: res }; 
  } catch (error: any) { 
    return { success: false, message: error.message }; 
  }
}

export async function addStockEntryAction(values: any): Promise<ActionResponse> {
    try { await getData().addStockEntryToDb({ ...values, date: dayjs(values.date).format('YYYY-MM-DD') }); revalidatePath('/stock-entries'); return { success: true, data: null }; } catch (e: any) { return { success: false, message: e.message }; }
}
export async function getStockEntriesAction(): Promise<StockEntry[]> { return getData().getStockEntriesFromDb() as Promise<StockEntry[]>; }
export async function deleteStockEntryAction(id: number): Promise<ActionResponse> {
    try { await getData().deleteStockEntryFromDb(id); revalidatePath('/stock-entries'); return { success: true, data: null }; } catch (e: any) { return { success: false, message: e.message }; }
}

export async function getDailyConsumptionReportAction(params: { year: number, month: number, lubricantType: string }): Promise<ActionResponse<DailyReportData>> {
    try { const data = await getData().getDailyConsumptionReportData(params.year, params.month, params.lubricantType); return { success: true, data }; } catch (error: any) { return { success: false, message: error.message }; }
}

export async function getMonthlyStockReportAction(params: any): Promise<ActionResponse<MonthlyStockReportData>> {
    try { const data = await getData().getMonthlyStockReportData(params.startYear, params.startMonth, params.lubricantType); return { success: true, data }; } catch (error: any) { return { success: false, message: error.message }; }
}

export async function deleteOperationAction(id: number): Promise<ActionResponse> {
    try { await getData().deleteOperationFromDb(id); revalidatePath('/operations'); return { success: true, data: null }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function getOperationByIdAction(id: number): Promise<any> { return getData().getOperationById(id); }
export async function updateOperationAction(id: number, values: any): Promise<ActionResponse> {
  try { await getData().updateOperationInDb(id, { ...values, date_entree: values.dateEntree }); revalidatePath('/operations'); return { success: true, data: null }; } catch (error: any) { return { success: false, message: error.message }; }
}

export async function getEquipmentForConsumptionAction(): Promise<any[]> { return getData().getEquipmentForConsumption(); }
export async function getConsumptionByIdAction(id: number): Promise<any> { return getData().getConsumptionById(id); }
export async function deleteConsumptionAction(id: number): Promise<ActionResponse> {
    try { await getData().deleteConsumption(id); revalidatePath('/consommations'); return { success: true, data: null }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function updateConsumptionAction(id: number, values: any): Promise<ActionResponse> {
  try { await getData().updateConsumption(id, values); revalidatePath('/consommations'); return { success: true, data: null }; } catch (error: any) { return { success: false, message: error.message }; }
}

export async function saveBonDeSortieAction(data: any): Promise<ActionResponse<{ bonId: number }>> {
  try { const id = await getData().saveBonDeSortie(data); revalidatePath('/bons-de-sortie'); return { success: true, data: { bonId: id } }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function getBonsDeSortieListAction(): Promise<BonListItem[]> { return await getData().getBonsDeSortieList() as BonListItem[]; }
export async function getBonDeSortieAction(id: number): Promise<BonDeSortie | null> { return getData().getBonDeSortieById(id); }
export async function updateBonDeSortieAction(id: number, data: any): Promise<ActionResponse<{ bonId: number }>> {
    try { await getData().updateBonDeSortie(id, data); revalidatePath('/bons-de-sortie'); return { success: true, data: { bonId: id } }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function getLastEntryDateAction(lube: string): Promise<ActionResponse<string | null>> {
    try { const date = await getData().getLastStockEntryDate(lube); return { success: true, data: date }; } catch (error: any) { return { success: false, message: error.message }; }
}
export async function deleteBonDeSortieAction(id: number): Promise<ActionResponse> {
    try { await getData().deleteBonDeSortie(id); revalidatePath('/bons-de-sortie'); return { success: true, data: null }; } catch (error: any) { return { success: false, message: error.message }; }
}

export async function handleFileUploadAction(tableName: string, fileContent?: string): Promise<ActionResponse> {
  try { 
    const result = await getData().reinitializeTableFromCsv(tableName, fileContent); 
    revalidatePath('/init-db'); 
    return { success: true, data: result }; 
  } catch (error: any) { 
    return { success: false, message: error.message }; 
  }
}

export async function getPlanningPage(year: number, page = 1, pageSize = 1, filter = ''): Promise<any> { return getData().getPlanningPage(year, page, pageSize, filter); }
export async function getFollowUpPage(year: number, page = 1, pageSize = 1, filter = ''): Promise<any> { return getData().getFollowUpPage(year, page, pageSize, filter); }
export async function getFollowUpStatistics(year: number): Promise<FollowUpStats> { return getData().getFollowUpStatistics(year); }

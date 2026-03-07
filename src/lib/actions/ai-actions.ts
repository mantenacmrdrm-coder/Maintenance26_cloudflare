'use server';

import { getCurativeDiagnosis } from '@/ai/flows/diagnose-curative-flow';

export async function getCurativeDiagnosisAction(matricule: string, panneDeclaree: string): Promise<{ success: boolean; data?: any; message?: string; }> {
  if (!matricule || !panneDeclaree) {
    return { success: false, message: 'Matricule et description de la panne sont requis.' };
  }
  try {
    const diagnosis = await getCurativeDiagnosis({ matricule, panneDeclaree });
    return { success: true, data: diagnosis };
  } catch (error: any) {
    console.error("Failed to get curative diagnosis:", error);
    return { success: false, message: error.message || 'Une erreur est survenue lors du diagnostic IA.' };
  }
}

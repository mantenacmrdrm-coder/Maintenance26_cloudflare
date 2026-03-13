'use server';
/**
 * @fileOverview An AI agent for diagnosing equipment breakdowns.
 */

import { ai } from '@/ai/genkit';
import { z } from 'zod';
import { getCurativeHistoryForEquipment, getEquipmentDetails } from '@/lib/data';
import type { CurativeMaintenanceEntry } from '@/lib/types';

const DiagnoseCurativeInputSchema = z.object({
  matricule: z.string().describe('The registration number of the equipment.'),
  panneDeclaree: z.string().describe('The user-provided description of the breakdown.'),
});
export type DiagnoseCurativeInput = z.infer<typeof DiagnoseCurativeInputSchema>;

const DiagnoseCurativeOutputSchema = z.object({
    typeDePanne: z.enum(['mécanique', 'électrique', 'hydraulique', 'autres']).describe("Categorize the breakdown."),
    causesPossibles: z.array(z.string()).describe("A list of 2 to 3 likely causes."),
    piecesSuggerees: z.array(z.string()).describe("A list of suggested parts to check."),
    actionsRecommandees: z.array(z.string()).describe("A list of recommended steps."),
});
export type DiagnoseCurativeOutput = z.infer<typeof DiagnoseCurativeOutputSchema>;

const diagnosisPrompt = ai.definePrompt({
    name: 'diagnoseCurativePromptSrc',
    input: { schema: z.object({ panneDeclaree: z.string(), equipmentInfo: z.string(), history: z.string() })},
    output: { schema: DiagnoseCurativeOutputSchema },
    prompt: `Analyze report in JSON format in French.
## Input Data
- Breakdown: {{panneDeclaree}}
- Equipment: {{equipmentInfo}}
- History: {{history}}`,
});

const diagnoseCurativeFlow = ai.defineFlow(
  { name: 'diagnoseCurativeFlowSrc', inputSchema: DiagnoseCurativeInputSchema, outputSchema: DiagnoseCurativeOutputSchema },
  async (input) => {
    const equipment = await getEquipmentDetails(input.matricule);
    const history = await getCurativeHistoryForEquipment(input.matricule);
    if (!equipment) throw new Error(`Equipment with matricule ${input.matricule} not found.`);
    const equipmentInfo = `- Designation: ${equipment.designation} - Marque: ${equipment.marque}`;
    const historySummary = history.length > 0 ? history.slice(0, 5).map((h: CurativeMaintenanceEntry) => `- ${h.dateEntree}: ${h.panneDeclaree}`).join('\n') : 'No history.';
    const { output } = await diagnosisPrompt({ panneDeclaree: input.panneDeclaree, equipmentInfo, history: historySummary });
    return output!;
  }
);

export async function getCurativeDiagnosis(input: DiagnoseCurativeInput): Promise<DiagnoseCurativeOutput> {
    return await diagnoseCurativeFlow(input);
}

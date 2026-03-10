'use server';
/**
 * @fileOverview An AI agent for diagnosing equipment breakdowns.
 *
 * - getCurativeDiagnosis - A function that suggests causes and remedies for a breakdown.
 * - DiagnoseCurativeInput - The input type for the getCurativeDiagnosis function.
 * - DiagnoseCurativeOutput - The return type for the getCurativeDiagnosis function.
 */

import { ai } from '@/ai/genkit';
import { z } from 'zod';
import { getCurativeHistoryForEquipment, getEquipmentDetails } from '@/lib/data';

const DiagnoseCurativeInputSchema = z.object({
  matricule: z.string().describe('The registration number of the equipment.'),
  panneDeclaree: z.string().describe('The user-provided description of the breakdown.'),
});
export type DiagnoseCurativeInput = z.infer<typeof DiagnoseCurativeInputSchema>;

const DiagnoseCurativeOutputSchema = z.object({
    typeDePanne: z.enum(['mécanique', 'électrique', 'hydraulique', 'autres']).describe("Categorize the breakdown into one of the provided types."),
    causesPossibles: z.array(z.string()).describe("A list of 2 to 3 likely causes for the breakdown."),
    piecesSuggerees: z.array(z.string()).describe("A list of suggested parts to check or replace. Be specific."),
    actionsRecommandees: z.array(z.string()).describe("A list of recommended steps to take for repair."),
});
export type DiagnoseCurativeOutput = z.infer<typeof DiagnoseCurativeOutputSchema>;


const diagnosisPrompt = ai.definePrompt({
    name: 'diagnoseCurativePrompt',
    input: { schema: z.object({
        panneDeclaree: z.string(),
        equipmentInfo: z.string(),
        history: z.string(),
    })},
    output: { schema: DiagnoseCurativeOutputSchema },
    prompt: `Analyze the following equipment breakdown report and provide a diagnosis in JSON format.

Your response must be in French.

## Instructions
1.  **Role**: You are an expert mechanic for heavy machinery.
2.  **Task**: Provide a preliminary diagnosis based on the provided data.
3.  **Analysis**: Consider the new breakdown description, the equipment's details, and its repair history.
4.  **Output**: Your diagnosis must be structured according to the provided JSON schema. It should include the type of breakdown, possible causes, suggested parts, and recommended actions.

## Input Data
- **Breakdown Description**: {{panneDeclaree}}
- **Equipment Information**: {{equipmentInfo}}
- **Repair History**: {{history}}
`,
});

const diagnoseCurativeFlow = ai.defineFlow(
  {
    name: 'diagnoseCurativeFlow',
    inputSchema: DiagnoseCurativeInputSchema,
    outputSchema: DiagnoseCurativeOutputSchema,
  },
  async (input) => {
    const [equipment, history] = await Promise.all([
      getEquipmentDetails(input.matricule),
      getCurativeHistoryForEquipment(input.matricule),
    ]);

    if (!equipment) {
      throw new Error(`Equipment with matricule ${input.matricule} not found.`);
    }

    const equipmentInfo = `
- Designation: ${equipment.designation}
- Marque: ${equipment.marque}
- Categorie: ${equipment.categorie}
    `;

const historySummary = history.length > 0
  ? history.slice(0, 5).map((h: { dateEntree: string; panneDeclaree: string; piecesRemplacees: string[] }) => `- ${h.dateEntree}: ${h.panneDeclaree} (Pièces: ${h.piecesRemplacees.join(', ') || 'N/A'})`).join('\n')
  : 'No relevant repair history found.';

    const { output } = await diagnosisPrompt({
        panneDeclaree: input.panneDeclaree,
        equipmentInfo: equipmentInfo,
        history: historySummary,
    });
    
    return output!;
  }
);


export async function getCurativeDiagnosis(input: DiagnoseCurativeInput): Promise<DiagnoseCurativeOutput> {
    return await diagnoseCurativeFlow(input);
}

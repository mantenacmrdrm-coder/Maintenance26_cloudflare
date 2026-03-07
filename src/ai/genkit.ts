import {genkit} from 'genkit';
import {googleAI} from '@genkit-ai/google-genai';

const plugins = [];

if (process.env.GEMINI_API_KEY) {
  plugins.push(googleAI());
}

export const ai = genkit({
  plugins,
});

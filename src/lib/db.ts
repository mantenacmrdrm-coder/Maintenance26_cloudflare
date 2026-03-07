import fs from 'fs';
import path from 'path';

const isHuggingFace = process.env.HOME === '/root';
const DATA_DIR = isHuggingFace ? '/data' : process.cwd();

// Vérification et création du dossier
if (!fs.existsSync(DATA_DIR)) {
    try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        console.log(`✅ Dossier créé : ${DATA_DIR}`);
    } catch (e) {
        console.error(`❌ Erreur création dossier ${DATA_DIR}:`, e);
    }
} else {
    console.log(`✅ Dossier existant : ${DATA_DIR}`);
}

export const DB_PATH = path.join(DATA_DIR, 'gmao_data.db');

console.log(`🗄️  Configuration BD -> Env: ${isHuggingFace ? 'Hugging Face' : 'Local'} | Chemin: ${DB_PATH}`);
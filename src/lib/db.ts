
import fs from 'fs';
import path from 'path';

const isHuggingFace = process.env.HOME === '/root';
const DATA_DIR = isHuggingFace ? '/data' : process.cwd();

// Ensure the data directory exists
if (!fs.existsSync(DATA_DIR)) {
    try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
    } catch (e) {
        console.error(`Error creating directory ${DATA_DIR}:`, e);
    }
}

export const DB_PATH = path.join(DATA_DIR, 'gmao_data.db');
export const IS_PRODUCTION = process.env.NODE_ENV === 'production';

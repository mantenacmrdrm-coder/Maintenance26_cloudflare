'use server';

import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { createClient } from '@libsql/client';
import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat';
import isoWeek from 'dayjs/plugin/isoWeek';
import csv from 'csv-parser';
import iconv from 'iconv-lite';
import { OFFICIAL_ENTRETIENS, HEADER_ORDER, planningOperationNameMapping, LUBRICANT_TYPES } from '@/lib/constants';
import isSameOrBefore from 'dayjs/plugin/isSameOrBefore';
import isSameOrAfter from 'dayjs/plugin/isSameOrAfter';
import type { BonDeSortie, PreventativeMaintenanceEntry, CurativeMaintenanceEntry, WeeklyReport, WeeklyReportItem, MonthlyCount, MonthlyPreventativeStats, DeclarationPanne, CurativeFicheData, OrdreTravailData, PreventiveFicheData, DailyReportData, MonthlyStockReportData } from '@/lib/types';
import { Readable } from 'stream';
import isBetween from 'dayjs/plugin/isBetween';
import initialStocks from './initial-stock.json';
import 'dayjs/locale/fr';

dayjs.locale('fr');
dayjs.extend(isBetween);
dayjs.extend(isSameOrBefore);
dayjs.extend(isSameOrAfter);
dayjs.extend(customParseFormat);
dayjs.extend(isoWeek);

const getVal = (row: any, ...keys: string[]): any => {
    for (const key of keys) {
        if (row[key] !== undefined) return row[key];
    }
    return undefined;
}

const CONSOLIDE_COLUMNS_ORDER = [
    'v', 'n', 'date', 'designation', 'matricule', 't32', '15w40_4400', 
    '10w', '15w40', '90', '15w40_v', 'hvol', 'tvol', 't30', 'graisse', 
    't46', '15w40_quartz', 'obs', 'entretien'
] as const;

const entretienSynonyms: { [synonym: string]: (typeof OFFICIAL_ENTRETIENS)[number] } = {
    'liquide refroidissement': 'Etanchéité de tous les circuits'
};

function findMatchedEntretien(piece: string): (typeof OFFICIAL_ENTRETIENS)[number] | undefined {
    const normalizedPiece = normalize(piece);

    // 1. Check for synonyms
    for (const synonym in entretienSynonyms) {
        const synonymWords = synonym.split(' ').map(w => normalize(w)).filter(Boolean);
        if (synonymWords.length > 0 && synonymWords.every(word => normalizedPiece.includes(word))) {
            return entretienSynonyms[synonym];
        }
    }

    // 2. Generic matching against official list, prioritizing longer, more specific matches
    const sortedEntretiens = [...OFFICIAL_ENTRETIENS].sort((a, b) => b.length - a.length);

    for (const entretien of sortedEntretiens) {
        // We already handled this via synonyms
        if (entretien === 'Etanchéité de tous les circuits') {
            continue;
        }

        const entretienWords = entretien.split(' ').map(w => normalize(w)).filter(Boolean);
        if (entretienWords.length === 0) continue;

        if (entretienWords.every(word => normalizedPiece.includes(word))) {
            return entretien;
        }
    }
    
    return undefined;
}

let historyCache: { headers: readonly string[]; rows: (string | null)[][]; } | null = null;
let historyCacheTimestamp: number | null = null;

const normalize = (str: string | null | undefined): string => {
    if (!str) return '';
    return str.toString().toLowerCase()
        .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // remove accents
        .replace(/\s+/g, '')
        .replace(/[^a-z0-9]/g, '');
};

// Configuration de la base de données
const getDb = async () => {
    // En mode build, retourner un client factice
    if (process.env.NEXT_PHASE === 'phase-production-build') {
        const { createClient } = await import('@libsql/client');
        return createClient({ url: 'file::memory:' });
    }

    // Utiliser Turso en production ou fichier local en développement
    const url = process.env.TURSO_DATABASE_URL || 'file:local-dev.db';
    const authToken = process.env.TURSO_AUTH_TOKEN;

    const db = createClient({ 
        url: url,
        authToken: authToken,
    });

    try {
        await db.execute('PRAGMA synchronous = FULL;');
    } catch (e) {
        // Ignorer si non supporté (D1)
    }
    
    return db;
};

type DbClient = Awaited<ReturnType<typeof getDb>>;

const withDb = async <T>(operation: (db: DbClient) => Promise<T>): Promise<T> => {
    let db: DbClient | null = null;
    try {
        db = await getDb();
        return await operation(db);
    } catch (error) {
        console.error('Database operation failed:', error);
        throw error;
    } finally {
        if (db && typeof db.close === 'function') {
            try {
                await db.close();
            } catch (e) {
                // Ignorer les erreurs de fermeture
            }
        }
    }
};

// Fonctions d'aide pour les requêtes
const runQuery = async (db: DbClient, sql: string, params: any[] = []) => {
    return await db.execute({
        sql,
        args: params
    });
};

const getQuery = async (db: DbClient, sql: string, params: any[] = []) => {
    const result = await db.execute({
        sql,
        args: params
    });
    return result.rows;
};

const getOne = async (db: DbClient, sql: string, params: any[] = []) => {
    const result = await db.execute({
        sql,
        args: params
    });
    return result.rows[0] || null;
};

const parseCsv = async (buffer: Buffer): Promise<any[]> => {
  return new Promise((resolve, reject) => {
    const rows: any[] = [];
    const stream = Readable.from(buffer);
    stream
      .pipe(iconv.decodeStream('utf-8'))
      .pipe(csv({ 
          separator: ';',
          mapHeaders: ({ header }) => header.trim().replace(/\s+/g, '_').replace(/[."(),/]/g, '').toLowerCase() || null
      }))
      .on('data', (data) => rows.push(data))
      .on('end', () => resolve(rows))
      .on('error', (error) => reject(error));
  });
};

const importCsvToTable = async (db: DbClient, tableName: string, csvFileName: string) => {
  try {
    const tableExists = await getOne(db, "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", [tableName]);
    if (tableExists) {
      return `Table ${tableName} already exists. Skipping import.`;
    }
  } catch (e) {
    // Si la table n'existe pas, on continue
  }
  
  // Récupération du fichier CSV
  const baseUrl = process.env.NEXT_PUBLIC_SITE_URL || 'http://localhost:3000';
  const fileUrl = `${baseUrl}/import/${csvFileName}`;

  let rows;
  try {
      const response = await fetch(fileUrl);
      
      if (!response.ok) {
          throw new Error(`Impossible de télécharger le CSV (HTTP ${response.status}). Vérifie que le fichier est dans 'public/import'.`);
      }
      
      const arrayBuffer = await response.arrayBuffer();
      const fileBuffer = Buffer.from(arrayBuffer);
      
      rows = await parseCsv(fileBuffer);
  } catch (e: any) {
    throw new Error(`Erreur lecture fichier ${csvFileName}: ${e.message}`);
  }

  if (!rows || rows.length === 0) {
    return `No data found in ${csvFileName}. Skipping table creation.`;
  }
  
  const headers = Object.keys(rows[0]).filter(h => h && h !== 'null');
  if (headers.length === 0) {
    throw new Error(`Could not determine headers for ${csvFileName}`);
  }
  
  try {
    const createColumns = headers.map(h => `"${h}" TEXT`).join(', ');
    await runQuery(db, `CREATE TABLE IF NOT EXISTS "${tableName}" (id INTEGER PRIMARY KEY AUTOINCREMENT, ${createColumns})`);
    
    const insertPlaceholders = headers.map(() => '?').join(', ');
    const insertSql = `INSERT INTO "${tableName}" (${headers.map(h => `"${h}"`).join(', ')}) VALUES (${insertPlaceholders})`;

    const statements = rows.map(row => {
      const values = headers.map(h => row[h] ?? null);
      return {
        sql: insertSql,
        args: values
      };
    });

    await db.batch(statements);

    return `Imported ${rows.length} rows into new table ${tableName}.`;
    
  } catch (dbError: any) {
    console.error(`DB Error for ${tableName}:`, dbError);
    throw new Error(`Failed to import ${tableName}: ${dbError.message}`);
  }
};

export async function initializeDatabase() {
    // Skip en mode build
    if (process.env.NEXT_PHASE === 'phase-production-build') {
        return { success: true, message: 'Build mode - DB initialization skipped' };
    }

    try {
        return await withDb(async (db) => {
             const EXCLUSIONS_NORM: Record<string, Set<string>> = {
                geg: new Set(['frein', 'chaine', 'pneu', 'moyeuderoue', 'graissagegeneral', 'boitedevitesse', 'cardan', 'embrayage', 'circuithydraulique', 'pompehydraulique', 'filtrehydraulique', 'reservoirhydraulique', 'faisceauxelectriques']),
                outillagedivers: new Set(['courroie', 'filtreahuile', 'vidangerlecartermoteur', 'filtreaair', 'filtrecarburant', 'soupape', 'alternateur', 'batterie', 'frein', 'chaine', 'pneu', 'moyeuderoue', 'graissagegeneral', 'boitedevitesse', 'cardan', 'embrayage', 'circuithydraulique', 'pompehydraulique', 'filtrehydraulique', 'reservoirhydraulique', 'faisceauxelectriques']),
                aircomprime: new Set(['frein', 'chaine', 'pneu', 'moyeuderoue', 'graissagegeneral', 'boitedevitesse', 'cardan', 'embrayage', 'circuithydraulique', 'pompehydraulique', 'faisceauxelectriques']),
                transmarchandise1: new Set(['niveaudhuileducarter', 'etanchitedetouslescircuits', 'courroie', 'filtreahuile', 'vidangerlecartermoteur', 'filtreaair', 'filtrecarburant', 'chaine', 'soupape', 'boitedevitesse', 'cardan', 'embrayage', 'circuithydraulique', 'pompehydraulique', 'filtrehydraulique', 'reservoirhydraulique', 'alternateur', 'batterie', 'faisceauxelectriques']),
                transetvspeciaux1: new Set(['niveaudhuileducarter', 'etanchitedetouslescircuits', 'courroie', 'filtreahuile', 'vidangerlecartermoteur', 'filtreaair', 'filtrecarburant', 'chaine', 'soupape', 'boitedevitesse', 'cardan', 'embrayage', 'circuithydraulique', 'pompehydraulique', 'filtrehydraulique', 'reservoirhydraulique', 'alternateur', 'batterie', 'faisceauxelectriques']),
                transpersonnel: new Set(['niveaudhuileducarter', 'circuithydraulique', 'pompehydraulique', 'filtrehydraulique', 'reservoirhydraulique', 'faisceauxelectriques']),
                transbenner: new Set(['embrayage', 'chaine', 'boitedevitesse', 'alternateur', 'faisceauxelectriques']),
                legeree: new Set(['graissagegeneral', 'circuithydraulique', 'pompehydraulique', 'filtrehydraulique', 'reservoirhydraulique']),
                legerd: new Set(['graissagegeneral', 'circuithydraulique', 'pompehydraulique', 'filtrehydraulique', 'reservoirhydraulique', 'faisceauxelectriques']),
                transforbeton: new Set(['frein', 'chaine', 'pneu', 'moyeuderoue', 'boitedevitesse', 'cardan', 'embrayage', 'circuithydraulique', 'pompehydraulique', 'filtrehydraulique', 'reservoirhydraulique', 'faisceauxelectriques']),
                manutention1: new Set(['filtreahuile', 'vidangerlecartermoteur', 'filtreaair', 'filtrecarburant', 'soupape', 'alternateur', 'frein', 'chaine', 'pneu', 'moyeuderoue', 'cardan', 'embrayage', 'circuithydraulique', 'pompehydraulique', 'filtrehydraulique', 'reservoirhydraulique', 'faisceauxelectriques']),
            };
            
            const messages = [];
            console.log('🚀 Début de l\'initialisation de la base de données...');
            
            const csvFiles = ['matrice.csv', 'Param.csv', 'vidange.csv', 'suivi_curatif.csv'];
            for (const file of csvFiles) {
                try {
                    const tableName = file.replace('.csv', '');
                    const msg = await importCsvToTable(db, tableName, file);
                    messages.push(msg);
                } catch (error: any) {
                    console.error(`❌ Erreur avec ${file}:`, error.message);
                    messages.push(`Error with ${file}: ${error.message}`);
                }
            }
            
            try {
                const tableName = 'consolide';
                const tableExists = await getOne(db, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", [tableName]);
                if (tableExists) {
                    messages.push(`Table ${tableName} already exists. Skipping import.`);
                } else {
                    const csvFileName = 'consolide.csv';
                    const createColumns = CONSOLIDE_COLUMNS_ORDER.map(col => `"${col}" TEXT`).join(', ');
                    await runQuery(db, `CREATE TABLE "${tableName}" (id INTEGER PRIMARY KEY AUTOINCREMENT, ${createColumns})`);
                    messages.push(`Table ${tableName} created with fixed schema.`);
                
                    const baseUrl = process.env.NEXT_PUBLIC_SITE_URL || 'http://localhost:3000';
                    const fileUrl = `${baseUrl}/import/${csvFileName}`;
                    const response = await fetch(fileUrl);
                    
                    if (!response.ok) {
                        throw new Error(`Impossible de télécharger ${csvFileName}`);
                    }
                    
                    const arrayBuffer = await response.arrayBuffer();
                    const fileBuffer = Buffer.from(arrayBuffer);
                    const rows = await parseCsv(fileBuffer);
                
                    if (rows && rows.length > 0) {
                        const placeholders = CONSOLIDE_COLUMNS_ORDER.map(() => '?').join(', ');
                        const insertSql = `INSERT INTO "${tableName}" (${CONSOLIDE_COLUMNS_ORDER.map(h => `"${h}"`).join(', ')}) VALUES (${placeholders})`;
                
                        const statements = rows.map(row => ({
                            sql: insertSql,
                            args: CONSOLIDE_COLUMNS_ORDER.map(header => row[header] ?? null)
                        }));
                        
                        await db.batch(statements);
                        messages.push(`Imported ${rows.length} rows into ${tableName}.`);
                    }
                }
            } catch (error: any) {
                messages.push(`Special handling for consolide.csv failed: ${error.message}`);
            }

            const migrationMessages = [];
            const tablesToMigrate = [
                { name: 'suivi_curatif', dateCol: 'date_entree' },
                { name: 'consolide', dateCol: 'date' },
                { name: 'vidange', dateCol: 'date' }
            ];

            for (const table of tablesToMigrate) {
                try {
                    const tableInfo = await getQuery(db, `PRAGMA table_info(${table.name})`);
                    const columnExists = tableInfo.some((col: any) => col.name === 'date_iso');

                    if (!columnExists) {
                        await runQuery(db, `ALTER TABLE ${table.name} ADD COLUMN date_iso TEXT;`);
                        await runQuery(db, `
                            UPDATE ${table.name}
                            SET date_iso = substr(${table.dateCol}, 7, 4) || '-' || substr(${table.dateCol}, 4, 2) || '-' || substr(${table.dateCol}, 1, 2)
                            WHERE ${table.dateCol} IS NOT NULL AND ${table.dateCol} != ''
                        `);
                        await runQuery(db, `CREATE INDEX IF NOT EXISTS idx_${table.name}_date_iso ON ${table.name}(date_iso);`);
                        migrationMessages.push(`Optimized dates for table '${table.name}'.`);
                    }
                } catch (e: any) {
                    if (!e.message.includes("no such table")) {
                        migrationMessages.push(`Failed to optimize dates for '${table.name}': ${e.message}`);
                    }
                }
            }
            if(migrationMessages.length > 0) messages.push(migrationMessages.join(' '));

            try {
              const planningMsg = await createPlanningCacheTable(db);
              messages.push(planningMsg);
            } catch (error: any) {
              messages.push(`Planning cache error: ${error.message}`);
            }
            try {
                const historyMsg = await createHistoryCacheTable(db);
                messages.push(historyMsg);
            } catch (error: any) {
                messages.push(`History cache error: ${error.message}`);
            }

            try {
                await runQuery(db, `
                    CREATE TABLE IF NOT EXISTS category_entretiens (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        category TEXT NOT NULL,
                        entretien TEXT NOT NULL,
                        is_active INTEGER DEFAULT 1 NOT NULL,
                        UNIQUE(category, entretien)
                    )
                `);

                const categoriesRows = await getQuery(db, 'SELECT DISTINCT categorie FROM matrice');
                const categories = categoriesRows.map((r: any) => r.categorie).filter(Boolean);
                
                const statements = [];
                for (const category of categories) {
                    const categoryNorm = normalize(category || '').replace(/,/g, '');
                    const exclusions = EXCLUSIONS_NORM[categoryNorm] || new Set();

                    for (const entretien of OFFICIAL_ENTRETIENS) {
                        const entretienNorm = normalize(entretien);
                        const isActive = !exclusions.has(entretienNorm);
                        statements.push({
                            sql: 'INSERT OR IGNORE INTO category_entretiens (category, entretien, is_active) VALUES (?, ?, ?)',
                            args: [category, entretien, isActive ? 1 : 0]
                        });
                    }
                }
                
                if (statements.length > 0) {
                    await db.batch(statements);
                }
                messages.push('Table category_entretiens created and populated.');
            } catch (error: any) {
                messages.push(`Category/Entretien setup error: ${error.message}`);
            }
            
            try {
                await runQuery(db, `
                    CREATE TABLE IF NOT EXISTS weekly_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        start_date TEXT NOT NULL,
                        end_date TEXT NOT NULL,
                        generated_at TEXT NOT NULL,
                        report_data_json TEXT NOT NULL
                    )
                `);
                messages.push('Table weekly_reports created.');
            } catch (error: any) {
                messages.push(`Weekly reports table creation error: ${error.message}`);
            }

            try {
                await runQuery(db, `
                    CREATE TABLE IF NOT EXISTS declarations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        operation_id INTEGER NOT NULL,
                        generated_at TEXT NOT NULL,
                        report_data_json TEXT NOT NULL
                    )
                `);
                messages.push('Table declarations created.');
            } catch (error: any) {
                messages.push(`Declarations table creation error: ${error.message}`);
            }

            try {
                await runQuery(db, `
                    CREATE TABLE IF NOT EXISTS stock_entries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        lubricant_type TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        reference TEXT
                    )
                `);
                messages.push('Table stock_entries created.');
            } catch (error: any) {
                messages.push(`Stock entries table creation error: ${error.message}`);
            }

            try {
                await runQuery(db, `
                    CREATE TABLE IF NOT EXISTS bons_de_sortie (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        destinataire_chantier TEXT NOT NULL,
                        destinataire_code TEXT,
                        transporteur_nom TEXT,
                        transporteur_immatriculation TEXT,
                        items_json TEXT NOT NULL,
                        generated_at TEXT NOT NULL
                    )
                `);
                messages.push('Table bons_de_sortie created.');
            } catch (error: any) {
                messages.push(`Bons de sortie table creation error: ${error.message}`);
            }

            try {
                // For joins and lookups by matricule
                await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_matrice_matricule ON matrice(matricule)');
                await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_suivi_curatif_matricule ON suivi_curatif(matricule)');
                await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_consolide_matricule ON consolide(matricule)');
                await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_vidange_matricule ON vidange(matricule)');

                // For sorting operations by date
                await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_suivi_curatif_date_iso_sort ON suivi_curatif(date_iso DESC, id DESC)');
                
                // For filtering and sorting consumptions by date
                await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_consolide_date_iso_sort ON consolide(date_iso)');
                
                // For other frequently accessed/sorted tables
                await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_declarations_operation_id ON declarations(operation_id)');
                await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_bons_de_sortie_generated_at ON bons_de_sortie(generated_at DESC)');
                await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_stock_entries_date ON stock_entries(date DESC)');

                messages.push('Database indexes created for performance optimization.');
            } catch (error: any) {
                messages.push(`Index creation failed: ${error.message}`);
            }

            return { success: true, message: `Vérification de la base de données terminée. Les tables et index manquants ont été créés. Les données existantes n'ont pas été modifiées.` };
        });
    } catch(e: any) {
        return { success: false, message: `Database file could not be created or opened. ${e.message}` };
    }
}

export async function getEquipmentCount(): Promise<number> {
    return withDb(async (db) => {
        try {
            const result = await getOne(db, 'SELECT COUNT(*) as count FROM matrice');
            return (result as any)?.count || 0;
        } catch (e: any) {
            console.error("Failed to get equipment count:", e.message);
            return 0;
        }
    });
}

export async function getOperationCountForYear(year: number): Promise<number> {
  return withDb(async (db) => {
    try {
      const result = await getOne(db,
        "SELECT COUNT(*) as count FROM suivi_curatif WHERE substr(date_iso, 1, 4) = ?",
        [year.toString()]
      );
      return (result as any)?.count || 0;
    } catch (e: any) {
      console.error(`Failed to get operation count for year ${year}:`, e.message);
      return 0;
    }
  });
}

export async function getRecentOperations(limit = 5): Promise<any[]> {
    return withDb(async (db) => {
        try {
            const rows = await getQuery(db, `
            SELECT 
                sc.*,
                m.designation
            FROM suivi_curatif sc
            LEFT JOIN matrice m ON sc.matricule = m.matricule
            ORDER BY sc.date_iso DESC, sc.id DESC
            LIMIT ?
            `, [limit]);
            return rows.map((row: any) => ({
                ...row,
                operation: row.panne_declaree || row.pieces || 'Opération non spécifiée',
            }));
        } catch (e: any) {
            console.error("An error occurred in getRecentOperations:", e.message);
            return [];
        }
    });
}

export async function getAllEquipment(): Promise<any[]> {
    try {
        return await withDb(async (db) => {
            return await getQuery(db, 'SELECT * FROM matrice');
        });
    } catch (e: any) {
        if (e.message?.includes('no such table')) {
            console.warn("Table 'matrice' not found. The database might not be initialized.");
        } else {
            console.error("An error occurred in getAllEquipment:", e.message);
        }
        return [];
    }
}

export async function getAllOperations(): Promise<any[]> {
    try {
        return await withDb(async (db) => {
            const rows = await getQuery(db, `
            SELECT 
                sc.*,
                m.designation
            FROM suivi_curatif sc
            LEFT JOIN matrice m ON sc.matricule = m.matricule
            ORDER BY sc.date_iso DESC, sc.id DESC
            `);
            
            return rows.map((row: any) => ({
                ...row,
                operation: row.panne_declaree || row.pieces || 'Opération non spécifiée',
                date_programmee: row.date_entree,
                date_realisation: row.date_sortie,
                nature: row.type_de_panne || 'non spécifié',
                niveau: 'Curatif'
            }));
        });
    } catch (e: any) {
        if (e.message?.includes('no such table')) {
            console.warn("Table 'suivi_curatif' or 'declarations' not found. The database might not be initialized.");
        } else {
            console.error("An error occurred in getAllOperations:", e.message);
        }
        return [];
    }
}

const createHistoryCacheTable = async (db: DbClient): Promise<string> => {
    try {
        await runQuery(db, `
            CREATE TABLE IF NOT EXISTS history_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                matricule TEXT,
                operation TEXT,
                date TEXT,
                releve_compteur REAL,
                source TEXT
            )
        `);
        await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_history_cache_matricule ON history_cache(matricule)');
        return 'Table history_cache created or verified successfully.';
    } catch(error: any) {
        throw new Error(`Failed to create history_cache table: ${error.message}`);
    }
  };
  
  function extraireReleve(txt: any): number | null {
    if (!txt) return null;
    const cleaned = txt.toString().replace(/[^0-9]/g, ' ');
    const tokens = cleaned.trim().split(/\s+/).filter(Boolean);
    for (const token of tokens) {
      const num = parseInt(token, 10);
      if (!isNaN(num) && num > 10) {
        return num;
      }
    }
    return null;
  }
  
  const parseDateSafe = (dateStr: any) => {
      if (!dateStr || typeof dateStr !== 'string') return null;
      const date = dayjs(dateStr.trim(), ['DD/MM/YYYY', 'D/M/YYYY', 'YYYY-MM-DD HH:mm:ss'], true);
      return date.isValid() ? date.format('DD/MM/YYYY') : null;
  };
  
export async function generateHistoryMatrix() {
  return withDb(async (db) => {
      await createHistoryCacheTable(db);
      console.log('📜 [DEBUG] Démarrage de la génération de l\'historique unifié...');
      
      const allOperations: {
          matricule: string;
          operation: string;
          date: string;
          releve: number | null;
          source: string;
      }[] = [];

      // 1. CURATIF
      const suiviData = await getQuery(db, 'SELECT matricule, date_entree, panne_declaree, pieces FROM suivi_curatif');
      for (const row of suiviData as any[]) {
          const matricule = row.matricule?.toString().trim();
          const date = parseDateSafe(row.date_entree);
          if (!matricule || !date) continue;
          
          let pieces = row.pieces?.toLowerCase();
          if (!pieces) continue;

          if (pieces.startsWith('remplacement de ') || pieces.startsWith('changement de ')) {
              pieces = pieces.substring(pieces.indexOf(' de ') + 4);
          }
          const piecesList = pieces.split('-').map((p: string) => p.trim()).filter(Boolean);
          
          const releve = extraireReleve(row.panne_declaree);

          for (const piece of piecesList) {
             const matchedEntretien = findMatchedEntretien(piece);
            if (matchedEntretien) {
                allOperations.push({ matricule, operation: matchedEntretien, date, releve, source: 'suivi_curatif' });
            }
          }
      }

      // 2. VIDANGE
      const vidanges = await getQuery(db, 'SELECT * FROM vidange');
      for (const row of vidanges as any[]) {
          const matricule = row.matricule?.toString().trim();
          const dateStr = getVal(row, 'date', 'date_entretien');
          const date = parseDateSafe(dateStr);
          if (!matricule || !date) continue;
          
          const counterStr = getVal(row, 'compteur_kmh', 'compteur_km_h');
          const releve = extraireReleve(counterStr);
          
          allOperations.push({ matricule, operation: 'Vidanger le carter moteur', date, releve, source: 'vidange' });
          
          const flag = (v: any) => typeof v === 'string' && ['*', '**'].includes(v.trim());
          const obs = (row.obs || '').toUpperCase();
          const rowKeys = Object.keys(row);

          const fhKey = rowKeys.find(k => k === 'fh' || k === 'f_h');
          const fgKey = rowKeys.find(k => k === 'fg' || k === 'f_g');
          const fairKey = rowKeys.find(k => k === 'fair' || k === 'f_air');
          const fhydKey = rowKeys.find(k => k === 'fhyd' || k === 'f_hyd');

          if ((fhKey && flag(row[fhKey])) || obs.includes('FH')) allOperations.push({ matricule, operation: 'Filtre à huile', date, releve, source: 'vidange' });
          if ((fgKey && flag(row[fgKey])) || obs.includes('FG')) allOperations.push({ matricule, operation: 'Filtre carburant', date, releve, source: 'vidange' });
          if ((fairKey && flag(row[fairKey])) || obs.includes('FAIR')) allOperations.push({ matricule, operation: 'Filtre à air', date, releve, source: 'vidange' });
          if ((fhydKey && flag(row[fhydKey])) || obs.includes('FHYD')) allOperations.push({ matricule, operation: 'Filtre hydraulique', date, releve, source: 'vidange' });
          if (obs.includes('CHAINE')) allOperations.push({ matricule, operation: 'chaine', date, releve, source: 'vidange' });
      }

      // 3. CONSOLIDE
      const consolideData = await getQuery(db, 'SELECT * FROM consolide');
      for (const row of consolideData as any[]) {
          const matricule = row.matricule?.toString().trim();
          if (!matricule) continue;
      
          const date = parseDateSafe(row['date']);
          if (!date) continue;
          
          const obsText = row['obs']?.toString().trim() || '';
          const releve = extraireReleve(obsText);
          
          const getQty = (colName: string): number => parseFloat(String(row[colName] || '0').replace(',', '.')) || 0;
      
          const engineOils = ['15w40', '15w40_v', '15w40_quartz', '15w40_4400'];
          const totalEngineOil = engineOils.reduce((sum, type) => sum + getQty(type), 0);
          if (totalEngineOil > 0) {
              const seuilVidange = getQty('v');
              if (seuilVidange > 0 && totalEngineOil >= seuilVidange) {
                  allOperations.push({ matricule, operation: 'Vidanger le carter moteur', date, releve, source: 'consolide' });
              } else {
                  allOperations.push({ matricule, operation: 'Niveau d\'huile du carter', date, releve, source: 'consolide' });
              }
          }
          
          const hydraulicOils = ['10w', 't32', 'hvol', 't46'];
          const totalHydraulicOil = hydraulicOils.reduce((sum, type) => sum + getQty(type), 0);
          if (totalHydraulicOil > 0) {
              allOperations.push({ matricule, operation: 'circuit hydraulique', date, releve, source: 'consolide' });
          }
      
          const transmissionOils = ['90', 'tvol', 't30'];
          const totalTransmissionOil = transmissionOils.reduce((sum, type) => sum + getQty(type), 0);
          if (totalTransmissionOil > 0) {
              allOperations.push({ matricule, operation: 'boite de vitesse', date, releve, source: 'consolide' });
          }
          
          if (getQty('graisse') > 0) {
              allOperations.push({ matricule, operation: 'Graissage général', date, releve, source: 'consolide' });
          }
      }
      
      // 4. SAUVEGARDE
      if (allOperations.length) {
          await runQuery(db, 'DELETE FROM history_cache');
          const statements = allOperations.map(o => ({
              sql: `INSERT INTO history_cache (matricule, operation, date, releve_compteur, source) VALUES (?, ?, ?, ?, ?)`,
              args: [o.matricule, o.operation, o.date, o.releve, o.source]
          }));
          await db.batch(statements);
          console.log(`[generateHistoryMatrix] Sauvegardé ${allOperations.length} opérations dans history_cache.`);
      }
      
      return getHistoryMatrixFromCache(db);
  });
}
  
export async function getHistoryMatrixFromCache(dbInstance?: DbClient) {
    const process = async (db: DbClient) => {
        const allOperations = await getQuery(db, 'SELECT * FROM history_cache') as any[];

        // --- Calculate Counts ---
        const counts: Record<string, number> = {};
        for (const op of allOperations) {
            if (op.operation) {
                counts[op.operation] = (counts[op.operation] || 0) + 1;
            }
        }
        const uniqueMatricules = new Set(allOperations.map(op => op.matricule));
        counts['MATRICULE'] = uniqueMatricules.size;
        const releveCount = allOperations.filter(op => op.releve_compteur != null).length;
        counts['relevé compteur'] = releveCount;


        const groupedByMatriculeAndMonth: { [key: string]: { [op: string]: string | number | null | undefined | Set<string>, releve?: number | null, dates: Set<string> } } = {};
        
        for (const op of allOperations) {
            const date = dayjs(op.date, 'DD/MM/YYYY');
            if (date.isValid() && op.operation && HEADER_ORDER.includes(op.operation as any)) {
                const key = `${op.matricule}|${date.format('YYYY-MM')}`;
                if (!groupedByMatriculeAndMonth[key]) {
                    groupedByMatriculeAndMonth[key] = { dates: new Set() };
                }
                groupedByMatriculeAndMonth[key][op.operation] = op.date;
                groupedByMatriculeAndMonth[key].dates.add(op.date);

                const releve = op.releve_compteur;
                if (releve) {
                    const existingReleve = groupedByMatriculeAndMonth[key].releve;
                    if (!existingReleve || releve > existingReleve) {
                        groupedByMatriculeAndMonth[key].releve = releve;
                    }
                }
            }
        }

        const sortedGroupKeys = Object.keys(groupedByMatriculeAndMonth).sort((a, b) => {
            const [matA, dateA] = a.split('|');
            const [matB, dateB] = b.split('|');
            if (matA < matB) return -1;
            if (matA > matB) return 1;
            if (dateA > dateB) return -1; // Sort by date descending for each matricule
            if (dateA < dateB) return 1;
            return 0;
        });

        const headers = HEADER_ORDER;
        const rows: (string | null)[][] = [];

        for (const key of sortedGroupKeys) {
            const [matricule] = key.split('|');
            const groupData = groupedByMatriculeAndMonth[key];
            const row: (string | null)[] = [matricule];

            for (let i = 1; i < headers.length; i++) {
                const header = headers[i];
                if (header === 'relevé compteur') {
                    row.push(groupData.releve?.toString() || null);
                } else {
                    const value = groupData[header];
                    row.push(typeof value === 'string' ? value : (typeof value === 'number' ? value.toString() : null));
                }
            }
            rows.push(row);
        }

        const result = { headers, rows, counts };
        console.log(`Historique chargé depuis le cache : ${result.rows.length} lignes de matrice créées.`);
        return result;
    };

    if (dbInstance) {
        return process(dbInstance);
    } else {
        return withDb(process);
    }
}


const createPlanningCacheTable = async (db: DbClient): Promise<string> => {
  try {
    await runQuery(db, `
      CREATE TABLE IF NOT EXISTS planning_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year INTEGER,
        matricule TEXT,
        categorie TEXT,
        entretien TEXT,
        date_programmee TEXT,
        intervalle INTEGER,
        niveau TEXT
      )
    `);
    await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_planning_year ON planning_cache(year)');
    await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_planning_mat ON planning_cache(matricule)');
    await runQuery(db, 'CREATE INDEX IF NOT EXISTS idx_planning_date ON planning_cache(date_programmee)');
    return 'Table planning_cache created or verified successfully.';
  } catch (error: any) {
    throw new Error(`Failed to create planning_cache table: ${error.message}`);
  }
};

export async function getHistoryForEquipment(matricule: string): Promise<any[]> {
    try {
        return await withDb(async (db) => {
            const operations = await getQuery(db, 'SELECT * FROM history_cache WHERE matricule = ?', [matricule]) as any[];
            return operations.map((op, index) => ({
                id: op.id || index,
                matricule: op.matricule,
                operation: op.operation,
                date_programmee: op.date, 
                date_realisation: op.date,
                nature: 'Réalisé', 
                niveau: 'Historique',
                intervalle_jours: null,
            }));
        });
    } catch(e) {
        console.error(`Error getting history for equipment ${matricule}`, e);
        return [];
    }
}

const savePlanningToDb = async (db: DbClient, year: number, data: any[]) => {
    if (!data || data.length === 0) {
      console.log(`[SAVE] Aucune donnée à sauvegarder pour l'année ${year}.`);
      return;
    }
    
    await runQuery(db, `DELETE FROM planning_cache WHERE year = ?`, [year]);
    
    const statements = data.map(r => ({
        sql: `INSERT INTO planning_cache (year, matricule, categorie, entretien, date_programmee, intervalle, niveau) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: [year, r.matricule, r.categorie, r.entretien, r.date_programmee, r.intervalle, r.niveau]
    }));

    await db.batch(statements);
    console.log(`[SAVE] ${data.length} entrées sauvegardées pour l'année ${year}.`);
};

export const generatePlanning = async (year: number) => {
    return withDb(async (db) => {
        const matrice = await getQuery(db, 'SELECT matricule, categorie FROM matrice') as any[];
        const paramsRaw = await getQuery(db, 'SELECT * FROM param') as any[];
        const fullHistory = await getQuery(db, 'SELECT matricule, operation, MAX(date) as last_date FROM history_cache GROUP BY matricule, operation') as any[];

        const allowedEntretiensByCategory: Record<string, Set<string>> = {};
        const hasCategoryRules = await getOne(db, "SELECT name FROM sqlite_master WHERE type='table' AND name='category_entretiens'") !== undefined;

        if (hasCategoryRules) {
            try {
                const allowedRows = await getQuery(db, 'SELECT category, entretien FROM category_entretiens WHERE is_active = 1') as any[];
                for (const row of allowedRows) {
                    const categoryNorm = normalize(row.category).replace(/,/g, '');
                    if (!allowedEntretiensByCategory[categoryNorm]) {
                        allowedEntretiensByCategory[categoryNorm] = new Set();
                    }
                    allowedEntretiensByCategory[categoryNorm].add(normalize(row.entretien));
                }
            } catch (e) {
                console.warn("Could not read from category_entretiens table. No category-based exclusions will be applied.", e);
            }
        }

        const lastDatesByMatricule: { [matricule: string]: { [opNorm: string]: dayjs.Dayjs } } = {};
        for (const h of fullHistory) {
            if (!h.matricule || !h.operation) continue;
            const matriculeNorm = normalize(h.matricule);
            if (!lastDatesByMatricule[matriculeNorm]) {
                lastDatesByMatricule[matriculeNorm] = {};
            }
            const date = dayjs(h.last_date, 'DD/MM/YYYY');
            if (date.isValid()) {
                const opNorm = normalize(h.operation);
                lastDatesByMatricule[matriculeNorm][opNorm] = date;
            }
        }
        
        const paramHeaders = Object.keys(paramsRaw[0] || {});
        const intervalCols = ['7', '30', '90', '180', '360'];
        
        const levelColNames: string[] = [];
        const levelCols: { name: string, level: 'C'|'N'|'CH' }[] = [
            { name: paramHeaders.find(h => h.toLowerCase().includes('contrôler')) || '', level: 'C' },
            { name: paramHeaders.find(h => h.toLowerCase().includes('nettoyage')) || '', level: 'N' },
            { name: paramHeaders.find(h => h.toLowerCase().includes('changement')) || '', level: 'CH' },
        ].filter((c): c is { name: string, level: 'C'|'N'|'CH' } => !!c.name).map(c => {
            levelColNames.push(c.name);
            return c;
        });

        const knownCols = new Set(['id', ...intervalCols, ...levelColNames]);
        const opCol = paramHeaders.find(h => !knownCols.has(h));

        if (!opCol) {
            console.error('CRITICAL: Could not determine the operation column in the "Param" table. Planning generation failed.');
            return { count: 0, rows: [] };
        }

        const levelPriority = { 'C': 1, 'N': 2, 'CH': 3 };

        let potentialResults: any[] = [];
        const startOfYear = dayjs(`${year}-01-01`);
        const endOfYear = dayjs(`${year}-12-31`);

        for (const engin of matrice) {
            const matricule = (engin.matricule || '').trim();
            if (!matricule) continue;
            
            const categorieNorm = normalize(engin.categorie || '').replace(/,/g, '');
            const allowedForCategory = allowedEntretiensByCategory[categorieNorm];

            for (const param of paramsRaw) {
                const entretienNameFromParam = (param[opCol] || '').trim();
                if (!entretienNameFromParam) continue;

                const officialEntretienName = OFFICIAL_ENTRETIENS.find(e => normalize(e) === normalize(entretienNameFromParam));
                if (!officialEntretienName) continue;
                
                const entretienNorm = normalize(officialEntretienName);
                if (hasCategoryRules && allowedForCategory !== undefined && !allowedForCategory.has(entretienNorm)) {
                    continue;
                }
                
                const lastDate = lastDatesByMatricule[normalize(matricule)]?.[entretienNorm];
                
                const activeIntervals = intervalCols
                    .map(intervalStr => ({
                        interval: parseInt(intervalStr),
                        symbol: param[intervalStr]?.toString().trim()
                    }))
                    .filter(item => item.symbol === '*' || item.symbol === '**');
                
                const activeLevels = levelCols
                    .filter(l => param[l.name])
                    .map(l => l.level)
                    .sort((a, b) => (levelPriority[a] || 0) - (levelPriority[b] || 0));

                activeIntervals.forEach((intervalItem, index) => {
                    const level = activeLevels[index];
                    if (!level) return;
                    
                    const { interval } = intervalItem;

                    const referenceDate = lastDate || dayjs(`${year}-01-01`);
                    let currentDate = referenceDate.clone();
                    
                    while (currentDate.isBefore(startOfYear)) {
                        currentDate = currentDate.add(interval, 'day');
                    }
                    
                    while (currentDate.isSameOrBefore(endOfYear)) {
                        potentialResults.push({
                            matricule: matricule,
                            categorie: engin.categorie,
                            entretien: officialEntretienName,
                            date_programmee: currentDate.format('DD/MM/YYYY'),
                            intervalle: interval,
                            niveau: level,
                            priority: levelPriority[level]
                        });
                        currentDate = currentDate.add(interval, 'day');
                    }
                });
            }
        }
        
        const finalResultsMap: Map<string, any> = new Map();
        
        potentialResults.sort((a,b) => b.priority - a.priority);

        for (const res of potentialResults) {
            const key = `${res.matricule}|${res.entretien}|${res.date_programmee}`;
            if (!finalResultsMap.has(key)) {
                finalResultsMap.set(key, res);
            }
        }
        
        const finalResults = Array.from(finalResultsMap.values());
        
        await savePlanningToDb(db, year, finalResults);
        console.log(`✅ Planning complet généré pour ${year} : ${finalResults.length} entrées.`);
        
        return { count: finalResults.length, rows: finalResults };
    });
};

const createPlanningMatrix = async (db: DbClient, year: number, filter = '', page = 1, pageSize = 1, forExport = false, applyFollowUp = false) => {
    const allMatriculesQuery = `SELECT DISTINCT matricule FROM matrice WHERE matricule IS NOT NULL AND matricule != '' ORDER BY matricule`;
    const allMatriculesRows = await getQuery(db, allMatriculesQuery) as any[];
    const allMatricules = allMatriculesRows.map(m => m.matricule);
    
    let filteredMatricules = allMatricules;
    if (filter) {
        filteredMatricules = allMatricules.filter(m => m.toLowerCase().includes(filter.toLowerCase()));
    }
    
    const total = filteredMatricules.length;
    const paginatedMatricules = forExport ? filteredMatricules : filteredMatricules.slice((page - 1) * pageSize, page * pageSize);

    if (paginatedMatricules.length === 0) {
        return { headers: ['MATRICULE', 'MOIS', ...OFFICIAL_ENTRETIENS], rows: [], total: 0 };
    }

    const matriculePlaceholders = paginatedMatricules.map(() => '?').join(',');
    const interventionsParams = [year, ...paginatedMatricules];

    const interventionsQuery = `
        SELECT id, matricule, entretien, date_programmee, niveau 
        FROM planning_cache 
        WHERE year = ? AND matricule IN (${matriculePlaceholders})
    `;
    
    const interventionsRows = await getQuery(db, interventionsQuery, interventionsParams) as any[];

    const curativeOps = applyFollowUp ? await getQuery(db, 'SELECT matricule, date_entree, date_sortie FROM suivi_curatif') as any[] : [];
    const breakdownIntervals: { [matricule: string]: { start: dayjs.Dayjs, end: dayjs.Dayjs }[] } = {};
    const today = dayjs();

    for (const op of curativeOps) {
        if (!op.matricule || !op.date_entree) continue;
        const start = dayjs(op.date_entree, 'DD/MM/YYYY');
        const endStr = (op.date_sortie || '').toLowerCase();
        const end = (endStr.includes('cour')) ? today : dayjs(op.date_sortie, 'DD/MM/YYYY');
        
        if (start.isValid() && end.isValid()) {
            if (!breakdownIntervals[op.matricule]) {
                breakdownIntervals[op.matricule] = [];
            }
            breakdownIntervals[op.matricule].push({ start, end });
        }
    }
    
    let processedInterventions = interventionsRows.map(inv => {
        const plannedDate = dayjs(inv.date_programmee, 'DD/MM/YYYY');
        let inBreakdown = false;
        if (applyFollowUp) {
            const matriculeIntervals = breakdownIntervals[inv.matricule];
            if (matriculeIntervals) {
                for (const interval of matriculeIntervals) {
                    if (plannedDate.isBetween(interval.start, interval.end, 'day', '[]')) {
                        inBreakdown = true;
                        break;
                    }
                }
            }
        }
        return {
            ...inv,
            date: plannedDate,
            realise: false,
            date_realisation: undefined as (string | undefined),
            status: 'planifié',
            inBreakdown
        };
    });

    if (applyFollowUp) {
        const historyData = await getProcessedHistory(db, year);
        const realizedInterventions = historyData
            .map(h => ({
                ...h,
                usedInMatch: false,
            }))
            .filter(h => h.date.isValid() && h.date.year() === year);

        let plannedInterventionsWithMatchInfo = processedInterventions.map(p => ({ ...p, usedInMatch: false }));

        for (const realized of realizedInterventions) {
            let bestMatch: { planned: (typeof plannedInterventionsWithMatchInfo)[number], diff: number } | null = null;
            
            for (const planned of plannedInterventionsWithMatchInfo) {
                if (!planned.usedInMatch &&
                    normalize(realized.matricule) === normalize(planned.matricule) &&
                    normalize(realized.operation) === normalize(planned.entretien)) {
                    
                    const diff = Math.abs(realized.date.diff(planned.date, 'day'));
                    if (diff <= 30) {
                        if (!bestMatch || diff < bestMatch.diff) {
                            bestMatch = { planned, diff };
                        }
                    }
                }
            }

            if (bestMatch) {
                bestMatch.planned.realise = true;
                bestMatch.planned.date_realisation = realized.date.format('DD/MM/YYYY');
                bestMatch.planned.status = 'réalisé';
                bestMatch.planned.usedInMatch = true;
                realized.usedInMatch = true;
                bestMatch.planned.month = realized.date.month();
            }
        }
        
        const horsPlanning = realizedInterventions
            .filter(r => !r.usedInMatch)
            .map(r => ({
                id: `hp-${r.id}`,
                matricule: r.matricule,
                entretien: r.operation,
                date_programmee: r.date.format('DD/MM/YYYY'),
                niveau: 'HP', 
                date: r.date,
                realise: true,
                usedInMatch: true,
                date_realisation: r.date.format('DD/MM/YYYY'),
                status: 'hors-planning',
                inBreakdown: false,
            }));
        
        processedInterventions = [...plannedInterventionsWithMatchInfo, ...horsPlanning];
    }
    
    const levelPriority: Record<string, number> = { 'C': 1, 'N': 2, 'CH': 3, 'HP': 4 };
    
    const interventionsByMatricule: { [key: string]: any[] } = {};
    for (const inv of processedInterventions) {
        if (!interventionsByMatricule[inv.matricule]) {
            interventionsByMatricule[inv.matricule] = [];
        }
        interventionsByMatricule[inv.matricule].push({
            ...inv,
            month: inv.date.month()
        });
    }

    const matrixRows: any[] = [];
    
    for (const equipmentMatricule of paginatedMatricules) {
        for (let monthIndex = 0; monthIndex < 12; monthIndex++) {
            const rowData: any[] = [equipmentMatricule, dayjs().month(monthIndex).format('MMMM')];
            const cells: (any | null)[] = Array.from({ length: OFFICIAL_ENTRETIENS.length }, () => null);

            const interventionsForMonth = (interventionsByMatricule[equipmentMatricule] || [])
                .filter(inv => inv.month === monthIndex);
            
            const byEntretien: Record<string, any[]> = {};
            for (const intervention of interventionsForMonth) {
                const entretienName = intervention.entretien;
                const officialMatch = OFFICIAL_ENTRETIENS.find(official => 
                    normalize(official) === normalize(entretienName)
                );
                
                if (officialMatch) {
                    if (!byEntretien[officialMatch]) {
                        byEntretien[officialMatch] = [];
                    }
                    byEntretien[officialMatch].push(intervention);
                }
            }
            
            for (const [entretienOfficial, interventions] of Object.entries(byEntretien)) {
                const colIndex = OFFICIAL_ENTRETIENS.indexOf(entretienOfficial as any);
                if (colIndex === -1) continue;
                
                const bestIntervention = interventions.reduce((best, current) => {
                    const bestPriority = levelPriority[best.niveau] || 0;
                    const currentPriority = levelPriority[current.niveau] || 0;
                    if (current.realise && !best.realise) return current;
                    if (!current.realise && best.realise) return best;
                    if (currentPriority > bestPriority) return current;
                    if (currentPriority === bestPriority && current.date.isAfter(best.date)) return current;
                    return best;
                });
                
                cells[colIndex] = {
                    date_programmee: bestIntervention.date_programmee,
                    niveau: bestIntervention.niveau,
                    realise: bestIntervention.realise,
                    date_realisation: bestIntervention.date_realisation,
                    inBreakdown: bestIntervention.inBreakdown,
                };
            }
            
            matrixRows.push([...rowData, ...cells]);
        }
    }

    const headers = ['MATRICULE', 'MOIS', ...OFFICIAL_ENTRETIENS];
    return { headers, rows: matrixRows, total };
};

export const getPlanningPage = async (
    year: number,
    page = 1,
    pageSize = 1,
    filter = ''
): Promise<{ headers: readonly string[]; rows: any[]; total: number }> => {
    return withDb(async (db) => {
        return createPlanningMatrix(db, year, filter, page, pageSize, false, false);
    });
};

export const getFollowUpPage = async (
    year: number,
    page = 1,
    pageSize = 1,
    filter = ''
): Promise<{ headers: readonly string[]; rows: any[]; total: number }> => {
    return withDb(async (db) => {
        return createPlanningMatrix(db, year, filter, page, pageSize, false, true);
    });
};

export const getPlanningMatrixForExport = async (
    year: number
  ): Promise<{ headers: readonly string[]; rows: any[] }> => {
    return withDb(async (db) => {
        try {
            const { total } = await createPlanningMatrix(db, year, '', 1, 1, false, false);
            return createPlanningMatrix(db, year, '', 1, total > 0 ? total : 1, true, false);
        } catch(e) {
            return { headers: [], rows: [] };
        }
    });
};

export const getFollowUpMatrixForExport = async (
    year: number
  ): Promise<{ headers: readonly string[]; rows: any[] }> => {
    return withDb(async (db) => {
        try {
            const { total } = await createPlanningMatrix(db, year, '', 1, 1, false, true);
            return createPlanningMatrix(db, year, '', 1, total > 0 ? total : 1, true, true);
        } catch(e) {
            return { headers: [], rows: [] };
        }
    });
  };

export const getAllPlanning = async () => {
    return withDb(async (db) => {
        return await getQuery(db, `SELECT * FROM planning_cache ORDER BY year, substr(date_programmee, 7, 4), substr(date_programmee, 4, 2), substr(date_programmee, 1, 2)`);
    });
};

export const getPreventativeHistoryForEquipment = async (matricule: string) => {
  return withDb(async (db) => {
    const preventativeEntries: PreventativeMaintenanceEntry[] = [];
    const consolideData = await getQuery(db, 'SELECT * FROM consolide WHERE matricule = ?', [matricule]) as any[];
    const oilColumns = ['t32', '10w', '15w40', '90', '15w40_v', 'hvol', 'tvol', 't30', 'graisse', 't46', '15w40_quartz'];

    for (const row of consolideData) {
        const entretienCode = row['entretien']?.toString().trim().toUpperCase();
        if (!entretienCode) continue;

        const officialOperationName = (planningOperationNameMapping as Record<string, string>)[entretienCode];
        if (!officialOperationName) continue;

        const date = parseDateSafe(row['date']);
        if (!date) continue;
        
        if (entretienCode === 'GR' && (!row['graisse'] || parseFloat(row['graisse'].toString().replace(',', '.')) <= 0)) {
            continue;
        }

        const details: string[] = [];
        const counter = extraireReleve(row['obs']);
        if (counter) {
            details.push(`Relevé compteur: ${counter.toLocaleString('fr-FR')}`);
        }

        for (const col of oilColumns) {
            if (row[col]) {
                const oilQty = parseFloat(row[col].toString().replace(',', '.') || '0');
                if (oilQty > 0) {
                    details.push(`Huile ${col.replace(/_/g, ' ').toUpperCase()}: ${oilQty}L`);
                }
            }
        }
        
        preventativeEntries.push({
            id: `consolide-${row.id}`,
            operation: officialOperationName,
            date: date,
            details: details,
        });
    }

    const vidangeData = await getQuery(db, 'SELECT * FROM vidange WHERE matricule = ?', [matricule]) as any[];
    for (const row of vidangeData) {
      const dateStr = getVal(row, 'date', 'date_entretien');
      const date = parseDateSafe(dateStr);
      if (!date) continue;

      const counterStr = getVal(row, 'compteur_kmh', 'compteur_km_h');
      const counter = extraireReleve(counterStr);
      const details = counter ? [`Relevé compteur: ${counter.toLocaleString('fr-FR')}`] : [];
      preventativeEntries.push({ id: `vidange-main-${row.id}`, operation: 'Vidanger le carter moteur', date, details });
      
      const flag = (v: any) => typeof v === 'string' && ['*', '**'].includes(v.trim());
      const obs = (row.obs || '').toUpperCase();
      const rowKeys = Object.keys(row);

      const fhKey = rowKeys.find(k => k === 'fh' || k === 'f_h');
      const fgKey = rowKeys.find(k => k === 'fg' || k === 'f_g');
      const fairKey = rowKeys.find(k => k === 'fair' || k === 'f_air');
      const fhydKey = rowKeys.find(k => k === 'fhyd' || k === 'f_hyd');

      if ((fhKey && flag(row[fhKey])) || obs.includes('FH')) preventativeEntries.push({ id: `vidange-fh-${row.id}`, operation: 'Filtre à huile', date, details });
      if ((fgKey && flag(row[fgKey])) || obs.includes('FG')) preventativeEntries.push({ id: `vidange-fg-${row.id}`, operation: 'Filtre carburant', date, details });
      if ((fairKey && flag(row[fairKey])) || obs.includes('FAIR')) preventativeEntries.push({ id: `vidange-fa-${row.id}`, operation: 'Filtre à air', date, details });
      if ((fhydKey && flag(row[fhydKey])) || obs.includes('FHYD')) preventativeEntries.push({ id: `vidange-fhyd-${row.id}`, operation: 'Filtre hydraulique', date, details });
      if (obs.includes('CHAINE')) preventativeEntries.push({ id: `vidange-chaine-${row.id}`, operation: 'chaine', date, details });
    }

    const grouped: Record<string, PreventativeMaintenanceEntry[]> = {};
    for (const entry of preventativeEntries) {
      if (!grouped[entry.operation]) {
        grouped[entry.operation] = [];
      }
      grouped[entry.operation].push(entry);
    }

    for (const op in grouped) {
      grouped[op].sort((a, b) => dayjs(b.date, 'DD/MM/YYYY').valueOf() - dayjs(a.date, 'DD/MM/YYYY').valueOf());
    }

    return grouped;
  });
}; 

export async function getCurativeHistoryForEquipment(matricule: string): Promise<CurativeMaintenanceEntry[]> {
  return withDb(async (db) => {
      const rows = await getQuery(db, 'SELECT * FROM suivi_curatif WHERE matricule = ? ORDER BY id DESC', [matricule]) as any[];

      const parsePieces = (text: string | null): string[] => {
          if (!text) return [];
          
          if (text.includes('-')) {
               let items = text.toLowerCase();
              if (items.startsWith('remplacement de ') || items.startsWith('changement de ')) {
                  items = items.substring(items.indexOf(' de ') + 4);
              }
              return items.split('-').map(item => item.trim()).filter(Boolean);
          }

          return [text];
      };

      return rows.map((row): CurativeMaintenanceEntry => {
          const dateEntree = dayjs(row.date_entree, 'DD/MM/YYYY');
          let dateSortie;
          let joursIndisponibilite: number | null;

          if (row.date_sortie && row.date_sortie.toLowerCase().includes('cour')) {
              dateSortie = dayjs(); 
          } else {
              dateSortie = dayjs(row.date_sortie, 'DD/MM/YYYY');
          }
          
          if (dateEntree.isValid() && dateSortie.isValid()) {
            const diff = dateSortie.diff(dateEntree, 'day');
            joursIndisponibilite = diff >= 0 ? diff + 1 : 1;
          } else {
            // Fallback to stored value if dates are invalid
            const storedValue = parseInt(row.nbr_indisponibilite, 10);
            joursIndisponibilite = !isNaN(storedValue) ? storedValue : 1;
          }

          const officialTags = OFFICIAL_ENTRETIENS.filter(entretien => 
              row.pieces?.toLowerCase().includes(entretien.toLowerCase())
          );

          return {
              id: row.id,
              panneDeclaree: row.panne_declaree,
              typePanne: row.type_de_panne || 'non spécifié',
              dateEntree: dateEntree.isValid() ? dateEntree.format('DD/MM/YYYY') : row.date_entree,
              dateSortie: dateSortie.isValid() ? dateSortie.format('DD/MM/YYYY') : row.date_sortie,
              dureeIntervention: joursIndisponibilite,
              piecesRemplacees: parsePieces(row.pieces),
              details: {
                  Intervenant: row.intervenant,
                  Affectation: row.affectation,
                  'Statut Actuel': row.sitactuelle,
                  'Jours Ouvrables': row.jour_ouvrable,
              },
              tags: officialTags,
          };
      });
  });
}


export async function getEquipmentDetails(matricule: string) {
    try {
        return await withDb(async (db) => {
            return await getOne(db, 'SELECT * FROM matrice WHERE matricule = ?', [matricule]);
        });
    } catch(e: any) {
        if (e.message?.includes('no such table')) {
            console.warn(`Table 'matrice' not found for getEquipmentDetails. DB not initialized?`);
        } else {
            console.error(`Could not get details for equipment ${matricule}`, e);
        }
        return null;
    }
}

export async function getEquipmentDynamicStatus(matricule: string): Promise<'En Marche' | 'En Panne' | 'Actif'> {
    try {
        return await withDb(async (db) => {
            try {
                const lastIntervention = await getOne(db,
                    `SELECT date_sortie FROM suivi_curatif WHERE matricule = ? 
                     ORDER BY date_iso DESC, id DESC 
                     LIMIT 1`,
                    [matricule]
                ) as any;

                if (!lastIntervention) {
                    return 'Actif';
                }
                
                if (lastIntervention.date_sortie && lastIntervention.date_sortie.toLowerCase().includes('cour')) {
                    return 'En Panne';
                }

                return 'En Marche';

            } catch (e: any) {
                 if (e.message?.includes('no such table')) {
                    return 'Actif';
                }
                console.error(`Could not determine dynamic status for ${matricule}`, e);
                return 'Actif';
            }
        });
    } catch (e) {
        console.error(`Could not connect to database for getEquipmentDynamicStatus`, e);
        return 'Actif';
    }
}

export async function getSuiviCuratifRaw() {
    return withDb(async (db) => {
        try {
            // We select all columns to provide a raw view, limited to the last 50 entries for performance.
            return await getQuery(db, 'SELECT * FROM suivi_curatif ORDER BY id DESC LIMIT 50');
        } catch (e: any) {
            if (e.message?.includes('no such table')) {
                console.warn("Table 'suivi_curatif' not found. The database might not be initialized.");
                return [];
            }
            throw e;
        }
    });
}

export async function getAllPlanningForYear(year: number) {
    return withDb(async (db) => {
        try {
            return await getQuery(db, `
                SELECT 
                    pc.matricule, 
                    m.designation,
                    pc.entretien as operation, 
                    pc.date_programmee,
                    pc.niveau
                FROM planning_cache pc
                LEFT JOIN matrice m ON pc.matricule = m.matricule
                WHERE pc.year = ?
            `, [year]);
        } catch (e: any) {
            if (e.message?.includes('no such table')) {
                console.warn("Table 'planning_cache' or 'matrice' not found. The database might not be initialized.");
                return [];
            }
            throw e;
        }
    });
}

export async function getParams() {
  return withDb(async (db) => {
    try {
      return await getQuery(db, 'SELECT * FROM param ORDER BY id');
    } catch (e: any) {
      if (e.message?.includes('no such table')) {
        console.warn("Table 'Param' not found. The database might not be initialized.");
        return [];
      }
      throw e;
    }
  });
}

export async function updateParam(id: number, column: string, value: string | null) {
  return withDb(async (db) => {
    const info = await getQuery(db, `PRAGMA table_info(Param)`) as any[];
    const allowedColumns = info.map((col: any) => col.name);

    if (!allowedColumns.includes(column)) {
      throw new Error(`Invalid column name provided: ${column}`);
    }

    const result = await runQuery(db, `UPDATE Param SET "${column}" = ? WHERE id = ?`, [value, id]);

    try {
        await runQuery(db, 'DELETE FROM planning_cache');
        console.log('Planning cache cleared due to parameter update.');
    } catch (e) {
        console.log('Could not clear planning cache (it may not exist yet).');
    }

    return { success: true };
  });
}

async function getProcessedHistory(db: DbClient, year: number): Promise<{ id: number, matricule: string, operation: string, date: dayjs.Dayjs, releve: number | null }[]> {
    const historyData = await getQuery(db,
        "SELECT id, matricule, operation, date, releve_compteur FROM history_cache WHERE substr(date, 7, 4) = ?",
        [year.toString()]
    ) as any[];
    
    return historyData.map(record => ({
        id: record.id,
        matricule: record.matricule,
        operation: record.operation,
        date: dayjs(record.date, 'DD/MM/YYYY'),
        releve: record.releve_compteur,
    })).filter(record => record.date.isValid() && record.operation);
}


export async function getFollowUpStatistics(year: number) {
  try {
    return await withDb(async (db) => {
        const plannedInterventions = await getQuery(db,
            'SELECT matricule, entretien, niveau FROM planning_cache WHERE year = ?',
            [year]
        ) as any[];
        const realizedInterventions = await getProcessedHistory(db, year);

        const plannedCounts: { [key: string]: number } = {};
        for (const p of plannedInterventions) {
            const key = `${normalize(p.matricule)}|${normalize(p.entretien)}`;
            plannedCounts[key] = (plannedCounts[key] || 0) + 1;
        }

        const realizedCounts: { [key: string]: number } = {};
        for (const r of realizedInterventions) {
            const key = `${normalize(r.matricule)}|${normalize(r.operation)}`;
            realizedCounts[key] = (realizedCounts[key] || 0) + 1;
        }

        let totalRealise = 0;
        const totalRealiseByEntretien: Record<string, number> = {};

        for (const key in plannedCounts) {
            const plannedCount = plannedCounts[key];
            const realizedCount = realizedCounts[key] || 0;
            const matches = Math.min(plannedCount, realizedCount);
            
            if (matches > 0) {
                totalRealise += matches;
                 const originalEntretien = plannedInterventions.find(p => key.endsWith(normalize(p.entretien)))?.entretien;
                 if (originalEntretien) {
                    totalRealiseByEntretien[originalEntretien] = (totalRealiseByEntretien[originalEntretien] || 0) + matches;
                 }
            }
        }
        
        const totalPlanifieByEntretien: Record<string, number> = {};
        const totalPlanifieByNiveau: Record<string, number> = { C: 0, N: 0, CH: 0 };
         for (const p of plannedInterventions) {
            totalPlanifieByEntretien[p.entretien] = (totalPlanifieByEntretien[p.entretien] || 0) + 1;
            const niveau = p.niveau as keyof typeof totalPlanifieByNiveau;
             if (totalPlanifieByNiveau[niveau] !== undefined) {
                totalPlanifieByNiveau[niveau]++;
            }
        }

        return {
            totalPlanifie: plannedInterventions.length,
            totalRealise: totalRealise,
            planifieByNiveau: totalPlanifieByNiveau,
            realiseByNiveau: { C: 0, N: 0, CH: 0 }, // Cannot be determined from current data
            realiseByEntretien: totalRealiseByEntretien,
            planifieByEntretien: totalPlanifieByEntretien,
        };
    });
  } catch (e: any) {
     if (e.message?.includes('no such table')) {
        console.warn("A required table for stats was not found. DB might not be initialized.");
        return null;
      }
      throw e;
  }
}

export async function getDistinctCategories() {
    return withDb(async (db) => {
        try {
            const rows = await getQuery(db, 'SELECT DISTINCT categorie FROM matrice') as any[];
            return rows.map(r => r.categorie).filter(Boolean).sort();
        } catch (e: any) {
            if (e.message?.includes('no such table')) {
                console.warn("Table 'matrice' not found.");
            } else {
                console.error("An error occurred in getDistinctCategories:", e.message);
            }
            return [];
        }
    });
}

export async function getCategoryEntretiens() {
    return withDb(async (db) => {
        try {
            const rows = await getQuery(db, 'SELECT category, entretien, is_active FROM category_entretiens') as any[];
            const data: Record<string, Record<string, boolean>> = {};
            for (const row of rows) {
                if (!data[row.category]) {
                    data[row.category] = {};
                }
                data[row.category][row.entretien] = !!row.is_active;
            }
            return data;
        } catch (e: any) {
             if (e.message?.includes('no such table')) {
                console.warn("Table 'category_entretiens' not found.");
            } else {
                console.error("An error occurred in getCategoryEntretiens:", e.message);
            }
            return {};
        }
    });
}

export async function updateCategoryEntretiens(category: string, entretien: string, isActive: boolean) {
    return withDb(async (db) => {
        const result = await runQuery(db,
            'UPDATE category_entretiens SET is_active = ? WHERE category = ? AND entretien = ?',
            [isActive ? 1 : 0, category, entretien]
        );
        
        // Try inserting if no rows were updated
        if ((result as any).changes === 0) {
          await runQuery(db,
            'INSERT OR IGNORE INTO category_entretiens (category, entretien, is_active) VALUES (?, ?, ?)',
            [category, entretien, isActive ? 1 : 0]
          );
        }

        try {
            await runQuery(db, 'DELETE FROM planning_cache');
            console.log('Planning cache cleared due to category parameter update.');
        } catch (e) {
            console.log('Could not clear planning cache (it may not exist yet).');
        }
        return { success: true };
    });
}

export async function addCurativeOperationToDb(operationData: any) {
  return withDb(async (db) => {
    const equipment = await getOne(db, 'SELECT categorie, designation FROM matrice WHERE matricule = ?', [operationData.matricule]) as any;

    if (!equipment) {
        throw new Error(`Matricule '${operationData.matricule}' non trouvé dans la table des équipements.`);
    }

    const dateEntreeDayjs = dayjs(operationData.date_entree, 'DD/MM/YYYY');
    const fullOperationData = {
        ...operationData,
        date_iso: dateEntreeDayjs.isValid() ? dateEntreeDayjs.format('YYYY-MM-DD') : null,
        categorie: equipment.categorie,
        designation: equipment.designation
    };

    const columns = [
        'categorie', 'designation', 'matricule', 'date_entree', 'date_iso',
        'panne_declaree', 'sitactuelle', 'pieces', 'date_sortie', 
        'intervenant', 'affectation', 'type_de_panne', 
        'nbr_indisponibilite', 'jour_ouvrable', 'ratio', 
        'jour_disponibilite', 'ratio2'
    ];

    const placeholders = columns.map(() => '?').join(', ');
    const sql = `INSERT INTO suivi_curatif (${columns.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`;

    const values = columns.map(col => fullOperationData[col] ?? null);

    const result = await runQuery(db, sql, values);
    const lastID = Number((result as any).lastInsertRowid);

    // Also add to history cache for immediate availability in other views
    if (lastID && fullOperationData.pieces) {
        const piecesList = fullOperationData.pieces.toLowerCase().split('-').map((p: string) => p.trim()).filter(Boolean);
        const releve = extraireReleve(fullOperationData.panne_declaree);
        
        const statements = [];
        for (const piece of piecesList) {
            const matchedEntretien = findMatchedEntretien(piece);
           if (matchedEntretien) {
                statements.push({
                    sql: `INSERT INTO history_cache (matricule, operation, date, releve_compteur, source) VALUES (?, ?, ?, ?, ?)`,
                    args: [fullOperationData.matricule, matchedEntretien, fullOperationData.date_entree, releve, 'suivi_curatif_new']
                });
           }
        }
        if (statements.length > 0) {
            await db.batch(statements);
        }
    }

    return { id: lastID };
  });
}

const parseReparationsForReport = (text: string | null): string[] => {
    if (!text) return [];
    if (text.includes('-')) {
        return text.split('-').map(p => p.trim()).filter(Boolean);
    }
    return [text.trim()];
};

export async function generateAndSaveWeeklyReport(targetDate?: Date) {
  return withDb(async (db) => {
    const referenceDate = dayjs(targetDate || new Date());
    
    const day = referenceDate.day(); // 0=Sun, 6=Sat
    const startOfWeek = referenceDate.subtract(day, 'day');
    const endOfWeek = startOfWeek.add(4, 'day');

    const searchEndDate = endOfWeek.format('YYYY-MM-DD');

    const allPannesFromDb = await getQuery(db, `
      SELECT 
        m.designation, 
        sc.id as operation_id,
        sc.matricule,
        sc.date_entree,
        sc.panne_declaree,
        sc.pieces,
        sc.date_sortie,
        sc.intervenant
      FROM suivi_curatif sc
      LEFT JOIN matrice m ON sc.matricule = m.matricule
      WHERE sc.date_iso <= ?
    `, [searchEndDate]) as any[];

    const allReportPannes: WeeklyReportItem[] = [];

    for (const panneData of allPannesFromDb) {
      let nature_panne = panneData.panne_declaree;
      let reparations_str = panneData.pieces;
      let date_sortie_str = panneData.date_sortie;
      let intervenant_str = panneData.intervenant;

      if (nature_panne && nature_panne.includes(';')) {
        const parts = nature_panne.split(';');
        if (parts.length >= 4) { 
          nature_panne = parts[0] || nature_panne;
          reparations_str = parts[2] || reparations_str;
          date_sortie_str = parts[3] || date_sortie_str;
          intervenant_str = parts[4]?.replace(/"/g, '') || intervenant_str;
        }
      }

      const dEnt = dayjs(panneData.date_entree, "DD/MM/YYYY");
      if (!dEnt.isValid()) continue;

      const dSort = (date_sortie_str && date_sortie_str.trim().toLowerCase() !== "en cours")
        ? dayjs(date_sortie_str, "DD/MM/YYYY")
        : null;

      let isRelevant = false;
      if (!dSort || !dSort.isValid()) {
        if (dEnt.isBetween(startOfWeek, endOfWeek, 'day', '[]')) {
          isRelevant = true;
        }
      } 
      else {
        if (dSort.isBetween(startOfWeek, endOfWeek, 'day', '[]')) {
          if (dEnt.isSameOrBefore(endOfWeek, 'day')) {
             isRelevant = true;
          }
        }
      }

      if (isRelevant) {
        allReportPannes.push({
          numero: 0, // Placeholder
          designation: panneData.designation,
          matricule: panneData.matricule,
          date_panne: dEnt.format("DD/MM/YYYY"),
          nature_panne: nature_panne,
          reparations: parseReparationsForReport(reparations_str),
          date_sortie: dSort && dSort.isValid() ? dSort.format("DD/MM/YYYY") : "En Cours",
          intervenant: intervenant_str,
          obs: ''
        });
      }
    }

    allReportPannes.sort((a, b) => dayjs(a.date_panne, "DD/MM/YYYY").unix() - dayjs(b.date_panne, "DD/MM/YYYY").unix());

    allReportPannes.forEach((p, index) => {
      p.numero = index + 1;
    });

    const reportData: Omit<WeeklyReport, 'id'> = {
      start_date: startOfWeek.format('YYYY-MM-DD'),
      end_date: endOfWeek.format('YYYY-MM-DD'),
      generated_at: dayjs().format(),
      pannes: allReportPannes
    };

    const result = await runQuery(db,
      `INSERT INTO weekly_reports (start_date, end_date, generated_at, report_data_json) VALUES (?, ?, ?, ?)`,
      [reportData.start_date, reportData.end_date, reportData.generated_at, JSON.stringify(reportData.pannes)]
    );

    return Number((result as any).lastInsertRowid);
  });
}

export async function getWeeklyReports() {
    return withDb(async (db) => {
        try {
            return await getQuery(db, 'SELECT id, start_date, end_date, generated_at FROM weekly_reports ORDER BY generated_at DESC');
        } catch (e) {
            console.error("Failed to get weekly reports list", e);
            return [];
        }
    });
}

export async function getWeeklyReport(id: number): Promise<WeeklyReport | null> {
     return withDb(async (db) => {
        try {
            const row = await getOne(db, 'SELECT * FROM weekly_reports WHERE id = ?', [id]) as any;
            if (!row) return null;

            return {
                id: row.id,
                start_date: row.start_date,
                end_date: row.end_date,
                generated_at: row.generated_at,
                pannes: JSON.parse(row.report_data_json),
            };

        } catch (e) {
            console.error(`Failed to get weekly report with id ${id}`, e);
            return null;
        }
    });
}

export async function deleteWeeklyReport(id: number) {
    return withDb(async (db) => {
        await runQuery(db, 'DELETE FROM weekly_reports WHERE id = ?', [id]);
        return { success: true };
    });
}

export async function getMonthlyCurativeCounts(year?: number): Promise<MonthlyCount[]> {
  const targetYear = (year || new Date().getFullYear()).toString();
  return withDb(async (db) => {
    try {
      const rows = await getQuery(db, `
        SELECT
          substr(date_iso, 6, 2) as month,
          COUNT(*) as count
        FROM suivi_curatif
        WHERE substr(date_iso, 1, 4) = ?
        GROUP BY month
        ORDER BY month
      `, [targetYear]) as any[];

      const monthMap = new Map(rows.map(r => [r.month, r.count]));

      const result = Array.from({ length: 12 }, (_, i) => {
        const monthNum = (i + 1).toString().padStart(2, '0');
        return {
          month: dayjs().month(i).format('MMM'),
          count: monthMap.get(monthNum) || 0,
        };
      });

      return result;

    } catch (e: any) {
      console.error("Failed to get monthly curative counts:", e.message);
      // Return a default structure on error
      return Array.from({ length: 12 }, (_, i) => ({ month: dayjs().month(i).format('MMM'), count: 0 }));
    }
  });
}

export async function getMonthlyPreventativeStats(year: number, month?: number): Promise<MonthlyPreventativeStats> {
    const defaultData = {
        monthlyData: Array.from({ length: 12 }, (_, i) => ({
            month: dayjs().month(i).format('MMM'),
            vidange: 0,
            graissage: 0,
            transmission: 0,
            hydraulique: 0,
            autres: 0
        })),
        totalOil: 0,
        oilByType: {}
    };

    try {
        return await withDb(async (db) => {
            let query = `SELECT * FROM consolide WHERE substr(date_iso, 1, 4) = ?`;
            const params: (string|number)[] = [year.toString()];
            if (month) {
                query += ` AND substr(date_iso, 6, 2) = ?`;
                params.push(month.toString().padStart(2, '0'));
            }
            const rows = await getQuery(db, query, params) as any[];

            const monthlyData = JSON.parse(JSON.stringify(defaultData.monthlyData));
            let totalHuile = 0;
            let graisseQty = 0;
            const oilByType: Record<string, number> = {};

            const oilColumns = {
                engine: ['15w40', '15w40_v', '15w40_quartz', '15w40_4400'],
                hydraulic: ['10w', 't32', 'hvol', 't46'],
                transmission: ['90', 'tvol', 't30'],
            };
            const allOilCols = Object.values(oilColumns).flat();

            for (const row of rows) {
                const date = dayjs(row.date, 'DD/MM/YYYY');
                if (!date.isValid()) continue;
                
                const monthIndex = date.month();
                
                const getQty = (colName: string): number => parseFloat(String(row[colName] || '0').replace(',', '.')) || 0;

                const seuilVidange = getQty('v');
                const totalEngineOil = oilColumns.engine.reduce((sum, type) => sum + getQty(type), 0);
                
                if (totalEngineOil > 0) {
                    if (seuilVidange > 0 && totalEngineOil >= seuilVidange) {
                        monthlyData[monthIndex].vidange++;
                    } else {
                        monthlyData[monthIndex].autres++;
                    }
                }
                
                if (getQty('graisse') > 0) {
                    monthlyData[monthIndex].graissage++;
                }
                
                const totalHydraulicOil = oilColumns.hydraulic.reduce((sum, type) => sum + getQty(type), 0);
                if (totalHydraulicOil > 0) {
                    monthlyData[monthIndex].hydraulique++;
                }

                const totalTransmissionOil = oilColumns.transmission.reduce((sum, type) => sum + getQty(type), 0);
                if (totalTransmissionOil > 0) {
                    monthlyData[monthIndex].transmission++;
                }

                // Calculate total oil consumption for the period
                for (const col of allOilCols) {
                    const qty = getQty(col);
                    if (qty > 0) {
                        totalHuile += qty;
                        oilByType[col] = (oilByType[col] || 0) + qty;
                    }
                }
                const currentGraisse = getQty('graisse');
                if(currentGraisse > 0) {
                    graisseQty += currentGraisse;
                    oilByType['graisse'] = (oilByType['graisse'] || 0) + currentGraisse;
                }
            }
            
            return { monthlyData, totalOil: totalHuile, oilByType };
        });
    } catch (e: any) {
        if (e.message?.includes('no such table')) {
             console.warn("Table 'consolide' not found. The database might not be initialized.");
        } else {
            console.error("Failed to get monthly preventative stats:", e.message);
        }
        return defaultData;
    }
}

export async function getOperationById(id: number) {
    return withDb(async (db) => {
        const row = await getOne(db, `
            SELECT sc.*, m.designation, m.marque, m.categorie
            FROM suivi_curatif sc
            LEFT JOIN matrice m on sc.matricule = m.matricule
            WHERE sc.id = ?
        `, [id]) as any;
        if (!row) return null;
        return {
            ...row,
            operation: row.panne_declaree || row.pieces || 'Opération non spécifiée',
            date_programmee: row.date_entree,
            date_realisation: row.date_sortie,
            nature: row.type_de_panne || 'non spécifié',
            niveau: 'Curatif'
        };
    });
}

export async function saveDeclaration(operationId: number, reportDataJson: string) {
    return withDb(async (db) => {
        const result = await runQuery(db,
            `INSERT INTO declarations (operation_id, generated_at, report_data_json) VALUES (?, ?, ?)`,
            [operationId, dayjs().format(), reportDataJson]
        );
        return Number((result as any).lastInsertRowid);
    });
}

export async function getDeclarationById(id: number): Promise<DeclarationPanne | null> {
    return withDb(async (db) => {
        const row = await getOne(db, 'SELECT * FROM declarations WHERE id = ?', [id]) as any;
        if (!row) return null;
        
        const reportData = JSON.parse(row.report_data_json);
        const operation = await getOperationById(row.operation_id);
        if (!operation) return null;

        const equipment = await getEquipmentDetails(operation.matricule);
        if (!equipment) return null;
        
        return {
            id: row.id,
            operation_id: row.operation_id,
            generated_at: row.generated_at,
            operation,
            equipment,
            ...reportData
        };
    });
}


export async function getDeclarationsList() {
    return withDb(async (db) => {
        try {
            const rows = await getQuery(db, `
                SELECT d.id, d.generated_at, s.matricule, s.panne_declaree, m.designation
                FROM declarations d
                JOIN suivi_curatif s ON d.operation_id = s.id
                LEFT JOIN matrice m ON s.matricule = m.matricule
                ORDER BY d.generated_at DESC
            `) as any[];
            return rows;
        } catch (e: any) {
            console.error("Failed to get declarations list", e);
            if (e.message?.includes('no such table')) {
                return [];
            }
            throw e;
        }
    });
}

export async function deleteDeclarationFromDb(id: number) {
    return withDb(async (db) => {
        await runQuery(db, 'DELETE FROM declarations WHERE id = ?', [id]);
        return { success: true };
    });
}

export async function updateDeclarationInDb(id: number, reportDataJson: string) {
    return withDb(async (db) => {
        await runQuery(db,
            `UPDATE declarations SET report_data_json = ? WHERE id = ?`,
            [reportDataJson, id]
        );
        return { success: true };
    });
}


export async function getEquipmentById(id: number) {
  return withDb(async (db) => {
    return await getOne(db, 'SELECT * FROM matrice WHERE id = ?', [id]);
  });
}

export async function addEquipmentToDb(data: any) {
  return withDb(async (db) => {
    const columns = Object.keys(data);
    const placeholders = columns.map(() => '?').join(', ');
    const sql = `INSERT INTO matrice (${columns.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`;
    const values = columns.map(col => data[col] ?? null);
    const result = await runQuery(db, sql, values);
    return { id: Number((result as any).lastInsertRowid) };
  });
}

export async function updateEquipmentInDb(id: number, data: any) {
  return withDb(async (db) => {
    const oldEquipment = await getOne(db, 'SELECT matricule FROM matrice WHERE id = ?', [id]) as any;
    if (!oldEquipment) {
      throw new Error(`Equipment with id ${id} not found.`);
    }
    const oldMatricule = oldEquipment.matricule;
    const newMatricule = data.matricule;

    const columns = Object.keys(data);
    const setClause = columns.map(col => `"${col}" = ?`).join(', ');
    const updateSql = `UPDATE matrice SET ${setClause} WHERE id = ?`;
    const updateValues = [...columns.map(col => data[col] ?? null), id];
    await runQuery(db, updateSql, updateValues);
    
    let matriculeChanged = false;
    if (oldMatricule && newMatricule && oldMatricule !== newMatricule) {
      matriculeChanged = true;
      const tablesToUpdate = ['suivi_curatif', 'vidange', 'consolide'];
      for (const table of tablesToUpdate) {
        await runQuery(db, `UPDATE ${table} SET matricule = ? WHERE matricule = ?`, [newMatricule, oldMatricule]);
      }
      await runQuery(db, 'DELETE FROM history_cache');
      await runQuery(db, 'DELETE FROM planning_cache');
    }
    
    return { success: true, matriculeChanged };
  });
}

export async function deleteEquipmentFromDb(id: number) {
  return withDb(async (db) => {
    const equipment = await getOne(db, 'SELECT matricule FROM matrice WHERE id = ?', [id]) as any;
    if (equipment) {
      const matricule = equipment.matricule;
      await runQuery(db, 'DELETE FROM suivi_curatif WHERE matricule = ?', [matricule]);
      await runQuery(db, 'DELETE FROM vidange WHERE matricule = ?', [matricule]);
      await runQuery(db, 'DELETE FROM consolide WHERE matricule = ?', [matricule]);
      await runQuery(db, 'DELETE FROM history_cache WHERE matricule = ?', [matricule]);
      await runQuery(db, 'DELETE FROM planning_cache WHERE matricule = ?', [matricule]);
      
      await runQuery(db, 'DELETE FROM matrice WHERE id = ?', [id]);
    } else {
      console.warn(`Attempted to delete equipment with id ${id}, but it was not found in 'matrice' table.`);
    }
    return { success: true };
  });
}

export async function getCurativeFiches(startDate: string, endDate: string, matricule?: string): Promise<CurativeFicheData[]> {
  return withDb(async (db) => {
    let query = `
      SELECT
        sc.id,
        sc.date_entree,
        sc.affectation,
        m.designation,
        sc.matricule,
        m.marque,
        m.categorie,
        sc.type_de_panne,
        sc.panne_declaree,
        sc.pieces,
        sc.intervenant,
        sc.sitactuelle,
        sc.nbr_indisponibilite
      FROM suivi_curatif sc
      LEFT JOIN matrice m ON sc.matricule = m.matricule
      WHERE 1=1
    `;

    const params: (string | number)[] = [];
    
    query += ` AND sc.date_iso BETWEEN ? AND ?`;
    params.push(startDate, endDate);

    if (matricule) {
      query += ` AND sc.matricule = ?`;
      params.push(matricule);
    }

    query += ` ORDER BY sc.date_iso`;

    const rows = await getQuery(db, query, params) as any[];
    
    return rows.map((row: any) => ({
      ...row,
      pieces: row.pieces ? row.pieces.split('-').map((p:string) => p.trim()).filter(Boolean) : [],
    }));
  });
}

export async function getOrdresDeTravail(startDate: string, endDate: string, matricule?: string): Promise<OrdreTravailData[]> {
  return withDb(async (db) => {
    let query = `
      SELECT
        sc.id,
        sc.date_entree,
        sc.affectation,
        m.designation,
        sc.matricule,
        m.marque,
        sc.type_de_panne,
        sc.panne_declaree
      FROM suivi_curatif sc
      LEFT JOIN matrice m ON sc.matricule = m.matricule
      WHERE 1=1
    `;

    const params: (string | number)[] = [];
        
    query += ` AND sc.date_iso BETWEEN ? AND ?`;
    params.push(startDate, endDate);

    if (matricule) {
      query += ` AND sc.matricule = ?`;
      params.push(matricule);
    }

    query += ` ORDER BY sc.date_iso`;

    const rows = await getQuery(db, query, params) as any[];
    return rows;
  });
}

export async function getPreventiveFichesFromDb(startDate: string, endDate: string, matricule?: string): Promise<PreventiveFicheData[]> {
    return withDb(async (db) => {
        let query = `
            SELECT c.*, m.designation, m.marque, m.code_barre
            FROM consolide c
            LEFT JOIN matrice m ON c.matricule = m.matricule
            WHERE 1=1
        `;
        const params: (string|number)[] = [];
        
        query += ` AND c.date_iso BETWEEN ? AND ?`;
        params.push(startDate, endDate);

        if (matricule) {
            query += ` AND c.matricule = ?`;
            params.push(matricule);
        }

        query += ` ORDER BY c.date_iso`;

        const rows = await getQuery(db, query, params) as any[];
        
        const oilColumnConfig: { [key: string]: { organe: string, travail: string, lubrifiant: string } } = {
            // Vidange Types (check against V)
            '15w40_4400': { organe: 'Moteur', travail: 'VIDANGE', lubrifiant: '15W40 (4400)' },
            '15w40': { organe: 'Moteur', travail: 'VIDANGE', lubrifiant: '15W40' },
            '15w40_v': { organe: 'Moteur', travail: 'VIDANGE', lubrifiant: '15W40 V' },
            '15w40_quartz': { organe: 'Moteur', travail: 'VIDANGE', lubrifiant: '15W40 QUARTZ' },

            // Hydraulique Types
            '10w': { organe: 'Réservoir hydraulique', travail: 'HYDRAULIQUE', lubrifiant: '10W' },
            't32': { organe: 'Réservoir hydraulique', travail: 'HYDRAULIQUE', lubrifiant: 'T32' },
            'hvol': { organe: 'Réservoir hydraulique', travail: 'HYDRAULIQUE', lubrifiant: 'H.VOL' },
            't46': { organe: 'Réservoir hydraulique', travail: 'HYDRAULIQUE', lubrifiant: 'T46' },

            // Transmission Types
            '90': { organe: 'Boite de vitesse', travail: 'TRANSMISSION', lubrifiant: '90' },
            'tvol': { organe: 'Boite de vitesse', travail: 'TRANSMISSION', lubrifiant: 'T.VOL' },
            't30': { organe: 'Boite de vitesse', travail: 'TRANSMISSION', lubrifiant: 'T30' },
            
            // Graissage
            'graisse': { organe: 'Tous les graisseurs', travail: 'GRAISSAGE', lubrifiant: 'GRAISSE' }
        };

        const fiches: PreventiveFicheData[] = [];

        for (const row of rows) {
            const travaux: PreventiveFicheData['travaux'] = [];
            const seuilVidange = parseFloat(String(row.v || '0').replace(',', '.'));

            for (const colName in oilColumnConfig) {
                const config = oilColumnConfig[colName];
                const quantite = parseFloat(String(row[colName] || '0').replace(',', '.'));

                if (quantite > 0) {
                    let travailType = config.travail;

                    if (config.travail === 'VIDANGE' && seuilVidange > 0 && quantite < seuilVidange) {
                        travailType = "NIVEAU D'HUILE";
                    }

                    travaux.push({
                        organe: config.organe,
                        travail: travailType,
                        date: row.date,
                        lubrifiant: config.lubrifiant,
                        quantite: quantite,
                    });
                }
            }

            const obs = String(row.obs || '').toUpperCase();
            const filtrations: PreventiveFicheData['filtrations'] = {
                air: { active: obs.includes('FAIR'), date: obs.includes('FAIR') ? row.date : null },
                huile: { active: obs.includes('FH'), date: obs.includes('FH') ? row.date : null },
                gasoil: { active: obs.includes('FG'), date: obs.includes('FG') ? row.date : null },
                bypass: { active: false, date: null },
                hydraulique: { active: obs.includes('FHYD'), date: obs.includes('FHYD') ? row.date : null },
            };

            fiches.push({
                id: row.id,
                equipment: {
                    machine: `${row.designation || ''} ${row.marque || ''}`.trim(),
                    designation: row.designation,
                    marque: row.marque,
                    code: row.code_barre,
                    matricule: row.matricule,
                },
                entretien: {
                    intervenant: null, // The 'N°' column is a stock symbol, not a person's name.
                    date: row.date,
                },
                travaux,
                filtrations,
                observation: row.obs,
            });
        }
        
        return fiches;
    });
}

export async function getEquipmentForConsumption() {
    return withDb(async (db) => {
        try {
            return await getQuery(db, 'SELECT matricule, designation, qte_vidange FROM matrice') as any[];
        } catch (e: any) {
            console.error('Failed to get equipment list for consumption form', e);
            return [];
        }
    });
}

const calculateEntretienServer = (lubricants: Record<string, number>, qteVidange: number | null | undefined): string => {
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

export async function addConsolideEntries(date: string, entries: any[]) {
    return withDb(async (db) => {
        const insertColumns = [...CONSOLIDE_COLUMNS_ORDER, 'date_iso'];
        
        const statements = [];
        const historyStatements = [];

        for (const entry of entries) {
            const entretien = calculateEntretienServer(entry.lubricants, entry.qte_vidange);
            
            const dataMap: { [key: string]: any } = {
                v: entry.qte_vidange || null,
                n: null,
                date: date,
                date_iso: dayjs(date, 'DD/MM/YYYY').format('YYYY-MM-DD'),
                designation: entry.designation || null,
                matricule: entry.matricule,
                t32: entry.lubricants['t32'] || null,
                '15w40_4400': entry.lubricants['15w40_4400'] || null,
                '10w': entry.lubricants['10w'] || null,
                '15w40': entry.lubricants['15w40'] || null,
                '90': entry.lubricants['90'] || null,
                '15w40_v': entry.lubricants['15w40_v'] || null,
                hvol: entry.lubricants['hvol'] || null,
                tvol: entry.lubricants['tvol'] || null,
                t30: entry.lubricants['t30'] || null,
                graisse: entry.lubricants['graisse'] || null,
                t46: entry.lubricants['t46'] || null,
                '15w40_quartz': entry.lubricants['15w40_quartz'] || null,
                obs: entry.obs || null,
                entretien: entretien
            };
            
            const values = insertColumns.map(col => dataMap[col]);
            statements.push({
                sql: `INSERT INTO consolide (${insertColumns.map(c => `"${c}"`).join(', ')}) VALUES (${insertColumns.map(() => '?').join(', ')})`,
                args: values
            });

            const releve = extraireReleve(entry.obs);
            const lubricants = entry.lubricants;
            const qte_vidange = entry.qte_vidange;

            const addHistory = (op: string) => {
                historyStatements.push({
                    sql: `INSERT INTO history_cache (matricule, operation, date, releve_compteur, source) VALUES (?, ?, ?, ?, ?)`,
                    args: [entry.matricule, op, date, releve, 'consolide_stock']
                });
            };

            const engineOils = ['15w40', '15w40_v', '15w40_quartz', '15w40_4400'];
            const totalEngineOil = engineOils.reduce((sum, type) => sum + (lubricants[type] || 0), 0);
            if (totalEngineOil > 0) {
                if (qte_vidange && qte_vidange > 0 && totalEngineOil >= qte_vidange) {
                    addHistory('Vidanger le carter moteur');
                } else {
                    addHistory('Niveau d\'huile du carter');
                }
            }
            
            const hydraulicOils = ['10w', 't32', 'hvol', 't46'];
            const totalHydraulicOil = hydraulicOils.reduce((sum, type) => sum + (lubricants[type] || 0), 0);
            if (totalHydraulicOil > 0) {
                addHistory('circuit hydraulique');
            }

            const transmissionOils = ['90', 'tvol', 't30'];
            const totalTransmissionOil = transmissionOils.reduce((sum, type) => sum + (lubricants[type] || 0), 0);
            if (totalTransmissionOil > 0) {
                addHistory('boite de vitesse');
            }

            if ((lubricants['graisse'] || 0) > 0) {
                addHistory('Graissage général');
            }
        }

        if (statements.length > 0) {
            await db.batch([...statements, ...historyStatements]);
        }

        return { success: true, count: entries.length };
    });
}
    
export async function getConsolideByDateRange(startDate: Date, endDate: Date) {
    return withDb(async (db) => {
        const startDayjs = dayjs(startDate);
        const endDayjs = dayjs(endDate);
        const startOfYear = startDayjs.startOf('year');

        const startDateString = startDayjs.format('YYYY-MM-DD');
        const endDateString = endDayjs.format('YYYY-MM-DD');
        const yearString = startDayjs.year().toString();

        try {
            // Get consumptions for the selected range
            const consumptionsQuery = `SELECT * FROM consolide WHERE date_iso BETWEEN ? AND ? ORDER BY id DESC`;
            const filteredConsumptions = await getQuery(db, consumptionsQuery, [startDateString, endDateString]) as any[];

            // Get entries for the selected range
            const entriesQuery = `SELECT * FROM stock_entries WHERE date BETWEEN ? AND ?`;
            const filteredEntries = await getQuery(db, entriesQuery, [startDateString, endDateString]) as any[];

            // Summarize consumptions for the selected range
            const consumedSummary: Record<string, number> = {};
            LUBRICANT_TYPES.forEach(type => consumedSummary[type] = 0);
            for (const row of filteredConsumptions) {
                for (const type of LUBRICANT_TYPES) {
                    if (row[type]) {
                        const qty = parseFloat(String(row[type]).replace(',', '.')) || 0;
                        consumedSummary[type] += qty;
                    }
                }
            }

            // Summarize entries for the selected range
            const entrySummary: Record<string, number> = {};
            LUBRICANT_TYPES.forEach(type => entrySummary[type] = 0);
            for (const row of filteredEntries) {
                 if (row.lubricant_type && LUBRICANT_TYPES.includes(row.lubricant_type as any)) {
                    const qty = parseFloat(String(row.quantity).replace(',', '.')) || 0;
                    entrySummary[row.lubricant_type] += qty;
                 }
            }
            
            // Calculate initial stock at start of period more efficiently
            const yearInitialStocks = (initialStocks as Record<string, any>)[yearString] || {};
            const initialStockSummary: Record<string, number> = {};

            const lubricantPlaceholders = LUBRICANT_TYPES.map(() => 'SUM(CASE WHEN lubricant_type = ? THEN quantity ELSE 0 END)').join(', ');
            const getInitialEntriesSumQuery = `
                SELECT ${lubricantPlaceholders} 
                FROM stock_entries
                WHERE date >= ? AND date < ?`;
            const initialEntriesResult = await getOne(db, getInitialEntriesSumQuery, [...LUBRICANT_TYPES, startOfYear.format('YYYY-MM-DD'), startDateString]) as any;

            const lubricantSumClauses = LUBRICANT_TYPES.map(type => `SUM(CAST(REPLACE("${type}", ',', '.') as REAL))`).join(', ');
            const getInitialConsumptionsSumQuery = `
                SELECT ${lubricantSumClauses}
                FROM consolide
                WHERE date_iso >= ? AND date_iso < ?`;
            const initialConsumptionsResult = await getOne(db, getInitialConsumptionsSumQuery, [startOfYear.format('YYYY-MM-DD'), startDateString]) as any;
            
            LUBRICANT_TYPES.forEach((type, index) => {
                const baseStock = yearInitialStocks[type] || 0;
                const entriesBefore = initialEntriesResult ? (initialEntriesResult[Object.keys(initialEntriesResult)[index]] || 0) : 0;
                const consumptionsBefore = initialConsumptionsResult ? (initialConsumptionsResult[Object.keys(initialConsumptionsResult)[index]] || 0) : 0;
                initialStockSummary[type] = baseStock + entriesBefore - consumptionsBefore;
            });

            return { consumptions: filteredConsumptions, summary: { consumed: consumedSummary, entries: entrySummary }, initialStockSummary };

        } catch (e: any) {
            if (e.message?.includes('no such table')) {
                console.warn("Table 'consolide' or 'stock_entries' not found.");
                return { consumptions: [], summary: { consumed: {}, entries: {} }, initialStockSummary: {} };
            }
            console.error("An error occurred in getConsolideByDateRange:", e.message);
            throw e;
        }
    });
}

export async function addStockEntryToDb(data: { date: string, lubricant_type: string, quantity: number, reference?: string | null }) {
    return withDb(async (db) => {
        try {
            const result = await runQuery(db,
                'INSERT INTO stock_entries (date, lubricant_type, quantity, reference) VALUES (?, ?, ?, ?)',
                [data.date, data.lubricant_type, data.quantity, data.reference]
            );
            return { id: Number((result as any).lastInsertRowid) };
        } catch (e: any) {
            console.error("Failed to add stock entry to DB:", e);
            throw e;
        }
    });
}

export async function getStockEntriesFromDb() {
    return withDb(async (db) => {
        try {
            return await getQuery(db, 'SELECT * FROM stock_entries ORDER BY date DESC, id DESC') as any[];
        } catch (e: any) {
            if (e.message?.includes('no such table')) {
                console.warn("Table 'stock_entries' not found. The database might not be initialized.");
                return [];
            }
            throw e;
        }
    });
}

export async function deleteStockEntryFromDb(id: number) {
    return withDb(async (db) => {
        try {
            await runQuery(db, 'DELETE FROM stock_entries WHERE id = ?', [id]);
            return { success: true };
        } catch(e: any) {
            console.error(`Failed to delete stock entry with id ${id}`, e);
            throw e;
        }
    });
}

export async function getDailyConsumptionReportData({ year, month, lubricantType }: { year: number, month: number, lubricantType: string }): Promise<DailyReportData> {
    return withDb(async (db) => {
        const reportMonthStartDate = dayjs(`${year}-${month}-01`).startOf('month');
        let monthInitialStock = 0;
        
        // Use year-based initial stock if month is January
        if (month === 1) {
            monthInitialStock = (initialStocks as Record<string, any>)[year.toString()]?.[lubricantType] || 0;
        } else {
            // Calculate recursively from the start of the year
            const yearStartDate = dayjs(`${year}-01-01`).startOf('year');
            const yearInitialStock = (initialStocks as Record<string, any>)[year.toString()]?.[lubricantType] || 0;

            const consumptionsBeforeMonth = await getQuery(db,
                `SELECT date_iso, "${lubricantType}" as qty FROM consolide WHERE substr(date_iso, 1, 4) = ? AND "${lubricantType}" IS NOT NULL`,
                [year.toString()]
            ) as any[];
            const entriesBeforeMonth = await getQuery(db,
                'SELECT date, quantity FROM stock_entries WHERE lubricant_type = ? AND substr(date, 1, 4) = ?',
                [lubricantType, year.toString()]
            ) as any[];
            
            monthInitialStock = yearInitialStock;

            consumptionsBeforeMonth.forEach(row => {
                const date = dayjs(row.date_iso, 'YYYY-MM-DD');
                if (date.isValid() && date.isBefore(reportMonthStartDate, 'month')) {
                    monthInitialStock -= parseFloat(String(row.qty || '0').replace(',', '.')) || 0;
                }
            });

            entriesBeforeMonth.forEach(row => {
                const date = dayjs(row.date, 'YYYY-MM-DD');
                if (date.isValid() && date.isBefore(reportMonthStartDate, 'month')) {
                    monthInitialStock += parseFloat(String(row.quantity).replace(',', '.')) || 0;
                }
            });
        }
        
        // Get movements for the current month
        const consumptionsInMonth = await getQuery(db,
             `SELECT date, "${lubricantType}" as qty FROM consolide WHERE substr(date_iso, 1, 4) = ? AND substr(date_iso, 6, 2) = ? AND "${lubricantType}" IS NOT NULL`,
            [year.toString(), month.toString().padStart(2, '0')]
        ) as any[];
        const entriesInMonth = await getQuery(db,
            'SELECT date, quantity FROM stock_entries WHERE lubricant_type = ? AND substr(date, 1, 4) = ? AND substr(date, 6, 2) = ?',
            [lubricantType, year.toString(), month.toString().padStart(2, '0')]
        ) as any[];

        const daysInMonth = reportMonthStartDate.daysInMonth();
        const dailyData: DailyReportData['dailyData'] = [];
        let totalSorties = 0;
        let totalEntrees = 0;

        for (let i = 1; i <= daysInMonth; i++) {
            const currentDate = reportMonthStartDate.date(i);
            
            const dailySorties = consumptionsInMonth
                .filter(row => dayjs(row.date, 'DD/MM/YYYY').isSame(currentDate, 'day'))
                .reduce((sum, row) => sum + (parseFloat(String(row.qty || '0').replace(',', '.')) || 0), 0);

            const dailyEntrees = entriesInMonth
                .filter(row => dayjs(row.date, 'YYYY-MM-DD').isSame(currentDate, 'day'))
                .reduce((sum, row) => sum + (parseFloat(String(row.quantity).replace(',', '.')) || 0), 0);

            dailyData.push({
                date: currentDate.format('YYYY-MM-DD'),
                entree: dailyEntrees,
                sortie: dailySorties
            });

            totalSorties += dailySorties;
            totalEntrees += dailyEntrees;
        }
        
        const finalStock = monthInitialStock + totalEntrees - totalSorties;

        return {
            initialStock: monthInitialStock,
            dailyData,
            totalEntrees,
            totalSorties,
            finalStock,
            lubricantType,
            month,
            year,
        };
    });
}

export async function getMonthlyStockReportData(params: {
  startYear: number;
  startMonth: number;
  endYear: number;
  endMonth: number;
  lubricantType: string;
}): Promise<MonthlyStockReportData> {
    return withDb(async (db) => {
        const { startYear, startMonth, endYear, endMonth, lubricantType } = params;
        
        const startDate = dayjs(`${startYear}-${startMonth}-01`).startOf('month');
        const endDate = dayjs(`${endYear}-${endMonth}-01`).endOf('month');
        
        // 1. Calculate stock at the beginning of the start period
        let overallInitialStock = (initialStocks as Record<string, any>)[startYear.toString()]?.[lubricantType] || 0;
        
        const consumptionsBeforePeriod = await getQuery(db,
            `SELECT date_iso, "${lubricantType}" as qty FROM consolide WHERE substr(date_iso, 1, 4) = ? AND "${lubricantType}" IS NOT NULL`, 
            [startYear.toString()]
        ) as any[];
        const entriesBeforePeriod = await getQuery(db,
            'SELECT date, quantity FROM stock_entries WHERE lubricant_type = ? AND substr(date, 1, 4) = ?', 
            [lubricantType, startYear.toString()]
        ) as any[];

        consumptionsBeforePeriod.forEach(row => {
            const date = dayjs(row.date_iso, 'YYYY-MM-DD');
            if (date.isValid() && date.isBefore(startDate, 'month')) {
                overallInitialStock -= parseFloat(String(row.qty || '0').replace(',', '.')) || 0;
            }
        });
        entriesBeforePeriod.forEach(row => {
            const date = dayjs(row.date, 'YYYY-MM-DD');
            if (date.isValid() && date.isBefore(startDate, 'month')) {
                overallInitialStock += parseFloat(String(row.quantity).replace(',', '.')) || 0;
            }
        });

        // 2. Get all relevant movements for the entire period
        const allConsumptions = await getQuery(db,
            `SELECT date_iso, "${lubricantType}" as qty FROM consolide WHERE "${lubricantType}" IS NOT NULL`
        ) as any[];
        const allEntries = await getQuery(db,
            'SELECT date, quantity FROM stock_entries WHERE lubricant_type = ?', 
            [lubricantType]
        ) as any[];

        // 3. Loop through each month in the period and calculate
        const reportData: MonthlyStockReportData['reportData'] = [];
        let currentStock = overallInitialStock;
        let totalEntries = 0;
        let totalSorties = 0;

        let loopDate = startDate.clone();
        while(loopDate.isBefore(endDate, 'month') || loopDate.isSame(endDate, 'month')) {
            const monthStart = loopDate.startOf('month');
            const monthEnd = loopDate.endOf('month');

            const monthEntries = allEntries
                .filter(row => dayjs(row.date, 'YYYY-MM-DD').isBetween(monthStart, monthEnd, 'day', '[]'))
                .reduce((sum, row) => sum + (parseFloat(String(row.quantity).replace(',', '.')) || 0), 0);

            const monthSorties = allConsumptions
                .filter(row => dayjs(row.date_iso, 'YYYY-MM-DD').isBetween(monthStart, monthEnd, 'day', '[]'))
                .reduce((sum, row) => sum + (parseFloat(String(row.qty || '0').replace(',', '.')) || 0), 0);

            const finalStock = currentStock + monthEntries - monthSorties;

            reportData.push({
                month: loopDate.format('MMMM YYYY'),
                initialStock: currentStock,
                entries: monthEntries,
                sorties: monthSorties,
                finalStock: finalStock
            });

            totalEntries += monthEntries;
            totalSorties += monthSorties;
            currentStock = finalStock; // The end of this month is the start of the next
            loopDate = loopDate.add(1, 'month');
        }

        return {
            reportData,
            totalEntries,
            totalSorties,
            overallInitialStock,
            overallFinalStock: currentStock,
            lubricantType,
            period: `du ${startDate.format('MMMM YYYY')} au ${endDate.format('MMMM YYYY')}`
        };
    });
}

export async function deleteOperationFromDb(id: number) {
  return withDb(async (db) => {
    await runQuery(db, 'DELETE FROM declarations WHERE operation_id = ?', [id]);
    await runQuery(db, 'DELETE FROM suivi_curatif WHERE id = ?', [id]);
    return { success: true };
  });
}

export async function updateOperationInDb(id: number, data: any) {
  return withDb(async (db) => {
    const dateEntreeDayjs = dayjs(data.date_entree, 'DD/MM/YYYY');
    const fullData = {
        ...data,
        date_iso: dateEntreeDayjs.isValid() ? dateEntreeDayjs.format('YYYY-MM-DD') : null
    };

    const columns = [
        'matricule', 'date_entree', 'date_iso', 'panne_declaree', 'sitactuelle', 'pieces', 
        'date_sortie', 'intervenant', 'affectation', 'type_de_panne', 
        'nbr_indisponibilite', 'jour_ouvrable', 'ratio', 'jour_disponibilite', 'ratio2',
        'categorie', 'designation'
    ];
    
    const setClause = columns.map(c => `"${c}" = ?`).join(', ');
    const sql = `UPDATE suivi_curatif SET ${setClause} WHERE id = ?`;
    
    const values = [...columns.map(col => fullData[col] ?? null), id];

    await runQuery(db, sql, values);

    return { success: true };
  });
}

export async function getConsumptionById(id: number) {
  return withDb(async (db) => {
    const row = await getOne(db, `
        SELECT c.*, m.designation as designation_matrice, m.qte_vidange 
        FROM consolide c
        LEFT JOIN matrice m ON c.matricule = m.matricule
        WHERE c.id = ?
    `, [id]) as any;
    
    if (row) {
        row.designation = row.designation_matrice || row.designation;
        delete row.designation_matrice;
    }
    return row;
  });
}

export async function updateConsumption(id: number, data: any) {
  return withDb(async (db) => {
    const dateDayjs = dayjs(data.date, 'DD/MM/YYYY');
    const payload = {
        ...data,
        date_iso: dateDayjs.isValid() ? dateDayjs.format('YYYY-MM-DD') : null
    };

    const columnsToUpdate = [...CONSOLIDE_COLUMNS_ORDER.filter(c => c !== 'n' && c !== 'designation' && data.hasOwnProperty(c)), 'date_iso'];
    if (columnsToUpdate.length === 0) {
        throw new Error("No valid columns to update.");
    }

    const setClause = columnsToUpdate.map(col => `"${col}" = ?`).join(', ');
    const values = columnsToUpdate.map(col => payload[col] ?? null);

    const sql = `UPDATE consolide SET ${setClause} WHERE id = ?`;
    await runQuery(db, sql, [...values, id]);
    
    // History cache is not updated automatically, user should regenerate
    
    return { success: true };
  });
}

export async function deleteConsumption(id: number) {
  return withDb(async (db) => {
    await runQuery(db, 'DELETE FROM consolide WHERE id = ?', [id]);
    return { success: true };
  });
}

export async function saveBonDeSortie(data: any) {
    return withDb(async (db) => {
    const { date, destinataire_chantier, items, ...rest } = data;
    const result = await runQuery(db,
        `INSERT INTO bons_de_sortie (date, destinataire_chantier, items_json, generated_at, destinataire_code, transporteur_nom, transporteur_immatriculation)
        VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [
        dayjs(date).format('YYYY-MM-DD'),
        destinataire_chantier,
        JSON.stringify(items),
        dayjs().format(),
        rest.destinataire_code || null,
        rest.transporteur_nom || null,
        rest.transporteur_immatriculation || null
        ]
    );
    return { id: Number((result as any).lastInsertRowid) };
    });
}

export async function getBonsDeSortieList() {
    return withDb(async (db) => {
    try {
        return await getQuery(db, 'SELECT id, date, destinataire_chantier, generated_at FROM bons_de_sortie ORDER BY generated_at DESC') as any[];
    } catch (e: any) {
        if (e.message?.includes('no such table')) return [];
        throw e;
    }
    });
}

export async function getBonDeSortieById(id: number): Promise<BonDeSortie | null> {
    return withDb(async (db) => {
    const row = await getOne(db, 'SELECT * FROM bons_de_sortie WHERE id = ?', [id]) as any;
    if (!row) return null;
    return {
        ...row,
        date: dayjs(row.date).format('DD/MM/YYYY'),
        items: JSON.parse(row.items_json || '[]'),
    };
    });
}

export async function updateBonDeSortie(id: number, data: any) {
    return withDb(async (db) => {
    const { date, destinataire_chantier, items, ...rest } = data;
    await runQuery(db,
        `UPDATE bons_de_sortie SET
        date = ?, destinataire_chantier = ?, items_json = ?,
        destinataire_code = ?, transporteur_nom = ?, transporteur_immatriculation = ?
        WHERE id = ?`,
        [
        dayjs(date).format('YYYY-MM-DD'),
        destinataire_chantier,
        JSON.stringify(items),
        rest.destinataire_code || null,
        rest.transporteur_nom || null,
        rest.transporteur_immatriculation || null,
        id
        ]
    );
    return { success: true };
    });
}

export async function deleteBonDeSortie(id: number) {
    return withDb(async (db) => {
    await runQuery(db, 'DELETE FROM bons_de_sortie WHERE id = ?', [id]);
    return { success: true };
    });
}

const parseCsvFromString = async (csvData: string): Promise<any[]> => {
  return new Promise((resolve, reject) => {
    const rows: any[] = [];
    const stream = Readable.from(csvData);
    stream
      .pipe(csv({ 
          separator: ';',
          mapHeaders: ({ header }) => header.trim().replace(/\s+/g, '_').replace(/[."(),/]/g, '').toLowerCase() || null
      }))
      .on('data', (data) => rows.push(data))
      .on('end', () => resolve(rows))
      .on('error', (error) => reject(error));
  });
};

export async function reinitializeTableFromCsv(tableName: string, csvData: string): Promise<{ message: string }> {
    const ALLOWED_TABLES = ['matrice', 'consolide', 'suivi_curatif', 'vidange', 'Param'];
    if (!ALLOWED_TABLES.includes(tableName)) {
        throw new Error(`Table '${tableName}' n'est pas autorisée pour la mise à jour.`);
    }

    return withDb(async (db) => {
        let rows;
        try {
            rows = await parseCsvFromString(csvData);
        } catch (e: any) {
            throw new Error(`Impossible de lire le fichier CSV pour ${tableName}. Assurez-vous que c'est un CSV valide avec un séparateur ';'. Erreur: ${e.message}`);
        }

        if (!rows || rows.length === 0) {
            throw new Error(`Aucune donnée trouvée dans le fichier CSV pour ${tableName}.`);
        }

        const headers = Object.keys(rows[0]).filter(h => h && h !== 'null');
        if (headers.length === 0) {
            throw new Error(`Impossible de déterminer les en-têtes pour ${tableName} depuis le CSV.`);
        }
        
        // Drop existing table
        await runQuery(db, `DROP TABLE IF EXISTS "${tableName}"`);
        
        // Recreate table
        const needsDateIso = ['suivi_curatif', 'consolide', 'vidange'].includes(tableName);
        const createColumns = headers.map(h => `"${h}" TEXT`).join(', ');
        const finalCreateColumns = needsDateIso ? `${createColumns}, "date_iso" TEXT` : createColumns;
        await runQuery(db, `CREATE TABLE "${tableName}" (id INTEGER PRIMARY KEY AUTOINCREMENT, ${finalCreateColumns})`);

        // Insert new data
        const insertHeaders = needsDateIso ? [...headers, 'date_iso'] : headers;
        const statements = rows.map(row => {
            const values = headers.map(h => row[h] ?? null);
            if (needsDateIso) {
                const dateColName = tableName === 'suivi_curatif' ? 'date_entree' : 'date';
                const dateVal = row[dateColName];
                const isoDate = dateVal ? dayjs(dateVal, 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
                values.push(isoDate);
            }
            return {
                sql: `INSERT INTO "${tableName}" (${insertHeaders.map(h => `"${h}"`).join(', ')}) VALUES (${insertHeaders.map(() => '?').join(', ')})`,
                args: values
            };
        });

        await db.batch(statements);

        // Special case for matrice change: need to clear caches
        if (tableName === 'matrice' || tableName === 'Param') {
            await runQuery(db, 'DELETE FROM planning_cache');
            await runQuery(db, 'DELETE FROM history_cache');
        }

        return { message: `Table '${tableName}' mise à jour avec succès avec ${rows.length} lignes.` };
    });
}

export async function getLastStockEntryDate(lubricantType: string): Promise<string | null> {
    return withDb(async (db) => {
        try {
            const row = await getOne(db,
                'SELECT date FROM stock_entries WHERE lubricant_type = ? ORDER BY date DESC LIMIT 1',
                [lubricantType]
            ) as any;
            return row ? dayjs(row.date, 'YYYY-MM-DD').format('DD/MM/YYYY') : null;
        } catch (e: any) {
            console.error(`Failed to get last entry date for ${lubricantType}`, e);
            return null;
        }
    });
}

// Fonction de test
export async function testEcritureDisque() {
    // Cette fonction ne fonctionnera pas sur Cloudflare car le filesystem n'est pas accessible
    return { 
        success: false, 
        error: "Cette fonction n'est pas disponible sur Cloudflare. Utilisez la base de données Turso/D1 pour stocker les données." 
    };
}
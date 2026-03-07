import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

export async function GET() {
    const dbPath = '/data/gmao_data.db';

    // 1. Vérifier si le fichier existe
    if (!fs.existsSync(dbPath)) {
        return NextResponse.json({ error: 'Base de données introuvable sur le serveur.' }, { status: 404 });
    }

    try {
        // 2. Lire le fichier
        const fileBuffer = fs.readFileSync(dbPath);
        
        // 3. Générer un nom de fichier avec la date du jour
        const dateStr = new Date().toISOString().split('T')[0];
        const fileName = `gmao_data_backup_${dateStr}.db`;

        // 4. Renvoyer le fichier au navigateur
        return new NextResponse(fileBuffer, {
            status: 200,
            headers: {
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': `attachment; filename="${fileName}"`,
            },
        });
    } catch (error) {
        console.error('Erreur lors de l\'export de la BD:', error);
        return NextResponse.json({ error: 'Erreur lors de la lecture de la base de données.' }, { status: 500 });
    }
}
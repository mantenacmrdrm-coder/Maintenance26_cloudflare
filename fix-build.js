const fs = require('fs');
const path = require('path');

const appDir = path.join(process.cwd(), 'app');

function addDynamicToFiles(dir) {
    const files = fs.readdirSync(dir);

    files.forEach(file => {
        const fullPath = path.join(dir, file);
        const stat = fs.statSync(fullPath);

        if (stat.isDirectory()) {
            addDynamicToFiles(fullPath); // Appel récursif pour les sous-dossiers
        } else if (file.endsWith('page.tsx') || file.endsWith('page.ts') || file.endsWith('page.js')) {
            let content = fs.readFileSync(fullPath, 'utf8');

            // On vérifie si la ligne est déjà là
            if (!content.includes("export const dynamic = 'force-dynamic'")) {
                
                // On essaie de trouver la fin des imports pour insérer juste après
                const importRegex = /(import\s+.*from\s+['"].*['"];?\s*)+/;
                const match = content.match(importRegex);

                if (match && match.index !== undefined) {
                    // Insérer après les imports
                    const insertIndex = match.index + match[0].length;
                    const newContent = content.slice(0, insertIndex) + 
                                        "\nexport const dynamic = 'force-dynamic';\n" + 
                                        content.slice(insertIndex);
                    fs.writeFileSync(fullPath, newContent);
                    console.log(`✅ Fix appliqué à : ${fullPath}`);
                } else {
                    // Si pas d'imports clairs, on met au début
                    const newContent = "export const dynamic = 'force-dynamic';\n\n" + content;
                    fs.writeFileSync(fullPath, newContent);
                    console.log(`✅ Fix appliqué (début) à : ${fullPath}`);
                }
            } else {
                console.log(`⏭️  Déjà corrigé : ${fullPath}`);
            }
        }
    });
}

if (fs.existsSync(appDir)) {
    console.log('🔧 Début de la correction automatique...');
    addDynamicToFiles(appDir);
    console.log('✨ Terminé ! Vous pouvez maintenant lancer npm run build.');
} else {
    console.log("❌ Dossier 'app' introuvable.");
}
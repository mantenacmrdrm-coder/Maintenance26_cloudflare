import { getParams, getCategoryEntretiens, getDistinctCategories } from '@/lib/actions/maintenance-actions';
import { ParametersTable } from './parameters-table';
import { CategoryParametersTable } from './category-parameters-table';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Cog } from 'lucide-react';
import { Separator } from '@/components/ui/separator';


export const dynamic = 'force-dynamic';
export default async function ParametersPage() {
  let params = [];
  let paramsError = null;

  let categoryEntretiens = {};
  let categories: string[] = [];
  let categoryError = null;

  try {
    params = await getParams();
  } catch (e: any) {
    paramsError = e.message;
  }

  try {
    categoryEntretiens = await getCategoryEntretiens();
    categories = await getDistinctCategories();
  } catch(e: any) {
    categoryError = e.message;
  }

  // We need to get the headers to pass them to the client component, because they can have weird characters.
  const headers = params.length > 0 ? Object.keys(params[0]) : [];

  const hasData = !paramsError && params.length > 0;
  const hasCategoryData = !categoryError && categories.length > 0;

  return (
    <div className="flex flex-col gap-8">
      <header>
        <h1 className="text-3xl font-bold tracking-tight">Paramètres de Maintenance</h1>
        <p className="text-muted-foreground">
          Modifiez les intervalles, les niveaux et les entretiens par catégorie.
          <br/>
          <span className='font-bold text-destructive'>Attention:</span> Toute modification ici effacera le planning généré, qui devra être recalculé.
        </p>
      </header>
       <main className="space-y-8">
        {paramsError ? (
           <Alert variant="destructive">
            <Cog className="h-4 w-4" />
            <AlertTitle>Erreur de chargement des paramètres</AlertTitle>
            <AlertDescription>
              Impossible de charger les paramètres d'intervalles. Message: {paramsError}
            </AlertDescription>
          </Alert>
        ) : hasData ? (
          <ParametersTable data={params} headers={headers} />
        ) : (
          <Alert variant="default" className="bg-amber-50 border-amber-200">
            <Cog className="h-4 w-4 !text-amber-600" />
            <AlertTitle className="text-amber-800 font-semibold">Aucun paramètre d'intervalle trouvé</AlertTitle>
            <AlertDescription className="text-amber-700">
              La table `Param` est vide ou n'a pas pu être lue.
              Assurez-vous que la base de données est correctement initialisée.
            </AlertDescription>
          </Alert>
        )}

        <Separator />
        
        {categoryError ? (
           <Alert variant="destructive">
            <Cog className="h-4 w-4" />
            <AlertTitle>Erreur de chargement des catégories</AlertTitle>
            <AlertDescription>
              Impossible de charger les paramètres de catégories. Message: {categoryError}
            </AlertDescription>
          </Alert>
        ) : hasCategoryData ? (
          <CategoryParametersTable categories={categories} initialData={categoryEntretiens} />
        ) : (
          <Alert variant="default" className="bg-amber-50 border-amber-200">
            <Cog className="h-4 w-4 !text-amber-600" />
            <AlertTitle className="text-amber-800 font-semibold">Aucun paramètre de catégorie trouvé</AlertTitle>
            <AlertDescription className="text-amber-700">
              Aucune catégorie d'équipement n'a été trouvée ou la table des associations est vide.
              Assurez-vous que les tables `matrice` et `category_entretiens` sont correctement initialisées.
            </AlertDescription>
          </Alert>
        )}

      </main>
    </div>
  );
}

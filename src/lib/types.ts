export type Equipment = {
  id: number;
  matricule: string;
  designation: string;
  marque: string | null;
  categorie: string | null;
  annee: string | null;
  qte_vidange: number | null;
  code_barre: string | null;
  pneumatique: string | null;
  [key: string]: any;
};

export type Operation = {
  id: number;
  matricule: string;
  operation: string;
  date_programmee: string;
  date_realisation: string | null;
  intervalle_jours?: number | null;
  nature: string;
  niveau: string;
  date_entree?: string;
  panne_declaree?: string;
  sitactuelle?: string;
  pieces?: string;
  date_sortie?: string;
  intervenant?: string;
  affectation?: string;
  type_de_panne?: string;
  designation?: string;
  marque?: string | null;
};

export type Alert = {
  equipmentId: string;
  equipmentDesignation?: string;
  operation: string;
  dueDate: string;
  urgency: string;
  niveau: string;
  status?: string;
};

export type PreventativeMaintenanceEntry = {
  id: string;
  operation: string;
  date: string;
  details: string[];
};

export type CurativeMaintenanceEntry = {
  id: number;
  panneDeclaree: string;
  typePanne: 'mécanique' | 'électrique' | 'hydraulique' | 'autres' | 'non spécifié';
  dateEntree: string;
  dateSortie: string;
  dureeIntervention: number | null;
  piecesRemplacees: string[];
  details: Record<string, any>;
  tags: readonly string[];
};

export type FollowUpStats = {
  totalPlanifie: number;
  totalRealise: number;
  planifieByNiveau: { [key: string]: number };
  realiseByNiveau: { [key: string]: number };
  realiseByEntretien: { [key: string]: number };
  planifieByEntretien: { [key: string]: number };
};

export type WeeklyReportItem = {
  obs: string;
  numero: number;
  designation: string;
  matricule: string;
  date_panne: string;
  nature_panne: string;
  reparations: string[];
  date_sortie: string;
  intervenant: string;
};

export type WeeklyReport = {
  id: number;
  start_date: string;
  end_date: string;
  generated_at: string;
  pannes: WeeklyReportItem[];
};

export type MonthlyCount = {
  month: string;
  count: number;
};

export type MonthlyPreventativeStats = {
  monthlyData: {
    month: string;
    vidange: number;
    graissage: number;
    transmission: number;
    hydraulique: number;
    autres: number;
  }[];
  totalOil: number;
  oilByType: Record<string, number>;
};

export type DashboardData = {
  equipmentCount: number;
  operationCount: number;
  followUpStats: FollowUpStats | null;
  monthlyCounts: MonthlyCount[];
  recentOperations: Operation[];
  error: string | null;
  breakdownsThisMonth: number | null;
  year: number;
  month?: number;
  preventativeStats: MonthlyPreventativeStats;
};

export type DeclarationPannePiece = {
  designation: string;
  reference: string;
  quantite: number;
  montant: number;
};

export type DeclarationPanneIntervenant = {
  type: string;
  description: string;
};

export type DeclarationPanne = {
  id: number;
  operation_id: number;
  generated_at: string;
  operation: Operation;
  equipment: Equipment;
  chauffeur_conducteur: string;
  diagnostique_intervenant: string;
  causes: string;
  pieces: DeclarationPannePiece[];
  intervenants: DeclarationPanneIntervenant[];
  montant_main_oeuvre: number;
  obs_reserves: string;
};

export type CurativeFicheData = {
  id: number;
  date_entree: string;
  affectation: string | null;
  designation: string | null;
  matricule: string;
  marque: string | null;
  categorie: string | null;
  type_de_panne: 'mécanique' | 'électrique' | 'autres' | 'non spécifié';
  panne_declaree: string;
  pieces: string[] | null;
  intervenant: string | null;
  sitactuelle: 'En Cours' | 'Réparée' | 'Dépanné';
  nbr_indisponibilite: number;
};

export type OrdreTravailData = {
  id: number;
  date_entree: string;
  affectation: string | null;
  designation: string | null;
  matricule: string;
  marque: string | null;
  type_de_panne: 'mécanique' | 'électrique' | 'autres' | 'non spécifié';
  panne_declaree: string;
};

export type PreventiveFicheTravail = {
  organe: string;
  travail: string;
  date: string;
  lubrifiant: string;
  quantite: number;
};

export type PreventiveFicheFiltration = {
  air: { active: boolean; date: string | null };
  huile: { active: boolean; date: string | null };
  gasoil: { active: boolean; date: string | null };
  bypass: { active: boolean; date: string | null };
  hydraulique: { active: boolean; date: string | null };
};

export type PreventiveFicheData = {
  id: number;
  equipment: {
    machine: string | null;
    designation: string | null;
    marque: string | null;
    code: string | null;
    matricule: string;
  };
  entretien: {
    intervenant: string | null;
    date: string;
  };
  travaux: PreventiveFicheTravail[];
  filtrations: PreventiveFicheFiltration;
  observation: string | null;
};

export type StockEntry = {
  id: number;
  date: string;
  lubricant_type: string;
  quantity: number;
  reference: string | null;
};

export type DailyReportData = {
    initialStock: number;
    dailyData: {
        date: string;
        entree: number;
        sortie: number;
    }[];
    totalEntrees: number;
    totalSorties: number;
    finalStock: number;
    lubricantType: string;
    month: number;
    year: number;
};

export type MonthlyStockReportData = {
    reportData: {
        month: string;
        initialStock: number;
        entries: number;
        sorties: number;
        finalStock: number;
    }[];
    totalEntries: number;
    totalSorties: number;
    overallInitialStock: number;
    overallFinalStock: number;
    lubricantType: string;
    period: string;
};

export type BonDeSortieItem = {
  code: string;
  designation: string;
  unite: string;
  quantite: number;
  montant?: number;
  pu?: number;
};

export type BonDeSortie = {
  id: number;
  generated_at: string;
  date: string;
  destinataire_chantier: string;
  destinataire_code: string | null;
  transporteur_nom: string | null;
  transporteur_immatriculation: string | null;
  items: BonDeSortieItem[];
};

export type BonListItem = {
    id: number;
    generated_at: string;
    date: string;
    destinataire_chantier: string;
};

export type DeclarationListItem = {
    id: number;
    generated_at: string;
    matricule: string;
    panne_declaree: string;
    designation: string;
};

export type ActionResponse<T = any> = 
  | { success: true; data: T; message?: string }
  | { success: false; message: string; data?: never };

export type ConsumptionQueryResult = {
    consumptions: any[];
    summary: {
        consumed: Record<string, number>;
        entries: Record<string, number>;
    };
    initialStockSummary: Record<string, number>;
};
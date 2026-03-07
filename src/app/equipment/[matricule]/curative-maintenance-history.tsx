'use client';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import type { CurativeMaintenanceEntry } from "@/lib/types";
import { Wrench, Zap, SlidersHorizontal, AlertTriangle, CalendarOff } from "lucide-react";
import React from "react";
import { Separator } from "@/components/ui/separator";

type Props = {
  history: CurativeMaintenanceEntry[];
};

const typePanneConfig = {
    'mécanique': { icon: Wrench, label: 'Mécanique', color: 'bg-blue-100 text-blue-800 border-blue-200' },
    'électrique': { icon: Zap, label: 'Électrique', color: 'bg-yellow-100 text-yellow-800 border-yellow-200' },
    'autres': { icon: SlidersHorizontal, label: 'Autres', color: 'bg-gray-100 text-gray-800 border-gray-200' },
    'non spécifié': { icon: AlertTriangle, label: 'Non Spécifié', color: 'bg-red-100 text-red-800 border-red-200' },
};

export function CurativeMaintenanceHistory({ history }: Props) {
  const hasHistory = history && history.length > 0;

  if (!hasHistory) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Maintenance Curative</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col items-center justify-center h-48 text-center bg-muted/50 rounded-lg">
            <CalendarOff className="h-12 w-12 text-muted-foreground mb-4" />
            <p className="text-lg font-medium">Aucun historique curatif</p>
            <p className="text-muted-foreground">Aucune panne ou réparation non planifiée n'a été enregistrée pour cet équipement.</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
        <Card>
            <CardHeader>
                <CardTitle>Maintenance Curative</CardTitle>
                <CardDescription>Historique des pannes et réparations non planifiées.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
                {history.map(entry => (
                    <Card key={entry.id} className="overflow-hidden">
                        <CardHeader className="p-4 bg-muted/50">
                             <div className="flex flex-col sm:flex-row gap-2 sm:gap-4 justify-between">
                                <div>
                                    <CardTitle className="text-base font-bold">{entry.panneDeclaree}</CardTitle>
                                    <div className="flex items-center gap-2 mt-1">
                                        <Badge className={`capitalize ${typePanneConfig[entry.typePanne].color}`}>
                                            {React.createElement(typePanneConfig[entry.typePanne].icon, { className: "h-3.5 w-3.5 mr-1" })}
                                            {typePanneConfig[entry.typePanne].label}
                                        </Badge>
                                        <p className="text-xs text-muted-foreground font-mono">{entry.dateEntree} &rarr; {entry.dateSortie}</p>
                                    </div>
                                </div>
                                <div className="text-left sm:text-right">
                                    <p className="text-sm font-semibold">Indisponibilité</p>
                                    <p className="text-lg font-bold text-destructive">{entry.dureeIntervention} jours</p>
                                </div>
                            </div>
                        </CardHeader>
                        <CardContent className="p-4 space-y-4">
                           {entry.piecesRemplacees.length > 0 && (
                             <div>
                                <h4 className="font-semibold text-sm mb-2">Pièces Remplacées</h4>
                                <ul className="list-disc list-inside bg-gray-50 p-3 rounded-md text-sm text-gray-700 space-y-1">
                                    {entry.piecesRemplacees.map((piece, i) => (
                                        <li key={i} className="capitalize">{piece}</li>
                                    ))}
                                </ul>
                            </div>
                           )}

                           <Separator />

                            <div>
                                <h4 className="font-semibold text-sm mb-2">Détails de l'intervention</h4>
                                <div className="grid grid-cols-2 md:grid-cols-3 gap-x-4 gap-y-2 text-sm">
                                    {Object.entries(entry.details).map(([key, value]) => value ? (
                                        <div key={key}>
                                            <span className="text-muted-foreground">{key}: </span>
                                            <span className="font-medium">{value}</span>
                                        </div>
                                    ) : null)}
                                </div>
                            </div>
                            
                            {entry.tags.length > 0 && (
                                <div>
                                    <h4 className="font-semibold text-sm mb-2">Mots-clés associés</h4>
                                    <div className="flex flex-wrap gap-2">
                                        {entry.tags.map(tag => (
                                            <Badge key={tag} variant="secondary">{tag}</Badge>
                                        ))}
                                    </div>
                                </div>
                            )}

                        </CardContent>
                    </Card>
                ))}
            </CardContent>
        </Card>
    </div>
  );
}

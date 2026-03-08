
'use client';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { PreventativeMaintenanceEntry } from "@/lib/types";
import { Circle, Wrench } from "lucide-react";

type Props = {
  history: Record<string, PreventativeMaintenanceEntry[]>;
};

export default PreventativeMaintenanceHistory({ history }: Props) {

  const hasHistory = history && Object.keys(history).length > 0;

  if (!hasHistory) {
    return (
        <Card>
             <CardHeader>
                <CardTitle>Maintenance Préventive</CardTitle>
            </CardHeader>
            <CardContent>
                <div className="flex flex-col items-center justify-center h-48 text-center bg-muted/50 rounded-lg">
                    <Wrench className="h-12 w-12 text-muted-foreground mb-4" />
                    <p className="text-lg font-medium">Aucun historique préventif</p>
                    <p className="text-muted-foreground">Aucune donnée de maintenance préventive trouvée pour cet équipement.</p>
                </div>
            </CardContent>
        </Card>
    );
  }

  const sortedOperations = Object.keys(history).sort((a,b) => a.localeCompare(b));

  return (
    <Card>
      <CardHeader>
        <CardTitle>Maintenance Préventive</CardTitle>
        <CardDescription>
          Historique des entretiens planifiés (vidanges, filtres, etc.).
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Accordion type="single" collapsible className="w-full">
            {sortedOperations.map(operationName => (
                <AccordionItem value={operationName} key={operationName}>
                    <AccordionTrigger className="text-base font-semibold">
                        {operationName}
                    </AccordionTrigger>
                    <AccordionContent>
                        <ul className="space-y-4 pl-4 border-l">
                            {history[operationName].map(entry => (
                                <li key={entry.id} className="relative">
                                    <span className="absolute -left-[21px] top-1.5 flex h-4 w-4 items-center justify-center rounded-full bg-primary">
                                        <Circle className="h-2 w-2 fill-current text-primary-foreground" />
                                    </span>
                                    <p className="font-bold text-primary">{entry.date}</p>
                                    {entry.details && entry.details.length > 0 && (
                                        <div className="text-sm text-muted-foreground mt-1 space-y-1">
                                            {entry.details.map((detail, i) => (
                                                <p key={i}>{detail}</p>
                                            ))}
                                        </div>
                                    )}
                                </li>
                            ))}
                        </ul>
                    </AccordionContent>
                </AccordionItem>
            ))}
        </Accordion>
      </CardContent>
    </Card>
  );
}

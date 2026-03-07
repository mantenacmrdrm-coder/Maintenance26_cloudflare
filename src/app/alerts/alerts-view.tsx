'use client';

import { useState, useTransition } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { useToast } from '@/hooks/use-toast';
import { getPreventativeAlerts } from '@/lib/actions/maintenance-actions';
import type { Alert } from '@/lib/types';
import { Loader2, AlertTriangle, CheckCircle, Info, Download, CalendarIcon, Search, ChevronDown } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuCheckboxItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import { OFFICIAL_ENTRETIENS } from '@/lib/constants';
import { cn } from '@/lib/utils';
import dayjs from 'dayjs';
import { Input } from '@/components/ui/input';

function AlertCard({ alert }: { alert: Alert }) {
  const urgencyConfig = {
    urgent: {
      variant: 'destructive',
      icon: <AlertTriangle className="h-4 w-4" />,
      label: 'Urgent',
    },
    near: {
      variant: 'default',
      icon: <Info className="h-4 w-4" />,
      label: 'Proche',
      className: 'bg-amber-500 text-white',
    },
    planned: {
      variant: 'secondary',
      icon: <CheckCircle className="h-4 w-4" />,
      label: 'Planifié',
    },
  }[alert.urgency] || {
    variant: 'outline',
    icon: <Info className="h-4 w-4" />,
    label: alert.urgency,
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-base font-medium">{alert.operation}</CardTitle>
        <div className="flex items-center gap-2">
            {alert.status && <Badge className="bg-black text-white hover:bg-black/80">{alert.status}</Badge>}
            <Badge variant={urgencyConfig.variant as any} className={urgencyConfig.className}>
              {urgencyConfig.icon}
              <span className='ml-2'>{urgencyConfig.label}</span>
            </Badge>
        </div>
      </CardHeader>
      <CardContent className="text-sm space-y-2">
        <div>
          <p className='font-semibold'>{alert.equipmentDesignation}</p>
          <p className='text-sm text-primary font-mono'>{alert.equipmentId}</p>
          <p className="text-muted-foreground mt-1">Échéance: {alert.dueDate}</p>
        </div>
        <Badge variant="secondary">Niveau: {alert.niveau}</Badge>
      </CardContent>
    </Card>
  );
}

export function AlertsView() {
  const [dateRange, setDateRange] = useState<{ from: Date | undefined, to: Date | undefined }>({
    from: new Date(),
    to: dayjs().add(30, 'day').toDate(),
  });
  const [selectedEntretiens, setSelectedEntretiens] = useState<string[]>([]);
  const [niveau, setNiveau] = useState('all');
  const [matricule, setMatricule] = useState('');
  const [alerts, setAlerts] = useState<Alert[] | null>(null);
  const [isPending, startTransition] = useTransition();
  const { toast } = useToast();

  const handleGenerateAlerts = () => {
    if (!dateRange.from || !dateRange.to) {
        toast({
            variant: 'destructive',
            title: 'Dates invalides',
            description: 'Veuillez sélectionner une plage de dates complète.',
        });
        return;
    }
    startTransition(async () => {
      try {
        const result = await getPreventativeAlerts({
            startDate: dateRange.from as Date,
            endDate: dateRange.to as Date,
            entretiens: selectedEntretiens,
            niveau: niveau,
            matricule: matricule,
        });
        setAlerts(result);
        if (result.length > 0) {
          toast({
            title: 'Succès',
            description: `${result.length} alerte(s) générée(s).`,
          });
        }
      } catch (error: any) {
        toast({
          variant: 'destructive',
          title: 'Erreur',
          description: error.message || 'Impossible de générer les alertes.',
        });
        setAlerts(null);
      }
    });
  };

  const handleExport = () => {
    if (!alerts || alerts.length === 0) {
      toast({
        variant: 'destructive',
        title: 'Aucune donnée',
        description: 'Il n\'y a aucune alerte à exporter.',
      });
      return;
    }

    const headers = ['Matricule', 'Désignation', 'Opération', 'Échéance', 'Urgence', 'Niveau', 'Statut'];
    const csvContent = [
      headers.join(';'),
      ...alerts.map(alert => [
        `"${alert.equipmentId}"`,
        `"${alert.equipmentDesignation || ''}"`,
        `"${alert.operation}"`,
        `"${alert.dueDate}"`,
        `"${alert.urgency}"`,
        `"${alert.niveau}"`,
        `"${alert.status || ''}"`,
      ].join(';'))
    ].join('\n');

    const blob = new Blob([`\uFEFF${csvContent}`], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `alertes_maintenance_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    toast({
      title: 'Exportation réussie',
      description: `${alerts.length} alertes ont été exportées.`,
    });
  };
  
  const getEntretiensButtonLabel = () => {
    if (selectedEntretiens.length === 0) return "Tous les entretiens";
    if (selectedEntretiens.length > 2) return `${selectedEntretiens.length} sélectionnés`;
    return selectedEntretiens.join(', ');
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Configuration des Alertes</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap items-end gap-4">
            <div className="grid gap-2">
                <label className="text-sm font-medium">Date de début</label>
                <Popover>
                    <PopoverTrigger asChild>
                    <Button variant={"outline"} className={cn("w-[240px] justify-start text-left font-normal", !dateRange.from && "text-muted-foreground")}>
                        <CalendarIcon className="mr-2 h-4 w-4" />
                        {dateRange.from ? dayjs(dateRange.from).format("DD/MM/YYYY") : <span>Choisir une date</span>}
                    </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0" align="start">
                    <Calendar mode="single" selected={dateRange.from} onSelect={(d) => setDateRange(prev => ({...prev, from: d}))} initialFocus />
                    </PopoverContent>
                </Popover>
            </div>
             <div className="grid gap-2">
                <label className="text-sm font-medium">Date de fin</label>
                <Popover>
                    <PopoverTrigger asChild>
                    <Button variant={"outline"} className={cn("w-[240px] justify-start text-left font-normal", !dateRange.to && "text-muted-foreground")}>
                        <CalendarIcon className="mr-2 h-4 w-4" />
                        {dateRange.to ? dayjs(dateRange.to).format("DD/MM/YYYY") : <span>Choisir une date</span>}
                    </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0" align="start">
                    <Calendar mode="single" selected={dateRange.to} onSelect={(d) => setDateRange(prev => ({...prev, to: d}))} initialFocus />
                    </PopoverContent>
                </Popover>
            </div>
            <div className="grid gap-2">
              <label className="text-sm font-medium">Type d'entretien</label>
              <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                      <Button variant="outline" className="w-[280px] justify-between">
                          <span className="truncate">{getEntretiensButtonLabel()}</span>
                          <ChevronDown className="h-4 w-4 opacity-50" />
                      </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent className="w-[280px] max-h-80 overflow-y-auto">
                      <DropdownMenuCheckboxItem
                          checked={selectedEntretiens.length === 0}
                          onCheckedChange={(checked) => {
                              if (checked) setSelectedEntretiens([]);
                          }}
                      >
                          Tous les entretiens
                      </DropdownMenuCheckboxItem>
                      <DropdownMenuSeparator />
                      {OFFICIAL_ENTRETIENS.map((item) => (
                          <DropdownMenuCheckboxItem
                              key={item}
                              checked={selectedEntretiens.includes(item)}
                              onCheckedChange={(checked) => {
                                  setSelectedEntretiens((prev) =>
                                      checked
                                          ? [...prev, item]
                                          : prev.filter((i) => i !== item)
                                  );
                              }}
                          >
                              {item}
                          </DropdownMenuCheckboxItem>
                      ))}
                  </DropdownMenuContent>
              </DropdownMenu>
            </div>
             <div className="grid gap-2">
                <label className="text-sm font-medium">Niveau</label>
                <Select value={niveau} onValueChange={setNiveau}>
                    <SelectTrigger className="w-[180px]">
                        <SelectValue placeholder="Filtrer par niveau..." />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="all">Tous les niveaux</SelectItem>
                        <SelectItem value="C">C - Contrôle</SelectItem>
                        <SelectItem value="N">N - Nettoyage</SelectItem>
                        <SelectItem value="CH">CH - Changement</SelectItem>
                    </SelectContent>
                </Select>
            </div>
            <div className="grid gap-2">
                <label className="text-sm font-medium">Matricule</label>
                <Input
                    placeholder="Filtrer par matricule..."
                    value={matricule}
                    onChange={(e) => setMatricule(e.target.value)}
                    className="w-[200px]"
                />
            </div>
            <Button onClick={handleGenerateAlerts} disabled={isPending}>
                {isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Search className="mr-2 h-4 w-4" />}
                Générer les Alertes
            </Button>
        </CardContent>
      </Card>

      {isPending && (
         <div className="flex min-h-[30vh] w-full items-center justify-center rounded-lg bg-muted/50">
            <div className='text-center space-y-2'>
                <Loader2 className="h-8 w-8 animate-spin text-primary mx-auto" />
                <p className='font-medium'>Analyse des données...</p>
                <p className='text-sm text-muted-foreground'>Cela peut prendre quelques instants.</p>
            </div>
        </div>
      )}

      {!isPending && alerts !== null && (
        <Card>
          <CardHeader>
            <div className="flex justify-between items-center">
              <CardTitle>Résultats</CardTitle>
              {alerts.length > 0 && (
                <Button onClick={handleExport} variant="outline" size="sm">
                  <Download className="mr-2 h-4 w-4" />
                  Exporter la liste
                </Button>
              )}
            </div>
          </CardHeader>
          <CardContent>
            {alerts.length > 0 ? (
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {alerts.map((alert, index) => (
                  <AlertCard key={index} alert={alert} />
                ))}
              </div>
            ) : (
                <div className="flex flex-col items-center justify-center h-48 text-center bg-muted/50 rounded-lg">
                    <CheckCircle className="h-12 w-12 text-green-500 mb-4" />
                    <p className="text-lg font-medium">Tout est en ordre !</p>
                    <p className="text-muted-foreground">Aucune alerte de maintenance préventive générée pour les critères sélectionnés.</p>
                </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}

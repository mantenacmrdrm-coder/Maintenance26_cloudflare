'use client';
import { useMemo } from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
import { CardDescription } from '../ui/card';
import type { FollowUpStats } from '@/lib/types';

type Props = {
  data: FollowUpStats | null;
};

export function CompletionChart({ data }: Props) {
  const chartData = useMemo(() => {
    if (!data || data.totalPlanifie === 0) {
      return {
        pieData: [],
        percentage: 0,
        totalPlanifie: 0,
        totalRealise: 0,
      };
    }

    const { totalPlanifie, totalRealise } = data;
    const nonRealise = totalPlanifie - totalRealise;
    const percentage = totalPlanifie > 0 ? (totalRealise / totalPlanifie) * 100 : 0;

    return {
      pieData: [
        { name: 'Réalisé', value: totalRealise },
        { name: 'Non Réalisé', value: nonRealise },
      ],
      percentage: Math.round(percentage),
      totalPlanifie,
      totalRealise,
    };
  }, [data]);

  const COLORS = ['hsl(var(--primary))', 'hsl(var(--muted))'];

  if (chartData.pieData.length === 0 || chartData.totalPlanifie === 0) {
    return (
      <div className="flex h-64 w-full items-center justify-center">
        <CardDescription>Aucune donnée de suivi préventif pour cette année.</CardDescription>
      </div>
    );
  }

  return (
    <div className="relative flex h-64 w-full items-center justify-center">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Tooltip
            contentStyle={{
              background: 'hsl(var(--background))',
              borderColor: 'hsl(var(--border))',
              borderRadius: 'var(--radius)',
            }}
          />
          <Pie
            data={chartData.pieData}
            cx="50%"
            cy="50%"
            innerRadius={60}
            outerRadius={80}
            paddingAngle={5}
            dataKey="value"
            stroke="none"
          >
            {chartData.pieData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>
      <div className="absolute flex flex-col items-center justify-center">
        <span className="text-4xl font-bold tracking-tight">{chartData.percentage}%</span>
        <span className="text-sm text-muted-foreground">Terminé</span>
      </div>
    </div>
  );
}

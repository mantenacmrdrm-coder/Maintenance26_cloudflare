
'use client';

import { useMemo } from 'react';
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  ChartLegend,
  ChartLegendContent,
} from '@/components/ui/chart';
import { PieChart, Pie, Cell, ResponsiveContainer } from 'recharts';
import type { Operation } from '@/lib/types';
import { CardDescription } from '@/components/ui/card';


type EquipmentStatusChartProps = {
  operations: Operation[];
};

const stringToColor = (str: string) => {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
    hash = hash & hash; // Convert to 32bit integer
  }
  const hue = Math.abs(hash % 360);
  return `hsl(${hue}, 70%, 50%)`;
};


export function EquipmentStatusChart({ operations }: EquipmentStatusChartProps) {
  const data = useMemo(() => {
    const operationCounts = operations.reduce(
      (acc, op) => {
        const opName = op.operation || 'Non spécifié';
        acc[opName] = (acc[opName] || 0) + 1;
        return acc;
      },
      {} as Record<string, number>
    );

    const chartData = Object.entries(operationCounts).map(([name, value]) => ({
      name,
      value,
      fill: stringToColor(name),
    }));
    
    const sortedData = chartData.sort((a,b) => b.value - a.value);
    const topN = sortedData.slice(0, 10);
    const otherValue = sortedData.slice(10).reduce((acc, curr) => acc + curr.value, 0);

    if (otherValue > 0) {
        topN.push({ name: 'Autres', value: otherValue, fill: 'hsl(var(--muted))' });
    }

    return topN;

  }, [operations]);

  if (operations.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-64 text-center">
        <CardDescription>Aucune donnée d'intervention trouvée pour cet équipement.</CardDescription>
      </div>
    );
  }

  const chartDynamicConfig = data.reduce((acc, item) => {
      acc[item.name] = { label: item.name, color: item.fill };
      return acc;
  }, {} as any);


  return (
    <ChartContainer config={chartDynamicConfig} className="min-h-[300px] w-full">
      <ResponsiveContainer width="100%" height={300}>
        <PieChart accessibilityLayer>
          <ChartTooltip content={<ChartTooltipContent hideLabel />} />
          <Pie
            data={data}
            dataKey="value"
            nameKey="name"
            cx="50%"
            cy="50%"
            outerRadius={100}
            labelLine={false}
            label={({ percent, name }) => (percent > 0.03 ? `${name}: ${(percent * 100).toFixed(0)}%` : null)}
          >
            {data.map(entry => (
              <Cell key={entry.name} fill={entry.fill} />
            ))}
          </Pie>
          <ChartLegend content={<ChartLegendContent nameKey="name" />} />
        </PieChart>
      </ResponsiveContainer>
    </ChartContainer>
  );
}

'use client';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { CardDescription } from '../ui/card';
import type { MonthlyPreventativeStats } from '@/lib/types';

type Props = {
  data: MonthlyPreventativeStats['monthlyData'];
};

const barConfig = {
    vidange: { color: 'hsl(var(--destructive))', label: 'Vidanges' },
    graissage: { color: 'hsl(var(--success))', label: 'Graissages' },
    transmission: { color: 'hsl(var(--warning))', label: 'Transmissions' },
    hydraulique: { color: 'hsl(var(--foreground))', label: 'Hydraulique' },
    autres: { color: 'hsl(var(--muted-foreground))', label: 'Autres' },
};

export function PreventativeOverviewChart({ data }: Props) {
  const hasData = data.some(month => Object.values(month).some(val => typeof val === 'number' && val > 0));

  if (!hasData) {
      return (
        <div className="h-[350px] w-full flex items-center justify-center">
            <CardDescription>Aucune donnée de maintenance préventive pour cette période.</CardDescription>
        </div>
      );
  }

  return (
    <div className="h-[350px] w-full">
        <ResponsiveContainer width="100%" height="100%">
        <BarChart
            data={data}
            margin={{
            top: 5,
            right: 30,
            left: 0,
            bottom: 5,
            }}
        >
            <CartesianGrid strokeDasharray="3 3" vertical={false} />
            <XAxis dataKey="month" tickLine={false} axisLine={false} />
            <YAxis tickLine={false} axisLine={false} allowDecimals={false} />
            <Tooltip 
                cursor={{ fill: 'hsl(var(--muted))' }}
                contentStyle={{ 
                    background: 'hsl(var(--background))',
                    borderColor: 'hsl(var(--border))',
                    borderRadius: 'var(--radius)'
                }}
            />
            <Legend iconType="circle" />
            <Bar dataKey="vidange" name={barConfig.vidange.label} stackId="a" fill={barConfig.vidange.color} radius={[0, 0, 0, 0]} />
            <Bar dataKey="graissage" name={barConfig.graissage.label} stackId="a" fill={barConfig.graissage.color} />
            <Bar dataKey="transmission" name={barConfig.transmission.label} stackId="a" fill={barConfig.transmission.color} />
            <Bar dataKey="hydraulique" name={barConfig.hydraulique.label} stackId="a" fill={barConfig.hydraulique.color} />
            <Bar dataKey="autres" name={barConfig.autres.label} stackId="a" fill={barConfig.autres.color} radius={[4, 4, 0, 0]}/>
        </BarChart>
        </ResponsiveContainer>
    </div>
  );
}

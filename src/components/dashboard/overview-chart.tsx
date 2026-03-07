'use client';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import type { MonthlyCount } from '@/lib/types';
import { CardDescription } from '../ui/card';

type Props = {
  data: MonthlyCount[];
};

export function OverviewChart({ data }: Props) {
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
            <YAxis tickLine={false} axisLine={false} />
            <Tooltip 
                cursor={{ fill: 'hsl(var(--muted))' }}
                contentStyle={{ 
                    background: 'hsl(var(--background))',
                    borderColor: 'hsl(var(--border))',
                    borderRadius: 'var(--radius)'
                }}
            />
            <Legend iconType="circle" />
            <Bar dataKey="count" name="Interventions" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} />
        </BarChart>
        </ResponsiveContainer>
    </div>
  );
}

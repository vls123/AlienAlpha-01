import { Database, Zap, Archive, AlertTriangle } from 'lucide-react';
import { Card, CardHeader } from './ui/card';

export function DataPipeline({ data }) {
    const streams = data?.streams || {};
    const arctic = data?.arctic || {};

    return (
        <Card className="h-full md:col-span-2 lg:col-span-1" delay={0.3}>
            <CardHeader title="Data Pipeline" icon={Database} color="text-emerald-400" />

            {/* ArcticDB (Historical) */}
            <div className="mb-6 p-4 rounded-lg bg-emerald-950/20 border border-emerald-500/10">
                <div className="flex items-center gap-2 mb-3 text-emerald-400">
                    <Archive size={16} />
                    <h3 className="text-sm font-bold uppercase tracking-wider">ArcticDB (Historical)</h3>
                </div>

                <div className="flex justify-between items-baseline mb-3">
                    <span className="text-xs text-slate-400">Symbols Stored</span>
                    <span className="text-2xl font-mono text-emerald-300 font-light">{arctic.symbol_count || 0}</span>
                </div>

                <div className="space-y-1">
                    {arctic.sample_dates && Object.entries(arctic.sample_dates).map(([sym, date]) => (
                        <div key={sym} className="flex justify-between text-[10px] text-slate-500 font-mono">
                            <span>{sym}</span>
                            <span className="text-slate-400">{date.split('+')[0]}</span>
                        </div>
                    ))}
                </div>

                {arctic.error && (
                    <div className="mt-3 flex items-center gap-2 text-xs text-red-400 bg-red-950/20 p-2 rounded border border-red-500/10">
                        <AlertTriangle size={12} />
                        {arctic.error}
                    </div>
                )}
            </div>

            {/* Redis Streams (Live) */}
            <div>
                <div className="flex items-center gap-2 mb-3 text-cyan-400 px-1">
                    <Zap size={16} />
                    <h3 className="text-sm font-bold uppercase tracking-wider">Redis Streams (Live)</h3>
                </div>

                <div className="grid grid-cols-2 gap-2">
                    {Object.entries(streams).map(([sym, count]) => (
                        <div key={sym} className="flex justify-between items-center bg-slate-800/30 p-2 rounded border border-slate-800 hover:border-cyan-500/30 transition group">
                            <span className="text-xs font-bold text-slate-400 group-hover:text-cyan-300">{sym}</span>
                            <span className="text-xs font-mono text-cyan-500">{count}</span>
                        </div>
                    ))}
                </div>
            </div>
        </Card>
    );
}

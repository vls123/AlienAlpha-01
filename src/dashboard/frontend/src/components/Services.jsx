import { Server, Container } from 'lucide-react';
import { Card, CardHeader } from './ui/card';
import { motion } from 'framer-motion';

export function Services({ services, containers }) {
    return (
        <Card className="h-full" delay={0.2}>
            <CardHeader title="Services" icon={Server} color="text-purple-400" />

            <div className="space-y-3">
                <StatusRow label="Redis Database" status={services?.redis} />
                <StatusRow label="Ingestion Engine" status={services?.ingestor} />

                <div className="pt-4 mt-4 border-t border-slate-800/50">
                    <h3 className="text-xs font-bold text-slate-500 uppercase mb-3 flex items-center gap-2">
                        <Container size={12} />
                        Docker Containers
                    </h3>
                    <div className="grid gap-2">
                        {containers?.map(c => (
                            <div key={c.id} className="flex justify-between items-center text-xs py-2 px-3 bg-slate-800/30 rounded border border-slate-800 hover:border-slate-700 transition">
                                <span className="text-slate-300 font-mono">{c.name}</span>
                                <span className={`px-2 py-0.5 rounded-full text-[10px] uppercase font-bold tracking-wider ${c.status === 'running'
                                        ? 'bg-green-500/10 text-green-400 border border-green-500/20 shadow-[0_0_8px_rgba(74,222,128,0.1)]'
                                        : 'bg-red-500/10 text-red-400 border border-red-500/20'
                                    }`}>
                                    {c.status}
                                </span>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </Card>
    );
}

function StatusRow({ label, status }) {
    const isUp = status === 'up' || status === 'running';

    return (
        <div className="flex justify-between items-center p-3 rounded-lg bg-slate-800/20 border border-slate-800/50">
            <span className="text-sm text-slate-300">{label}</span>
            <motion.div
                key={status} // Trigggers animation on change
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                className={`flex items-center gap-2 text-xs font-bold px-2 py-1 rounded border ${isUp
                        ? 'bg-green-500/10 text-green-400 border-green-500/20 shadow-[0_0_10px_rgba(74,222,128,0.2)]'
                        : 'bg-red-500/10 text-red-400 border-red-500/20'
                    }`}
            >
                <span className={`w-1.5 h-1.5 rounded-full ${isUp ? 'bg-green-400 animate-pulse' : 'bg-red-400'}`} />
                {status?.toUpperCase() || 'UNKNOWN'}
            </motion.div>
        </div>
    );
}

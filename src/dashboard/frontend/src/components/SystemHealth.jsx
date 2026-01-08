import { Activity, HardDrive, Cpu, Database } from 'lucide-react';
import { Card, CardHeader } from './ui/card';

export function SystemHealth({ system }) {
    if (!system) return null;

    return (
        <Card className="h-full" delay={0.1}>
            <CardHeader title="System Health" icon={Activity} color="text-cyan-400" />
            <div className="space-y-6">
                <Metric label="CPU Usage" value={system.cpu} unit="%" icon={Cpu} />
                <Metric label="RAM Usage" value={system.ram} unit="%" icon={Database} />
                <Metric label="Disk Usage" value={system.disk} unit="%" icon={HardDrive} />
            </div>
        </Card>
    );
}

function Metric({ label, value, unit, icon: Icon }) {
    const color = value > 80 ? 'bg-red-500' : value > 50 ? 'bg-yellow-500' : 'bg-cyan-500';
    const glow = value > 80 ? 'shadow-[0_0_10px_rgba(239,68,68,0.5)]' : 'shadow-[0_0_10px_rgba(6,182,212,0.5)]';

    return (
        <div className="group">
            <div className="flex justify-between text-sm mb-2 text-slate-400 group-hover:text-slate-200 transition-colors">
                <div className="flex items-center gap-2">
                    <Icon size={14} />
                    <span>{label}</span>
                </div>
                <span className="font-mono">{value}{unit}</span>
            </div>
            <div className="w-full bg-slate-800/50 rounded-full h-1.5 overflow-hidden">
                <div
                    className={`h-full rounded-full transition-all duration-1000 ease-out ${color} ${glow}`}
                    style={{ width: `${value}%` }}
                />
            </div>
        </div>
    );
}

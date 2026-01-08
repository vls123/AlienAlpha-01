import React from 'react';
import { motion } from 'framer-motion';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs) {
    return twMerge(clsx(inputs));
}

export const Card = ({ children, className, delay = 0 }) => {
    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay }}
            className={cn(
                "relative overflow-hidden rounded-xl border border-slate-800 bg-slate-900/40 p-6 backdrop-blur-xl transition-colors hover:bg-slate-900/60 shadow-2xl",
                className
            )}
        >
            <div className="absolute inset-0 bg-gradient-to-br from-cyan-500/5 via-transparent to-blue-500/5 opacity-0 transition-opacity hover:opacity-100 pointer-events-none" />
            {children}
        </motion.div>
    );
};

export const CardHeader = ({ title, icon: Icon, color = "text-cyan-400" }) => (
    <div className="flex items-center gap-3 mb-6">
        {Icon && (
            <div className={cn("p-2 rounded-lg bg-slate-800/50 border border-slate-700/50", color)}>
                <Icon size={20} />
            </div>
        )}
        <h2 className={cn("text-lg font-bold tracking-tight", color)}>
            {title}
        </h2>
    </div>
);

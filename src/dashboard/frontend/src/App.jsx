import { useState, useEffect } from 'react';
import { Zap, Database, Archive, Activity, HardDrive, Cpu, Server, Container, Atom } from 'lucide-react';
import { SystemHealth } from './components/SystemHealth';
import { Services } from './components/Services';
import { DataPipeline } from './components/DataPipeline';

const API_BASE = import.meta.env.DEV ? 'http://localhost:8000' : '';

function App() {
  const [data, setData] = useState({ system: null, services: null, data: null })
  const [lastUpdate, setLastUpdate] = useState(null)

  const fetchData = async () => {
    try {
      const [sysRes, servRes, dataRes] = await Promise.all([
        fetch(`${API_BASE}/status/system`),
        fetch(`${API_BASE}/status/services`),
        fetch(`${API_BASE}/status/data`)
      ])

      setData({
        system: await sysRes.json(),
        services: await servRes.json(),
        data: await dataRes.json()
      })
      setLastUpdate(new Date())
    } catch (e) {
      console.error("Fetch failed", e)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 2000)
    return () => clearInterval(interval)
  }, [])

  if (!data.system) {
    return (
      <div className="min-h-screen bg-slate-950 flex items-center justify-center">
        <div className="text-cyan-500 animate-pulse font-mono tracking-widest text-sm">INITIALIZING SYSTEM...</div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-50 p-4 md:p-8 font-sans selection:bg-cyan-500/20">

      {/* Background Ambient Glow */}
      <div className="fixed inset-0 pointer-events-none overflow-hidden">
        <div className="absolute -top-[20%] -left-[10%] w-[50%] h-[50%] bg-cyan-500/10 blur-[120px] rounded-full" />
        <div className="absolute bottom-[0%] right-[0%] w-[40%] h-[40%] bg-purple-500/10 blur-[120px] rounded-full" />
      </div>

      <div className="relative z-10 max-w-7xl mx-auto">
        <header className="mb-10 flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
          <div className="flex items-center gap-4">
            {/* Logo */}
            <div className="w-12 h-12 relative flex items-center justify-center">
              <div className="absolute inset-0 bg-cyan-500/20 blur-xl rounded-full animate-pulse" />
              <Atom size={40} className="text-cyan-400 relative z-10 animate-[spin_10s_linear_infinite]" />
            </div>
            <div>
              <h1 className="text-3xl font-bold tracking-tight text-white flex items-center gap-2">
                AlienAlpha <span className="text-cyan-500">Monitor</span>
              </h1>
              <p className="text-slate-400 text-sm">System Operations Center</p>
            </div>
          </div>
          <div className="flex items-center gap-3 bg-slate-900/50 p-2 rounded-lg border border-slate-800 backdrop-blur-sm">
            <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
            <div className="text-xs font-mono text-slate-400">
              UPDATED: {lastUpdate?.toLocaleTimeString()}
            </div>
          </div>
        </header>

        {/* Bento Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 auto-rows-fr">
          <SystemHealth system={data.system} />
          <Services services={data.services} containers={data.system.containers} />
          <DataPipeline data={data.data} />
        </div>
      </div>
    </div>
  )
}

export default App

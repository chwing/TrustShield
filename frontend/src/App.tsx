import React, { useState, useEffect } from 'react';
import { 
  Shield, 
  Search, 
  BarChart3, 
  AlertTriangle, 
  CheckCircle, 
  ExternalLink,
  RefreshCw,
  LayoutDashboard,
  Database
} from 'lucide-react';
import { 
  PieChart, 
  Pie, 
  Cell, 
  ResponsiveContainer, 
  Tooltip, 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis 
} from 'recharts';

const API_BASE = "http://localhost:8000"; // Update this to your FastAPI URL

const parseEntities = (entities: unknown): Array<{ entity: string; word: string }> => {
  if (Array.isArray(entities)) {
    return entities as Array<{ entity: string; word: string }>;
  }

  if (typeof entities === 'string' && entities.trim()) {
    try {
      const parsed = JSON.parse(entities);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  return [];
};

export default function App() {
  const [articles, setArticles] = useState([]);
  const [stats, setStats] = useState(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [loading, setLoading] = useState(true);

  // 1. Fetch data on load
  const fetchData = async () => {
    setLoading(true);
    try {
      const [artRes, statRes] = await Promise.all([
        fetch(`${API_BASE}/articles?limit=10`),
        fetch(`${API_BASE}/stats`)
      ]);
      setArticles(await artRes.json());
      setStats(await statRes.json());
    } catch (err) {
      console.error("Failed to fetch data:", err);
    }
    setLoading(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  // 2. Handle search
  const handleSearch = async (e) => {
    e.preventDefault();
    if (!searchTerm) return fetchData();
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/search?q=${searchTerm}`);
      setArticles(await res.json());
    } catch (err) {
      console.error("Search failed:", err);
    }
    setLoading(false);
  };

  const COLORS = ['#10b981', '#f59e0b', '#ef4444']; // Green, Amber, Red
  const SOURCE_COLORS = ['#38bdf8', '#a78bfa', '#f472b6', '#34d399', '#fbbf24', '#fb7185'];
  const riskData = stats ? Object.entries(stats.risk_distribution || {}).map(([name, value]) => ({ name, value })) : [];
  const sourceData = stats ? Object.entries(stats.top_sources || {}).map(([name, value]) => ({ name, value })) : [];

  return (
    <div className="flex h-screen bg-slate-950 text-slate-100 overflow-hidden">
      {/* Sidebar */}
      <aside className="w-64 bg-slate-900 border-r border-slate-800 flex flex-col p-6 space-y-8">
        <div className="flex items-center gap-3">
          <Shield className="w-10 h-10 text-emerald-400" />
          <h1 className="text-xl font-bold tracking-tight">TrustShield</h1>
        </div>
        
        <nav className="flex-1 space-y-2">
          <button className="flex items-center gap-3 w-full p-3 rounded-lg bg-slate-800 text-emerald-400 font-medium">
            <LayoutDashboard className="w-5 h-5" /> Dashboard
          </button>
          <button className="flex items-center gap-3 w-full p-3 rounded-lg hover:bg-slate-800 text-slate-400 hover:text-white transition-colors">
            <Database className="w-5 h-5" /> Data Lake
          </button>
          <button className="flex items-center gap-3 w-full p-3 rounded-lg hover:bg-slate-800 text-slate-400 hover:text-white transition-colors">
            <BarChart3 className="w-5 h-5" /> Reports
          </button>
        </nav>
        
        <div className="p-4 bg-slate-800/50 rounded-xl border border-slate-700">
          <p className="text-xs text-slate-500 uppercase font-bold mb-2">System Status</p>
          <div className="flex items-center gap-2 text-sm">
            <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></div>
            <span>AI Models Online</span>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-y-auto p-10 space-y-10">
        
        {/* Top Header & Search */}
        <header className="flex flex-col md:flex-row md:items-center justify-between gap-6">
          <div>
            <h2 className="text-3xl font-bold">Verification Overview</h2>
            <p className="text-slate-400 mt-1">Real-time analysis of media credibility.</p>
          </div>
          <form onSubmit={handleSearch} className="relative group">
            <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500 group-focus-within:text-emerald-400 transition-colors" />
            <input 
              type="text" 
              placeholder="Search claims or keywords..." 
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="bg-slate-900 border border-slate-800 rounded-full py-3 pl-12 pr-6 w-[400px] focus:outline-none focus:border-emerald-500/50 focus:ring-4 focus:ring-emerald-500/10 transition-all shadow-lg"
            />
          </form>
        </header>

        {/* Analytics Section */}
        <section className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          <div className="lg:col-span-2 bg-slate-900 rounded-2xl p-8 border border-slate-800 shadow-xl">
            <h3 className="text-lg font-semibold mb-6">Article Risk Distribution</h3>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={riskData}>
                  <XAxis dataKey="name" stroke="#64748b" />
                  <YAxis stroke="#64748b" />
                  <Tooltip 
                    contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '12px' }}
                  />
                  <Bar dataKey="value" fill="#10b981" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
          
          <div className="bg-slate-900 rounded-2xl p-8 border border-slate-800 shadow-xl">
            <h3 className="text-lg font-semibold mb-6">Top Data Sources</h3>
            <div className="h-64">
              {sourceData.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={sourceData}
                      innerRadius={60}
                      outerRadius={80}
                      paddingAngle={3}
                      dataKey="value"
                      nameKey="name"
                    >
                      {sourceData.map((_, index) => (
                        <Cell key={`source-cell-${index}`} fill={SOURCE_COLORS[index % SOURCE_COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '12px' }} />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-full flex items-center justify-center text-sm text-slate-400">
                  No indexed sources yet.
                </div>
              )}
            </div>
          </div>
        </section>

        {/* Live Article Feed */}
        <section className="space-y-6">
          <div className="flex items-center justify-between">
            <h3 className="text-xl font-bold flex items-center gap-2">
              <RefreshCw className={`w-5 h-5 text-emerald-400 ${loading ? 'animate-spin' : ''}`} />
              Latest Verifications
            </h3>
            <span className="text-sm text-slate-500">Showing {articles.length} results</span>
          </div>

          <div className="grid grid-cols-1 gap-4">
            {articles.length === 0 ? (
              <div className="bg-slate-900 border border-slate-800 p-6 rounded-2xl text-slate-400">
                No indexed articles yet. Run ingestion + inference + indexing DAGs in Airflow, then refresh.
              </div>
            ) : articles.map((article, idx) => (
              <div 
                key={idx} 
                className="group bg-slate-900 hover:bg-slate-800/80 border border-slate-800 hover:border-slate-700 p-6 rounded-2xl transition-all duration-300 shadow-md hover:shadow-2xl flex flex-col md:flex-row gap-6"
              >
                <div className="flex-1 space-y-4">
                  <div className="flex items-center gap-3">
                    <span className={`px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wider border ${
                      article.credibility_category === 'High Risk' ? 'bg-red-500/10 border-red-500/50 text-red-400' :
                      article.credibility_category === 'Medium Risk' ? 'bg-amber-500/10 border-amber-500/50 text-amber-400' :
                      'bg-emerald-500/10 border-emerald-500/50 text-emerald-400'
                    }`}>
                      {article.credibility_category}
                    </span>
                    <span className="text-sm text-slate-500">{article.source_name || 'Unknown Source'} • {article.timestamp}</span>
                  </div>
                  
                  <h4 className="text-lg font-semibold leading-tight group-hover:text-emerald-400 transition-colors">{article.content_title}</h4>
                  <p className="text-slate-400 text-sm italic">" {article.explanation} "</p>
                  
                  {/* Entities */}
                  <div className="flex flex-wrap gap-2">
                    {parseEntities(article.entities).slice(0, 3).map((ent, i) => (
                      <span key={i} className="text-[10px] bg-slate-800 px-2 py-1 rounded border border-slate-700 text-slate-300 uppercase font-mono">
                        {ent.entity}: {ent.word}
                      </span>
                    ))}
                  </div>
                </div>

                <div className="flex md:flex-col items-center justify-center gap-4 md:border-l border-slate-800 md:pl-6 md:min-w-[120px]">
                  <div className="text-center">
                    <p className="text-xs text-slate-500 uppercase font-bold mb-1">Risk Score</p>
                    <p className={`text-2xl font-black ${
                      article.misinfo_probability > 0.6 ? 'text-red-400' :
                      article.misinfo_probability > 0.3 ? 'text-amber-400' : 'text-emerald-400'
                    }`}>
                      {Math.round(article.misinfo_probability * 100)}%
                    </p>
                  </div>
                  <a 
                    href={article.url} 
                    target="_blank" 
                    rel="noreferrer"
                    className="p-3 bg-slate-800 hover:bg-emerald-500/20 text-slate-400 hover:text-emerald-400 rounded-xl transition-all border border-slate-700 hover:border-emerald-500/30"
                  >
                    <ExternalLink className="w-5 h-5" />
                  </a>
                </div>
              </div>
            ))}
          </div>
        </section>
      </main>
    </div>
  );
}

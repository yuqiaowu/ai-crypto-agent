import { useState } from 'react';
import { ProfitChart } from './components/ProfitChart';
import { PositionsTab } from './components/PositionsTab';
import { HistoryTab } from './components/HistoryTab';
import { TrendingUp, Wallet, History } from 'lucide-react';

export default function App() {
  const [activeTab, setActiveTab] = useState<'positions' | 'history'>('positions');

  return (
    <div className="min-h-screen bg-[#1a1d24] text-white p-4">
      <div className="mx-auto" style={{ width: '95%' }}>
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center gap-4 mb-2">
            <div>
              <h1 className="flex items-baseline gap-3 text-5xl">
                <span className="text-white">Crypto</span>
                <span className="text-lime-400">Quant</span>
                <span className="text-white">Dashboard</span>
              </h1>
              <div className="flex items-center gap-2 mt-2">
                <p className="text-gray-400">结合qlib+deepseek 的量化交易策略</p>
              </div>
            </div>
          </div>
        </div>

        {/* Main Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Left: Profit Chart */}
          <div className="bg-[#2a2d35] rounded-xl border border-gray-700 p-6 shadow-2xl">
            <h2 className="text-lime-400 mb-4 flex items-center gap-2">
              <TrendingUp className="w-5 h-5" />
              收益曲线
            </h2>
            <ProfitChart />
          </div>

          {/* Right: Tabs */}
          <div className="bg-[#2a2d35] rounded-xl border border-gray-700 shadow-2xl">
            {/* Tab Headers */}
            <div className="flex border-b border-gray-700">
              <button
                onClick={() => setActiveTab('positions')}
                className={`flex-1 flex items-center justify-center gap-2 px-6 py-4 transition-all ${activeTab === 'positions'
                  ? 'bg-lime-500/10 text-lime-400 border-b-2 border-lime-400'
                  : 'text-gray-400 hover:text-gray-300 hover:bg-gray-700/30'
                  }`}
              >
                <Wallet className="w-4 h-4" />
                当前持仓
              </button>
              <button
                onClick={() => setActiveTab('history')}
                className={`flex-1 flex items-center justify-center gap-2 px-6 py-4 transition-all ${activeTab === 'history'
                  ? 'bg-lime-500/10 text-lime-400 border-b-2 border-lime-400'
                  : 'text-gray-400 hover:text-gray-300 hover:bg-gray-700/30'
                  }`}
              >
                <History className="w-4 h-4" />
                历史记录
              </button>
            </div>

            {/* Tab Content */}
            <div className="p-6">
              {activeTab === 'positions' ? <PositionsTab /> : <HistoryTab />}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
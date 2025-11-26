/// <reference types="vite/client" />
import { useState, useEffect } from 'react';
import { TrendingUp, TrendingDown, Clock, RefreshCw } from 'lucide-react';

interface HistoryRecord {
  id: string;
  symbol: string;
  type: 'long' | 'short';
  entryPrice: number;
  exitPrice: number;
  amount: number;
  pnl: number;
  pnlPercent: number;
  entryTime: string;
  exitTime: string;
}

export function HistoryTab() {
  const [history, setHistory] = useState<HistoryRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchHistory = async () => {
    try {
      setLoading(true);

      if (import.meta.env.MODE === 'production') {
        // For now, return empty history in production as we don't have a JSON source yet
        // TODO: Parse trade_log.csv or update backend to generate trade_log.json
        setHistory([]);
        setLoading(false);
        return;
      }

      const response = await fetch('http://localhost:5001/api/history');
      if (!response.ok) {
        throw new Error('Failed to fetch history');
      }
      const data = await response.json();
      setHistory(data);
      setError(null);
    } catch (err) {
      setError('无法连接到交易服务器');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchHistory();
    const interval = setInterval(fetchHistory, 30000); // Poll every 30s
    return () => clearInterval(interval);
  }, []);

  const totalPnl = history.reduce((sum, record) => sum + record.pnl, 0);
  const winCount = history.filter(r => r.pnl > 0).length;
  const winRate = history.length > 0 ? ((winCount / history.length) * 100).toFixed(1) : '0.0';

  if (loading && history.length === 0) {
    return <div className="text-gray-400 p-4">加载中...</div>;
  }

  return (
    <div>
      {/* Summary Stats */}
      <div className="grid grid-cols-3 gap-3 mb-4">
        <div className="bg-[#1f2229] rounded-lg p-3 border border-gray-700/50">
          <div className="text-gray-400 text-sm mb-1">总盈亏</div>
          <div className={`font-['DIN_Alternate',sans-serif] ${totalPnl >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
            {totalPnl >= 0 ? '+' : ''}${totalPnl.toFixed(0)}
          </div>
        </div>
        <div className="bg-[#1f2229] rounded-lg p-3 border border-gray-700/50">
          <div className="text-gray-400 text-sm mb-1">胜率</div>
          <div className="text-lime-400 font-['DIN_Alternate',sans-serif]">{winRate}%</div>
        </div>
        <div className="bg-[#1f2229] rounded-lg p-3 border border-gray-700/50">
          <div className="text-gray-400 text-sm mb-1">交易次数</div>
          <div className="text-white font-['DIN_Alternate',sans-serif]">{history.length}</div>
        </div>
      </div>

      {/* History List */}
      <div className="space-y-2 max-h-[480px] overflow-y-auto pr-2">
        {history.length === 0 ? (
          <div className="text-gray-500 text-center py-8">暂无历史记录</div>
        ) : (
          history.map((record) => (
            <div
              key={record.id}
              className={`rounded-lg p-4 border transition-all ${record.pnl >= 0
                ? 'bg-lime-500/5 border-lime-500/20 hover:border-lime-500/40'
                : 'bg-red-500/5 border-red-500/20 hover:border-red-500/40'
                }`}
            >
              <div className="flex justify-between items-start mb-2">
                <div className="flex items-center gap-2">
                  <span className="text-lime-400">{record.symbol}</span>
                  <span className={`px-2 py-0.5 rounded text-xs ${record.type === 'long'
                    ? 'bg-lime-500/20 text-lime-400'
                    : 'bg-orange-500/20 text-orange-400'
                    }`}>
                    {record.type === 'long' ? '做多' : '做空'}
                  </span>
                </div>
                <div className="flex items-center gap-1">
                  {record.pnl >= 0 ? (
                    <TrendingUp className="w-4 h-4 text-lime-400" />
                  ) : (
                    <TrendingDown className="w-4 h-4 text-red-400" />
                  )}
                  <span className={`font-['DIN_Alternate',sans-serif] ${record.pnl >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
                    {record.pnl >= 0 ? '+' : ''}{record.pnlPercent.toFixed(2)}%
                  </span>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-2 text-sm mb-2">
                <div>
                  <span className="text-gray-500">开仓: </span>
                  <span className="text-white font-['DIN_Alternate',sans-serif]">${record.entryPrice.toLocaleString()}</span>
                </div>
                <div>
                  <span className="text-gray-500">平仓: </span>
                  <span className="text-white font-['DIN_Alternate',sans-serif]">${record.exitPrice.toLocaleString()}</span>
                </div>
              </div>

              <div className="flex justify-between items-center text-xs">
                <div className="flex items-center gap-1 text-gray-500">
                  <Clock className="w-3 h-3" />
                  {record.entryTime} - {record.exitTime}
                </div>
                <div className={`font-['DIN_Alternate',sans-serif] ${record.pnl >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
                  {record.pnl >= 0 ? '+' : ''}${record.pnl.toFixed(2)}
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
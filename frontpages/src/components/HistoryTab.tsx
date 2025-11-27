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
        try {
          const response = await fetch('/data/trade_log.csv');
          if (response.ok) {
            const text = await response.text();
            const lines = text.trim().split('\n');
            const parsedHistory: HistoryRecord[] = [];

            // Skip header (time,symbol,action,side,qty,price,notional,margin,fee,realized_pnl,nav_after)
            for (let i = 1; i < lines.length; i++) {
              const line = lines[i].trim();
              if (!line) continue;

              const [time, symbol, action, side, qty, price, notional, margin, fee, realized_pnl] = line.split(',');

              // Only show closed positions (action == 'close_position')
              if (action === 'close_position') {
                // We need entry info. In a real app we'd match with open, but here the log has the close info.
                // The CSV structure for close_position row:
                // time, symbol, close_position, side, qty, exit_price, notional, margin, fee, realized_pnl
                // It doesn't explicitly have entry price/time in the same row easily without calculation or looking back.
                // But wait, the mock executor writes:
                // "realized_pnl": pnl - fee
                // pnl = (exit_price - entry_price) * qty
                // So entry_price = exit_price - (pnl / qty)

                const exitPrice = parseFloat(price);
                const quantity = parseFloat(qty);
                const pnlVal = parseFloat(realized_pnl); // This is net pnl (pnl - fee)
                const feeVal = parseFloat(fee);
                const rawPnl = pnlVal + feeVal; // Gross PnL

                // Calculate entry price
                // For Long: PnL = (Exit - Entry) * Qty => Entry = Exit - (PnL / Qty)
                // For Short: PnL = (Entry - Exit) * Qty => Entry = Exit + (PnL / Qty)

                let entryPrice = 0;
                if (side === 'long') {
                  entryPrice = exitPrice - (rawPnl / quantity);
                } else {
                  entryPrice = exitPrice + (rawPnl / quantity);
                }

                const pnlPercent = (rawPnl / (parseFloat(margin) || (entryPrice * quantity / 2))) * 100; // Estimate margin if missing

                parsedHistory.push({
                  id: `${time}-${symbol}`,
                  symbol: symbol,
                  type: side as 'long' | 'short',
                  entryPrice: entryPrice,
                  exitPrice: exitPrice,
                  amount: quantity,
                  pnl: pnlVal,
                  pnlPercent: pnlPercent,
                  entryTime: 'Unknown', // We don't have entry time in this row
                  exitTime: time
                });
              }
            }
            // Sort by time desc
            parsedHistory.sort((a, b) => new Date(b.exitTime).getTime() - new Date(a.exitTime).getTime());
            setHistory(parsedHistory);
          } else {
            setHistory([]);
          }
        } catch (e) {
          console.error("Failed to load history CSV", e);
          setHistory([]);
        }
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
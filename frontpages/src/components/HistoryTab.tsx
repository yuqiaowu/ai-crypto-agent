/// <reference types="vite/client" />
import { useState, useEffect } from 'react';
import { TrendingUp, TrendingDown, Clock } from 'lucide-react';

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
  leverage: number;
  notional: number;
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

            // Track open positions to match with close records
            // Key: symbol, Value: { entryTime, entryPrice, qty, notional, margin, leverage }
            const openPositions: Record<string, { entryTime: string, entryPrice: number, qty: number, notional: number, margin: number, leverage: number }> = {};

            for (let i = 1; i < lines.length; i++) {
              const line = lines[i].trim();
              if (!line) continue;

              const fields = line.split(',');
              // Old format: time,symbol,action,side,qty,price,notional,margin,fee,realized_pnl,nav_after,reason
              // New format: time,symbol,action,side,qty,price,notional,margin,leverage,fee,realized_pnl,nav_after,reason
              const hasLeverage = fields.length >= 13; // New format has 13+ fields

              const time = fields[0];
              const symbol = fields[1];
              const action = fields[2];
              const side = fields[3];
              const qty = fields[4];
              const price = fields[5];
              const notional = fields[6];
              const margin = fields[7];

              // Correct mapping based on CSV header:
              // time,symbol,action,side,qty,price,notional,margin,fee,realized_pnl,nav_after,reason,leverage
              const fee = fields[8];
              const realized_pnl = fields[9];
              // fields[10] is nav_after
              // fields[11] is reason
              const leverage = fields.length > 12 ? fields[12] : null;

              if (action === 'open_long' || action === 'open_short') {
                openPositions[symbol] = {
                  entryTime: time,
                  entryPrice: parseFloat(price),
                  qty: parseFloat(qty),
                  notional: parseFloat(notional),
                  margin: parseFloat(margin),
                  leverage: leverage ? parseFloat(leverage) : (parseFloat(notional) / parseFloat(margin))
                };
              } else if (action === 'close_position') {
                const exitPrice = parseFloat(price);
                const quantity = parseFloat(qty);
                const pnlVal = parseFloat(realized_pnl);
                const feeVal = parseFloat(fee);
                const rawPnl = pnlVal + feeVal;

                // Try to find matching open record
                const openInfo = openPositions[symbol];

                let entryPrice = 0;
                let entryTime = 'Unknown';

                if (openInfo) {
                  entryPrice = openInfo.entryPrice;
                  entryTime = openInfo.entryTime;
                  // Optional: Verify quantity matches or handle partial closes (assuming full close for now)
                  delete openPositions[symbol]; // Remove after closing
                } else {
                  // Fallback calculation if open record missing in this log file (e.g. rotated log)
                  if (side === 'long') {
                    entryPrice = exitPrice - (rawPnl / quantity);
                  } else {
                    entryPrice = exitPrice + (rawPnl / quantity);
                  }
                }

                const pnlPercent = (rawPnl / (parseFloat(margin) || (entryPrice * quantity / 2))) * 100;

                // Use leverage from open position if available, otherwise calculate from close position
                const leverageVal = openInfo?.leverage || (leverage ? parseFloat(leverage) : (parseFloat(notional) / parseFloat(margin)));
                const notionalVal = openInfo?.notional || parseFloat(notional);

                parsedHistory.push({
                  id: `${time}-${symbol}-${Math.random()}`,
                  symbol: symbol,
                  type: side as 'long' | 'short',
                  entryPrice: entryPrice,
                  exitPrice: exitPrice,
                  amount: quantity,
                  pnl: pnlVal,
                  pnlPercent: pnlPercent,
                  entryTime: entryTime,
                  exitTime: time,
                  leverage: leverageVal,
                  notional: notionalVal
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

  if (error && history.length === 0) {
    return <div className="text-red-400 p-4 text-center">{error}</div>;
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
          <div className="text-white font-['DIN_Alternate',sans-serif]">{history.length}</div>
        </div>
      </div>

      {/* History List */}
      <div className="space-y-2 overflow-y-auto pr-2" style={{ maxHeight: 'calc(100vh - 250px)' }}>
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
                  <span className="text-white font-['DIN_Alternate',sans-serif]">${record.entryPrice.toFixed(2)}</span>
                </div>
                <div>
                  <span className="text-gray-500">平仓: </span>
                  <span className="text-white font-['DIN_Alternate',sans-serif]">${record.exitPrice.toFixed(2)}</span>
                </div>
                <div>
                  <span className="text-gray-500">数量: </span>
                  <span className="text-white font-['DIN_Alternate',sans-serif]">{record.amount.toFixed(4)}</span>
                </div>
                <div>
                  <span className="text-gray-500">杠杆: </span>
                  <span className="text-orange-400 font-['DIN_Alternate',sans-serif]">{record.leverage.toFixed(1)}x</span>
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
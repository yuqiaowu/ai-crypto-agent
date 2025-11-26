/// <reference types="vite/client" />
import { useState, useEffect } from 'react';
import { TrendingUp, TrendingDown, ArrowUpCircle, ArrowDownCircle, RefreshCw } from 'lucide-react';

interface Position {
  symbol: string;
  name: string;
  entryPrice: number;
  currentPrice: number;
  stopLoss: number;
  takeProfit: number;
  amount: number;
  pnl: number;
  pnlPercent: number;
}

interface PortfolioState {
  nav: number;
  cash: number;
  positions: {
    symbol: string;
    entry_price: number;
    current_price: number;
    quantity: number;
    side: string;
    exit_plan: {
      stop_loss: number;
      take_profit: number;
    };
  }[];
}

const COIN_NAMES: Record<string, string> = {
  "BTC": "Bitcoin",
  "ETH": "Ethereum",
  "SOL": "Solana",
  "BNB": "BNB",
  "DOGE": "Dogecoin"
};

export function PositionsTab() {
  const [positions, setPositions] = useState<Position[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchPositions = async () => {
    try {
      setLoading(true);

      let data: Position[] = [];

      if (import.meta.env.MODE === 'production') {
        // Fetch from static file in production
        const response = await fetch('/data/portfolio_state.json');
        if (!response.ok) throw new Error('Failed to fetch data');
        const state: PortfolioState = await response.json();

        // Transform raw state to Position interface
        data = (state.positions || []).map(pos => {
          const entryPrice = pos.entry_price;
          const currentPrice = pos.current_price;
          const quantity = pos.quantity;
          let pnl, pnlPercent;

          if (pos.side === 'long') {
            pnl = (currentPrice - entryPrice) * quantity;
            pnlPercent = ((currentPrice - entryPrice) / entryPrice) * 100;
          } else {
            pnl = (entryPrice - currentPrice) * quantity;
            pnlPercent = ((entryPrice - currentPrice) / entryPrice) * 100;
          }

          return {
            symbol: pos.symbol,
            name: COIN_NAMES[pos.symbol] || pos.symbol,
            entryPrice,
            currentPrice,
            stopLoss: pos.exit_plan.stop_loss,
            takeProfit: pos.exit_plan.take_profit,
            amount: quantity,
            pnl,
            pnlPercent
          };
        });
      } else {
        // Fetch from API in development
        const response = await fetch('http://localhost:5001/api/positions');
        if (!response.ok) throw new Error('Failed to fetch positions');
        data = await response.json();
      }

      setPositions(data);
      setError(null);
    } catch (err) {
      console.error(err);
      // Don't show error in UI immediately if it's just a poll failure, keep old data if available
      if (positions.length === 0) {
        setError('无法连接到数据源');
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPositions();
    // Poll every 10 seconds
    const interval = setInterval(fetchPositions, 10000);
    return () => clearInterval(interval);
  }, []);

  const totalPnl = positions.reduce((sum, pos) => sum + pos.pnl, 0);

  if (loading && positions.length === 0) {
    return <div className="text-gray-400 p-4">加载中...</div>;
  }

  if (error && positions.length === 0) {
    return (
      <div className="text-red-400 p-4 border border-red-500/20 rounded bg-red-500/10">
        {error}
        <button onClick={fetchPositions} className="ml-4 underline">重试</button>
      </div>
    );
  }

  return (
    <div>
      {/* Summary */}
      <div className="bg-[#1f2229] rounded-lg p-4 mb-4 border border-gray-700/50 flex justify-between items-center">
        <div className="flex items-center gap-4">
          <div>
            <div className="text-gray-400 text-sm">持仓盈亏</div>
            <div className={`flex items-center gap-1 font-['DIN_Alternate',sans-serif] text-xl ${totalPnl >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
              {totalPnl >= 0 ? <TrendingUp className="w-5 h-5" /> : <TrendingDown className="w-5 h-5" />}
              {totalPnl >= 0 ? '+' : ''}${totalPnl.toFixed(2)}
            </div>
          </div>
          <div className="h-8 w-px bg-gray-700 mx-2"></div>
          <div>
            <div className="text-gray-400 text-sm">持仓数量</div>
            <div className="text-white font-['DIN_Alternate',sans-serif] text-xl">{positions.length}</div>
          </div>
        </div>
        <button onClick={fetchPositions} className="p-2 hover:bg-gray-700 rounded-full transition-colors text-gray-400 hover:text-white">
          <RefreshCw className="w-4 h-4" />
        </button>
      </div>

      {/* Positions List */}
      <div className="space-y-3 max-h-[500px] overflow-y-auto">
        {positions.length === 0 ? (
          <div className="text-gray-500 text-center py-8">暂无持仓</div>
        ) : (
          positions.map((position) => (
            <div
              key={position.symbol}
              className="bg-[#1f2229] rounded-lg p-4 border border-gray-700/50 hover:border-lime-500/50 transition-all"
            >
              {/* Header */}
              <div className="flex justify-between items-start mb-3">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="text-lime-400 text-lg font-bold">{position.symbol}</span>
                    <span className="text-gray-500 text-sm">{position.name}</span>
                  </div>
                  <div className="text-gray-400 text-sm mt-1 font-['DIN_Alternate',sans-serif]">
                    持仓: {position.amount} {position.symbol}
                  </div>
                </div>
                <div className={`px-2 py-1 rounded text-sm font-['DIN_Alternate',sans-serif] ${position.pnl >= 0
                  ? 'bg-lime-500/20 text-lime-400'
                  : 'bg-red-500/20 text-red-400'
                  }`}>
                  {position.pnl >= 0 ? '+' : ''}{position.pnlPercent.toFixed(2)}%
                </div>
              </div>

              {/* Price Info */}
              <div className="grid grid-cols-2 gap-3 text-sm">
                <div>
                  <div className="text-gray-500 mb-1">开仓价</div>
                  <div className="text-white font-['DIN_Alternate',sans-serif]">${position.entryPrice.toLocaleString()}</div>
                </div>
                <div>
                  <div className="text-gray-500 mb-1">当前价</div>
                  <div className="text-white font-['DIN_Alternate',sans-serif]">${position.currentPrice.toLocaleString()}</div>
                </div>
                <div>
                  <div className="text-gray-500 mb-1 flex items-center gap-1">
                    <ArrowDownCircle className="w-3 h-3 text-red-400" />
                    止损价
                  </div>
                  <div className="text-red-400 font-['DIN_Alternate',sans-serif]">${position.stopLoss.toLocaleString()}</div>
                </div>
                <div>
                  <div className="text-gray-500 mb-1 flex items-center gap-1">
                    <ArrowUpCircle className="w-3 h-3 text-lime-400" />
                    止盈价
                  </div>
                  <div className="text-lime-400 font-['DIN_Alternate',sans-serif]">${position.takeProfit.toLocaleString()}</div>
                </div>
              </div>

              {/* PnL */}
              <div className="mt-3 pt-3 border-t border-gray-700/50">
                <div className="flex justify-between items-center">
                  <span className="text-gray-500 text-sm">盈亏</span>
                  <span className={`font-['DIN_Alternate',sans-serif] text-lg ${position.pnl >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
                    {position.pnl >= 0 ? '+' : ''}${position.pnl.toFixed(2)}
                  </span>
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
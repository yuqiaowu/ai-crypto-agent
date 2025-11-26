/// <reference types="vite/client" />
import { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart, ReferenceLine } from 'recharts';
import { TrendingUp, TrendingDown, DollarSign } from 'lucide-react';

// 生成模拟收益数据 - 更曲折的曲线
const generateProfitData = (finalValue: number = 10000) => {
  const data = [];
  const days = 30;

  // Simplified approach: Generate random steps, then correct the drift
  let current = 10000;
  const steps = [];
  for (let i = 0; i < days; i++) {
    steps.push((Math.random() - 0.5) * 800);
  }

  // Calculate uncorrected end
  const uncorrectedEnd = 10000 + steps.reduce((a, b) => a + b, 0);
  const error = finalValue - uncorrectedEnd;
  const correctionPerStep = error / days;

  let value = 10000;
  for (let i = 0; i < days; i++) {
    value += steps[i] + correctionPerStep;
    data.push({
      date: `${i + 1}日`,
      value: Math.round(value),
      profit: Math.round(value - 10000),
    });
  }

  // Force exact end value
  data[days - 1].value = finalValue;
  data[days - 1].profit = finalValue - 10000;

  return data;
};

export function ProfitChart() {
  const [data, setData] = useState<any[]>([]);
  const [currentValue, setCurrentValue] = useState(10000);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        let nav = 10000;
        if (import.meta.env.MODE === 'production') {
          const response = await fetch('/data/portfolio_state.json');
          if (response.ok) {
            const state = await response.json();
            nav = state.nav || 10000;
          }
        } else {
          // Dev mode: fetch from API
          try {
            const response = await fetch('http://localhost:5001/api/summary');
            if (response.ok) {
              const summary = await response.json();
              nav = summary.nav || 10000;
            }
          } catch (e) {
            console.warn("API fetch failed, using default");
          }
        }

        setCurrentValue(nav);
        setData(generateProfitData(nav));
      } catch (e) {
        console.error(e);
        setData(generateProfitData(10000));
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  const totalProfit = currentValue - 10000;
  const profitPercentage = ((totalProfit / 10000) * 100).toFixed(2);


  // 计算Y轴的范围
  const minValue = Math.min(...data.map(d => d.value));
  const maxValue = Math.max(...data.map(d => d.value));
  const padding = (maxValue - minValue) * 0.1; // 10%的上下边距

  const isPositive = totalProfit >= 0;
  const color = isPositive ? '#a3e635' : '#ef4444'; // Green or Red

  return (
    <div>
      {/* Stats */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <div className="bg-[#1f2229] rounded-lg p-4 border border-gray-700/50">
          <div className="text-gray-400 text-sm mb-1">初始资金</div>
          <div className="text-white font-['DIN_Alternate',sans-serif]">$10,000</div>
        </div>
        <div className="bg-[#1f2229] rounded-lg p-4 border border-gray-700/50">
          <div className="text-gray-400 text-sm mb-1">当前净值</div>
          <div className="text-white font-['DIN_Alternate',sans-serif]">${currentValue.toLocaleString()}</div>
        </div>
        <div className={`bg-[#1f2229] rounded-lg p-4 border ${isPositive ? 'border-lime-500/30' : 'border-red-500/30'}`}>
          <div className="text-gray-400 text-sm mb-1">总收益</div>
          <div className={`flex items-center gap-1 font-['DIN_Alternate',sans-serif] ${isPositive ? 'text-lime-400' : 'text-red-400'}`}>
            {isPositive ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
            {isPositive ? '+' : ''}{profitPercentage}%
          </div>
        </div>
      </div>

      {/* Chart */}
      <ResponsiveContainer width="100%" height={500}>
        <AreaChart data={data}>
          <defs>
            <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={color} stopOpacity={0.4} />
              <stop offset="95%" stopColor={color} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="date"
            stroke="#6b7280"
            tick={{ fill: '#9ca3af', fontFamily: 'DIN Alternate, sans-serif' }}
          />
          <YAxis
            stroke="#6b7280"
            tick={{ fill: '#9ca3af', fontFamily: 'DIN Alternate, sans-serif' }}
            tickFormatter={(value) => `$${(value / 1000).toFixed(1)}k`}
            domain={[minValue - padding, maxValue + padding]}
          />
          <ReferenceLine y={10000} stroke="#6b7280" strokeDasharray="3 3" />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1f2229',
              border: `1px solid ${color}`,
              borderRadius: '8px',
              color: '#fff',
              fontFamily: 'DIN Alternate, sans-serif'
            }}
            formatter={(value: number) => [`$${value.toLocaleString()}`, '净值']}
          />
          <Area
            type="monotone"
            dataKey="value"
            stroke={color}
            strokeWidth={2}
            fill="url(#colorValue)"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
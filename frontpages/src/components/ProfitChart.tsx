/// <reference types="vite/client" />
import { useState, useEffect } from 'react';
import { XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart, ReferenceLine } from 'recharts';
import { TrendingUp, TrendingDown } from 'lucide-react';



export function ProfitChart() {
  const [data, setData] = useState<any[]>([]);
  const [currentValue, setCurrentValue] = useState(10000);

  useEffect(() => {
    const fetchData = async () => {
      try {
        let historyData = [];
        let latestNav = 10000;

        if (import.meta.env.MODE === 'production') {
          // Fetch CSV directly
          const response = await fetch('/data/nav_history.csv');
          if (response.ok) {
            const text = await response.text();
            // Parse CSV: timestamp,nav
            const lines = text.trim().split('\n');
            // Skip header if present
            const startIndex = lines[0].startsWith('timestamp') ? 1 : 0;

            for (let i = startIndex; i < lines.length; i++) {
              const [ts, navStr] = lines[i].split(',');
              if (ts && navStr) {
                const date = new Date(ts);
                // Format date as MM-DD HH:mm
                const dateStr = `${(date.getMonth() + 1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')} ${date.getHours().toString().padStart(2, '0')}:00`;
                const val = parseFloat(navStr);
                historyData.push({
                  date: dateStr,
                  value: val,
                  profit: val - 10000,
                  fullDate: ts
                });
                latestNav = val;
              }
            }
          }
        } else {
          // Dev mode: try fetch from API or fallback to CSV in public
          // For now, let's assume we can fetch the CSV from public/data in dev too if served
          // Or use the API if we built one. Let's try fetching the file directly assuming vite serves it.
          try {
            const response = await fetch('/data/nav_history.csv');
            if (response.ok) {
              const text = await response.text();
              const lines = text.trim().split('\n');
              const startIndex = lines[0].startsWith('timestamp') ? 1 : 0;
              for (let i = startIndex; i < lines.length; i++) {
                const [ts, navStr] = lines[i].split(',');
                if (ts && navStr) {
                  const date = new Date(ts);
                  const dateStr = `${(date.getMonth() + 1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')} ${date.getHours().toString().padStart(2, '0')}:00`;
                  const val = parseFloat(navStr);
                  historyData.push({
                    date: dateStr,
                    value: val,
                    profit: val - 10000,
                    fullDate: ts
                  });
                  latestNav = val;
                }
              }
            }
          } catch (e) {
            console.warn("Failed to fetch nav history in dev");
          }
        }

        if (historyData.length > 0) {
          setData(historyData);
          setCurrentValue(latestNav);
        } else {
          // Fallback if no data yet
          setData([{ date: 'Start', value: 10000, profit: 0 }]);
          setCurrentValue(10000);
        }

      } catch (e) {
        console.error(e);
      }
    };

    fetchData();
  }, []);

  const totalProfit = currentValue - 10000;
  const profitPercentage = ((totalProfit / 10000) * 100).toFixed(2);

  // Calculate Y axis range
  const minValue = Math.min(...data.map(d => d.value), 10000);
  const maxValue = Math.max(...data.map(d => d.value), 10000);
  const padding = (maxValue - minValue) * 0.1 || 100; // Default padding if flat

  const isPositive = totalProfit >= 0;
  const color = isPositive ? '#a3e635' : '#ef4444';

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
          <div className="text-white font-['DIN_Alternate',sans-serif]">${currentValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
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
            tick={{ fill: '#9ca3af', fontFamily: 'DIN Alternate, sans-serif', fontSize: 12 }}
            minTickGap={30}
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
            labelStyle={{ color: '#9ca3af', marginBottom: '0.5rem' }}
            formatter={(value: number) => [`$${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`, '净值']}
          />
          <Area
            type="monotone"
            dataKey="value"
            stroke={color}
            strokeWidth={2}
            fill="url(#colorValue)"
            animationDuration={1000}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
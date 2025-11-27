/// <reference types="vite/client" />
import { useState, useEffect } from 'react';
import { Brain, AlertTriangle, CheckCircle, ArrowRight } from 'lucide-react';

interface AgentAction {
    symbol: string;
    action: string;
    leverage: number | null;
    position_size_usd: number | null;
    entry_reason: string | null;
    exit_plan: {
        take_profit?: number | null;
        stop_loss?: number | null;
        invalidation?: string | null;
    };
}

interface AgentDecision {
    analysis_summary: string;
    actions: AgentAction[];
    timestamp?: string;
}

export function AgentLogTab() {
    const [decisions, setDecisions] = useState<AgentDecision[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchLog = async () => {
            try {
                setLoading(true);
                let data: AgentDecision | AgentDecision[];

                if (import.meta.env.MODE === 'production') {
                    const response = await fetch('/data/agent_decision_log.json');
                    if (!response.ok) throw new Error('Failed to fetch agent log');
                    data = await response.json();
                } else {
                    const response = await fetch('http://localhost:5001/api/agent-decision');
                    if (!response.ok) throw new Error('Failed to fetch agent log');
                    data = await response.json();
                }

                // Normalize to array
                if (Array.isArray(data)) {
                    setDecisions(data);
                } else if (data) {
                    setDecisions([data]);
                } else {
                    setDecisions([]);
                }
                setError(null);
            } catch (err) {
                console.error(err);
                setError('无法加载模型决策日志');
            } finally {
                setLoading(false);
            }
        };

        fetchLog();
    }, []);

    if (loading) return <div className="text-gray-400 p-4">加载模型思考中...</div>;
    if (error) return <div className="text-red-400 p-4">{error}</div>;
    if (decisions.length === 0) return <div className="text-gray-500 p-4">暂无决策记录</div>;

    return (
        <div className="space-y-8">
            {decisions.map((decision, dIdx) => (
                <div key={dIdx} className="relative pl-6 border-l border-gray-700/50 pb-8 last:pb-0 last:border-0">
                    {/* Timeline Dot */}
                    <div className="absolute -left-[5px] top-0 w-2.5 h-2.5 rounded-full bg-lime-500/50 border border-lime-400"></div>

                    {/* Header: Timestamp */}
                    <div className="text-gray-400 text-xs font-['DIN_Alternate',sans-serif] mb-4 flex items-center gap-2">
                        {(() => {
                            if (!decision.timestamp) return 'Unknown Time';
                            try {
                                // Backend sends UTC time like "2025-11-27 20:13:52"
                                // Replace space with T and append Z to ensure UTC parsing
                                const utcTime = decision.timestamp.replace(' ', 'T') + 'Z';
                                const date = new Date(utcTime);
                                return date.toLocaleString('zh-CN', {
                                    year: 'numeric',
                                    month: '2-digit',
                                    day: '2-digit',
                                    hour: '2-digit',
                                    minute: '2-digit',
                                    second: '2-digit',
                                    timeZoneName: 'short',
                                    hour12: false
                                });
                            } catch (e) {
                                return decision.timestamp + ' (UTC)';
                            }
                        })()}
                        {dIdx === 0 && <span className="bg-lime-500/20 text-lime-400 px-1.5 py-0.5 rounded text-[10px]">LATEST</span>}
                    </div>

                    {/* Analysis Summary */}
                    <div className="bg-[#1f2229] rounded-lg p-4 border border-gray-700/50 mb-4">
                        <h3 className="text-lime-400 mb-3 flex items-center gap-2 font-bold text-sm uppercase tracking-wider">
                            <Brain className="w-4 h-4" />
                            市场分析
                        </h3>
                        <p className="text-gray-300 leading-relaxed text-sm">
                            {decision.analysis_summary}
                        </p>
                    </div>

                    {/* Actions */}
                    <div className="space-y-4">
                        <h3 className="text-white font-bold flex items-center gap-2 text-sm mb-4">
                            <CheckCircle className="w-4 h-4 text-lime-400" />
                            执行动作 ({decision.actions.length})
                        </h3>

                        {decision.actions.length === 0 ? (
                            <div className="text-gray-500 text-sm italic pl-1">本次无交易操作 (观望)</div>
                        ) : (
                            decision.actions.map((action, idx) => (
                                <div key={idx} className="bg-[#1f2229] rounded-lg p-4 border border-gray-700/50 hover:border-lime-500/30 transition-all">
                                    {/* Action Header */}
                                    <div className="flex justify-between items-start mb-3 pb-3 border-b border-gray-700/30">
                                        <div className="flex items-center gap-2">
                                            <span className="text-lg font-bold text-white">{action.symbol}</span>
                                            <span className={`px-2 py-0.5 rounded text-xs font-bold uppercase ${action.action.includes('long') ? 'bg-lime-500/20 text-lime-400' :
                                                action.action.includes('short') ? 'bg-red-500/20 text-red-400' :
                                                    'bg-gray-500/20 text-gray-400'
                                                }`}>
                                                {action.action.replace('_', ' ')}
                                            </span>
                                            {action.leverage && action.leverage > 1 && (
                                                <span className="text-xs text-orange-400 border border-orange-400/30 px-1.5 py-0.5 rounded font-['DIN_Alternate',sans-serif]">
                                                    {action.leverage}x
                                                </span>
                                            )}
                                        </div>
                                        <div className="text-right">
                                            <div className="text-white font-['DIN_Alternate',sans-serif] text-base">
                                                {action.position_size_usd ? `$${action.position_size_usd.toLocaleString()}` : '-'}
                                            </div>
                                            <div className="text-[10px] text-gray-500 uppercase tracking-wider">Position Size</div>
                                        </div>
                                    </div>

                                    {/* Reason */}
                                    <div className="mb-3">
                                        <div className="text-[10px] text-gray-500 mb-1.5 uppercase tracking-wider pl-3">Strategy Logic</div>
                                        <div className="text-sm text-gray-300 bg-black/20 p-3 rounded border border-gray-700/30 leading-relaxed">
                                            {action.entry_reason}
                                        </div>
                                    </div>

                                    {/* Exit Plan */}
                                    <div className="grid grid-cols-2 gap-3 text-xs bg-black/10 p-3 rounded border border-gray-700/20">
                                        {action.exit_plan.take_profit && (
                                            <div className="flex items-center gap-2 text-lime-400/90">
                                                <ArrowRight className="w-3.5 h-3.5" />
                                                <span className="font-['DIN_Alternate',sans-serif]">TP: ${action.exit_plan.take_profit.toLocaleString()}</span>
                                            </div>
                                        )}
                                        {action.exit_plan.stop_loss && (
                                            <div className="flex items-center gap-2 text-red-400/90">
                                                <AlertTriangle className="w-3.5 h-3.5" />
                                                <span className="font-['DIN_Alternate',sans-serif]">SL: ${action.exit_plan.stop_loss.toLocaleString()}</span>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>
            ))}
        </div>
    );
}

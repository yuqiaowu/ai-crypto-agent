/// <reference types="vite/client" />
import { useState, useEffect } from 'react';
import { Brain, AlertTriangle, CheckCircle, ArrowRight } from 'lucide-react';

interface AgentAction {
    symbol: string;
    action: string;
    leverage: number;
    position_size_usd: number;
    entry_reason: string;
    exit_plan: {
        take_profit?: number;
        stop_loss?: number;
        invalidation?: string;
    };
}

interface AgentDecision {
    analysis_summary: string;
    actions: AgentAction[];
    timestamp?: string; // We might want to add this to the log generation if not present
}

export function AgentLogTab() {
    const [decision, setDecision] = useState<AgentDecision | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchLog = async () => {
            try {
                setLoading(true);
                let data: AgentDecision;

                if (import.meta.env.MODE === 'production') {
                    const response = await fetch('/data/agent_decision_log.json');
                    if (!response.ok) throw new Error('Failed to fetch agent log');
                    data = await response.json();
                } else {
                    const response = await fetch('http://localhost:5001/api/agent-decision');
                    if (!response.ok) throw new Error('Failed to fetch agent log');
                    data = await response.json();
                }

                setDecision(data);
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
    if (!decision) return <div className="text-gray-500 p-4">暂无决策记录</div>;

    return (
        <div className="space-y-6">
            {/* Analysis Summary */}
            <div className="bg-[#1f2229] rounded-lg p-4 border border-gray-700/50">
                <h3 className="text-lime-400 mb-2 flex items-center gap-2 font-bold">
                    <Brain className="w-5 h-5" />
                    市场分析
                </h3>
                <p className="text-gray-300 leading-relaxed text-sm">
                    {decision.analysis_summary}
                </p>
            </div>

            {/* Actions */}
            <div className="space-y-3">
                <h3 className="text-white font-bold flex items-center gap-2">
                    <CheckCircle className="w-4 h-4 text-lime-400" />
                    执行动作 ({decision.actions.length})
                </h3>

                {decision.actions.length === 0 ? (
                    <div className="text-gray-500 text-sm italic">本次无交易操作 (观望)</div>
                ) : (
                    decision.actions.map((action, idx) => (
                        <div key={idx} className="bg-[#1f2229] rounded-lg p-4 border border-gray-700/50 hover:border-lime-500/30 transition-all">
                            <div className="flex justify-between items-start mb-3">
                                <div className="flex items-center gap-2">
                                    <span className="text-lg font-bold text-white">{action.symbol}</span>
                                    <span className={`px-2 py-0.5 rounded text-xs font-bold ${action.action.includes('long') ? 'bg-lime-500/20 text-lime-400' :
                                        action.action.includes('short') ? 'bg-red-500/20 text-red-400' :
                                            'bg-gray-500/20 text-gray-400'
                                        }`}>
                                        {action.action.toUpperCase().replace('_', ' ')}
                                    </span>
                                    {action.leverage > 1 && (
                                        <span className="text-xs text-orange-400 border border-orange-400/30 px-1 rounded">
                                            {action.leverage}x
                                        </span>
                                    )}
                                </div>
                                <div className="text-right">
                                    <div className="text-white font-['DIN_Alternate',sans-serif]">${action.position_size_usd.toLocaleString()}</div>
                                    <div className="text-xs text-gray-500">仓位大小</div>
                                </div>
                            </div>

                            <div className="mb-3">
                                <div className="text-xs text-gray-500 mb-1">决策理由</div>
                                <div className="text-sm text-gray-300 bg-black/20 p-2 rounded border border-gray-700/30">
                                    {action.entry_reason}
                                </div>
                            </div>

                            <div className="grid grid-cols-2 gap-2 text-xs">
                                {action.exit_plan.take_profit && (
                                    <div className="flex items-center gap-1 text-lime-400/80">
                                        <ArrowRight className="w-3 h-3" /> TP: ${action.exit_plan.take_profit}
                                    </div>
                                )}
                                {action.exit_plan.stop_loss && (
                                    <div className="flex items-center gap-1 text-red-400/80">
                                        <AlertTriangle className="w-3 h-3" /> SL: ${action.exit_plan.stop_loss}
                                    </div>
                                )}
                            </div>
                        </div>
                    ))
                )}
            </div>
        </div>
    );
}

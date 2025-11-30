import pandas as pd
import numpy as np
from pathlib import Path

BACKTEST_DIR = Path("backtest")
OUT_DIR = Path("backtest")
OUT_DIR.mkdir(exist_ok=True)

# 策略文件映射
STRATEGY_FILES = {
    "custom_v2": "ETH_4h_custom_signal_v2_backtest.csv",
    "flowchart": "ETH_4h_flowchart_strategy_backtest.csv",
    "optimized": "ETH_4h_trend_filtered_backtest.csv",
    "regime": "ETH_4h_trend_C_regime_backtest.csv",
    "regime_tp": "ETH_4h_regime_takeprofit_backtest.csv",
    "official_v1": "ETH_4h_regime_official_v1_backtest.csv",
    "enhanced": "ETH_4h_trend_B_enhanced_backtest.csv",
    "pullback_add_vol": "ETH_4h_regime_pullback_add_vol_backtest.csv",
}

BARS_PER_DAY = 24 / 4  # 4h 一天 6 根
BARS_PER_YEAR = 365 * BARS_PER_DAY


def load_strategy_returns() -> tuple[pd.DataFrame, pd.Series]:
    """
    读取所有策略 backtest csv，返回：
    - returns_df: 每列一个策略的 4H 收益率
    - buy_hold_equity: 用第一个文件里的 buy_hold_equity 作为基准
    """
    returns = {}
    buy_hold_equity = None

    for name, filename in STRATEGY_FILES.items():
        path = BACKTEST_DIR / filename
        if not path.exists():
            print(f"⚠️  警告: {filename} 不存在，跳过该策略")
            continue
            
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)

        if buy_hold_equity is None:
            buy_hold_equity = (
                df.set_index("date")["buy_hold_equity"]
                if "buy_hold_equity" in df.columns
                else (1 + df["close"].pct_change().fillna(0)).cumprod().set_axis(df["date"].values)
            )

        # 从 strategy_equity 推算每根 bar 的收益率
        if "strategy_equity" not in df.columns:
            print(f"⚠️  警告: {filename} 中找不到 strategy_equity 列，跳过")
            continue

        equity = df.set_index("date")["strategy_equity"]
        ret = equity.pct_change().fillna(0.0)
        returns[name] = ret

    if not returns:
        raise ValueError("没有成功加载任何策略数据！")

    # 拼成一个 DataFrame，并对齐索引（交集）
    returns_df = pd.DataFrame(returns).dropna(how="any")
    buy_hold_equity = buy_hold_equity.reindex(returns_df.index, method="ffill")

    return returns_df, buy_hold_equity


def portfolio_metrics(portfolio_ret: pd.Series) -> dict:
    """计算组合的关键指标"""
    equity = (1 + portfolio_ret).cumprod()
    roll_max = equity.cummax()
    dd = equity / roll_max - 1.0
    max_dd = dd.min()

    mean_ret = portfolio_ret.mean() * BARS_PER_YEAR
    vol = portfolio_ret.std() * np.sqrt(BARS_PER_YEAR)
    sharpe = mean_ret / vol if vol > 0 else np.nan
    
    # 计算 Sortino Ratio (只考虑下行波动)
    downside_ret = portfolio_ret[portfolio_ret < 0]
    downside_vol = downside_ret.std() * np.sqrt(BARS_PER_YEAR) if len(downside_ret) > 0 else 0
    sortino = mean_ret / downside_vol if downside_vol > 0 else np.nan
    
    # 计算 Calmar Ratio (年化收益 / 最大回撤)
    calmar = mean_ret / abs(max_dd) if max_dd < 0 else np.nan

    return {
        "final_equity": float(equity.iloc[-1]),
        "total_return_pct": float(equity.iloc[-1] - 1.0) * 100,
        "annualized_return_pct": float(mean_ret) * 100,
        "max_drawdown_pct": float(max_dd) * 100,
        "volatility_pct": float(vol) * 100,
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "calmar": float(calmar),
    }


def build_portfolio(weights: pd.Series, returns_df: pd.DataFrame, name: str):
    """
    根据给定权重构建组合，保存到 csv，并打印关键指标。
    """
    weights = weights / weights.sum()
    portfolio_ret = (returns_df * weights).sum(axis=1)
    metrics = portfolio_metrics(portfolio_ret)

    print(f"\n{'='*60}")
    print(f"📊 {name} 组合结果")
    print(f"{'='*60}")
    print("\n权重分配：")
    for strategy, weight in weights.sort_values(ascending=False).items():
        print(f"  {strategy:20s}: {weight:>6.2%}")
    
    print(f"\n绩效指标：")
    print(f"  最终净值:     {metrics['final_equity']:>8.4f}")
    print(f"  总收益率:     {metrics['total_return_pct']:>7.2f}%")
    print(f"  年化收益率:   {metrics['annualized_return_pct']:>7.2f}%")
    print(f"  最大回撤:     {metrics['max_drawdown_pct']:>7.2f}%")
    print(f"  年化波动率:   {metrics['volatility_pct']:>7.2f}%")
    print(f"\n风险调整指标：")
    print(f"  Sharpe Ratio:  {metrics['sharpe']:>7.2f}")
    print(f"  Sortino Ratio: {metrics['sortino']:>7.2f}")
    print(f"  Calmar Ratio:  {metrics['calmar']:>7.2f}")

    equity = (1 + portfolio_ret).cumprod()
    out = pd.DataFrame(
        {
            "date": returns_df.index,
            "portfolio_equity": equity.values,
            "portfolio_ret": portfolio_ret.values,
        }
    )
    out_path = OUT_DIR / f"ETH_4h_portfolio_{name}.csv"
    out.to_csv(out_path, index=False)
    print(f"\n💾 组合净值已保存到: {out_path}")


def main():
    print("🚀 开始构建策略组合...\n")
    returns_df, buy_hold_equity = load_strategy_returns()
    
    print(f"✅ 成功加载 {len(returns_df.columns)} 个策略")
    print(f"📅 数据时间范围: {returns_df.index[0]} 至 {returns_df.index[-1]}")
    print(f"📊 总共 {len(returns_df)} 根K线\n")

    # ========== A: 等权重组合 ==========
    weights_A = pd.Series(1.0, index=returns_df.columns)
    build_portfolio(weights_A, returns_df, name="A_equal_weight")

    # ========== B: 风险平价 / 逆波动率权重 ==========
    vol = returns_df.std() * np.sqrt(BARS_PER_YEAR)  # 年化波动
    inv_vol = 1.0 / vol.replace(0, np.nan)
    weights_B = inv_vol / inv_vol.sum()
    build_portfolio(weights_B, returns_df, name="B_risk_parity")

    # ========== C: Sharpe 比例权重 ==========
    mean_ret = returns_df.mean() * BARS_PER_YEAR
    vol = returns_df.std() * np.sqrt(BARS_PER_YEAR)
    sharpe = mean_ret / vol.replace(0, np.nan)

    # 只对 Sharpe>0 的策略分配权重
    positive_sharpe = sharpe.clip(lower=0)
    if positive_sharpe.sum() <= 0:
        print("\n⚠️  所有 Sharpe <= 0，Sharpe 组合退化为风险平价权重。")
        weights_C = weights_B.copy()
    else:
        weights_C = positive_sharpe / positive_sharpe.sum()

    build_portfolio(weights_C, returns_df, name="C_sharpe_weighted")

    # ========== D: 最优化组合 (最大Sharpe) ==========
    # 使用简化的均值-方差优化
    try:
        from scipy.optimize import minimize
        
        mean_returns = returns_df.mean() * BARS_PER_YEAR
        cov_matrix = returns_df.cov() * BARS_PER_YEAR
        
        def neg_sharpe(weights):
            port_return = np.dot(weights, mean_returns)
            port_vol = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
            return -port_return / port_vol if port_vol > 0 else 0
        
        n_assets = len(returns_df.columns)
        constraints = {'type': 'eq', 'fun': lambda x: np.sum(x) - 1}
        bounds = tuple((0, 1) for _ in range(n_assets))
        initial_guess = np.array([1/n_assets] * n_assets)
        
        result = minimize(neg_sharpe, initial_guess, method='SLSQP', 
                         bounds=bounds, constraints=constraints)
        
        if result.success:
            weights_D = pd.Series(result.x, index=returns_df.columns)
            build_portfolio(weights_D, returns_df, name="D_max_sharpe")
        else:
            print("\n⚠️  最优化求解失败，跳过 Max Sharpe 组合")
    except ImportError:
        print("\n⚠️  scipy 未安装，跳过 Max Sharpe 组合（需要: pip install scipy）")

    # ========== 基准：Buy & Hold ==========
    bh_ret = buy_hold_equity.pct_change().fillna(0.0)
    bh_metrics = portfolio_metrics(bh_ret)
    
    print(f"\n{'='*60}")
    print(f"📈 Buy & Hold 基准")
    print(f"{'='*60}")
    print(f"\n绩效指标：")
    print(f"  最终净值:     {bh_metrics['final_equity']:>8.4f}")
    print(f"  总收益率:     {bh_metrics['total_return_pct']:>7.2f}%")
    print(f"  年化收益率:   {bh_metrics['annualized_return_pct']:>7.2f}%")
    print(f"  最大回撤:     {bh_metrics['max_drawdown_pct']:>7.2f}%")
    print(f"  年化波动率:   {bh_metrics['volatility_pct']:>7.2f}%")
    print(f"\n风险调整指标：")
    print(f"  Sharpe Ratio:  {bh_metrics['sharpe']:>7.2f}")
    print(f"  Sortino Ratio: {bh_metrics['sortino']:>7.2f}")
    print(f"  Calmar Ratio:  {bh_metrics['calmar']:>7.2f}")
    
    print(f"\n{'='*60}")
    print("✅ 所有组合构建完成！")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

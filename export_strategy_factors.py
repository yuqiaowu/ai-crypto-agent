import pandas as pd
import numpy as np
from pathlib import Path

BACKTEST_DIR = Path("backtest")
OUT_DIR = Path("qlib_data")
OUT_DIR.mkdir(exist_ok=True)

# 8个核心策略（按收益率排序）
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


def load_single_strategy(name: str, filename: str) -> pd.DataFrame:
    """加载单个策略并提取因子"""
    path = BACKTEST_DIR / filename
    if not path.exists():
        print(f"⚠️  警告: {path} 不存在，跳过策略 {name}")
        return pd.DataFrame()

    df = pd.read_csv(path)
    if "date" not in df.columns:
        raise ValueError(f"{filename} 中没有 'date' 列")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 计算收益率
    # 计算收益率
    # 优先从净值计算真实的策略收益率
    if "strategy_equity" in df.columns:
        ret = df["strategy_equity"].astype(float).pct_change().fillna(0.0)
    elif "ret" in df.columns:
        # 如果没有净值列，才使用 ret (注意：这可能是市场收益率，取决于源文件)
        # 最好是 position * ret，但这里先保持兼容
        if "position" in df.columns:
             ret = df["ret"] * df["position"].shift(1).fillna(0)
        else:
             ret = df["ret"].astype(float).fillna(0.0)
    else:
        raise ValueError(f"{filename} 既没有 'strategy_equity' 也没有 'ret'")

    # 策略净值
    if "strategy_equity" in df.columns:
        equity = df["strategy_equity"].astype(float)
    else:
        equity = (1 + ret).cumprod()

    # 持仓信号（如果有）
    position = df["position"].astype(float) if "position" in df.columns else pd.Series(1.0, index=df.index)

    # ========== 因子工程 ==========
    
    # 1. 滚动收益率（多个窗口）
    ret_5 = ret.rolling(window=5, min_periods=1).sum()    # 5根K累计收益
    ret_20 = ret.rolling(window=20, min_periods=1).sum()  # 20根K累计收益
    ret_60 = ret.rolling(window=60, min_periods=1).sum()  # 60根K累计收益
    
    # 2. 滚动波动率（风险因子）
    vol_10 = ret.rolling(window=10, min_periods=5).std()
    vol_30 = ret.rolling(window=30, min_periods=10).std()
    vol_60 = ret.rolling(window=60, min_periods=20).std()
    
    # 3. 夏普比率（滚动）
    sharpe_30 = ret.rolling(window=30, min_periods=10).mean() / vol_30
    sharpe_60 = ret.rolling(window=60, min_periods=20).mean() / vol_60
    
    # 4. 最大回撤（滚动）
    def rolling_max_dd(series, window):
        def max_dd(x):
            if len(x) < 2:
                return 0
            cummax = (1 + x).cumprod().cummax()
            dd = (1 + x).cumprod() / cummax - 1
            return dd.min()
        return series.rolling(window=window, min_periods=window//2).apply(max_dd, raw=False)
    
    max_dd_30 = rolling_max_dd(ret, 30)
    max_dd_60 = rolling_max_dd(ret, 60)
    
    # 5. 动量因子（收益率排名）
    momentum_rank = ret_20.rank(pct=True)  # 百分位排名
    
    # 6. 趋势强度（连续正/负收益天数）
    def trend_strength(series):
        sign = np.sign(series)
        # 计算连续相同符号的长度
        groups = (sign != sign.shift()).cumsum()
        return sign.groupby(groups).cumsum()
    
    trend_str = trend_strength(ret)
    
    # 7. 胜率（滚动）
    def rolling_win_rate(series, window):
        return (series > 0).rolling(window=window, min_periods=window//2).mean()
    
    win_rate_30 = rolling_win_rate(ret, 30)
    win_rate_60 = rolling_win_rate(ret, 60)

    # 构建输出DataFrame
    out = pd.DataFrame({
        "date": df["date"],
        
        # 基础因子
        f"{name}_ret": ret,
        f"{name}_equity": equity,
        f"{name}_position": position,
        
        # 收益率因子
        f"{name}_ret_5": ret_5,
        f"{name}_ret_20": ret_20,
        f"{name}_ret_60": ret_60,
        
        # 波动率因子
        f"{name}_vol_10": vol_10,
        f"{name}_vol_30": vol_30,
        f"{name}_vol_60": vol_60,
        
        # 风险调整收益因子
        f"{name}_sharpe_30": sharpe_30,
        f"{name}_sharpe_60": sharpe_60,
        
        # 回撤因子
        f"{name}_max_dd_30": max_dd_30,
        f"{name}_max_dd_60": max_dd_60,
        
        # 动量因子
        f"{name}_momentum_rank": momentum_rank,
        f"{name}_trend_strength": trend_str,
        
        # 胜率因子
        f"{name}_win_rate_30": win_rate_30,
        f"{name}_win_rate_60": win_rate_60,
    })
    
    return out


def build_strategy_factors() -> None:
    """构建多策略因子表"""
    print("🚀 开始构建策略因子表...\n")
    
    combined = None
    loaded_count = 0

    for name, filename in STRATEGY_FILES.items():
        print(f"📥 读取策略 {name}: {filename}")
        sdf = load_single_strategy(name, filename)
        if sdf.empty:
            continue

        loaded_count += 1
        if combined is None:
            combined = sdf
        else:
            # 按日期 outer merge，保证所有策略的时间轴统一
            combined = combined.merge(sdf, on="date", how="outer")

    if combined is None or combined.empty:
        print("\n❌ 没有成功加载任何策略，检查 backtest 文件夹")
        return

    combined = combined.sort_values("date").reset_index(drop=True)
    
    # 填充NaN（前向填充）
    combined = combined.fillna(method='ffill').fillna(0)

    # 保存完整因子表
    out_path = OUT_DIR / "ETH_4h_strategy_factors.csv"
    combined.to_csv(out_path, index=False)

    print(f"\n✅ 成功加载 {loaded_count} 个策略")
    print(f"📊 总共 {len(combined)} 行数据")
    print(f"📁 因子表已保存到: {out_path}")
    print(f"📈 总共 {len(combined.columns)-1} 个因子列\n")
    
    # 统计信息
    print("因子统计：")
    print(f"  - 基础因子 (ret, equity, position): {loaded_count * 3}")
    print(f"  - 收益率因子 (ret_5/20/60): {loaded_count * 3}")
    print(f"  - 波动率因子 (vol_10/30/60): {loaded_count * 3}")
    print(f"  - 风险调整因子 (sharpe_30/60): {loaded_count * 2}")
    print(f"  - 回撤因子 (max_dd_30/60): {loaded_count * 2}")
    print(f"  - 动量因子 (momentum_rank, trend_strength): {loaded_count * 2}")
    print(f"  - 胜率因子 (win_rate_30/60): {loaded_count * 2}")
    
    print("\n前5行预览：")
    print(combined.head())
    
    print("\n数据范围：")
    print(f"  起始日期: {combined['date'].min()}")
    print(f"  结束日期: {combined['date'].max()}")
    print(f"  时间跨度: {(combined['date'].max() - combined['date'].min()).days} 天")
    
    # 额外保存一个简化版（只包含收益率和持仓）
    simple_cols = ["date"] + [col for col in combined.columns if "_ret" in col or "_position" in col]
    simple_df = combined[simple_cols]
    simple_path = OUT_DIR / "ETH_4h_strategy_returns_simple.csv"
    simple_df.to_csv(simple_path, index=False)
    print(f"\n💡 简化版（仅收益率+持仓）已保存到: {simple_path}")


if __name__ == "__main__":
    build_strategy_factors()

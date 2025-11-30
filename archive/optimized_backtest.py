import pandas as pd
from pathlib import Path

SIGNAL_DIR = Path("signals")
OUT_DIR = Path("backtest")
OUT_DIR.mkdir(exist_ok=True)


def run_backtest():
    df = pd.read_csv(SIGNAL_DIR / "ETHUSDT_4h_signals.csv")

    # 确保按时间排序
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 4H 收益，用收盘价
    df["ret"] = df["close"].pct_change().fillna(0.0)

    positions = []
    current_pos = 0   # 0: 空仓, 1: 做多

    # 用来算胜率
    trades_pnl = []
    entry_equity = None

    # 先构建买入持仓 & 平仓逻辑
    for i, row in df.iterrows():
        trend_label = row["trend_label"]
        trend_score = row["trend_score"]
        rsi = row["rsi_14"]
        price_pos = row["price_position_20"]

        # 默认保持当前仓位
        pos = current_pos

        # ====== 平仓逻辑（先判断平仓） ======
        if current_pos == 1:
            if (
                trend_score < 0.3
                or trend_label != "up"
                or rsi > 80
            ):
                # 平仓
                pos = 0
                current_pos = 0

        # ====== 开仓逻辑（当前空仓才考虑买入） ======
        if current_pos == 0:
            if (
                trend_label == "up"
                and trend_score > 0.5
                and rsi < 70
                and price_pos > 0.3
            ):
                pos = 1
                current_pos = 1

        positions.append(pos)

    df["position"] = positions

    # 策略收益：用上一根 K 线的仓位乘当前收益
    df["strategy_ret"] = df["position"].shift(1).fillna(0) * df["ret"]

    # 累计收益
    df["buy_hold_equity"] = (1 + df["ret"]).cumprod()
    df["strategy_equity"] = (1 + df["strategy_ret"]).cumprod()

    # ========== 计算最大回撤 ==========
    roll_max = df["strategy_equity"].cummax()
    drawdown = df["strategy_equity"] / roll_max - 1.0
    max_drawdown = drawdown.min()

    # ========== 计算胜率 ==========
    # 根据 position 的变化识别每一笔交易的区间
    in_trade = False
    entry_equity = 1.0
    last_equity = 1.0
    equity_series = df["strategy_equity"].tolist()

    for i in range(len(df)):
        pos = df["position"].iloc[i]
        eq = equity_series[i]

        if not in_trade and pos == 1:
            # 新开仓 (Note: equity at this point reflects return from previous candle. 
            # But we enter at Open of T+1. The return we get is at T+1.
            # The equity curve updates at T.
            # Let's simplify: Entry equity is the equity at the START of the trade.
            # Since we use shift(1) for returns, the return at index i comes from position at i-1.
            # So if position changes 0->1 at index i, we start holding at i+1?
            # No, loop index i determines position[i].
            # strategy_ret[i] = position[i-1] * ret[i].
            # So if position[i-1] is 0 and position[i] is 1:
            # At i+1, strategy_ret[i+1] = position[i] * ret[i+1] = 1 * ret[i+1].
            # So the first return is at i+1.
            # The equity before the trade is equity[i].
            in_trade = True
            entry_equity = eq
        elif in_trade and pos == 0:
            # 平仓. The last return was at index i (from position[i-1]=1).
            # position[i]=0 means next return will be 0.
            # So the equity at index i is the final equity of the trade.
            pnl = (eq / entry_equity) - 1.0
            trades_pnl.append(pnl)
            in_trade = False

    if trades_pnl:
        wins = sum(1 for p in trades_pnl if p > 0)
        win_rate = wins / len(trades_pnl)
    else:
        win_rate = 0.0

    out_path = OUT_DIR / "ETH_4h_trend_filtered_backtest.csv"
    df.to_csv(out_path, index=False)

    print(f"Backtest saved -> {out_path}")
    print(f"Buy & Hold Final Equity:  {df['buy_hold_equity'].iloc[-1]:.4f}")
    print(f"Strategy Final Equity:    {df['strategy_equity'].iloc[-1]:.4f}")
    print(f"Strategy Max Drawdown:    {max_drawdown:.2%}")
    print(f"Trades: {len(trades_pnl)}, Win Rate: {win_rate:.2%}")


if __name__ == "__main__":
    run_backtest()

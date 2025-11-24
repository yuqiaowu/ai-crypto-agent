import pandas as pd
from pathlib import Path

SIGNAL_DIR = Path("signals")
OUT_DIR = Path("backtest")
OUT_DIR.mkdir(exist_ok=True)


def classify_regime(trend_score: float) -> str:
    if trend_score > 0.5:
        return "bull"
    elif trend_score > 0.3:
        return "range"
    else:
        return "bear"


def run_backtest():
    df = pd.read_csv(SIGNAL_DIR / "ETHUSDT_4h_signals.csv")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 4H 收益
    df["ret"] = df["close"].pct_change().fillna(0.0)

    # regime
    df["regime"] = df["trend_score"].apply(classify_regime)

    positions = []
    strategy_equity = 1.0
    buy_hold_equity = 1.0

    prev_pos = 0.0  # 上一根K线的仓位，用来算策略收益

    # 记录交易
    in_trade = False
    entry_equity = None
    entry_price = None
    highest_close = None
    trades_pnl = []

    equity_series = []

    for i, row in df.iterrows():
        price = row["close"]
        low_price = row["low"]
        rsi = row["rsi_14"]
        trend_score = row["trend_score"]
        trend_label = row["trend_label"]
        price_pos = row["price_position_20"]
        bb_pos = row.get("bb_pos_20", 0.5)
        macd_hist = row.get("macd_hist", 0.0)
        atr = row.get("atr_14", None)

        regime = row["regime"]

        pos = prev_pos  # 默认保持上一个仓位

        # ====== 更新最高价（用于追踪止盈） ======
        if in_trade:
            if highest_close is None:
                highest_close = price
            else:
                highest_close = max(highest_close, price)

        # ====== 平仓逻辑（先处理离场） ======
        if prev_pos > 0:
            bear_like = (regime == "bear") or (trend_score < 0.3) or (trend_label != "up")
            overbought = (rsi is not None) and (rsi > 80)

            hit_sl = False
            hit_trail = False

            # 初始止损：entry_price - 2*ATR
            if entry_price is not None and atr is not None:
                stop_loss_price = entry_price - 2.0 * atr
                if low_price <= stop_loss_price:
                    hit_sl = True

            # 追踪止盈：highest_close - 2*ATR
            if highest_close is not None and atr is not None:
                trail_price = highest_close - 2.0 * atr
                if low_price <= trail_price:
                    hit_trail = True

            if bear_like or overbought or hit_sl or hit_trail:
                # 平到 0 仓
                pos = 0.0

                if in_trade:
                    # 这里简单近似：用当根K线收盘价计算最终权益（如果是止损，实际应该更低，这里略微高估了）
                    # 但为了统一比较，暂且如此。
                    exit_equity = strategy_equity * (1 + prev_pos * row["ret"])
                    pnl = (exit_equity / entry_equity) - 1.0
                    trades_pnl.append(pnl)
                    in_trade = False
                    entry_price = None
                    entry_equity = None
                    highest_close = None

        # ====== 开仓 & 加仓逻辑（只在 bull regime） ======
        if pos == 0.0:
            # 开 0.5 仓（试探入场）
            if (
                regime == "bull"
                and trend_score > 0.5
                and rsi < 70
                and price_pos > 0.3
                and macd_hist > 0
                and bb_pos > 0.5
            ):
                pos = 0.5
                in_trade = True
                entry_price = price
                entry_equity = strategy_equity
                highest_close = price

        elif pos == 0.5:
            # 满足加仓条件 → 提到 1.0
            if (
                regime == "bull"
                and trend_score > 0.7
                and macd_hist > 0
                and price >= entry_price * 1.02
                and rsi < 75
            ):
                pos = 1.0
                # 加仓后，entry_equity 不变，整个持仓当作一笔交易看待

        # ====== 更新权益曲线 ======
        # 先更新 buy & hold
        buy_hold_equity *= (1 + row["ret"])

        # 策略：用上一根仓位乘当前收益
        strategy_equity *= (1 + prev_pos * row["ret"])

        equity_series.append(strategy_equity)
        positions.append(pos)
        prev_pos = pos

    # 如果最后还在持仓，补记录最后一笔交易
    if in_trade and entry_equity is not None:
        pnl = (strategy_equity / entry_equity) - 1.0
        trades_pnl.append(pnl)

    df["position"] = positions
    df["buy_hold_equity"] = (1 + df["ret"]).cumprod()
    df["strategy_equity"] = equity_series

    # 最大回撤
    roll_max = df["strategy_equity"].cummax()
    drawdown = df["strategy_equity"] / roll_max - 1.0
    max_drawdown = drawdown.min()

    # 胜率
    if trades_pnl:
        wins = sum(1 for p in trades_pnl if p > 0)
        win_rate = wins / len(trades_pnl)
    else:
        win_rate = 0.0

    out_path = OUT_DIR / "ETH_4h_trend_B_enhanced_backtest.csv"
    df.to_csv(out_path, index=False)

    print(f"Backtest saved -> {out_path}")
    print(f"Buy & Hold Final Equity:  {df['buy_hold_equity'].iloc[-1]:.4f}")
    print(f"Strategy Final Equity:    {df['strategy_equity'].iloc[-1]:.4f}")
    print(f"Strategy Max Drawdown:    {max_drawdown:.2%}")
    print(f"Trades: {len(trades_pnl)}, Win Rate: {win_rate:.2%}")


if __name__ == "__main__":
    run_backtest()

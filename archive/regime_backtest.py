import pandas as pd
from pathlib import Path

SIGNAL_DIR = Path("signals")
OUT_DIR = Path("backtest")
OUT_DIR.mkdir(exist_ok=True)


def classify_regime(trend_score: float) -> str:
    """
    根据 trend_score 划分市场状态：
    > 0.5  → bull
    0.3~0.5 → range
    <= 0.3 → bear
    """
    if trend_score > 0.5:
        return "bull"
    elif trend_score > 0.3:
        return "range"
    else:
        return "bear"


def run_backtest():
    df = pd.read_csv(SIGNAL_DIR / "ETHUSDT_4h_signals.csv")

    # 确保按时间排序
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 收益（4h close-to-close）
    df["ret"] = df["close"].pct_change().fillna(0.0)

    # 先生成 regime 列
    df["regime"] = df["trend_score"].apply(classify_regime)

    positions = []
    current_pos = 0        # 0: 空仓, 1: 做多
    in_trade = False
    entry_price = None
    stop_loss_price = None

    # 用来统计单笔交易盈亏
    trades_pnl = []
    equity_series = []

    # 策略权益从 1 开始
    strategy_equity = 1.0
    buy_hold_equity = 1.0

    prev_pos = 0  # 用于计算策略收益

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

        # ====== 默认保持当前仓位 ======
        pos = current_pos

        # ====== 先看是否需要因为熊市 / 趋势转弱而平仓 ======
        if current_pos == 1:
            bear_like = (regime == "bear") or (trend_score < 0.3) or (trend_label != "up")
            overbought = (rsi is not None) and (rsi > 80)

            # 动态止损（用 ATR * 2）
            hit_stop = False
            if entry_price is not None and atr is not None:
                sl = entry_price - 2.0 * atr
                # 如果这根K线最低价跌破止损价，认为止损触发
                if low_price <= sl:
                    hit_stop = True

            if bear_like or overbought or hit_stop:
                # 平仓
                pos = 0
                current_pos = 0

                # 记录这笔交易的 pnl：用当前 strategy_equity 与进场时的对比
                if in_trade:
                    exit_equity = strategy_equity
                    # 这里用 exit_equity / entry_equity 近似计算单笔盈亏
                    # 注意：strategy_equity 已经包含了当根K线的盈亏（如果 prev_pos=1）
                    # 但这里逻辑是：如果当根K线触发平仓，我们假设是以 Close 价平仓（除了止损）
                    # 如果是止损，实际平仓价应该是 sl。但这里为了简化，还是用 Close 计算了当根收益。
                    # 如果要更精确，应该在 hit_stop 时重新计算当根收益。
                    # 简化起见，保持原逻辑，仅统计次数。
                    pnl = (exit_equity / entry_equity) - 1.0
                    trades_pnl.append(pnl)
                    in_trade = False
                    entry_price = None
                    stop_loss_price = None

        # ====== 若当前空仓，再看能不能开多 ======
        if current_pos == 0:
            # 只允许在 bull regime 下开多
            if regime == "bull":
                # 趋势足够强 + 不是超买 + 价格不是极端低位 + 动能正向 + 位于布林中上部
                if (
                    trend_score > 0.5
                    and rsi < 70
                    and price_pos > 0.3
                    and macd_hist > 0
                    and bb_pos > 0.5
                ):
                    # 开多
                    pos = 1
                    current_pos = 1
                    in_trade = True
                    entry_price = price
                    entry_equity = strategy_equity
                    if atr is not None:
                        stop_loss_price = entry_price - 2.0 * atr
                    else:
                        stop_loss_price = None

        positions.append(pos)

        # ====== 更新权益曲线 ======
        # 先更新 buy & hold
        buy_hold_equity *= (1 + row["ret"])

        # 策略：用上一根的仓位乘当前收益
        strategy_equity *= (1 + prev_pos * row["ret"])
        prev_pos = pos

        equity_series.append(strategy_equity)

    # 如果最后还在持仓，补记录最后一笔交易
    if in_trade:
        exit_equity = strategy_equity
        pnl = (exit_equity / entry_equity) - 1.0
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

    out_path = OUT_DIR / "ETH_4h_trend_C_regime_backtest.csv"
    df.to_csv(out_path, index=False)

    print(f"Backtest saved -> {out_path}")
    print(f"Buy & Hold Final Equity:  {df['buy_hold_equity'].iloc[-1]:.4f}")
    print(f"Strategy Final Equity:    {df['strategy_equity'].iloc[-1]:.4f}")
    print(f"Strategy Max Drawdown:    {max_drawdown:.2%}")
    print(f"Trades: {len(trades_pnl)}, Win Rate: {win_rate:.2%}")


if __name__ == "__main__":
    run_backtest()

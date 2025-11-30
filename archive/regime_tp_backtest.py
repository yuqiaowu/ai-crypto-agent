import pandas as pd
from pathlib import Path

SIGNAL_DIR = Path("signals")
OUT_DIR = Path("backtest")
OUT_DIR.mkdir(exist_ok=True)


def classify_regime(trend_score: float) -> str:
    """当没有 regime 列时，根据 trend_score 粗略分类."""
    if trend_score > 0.5:
        return "bull"
    elif trend_score > 0.3:
        return "range"
    else:
        return "bear"


def run_backtest():
    # 用的是 4H 信号文件：你之前回测 C 的那个
    df = pd.read_csv(SIGNAL_DIR / "ETHUSDT_4h_signals.csv")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 收益率（4H 简单收益）
    df["ret"] = df["close"].pct_change().fillna(0.0)

    # 如果没有 regime 列，就用 trend_score 派生一个
    if "regime" not in df.columns:
        df["regime"] = df["trend_score"].apply(classify_regime)

    positions = []          # 每根K的仓位（0~1）
    strategy_equity = 1.0   # 策略净值
    buy_hold_equity = 1.0   # 买入持有净值
    equity_series = []

    prev_pos = 0.0

    # 交易跟踪（把一次完整进场→最终清仓算一笔交易）
    in_trade = False
    entry_equity = None     # 初次建仓时的策略净值
    entry_price = None      # 初次建仓时的价格（用于止损）
    trades_pnl = []

    # 分批止盈状态：0=未止盈；1=RSI>75 减过一次；2=RSI>80 再减一次；3=RSI>85 再减一次（底仓）
    tp_stage = 0

    for i, row in df.iterrows():
        price = row["close"]
        low_price = row["low"]

        rsi = row.get("rsi_14", None)
        trend_score = row.get("trend_score", 0.0)
        trend_label = row.get("trend_label", "neutral")
        price_pos = row.get("price_position_20", 0.5)
        macd_hist = row.get("macd_hist", 0.0)
        atr = row.get("atr_14", None)
        regime = row.get("regime", "range")

        pos = prev_pos  # 默认沿用上一根的仓位

        # ====== 1. 多头趋势判断（Regime 主过滤） ======
        uptrend = (
            (regime == "bull")
            and (trend_score > 0.5)
            and (trend_label == "up")
        )

        # ====== 2. 先处理“清仓”逻辑：趋势反转 / 止损 ======
        if prev_pos > 0.0:
            # 趋势变坏
            bear_like = (regime == "bear") or (trend_score < 0.3) or (trend_label != "up")

            # 超级超买时也可以视为准备撤退（但这里主要靠分批止盈+止损）
            over_overbought = (rsi is not None and rsi > 90)

            # ATR 止损：跌破 entry_price - 2*ATR
            hit_sl = False
            if entry_price is not None and atr is not None:
                stop_loss_price = entry_price - 2.0 * atr
                if low_price <= stop_loss_price:
                    hit_sl = True

            if bear_like or hit_sl or over_overbought:
                # 直接全部清仓
                pos = 0.0
                if in_trade:
                    exit_equity = strategy_equity * (1 + prev_pos * row["ret"])
                    pnl = (exit_equity / entry_equity) - 1.0
                    trades_pnl.append(pnl)
                    in_trade = False
                    entry_price = None
                    entry_equity = None
                    tp_stage = 0  # 重置止盈阶段

        # ====== 3. 建仓逻辑：在多头 Regime 下，全仓打入（进攻型） ======
        if pos == 0.0 and uptrend:
            # 一些基本过滤：避免在极高 RSI / 过高价位追
            ok_rsi = (rsi is not None and rsi < 75)
            ok_price_pos = (price_pos is not None and price_pos > 0.3)
            ok_macd = (macd_hist > 0)

            if ok_rsi and ok_price_pos and ok_macd:
                pos = 1.0                 # 进攻：初次建仓直接 100%
                in_trade = True
                entry_price = price       # 用初次价格做 ATR 止损参考
                entry_equity = strategy_equity
                tp_stage = 0              # 止盈阶段从 0 开始

        # ====== 4. 分批止盈逻辑（只减仓，不加仓；底仓保留 10%） ======
        if pos > 0.0:
            # 只有有 RSI 时才做分批止盈
            if rsi is not None:
                # 第一档：RSI > 75，减到 75% 仓位
                if rsi > 75 and tp_stage < 1:
                    new_pos = max(pos * 0.75, 0.75)  # 如果原来就是 1，则变 0.75
                    # 不允许减到 10% 以下，这里只阶段性减
                    pos = max(new_pos, 0.1)
                    tp_stage = 1

                # 第二档：RSI > 80，减到 40% 仓位
                if rsi > 80 and tp_stage < 2:
                    pos = max(0.40, 0.10)  # 至少 10%
                    tp_stage = 2

                # 第三档：RSI > 85，减到 10% 底仓
                if rsi > 85 and tp_stage < 3:
                    pos = 0.10
                    tp_stage = 3

                # 如果 RSI 回落比较多（比如 < 60），可以允许未来重新触发第一档
                # 但这里先简单一点：只要开始减过，就不反复加仓，只交给趋势+止损处理。
                # 想要更复杂逻辑可以以后再做。

        # ====== 5. 更新净值曲线 ======
        # 买入持有：每根K都 100% 暴露
        buy_hold_equity *= (1 + row["ret"])
        # 策略：按上一根的仓位暴露
        strategy_equity *= (1 + prev_pos * row["ret"])

        positions.append(pos)
        equity_series.append(strategy_equity)
        prev_pos = pos

    # ====== 6. 如果最后还在持仓，补记一笔交易 ======
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

    out_path = OUT_DIR / "ETH_4h_regime_takeprofit_backtest.csv"
    df.to_csv(out_path, index=False)

    print(f"Backtest saved -> {out_path}")
    print(f"Buy & Hold Final Equity:  {df['buy_hold_equity'].iloc[-1]:.4f}")
    print(f"Strategy Final Equity:    {df['strategy_equity'].iloc[-1]:.4f}")
    print(f"Strategy Max Drawdown:    {max_drawdown:.2%}")
    print(f"Trades: {len(trades_pnl)}, Win Rate: {win_rate:.2%}")


if __name__ == "__main__":
    run_backtest()

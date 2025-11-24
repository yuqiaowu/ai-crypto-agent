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


def calc_pos_bucket(atr, price):
    """
    根据 ATR% 决定初始仓位和加仓幅度.
    返回 (base_pos, add_pos)
    """
    if atr is None or price is None or price <= 0:
        return 0.3, 0.15  # fallback

    atr_pct = float(atr) / float(price)

    if atr_pct <= 0.015:        # 波动 ≤ 1.5%
        return 0.40, 0.20
    elif atr_pct <= 0.03:       # 1.5% ~ 3%
        return 0.30, 0.15
    else:                       # > 3%
        return 0.20, 0.10


def run_backtest():
    # 用你之前的 4H 信号文件
    df = pd.read_csv(SIGNAL_DIR / "ETHUSDT_4h_signals.csv")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 简单收益
    df["ret"] = df["close"].pct_change().fillna(0.0)

    # 如果没有 regime 列，就由 trend_score 推
    if "regime" not in df.columns:
        df["regime"] = df["trend_score"].apply(classify_regime)

    positions = []
    strategy_equity = 1.0
    buy_hold_equity = 1.0
    equity_series = []

    prev_pos = 0.0

    # 交易跟踪
    in_trade = False
    entry_equity = None
    entry_price = None
    trades_pnl = []
    num_adds = 0
    base_pos_for_trade = None
    add_pos_for_trade = None
    max_pos_for_trade = None

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

        pos = prev_pos  # 默认承接上一根仓位

        # --------- 多头趋势定义 ----------
        uptrend = (
            (regime == "bull") and
            (trend_score > 0.5) and
            (trend_label == "up")
        )

        # ============ 1. 先处理平仓逻辑（已有仓位） ============
        if prev_pos > 0.0:
            bear_like = (regime == "bear") or (trend_score < 0.3) or (trend_label != "up")
            overbought = (rsi is not None and rsi > 80)

            hit_sl = False
            if entry_price is not None and atr is not None:
                stop_loss_price = entry_price - 2.0 * atr
                if low_price <= stop_loss_price:
                    hit_sl = True

            if bear_like or overbought or hit_sl:
                # 清仓
                pos = 0.0
                if in_trade:
                    # 本根K的收益仍然用上一根的仓位 prev_pos 来计算
                    exit_equity = strategy_equity * (1 + prev_pos * row["ret"])
                    pnl = (exit_equity / entry_equity) - 1.0
                    trades_pnl.append(pnl)

                    in_trade = False
                    entry_equity = None
                    entry_price = None
                    num_adds = 0
                    base_pos_for_trade = None
                    add_pos_for_trade = None
                    max_pos_for_trade = None

        # ============ 2. 建仓逻辑：空仓 & 多头 ============
        if pos == 0.0 and uptrend:
            ok_rsi = (rsi is not None and rsi < 70)
            ok_price_pos = (price_pos is not None and price_pos > 0.3)
            ok_macd = (macd_hist > 0)

            if ok_rsi and ok_price_pos and ok_macd:
                base_pos, add_pos = calc_pos_bucket(atr, price)
                max_pos = min(1.0, base_pos + 3 * add_pos)  # B: 最多 4 档仓位

                pos = base_pos
                in_trade = True
                entry_price = price
                entry_equity = strategy_equity

                num_adds = 0
                base_pos_for_trade = base_pos
                add_pos_for_trade = add_pos
                max_pos_for_trade = max_pos

        # ============ 3. 回调加仓逻辑 ============
        if pos > 0.0 and in_trade and uptrend:
            if (add_pos_for_trade is not None) and (max_pos_for_trade is not None) and (num_adds < 3):
                # 回调区间：RSI 40~60，价格在布林中下部，MACD 仍然为正
                in_pullback_zone = (
                    (rsi is not None and 40 <= rsi <= 60) and
                    (price_pos is not None and price_pos < 0.7) and
                    (macd_hist > 0)
                )

                if in_pullback_zone and pos < max_pos_for_trade:
                    new_pos = min(pos + add_pos_for_trade, max_pos_for_trade)
                    if new_pos > pos:
                        # 更新加权成本价（用于 ATR 止损参考）
                        if entry_price is not None:
                            notional_before = entry_price * pos
                            notional_add = price * (new_pos - pos)
                            entry_price = (notional_before + notional_add) / new_pos

                        pos = new_pos
                        num_adds += 1

        # ============ 4. 更新净值曲线 ============
        buy_hold_equity *= (1 + row["ret"])
        strategy_equity *= (1 + prev_pos * row["ret"])

        positions.append(pos)
        equity_series.append(strategy_equity)
        prev_pos = pos

    # ============ 5. 收尾：如果最后仍持仓，记一笔交易 ============
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

    out_path = OUT_DIR / "ETH_4h_regime_official_v1_backtest.csv"
    df.to_csv(out_path, index=False)

    print(f"Backtest saved -> {out_path}")
    print(f"Buy & Hold Final Equity:  {df['buy_hold_equity'].iloc[-1]:.4f}")
    print(f"Strategy Final Equity:    {df['strategy_equity'].iloc[-1]:.4f}")
    print(f"Strategy Max Drawdown:    {max_drawdown:.2%}")
    print(f"Trades: {len(trades_pnl)}, Win Rate: {win_rate:.2%}")


if __name__ == "__main__":
    run_backtest()

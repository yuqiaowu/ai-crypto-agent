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


def calc_vol_bucket(atr, price):
    """
    根据 ATR% 决定仓位增量（低波加多，高波加少）
    返回 (entry_pos, add_pos)
    """
    if atr is None or price is None or price <= 0:
        return 0.2, 0.15  # 兜底

    atr_pct = float(atr) / float(price)

    if atr_pct <= 0.015:        # 波动 ≤ 1.5%
        return 0.40, 0.30       # 初始 40%，加仓 30%
    elif atr_pct <= 0.03:       # 1.5% ~ 3%
        return 0.25, 0.20
    else:                       # > 3%
        return 0.15, 0.10


def run_backtest():
    df = pd.read_csv(SIGNAL_DIR / "ETHUSDT_4h_signals.csv")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 收益率
    df["ret"] = df["close"].pct_change().fillna(0.0)

    # regime 如果不存在就从 trend_score 派生
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

    # 用于回调确认
    prev_rsi = None
    prev_price_pos = None
    prev_macd_hist = None
    prev_in_pullback_zone = False

    for i, row in df.iterrows():
        price = row["close"]
        low_price = row["low"]
        rsi_4h = row["rsi_14"]
        trend_score = row["trend_score"]
        trend_label = row["trend_label"]
        price_pos_4h = row["price_position_20"]
        macd_hist_4h = row.get("macd_hist", 0.0)
        atr = row.get("atr_14", None)
        regime = row["regime"]

        pos = prev_pos  # 默认延续上一根仓位

        # ---- uptrend 判断（多头主环境）----
        uptrend = (
            (regime == "bull") and
            (trend_score > 0.5) and
            (trend_label == "up")
        )

        # ========= 先处理平仓逻辑（沿用 C） =========
        if prev_pos > 0.0:
            bear_like = (regime == "bear") or (trend_score < 0.3) or (trend_label != "up")
            overbought = (rsi_4h is not None and rsi_4h > 80)

            hit_sl = False
            if entry_price is not None and atr is not None:
                stop_loss_price = entry_price - 2.0 * atr
                if low_price <= stop_loss_price:
                    hit_sl = True

            if bear_like or overbought or hit_sl:
                pos = 0.0
                if in_trade:
                    exit_equity = strategy_equity * (1 + prev_pos * row["ret"])
                    pnl = (exit_equity / entry_equity) - 1.0
                    trades_pnl.append(pnl)
                    in_trade = False
                    entry_price = None
                    entry_equity = None

        # ========= 建仓 & 加仓逻辑 =========

        # 当前 ATR 决定仓位增量
        base_pos_size, add_pos_size = calc_vol_bucket(atr, price)

        # --- 初次建仓：pos == 0 且满足 C 的多头条件 ---
        if pos == 0.0 and uptrend:
            ok_rsi_4h = (rsi_4h is not None and rsi_4h < 70)
            ok_price_pos = (price_pos_4h is not None and price_pos_4h > 0.3)
            ok_macd_4h = (macd_hist_4h > 0)

            if ok_rsi_4h and ok_price_pos and ok_macd_4h:
                pos = min(base_pos_size, 1.0)
                in_trade = True
                entry_price = price
                entry_equity = strategy_equity

        # --- 回调 + 恢复确认后加仓：pos > 0 且 未满仓 ---
        if pos > 0.0 and pos < 1.0 and uptrend:
            # 上一根是否处于“回调区”
            if prev_rsi is not None and prev_price_pos is not None and prev_macd_hist is not None:
                prev_in_pullback_zone = (
                    (45 <= prev_rsi <= 60) and
                    (prev_price_pos < 0.5) and
                    (prev_macd_hist > 0)
                )
            else:
                prev_in_pullback_zone = False

            # 当前这根是否属于“恢复确认”
            recover = False
            if (
                prev_in_pullback_zone
                and (rsi_4h is not None)
                and (price_pos_4h is not None)
                and (prev_macd_hist is not None)
            ):
                recover = (
                    (rsi_4h > 55) and
                    (price_pos_4h > 0.5) and
                    (macd_hist_4h >= prev_macd_hist)
                )

            if recover:
                # 根据波动加仓
                add_pos = add_pos_size
                new_pos = min(pos + add_pos, 1.0)

                if new_pos > pos:
                    # 更新加权平均入场价
                    if entry_price is not None:
                        notional_before = entry_price * pos
                        notional_add = price * (new_pos - pos)
                        entry_price = (notional_before + notional_add) / new_pos

                    pos = new_pos
                    in_trade = True
                    # entry_equity 仍然用首次建仓时的权益，整笔交易统一算一笔

        # ========= 更新权益曲线 =========
        buy_hold_equity *= (1 + row["ret"])
        strategy_equity *= (1 + prev_pos * row["ret"])

        positions.append(pos)
        equity_series.append(strategy_equity)
        prev_pos = pos

        # 保存当前值供下一根使用
        prev_rsi = rsi_4h
        prev_price_pos = price_pos_4h
        prev_macd_hist = macd_hist_4h

    # 收尾：如果最后还在持仓，记一笔交易
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

    out_path = OUT_DIR / "ETH_4h_regime_pullback_add_vol_backtest.csv"
    df.to_csv(out_path, index=False)

    print(f"Backtest saved -> {out_path}")
    print(f"Buy & Hold Final Equity:  {df['buy_hold_equity'].iloc[-1]:.4f}")
    print(f"Strategy Final Equity:    {df['strategy_equity'].iloc[-1]:.4f}")
    print(f"Strategy Max Drawdown:    {max_drawdown:.2%}")
    print(f"Trades: {len(trades_pnl)}, Win Rate: {win_rate:.2%}")


if __name__ == "__main__":
    run_backtest()

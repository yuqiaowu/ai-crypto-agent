import pandas as pd
from pathlib import Path

SIGNAL_DIR = Path("signals")
OUT_DIR = Path("backtest")
OUT_DIR.mkdir(exist_ok=True)

def run_backtest():
    # 使用 4H 信号文件
    df = pd.read_csv(SIGNAL_DIR / "ETHUSDT_4h_signals.csv")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 收益率
    df["ret"] = df["close"].pct_change().fillna(0.0)

    positions = []
    strategy_equity = 1.0
    buy_hold_equity = 1.0
    equity_series = []

    prev_pos = 0.0
    prev_rsi = None

    # 交易跟踪
    in_trade = False
    entry_equity = None
    entry_price = None
    entry_side = None  # "long" or "short"
    trades_pnl = []
    long_trades = 0
    short_trades = 0

    for i, row in df.iterrows():
        price = row["close"]
        high_price = row["high"]
        low_price = row["low"]

        rsi = row.get("rsi_14", None)
        trend_score = row.get("trend_score", 0.0)
        trend_label = row.get("trend_label", "neutral")
        macd_hist = row.get("macd_hist", 0.0)
        atr = row.get("atr_14", None)

        pos = prev_pos

        # =============================
        # 1. 平仓逻辑 (Exit Logic)
        # =============================
        if prev_pos != 0.0:
            if entry_side == "long":
                # Long Exit Conditions:
                # 1. RSI > 70
                # 2. MACD Dead Cross (macd_hist < 0)
                # 3. Stop Loss (Price < Entry - 2*ATR)
                
                cond_rsi_exit = (rsi is not None and rsi > 70)
                cond_macd_exit = (macd_hist < 0)
                
                hit_sl = False
                if entry_price is not None and atr is not None:
                    sl_price = entry_price - 2.0 * atr
                    if low_price <= sl_price:
                        hit_sl = True

                if cond_rsi_exit or cond_macd_exit or hit_sl:
                    pos = 0.0
                    if in_trade:
                        exit_equity = strategy_equity * (1 + prev_pos * row["ret"])
                        pnl = (exit_equity / entry_equity) - 1.0
                        trades_pnl.append(pnl)
                        long_trades += 1
                        in_trade = False
                        entry_price = None
                        entry_equity = None
                        entry_side = None

            elif entry_side == "short":
                # Short Exit Conditions:
                # 1. RSI < 30
                # 2. MACD Golden Cross (macd_hist > 0)
                # 3. Stop Loss (Price > Entry + 2*ATR)

                cond_rsi_exit = (rsi is not None and rsi < 30)
                cond_macd_exit = (macd_hist > 0)

                hit_sl = False
                if entry_price is not None and atr is not None:
                    sl_price = entry_price + 2.0 * atr
                    if high_price >= sl_price:
                        hit_sl = True

                if cond_rsi_exit or cond_macd_exit or hit_sl:
                    pos = 0.0
                    if in_trade:
                        exit_equity = strategy_equity * (1 + prev_pos * row["ret"])
                        pnl = (exit_equity / entry_equity) - 1.0
                        trades_pnl.append(pnl)
                        short_trades += 1
                        in_trade = False
                        entry_price = None
                        entry_equity = None
                        entry_side = None

        # =============================
        # 2. 开仓逻辑 (Entry Logic)
        # =============================
        if pos == 0.0:
            # ----- Long Entry -----
            # 1. Trend Up (trend_score > 0.5)
            # 2. RSI > 50
            # 3. RSI Rising (Current > Prev)
            # 4. MACD > 0 (macd_hist > 0)
            
            is_uptrend = (trend_score > 0.5)
            rsi_gt_50 = (rsi is not None and rsi > 50)
            rsi_rising = (rsi is not None and prev_rsi is not None and rsi > prev_rsi)
            macd_bullish = (macd_hist > 0)

            if is_uptrend and rsi_gt_50 and rsi_rising and macd_bullish:
                pos = 1.0
                in_trade = True
                entry_price = price
                entry_equity = strategy_equity
                entry_side = "long"

            # ----- Short Entry -----
            # 1. Trend Down (trend_score < 0.3) -- using 0.3 for stronger bear confirmation
            # 2. RSI < 50
            # 3. RSI Falling (Current < Prev)
            # 4. MACD < 0 (macd_hist < 0)
            
            elif (trend_score < 0.3) and (rsi is not None and rsi < 50) and \
                 (rsi is not None and prev_rsi is not None and rsi < prev_rsi) and \
                 (macd_hist < 0):
                pos = -1.0
                in_trade = True
                entry_price = price
                entry_equity = strategy_equity
                entry_side = "short"

        # =============================
        # 3. Update Equity
        # =============================
        buy_hold_equity *= (1 + row["ret"])
        strategy_equity *= (1 + prev_pos * row["ret"])

        positions.append(pos)
        equity_series.append(strategy_equity)
        
        prev_pos = pos
        prev_rsi = rsi

    # Final Trade
    if in_trade and entry_equity is not None:
        pnl = (strategy_equity / entry_equity) - 1.0
        trades_pnl.append(pnl)
        if entry_side == "long":
            long_trades += 1
        elif entry_side == "short":
            short_trades += 1

    df["position"] = positions
    df["buy_hold_equity"] = (1 + df["ret"]).cumprod()
    df["strategy_equity"] = equity_series

    # Metrics
    roll_max = df["strategy_equity"].cummax()
    drawdown = df["strategy_equity"] / roll_max - 1.0
    max_drawdown = drawdown.min()

    if trades_pnl:
        wins = sum(1 for p in trades_pnl if p > 0)
        win_rate = wins / len(trades_pnl)
    else:
        win_rate = 0.0

    out_path = OUT_DIR / "ETH_4h_flowchart_strategy_backtest.csv"
    df.to_csv(out_path, index=False)

    print(f"Backtest saved -> {out_path}")
    print(f"Buy & Hold Final Equity:  {df['buy_hold_equity'].iloc[-1]:.4f}")
    print(f"Strategy Final Equity:    {df['strategy_equity'].iloc[-1]:.4f}")
    print(f"Strategy Max Drawdown:    {max_drawdown:.2%}")
    print(f"Total Trades: {len(trades_pnl)}, Win Rate: {win_rate:.2%}")
    print(f"  Long trades:  {long_trades}")
    print(f"  Short trades: {short_trades}")

if __name__ == "__main__":
    run_backtest()

import pandas as pd
import numpy as np
from pathlib import Path

SIGNAL_DIR = Path("signals")
OUT_DIR = Path("backtest")
OUT_DIR.mkdir(exist_ok=True)

def calculate_max_drawdown(equity_curve):
    """
    Calculate Max Drawdown from equity curve.
    """
    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max
    return drawdown.min()

def run_backtest():
    df = pd.read_csv(SIGNAL_DIR / "ETHUSDT_4h_signals.csv")

    # Ensure sorted by date
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 4h returns (using close price)
    df["ret"] = df["close"].pct_change().fillna(0.0)

    # ========== Generate Position ==========
    position = []

    current_pos = 0  # 0: Empty, 1: Long
    for _, row in df.iterrows():
        trend_label = row["trend_label"]
        exhaustion_label = row["exhaustion_label"]

        # Exit condition: Trend down OR Overbought
        if trend_label == "down" or exhaustion_label == "overbought":
            current_pos = 0
        # Entry condition: Trend up AND Not Overbought
        elif trend_label == "up" and exhaustion_label != "overbought":
            current_pos = 1

        position.append(current_pos)

    df["position"] = position

    # Strategy Return: Position from previous candle * Current candle return
    # We shift position by 1 because the signal at T determines position for T+1
    df["strategy_ret"] = df["position"].shift(1).fillna(0) * df["ret"]

    # Cumulative Equity (starting from 1)
    df["buy_hold_equity"] = (1 + df["ret"]).cumprod()
    df["strategy_equity"] = (1 + df["strategy_ret"]).cumprod()

    # ========== Metrics ==========
    bh_dd = calculate_max_drawdown(df["buy_hold_equity"])
    strat_dd = calculate_max_drawdown(df["strategy_equity"])
    
    # Calculate Win Rate (Per Trade)
    # Identify trade blocks: A trade starts when position goes 0->1
    # We group consecutive 1s as a single trade.
    # Note: We only care about periods where we actually held the position (prev_pos == 1)
    
    # Mark where a trade starts (signal generated at T, position held at T+1)
    # df['position'] is the target position for the NEXT period? 
    # No, in the loop: current_pos is appended. This is the position we WANT to be in at the END of this candle?
    # Usually: Signal at Close[T] -> Enter at Open[T+1] -> Return is Ret[T+1].
    # The code `df["strategy_ret"] = df["position"].shift(1) * df["ret"]` matches this.
    # So `position` column represents the position we hold during the *next* candle? 
    # No, `position` at row T is the position determined by data at T.
    # `shift(1)` moves it to T+1. So at T+1, we hold `position[T]`.
    # So we are holding `position[T]` during candle T+1.
    
    # To identify trades, we look at `position` column.
    # A block of 1s in `position` means we signaled to hold for those candles.
    # But the return is realized in the *next* candles.
    
    # Let's define a trade as a contiguous block of 1s in the `position` column.
    df['trade_start'] = (df['position'] == 1) & (df['position'].shift(1) == 0)
    df['trade_group'] = df['trade_start'].cumsum()
    
    # Filter for rows where we actually have a return from the strategy (i.e., we held a position)
    # This corresponds to rows where position.shift(1) == 1
    active_holding_rows = df[df['position'].shift(1) == 1].copy()
    
    if not active_holding_rows.empty:
        # We need to map these rows back to their trade_group.
        # Since trade_group is defined on `position` (signal time), and we are looking at T+1 (return time),
        # the trade_group for return at T+1 should come from T?
        # Yes. `trade_group` at T is the trade ID for the signal.
        # So we should shift trade_group too?
        # Let's just use the shifted trade_group.
        active_holding_rows['trade_id'] = df['trade_group'].shift(1).loc[active_holding_rows.index]
        
        # Calculate compound return for each trade
        trade_rets = active_holding_rows.groupby('trade_id')['strategy_ret'].apply(lambda x: (1 + x).prod() - 1)
        
        win_rate = (trade_rets > 0).mean()
        num_trades = len(trade_rets)
        avg_trade_ret = trade_rets.mean()
    else:
        win_rate = 0.0
        num_trades = 0
        avg_trade_ret = 0.0

    out_path = OUT_DIR / "ETH_4h_simple_long_backtest.csv"
    df.to_csv(out_path, index=False)
    
    print(f"Backtest saved -> {out_path}")
    print("-" * 30)
    print(f"Buy & Hold Final Equity: {df['buy_hold_equity'].iloc[-1]:.4f}")
    print(f"Buy & Hold Max Drawdown: {bh_dd:.2%}")
    print("-" * 30)
    print(f"Strategy Final Equity:   {df['strategy_equity'].iloc[-1]:.4f}")
    print(f"Strategy Max Drawdown:   {strat_dd:.2%}")
    print(f"Strategy Win Rate:       {win_rate:.2%} ({num_trades} trades)")
    print(f"Avg Trade Return:        {avg_trade_ret:.2%}")
    print("-" * 30)

if __name__ == "__main__":
    run_backtest()

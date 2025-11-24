import pandas as pd
import numpy as np
from pathlib import Path

SIGNAL_DIR = Path("signals")
OUT_DIR = Path("backtest")
OUT_DIR.mkdir(exist_ok=True)


def compute_rsi(series: pd.Series, period: int) -> pd.Series:
    """采用 Wilder 平滑方法计算 RSI."""
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def add_bollinger_bands(df: pd.DataFrame, window: int = 20, num_std: float = 2.0) -> pd.DataFrame:
    """计算布林带及上下轨斜率."""
    rolling_mean = df["close"].rolling(window=window, min_periods=window).mean()
    rolling_std = df["close"].rolling(window=window, min_periods=window).std(ddof=0)
    df["bb_mid"] = rolling_mean
    df["bb_upper"] = rolling_mean + num_std * rolling_std
    df["bb_lower"] = rolling_mean - num_std * rolling_std
    
    # 计算布林带上下轨斜率（百分比变化）
    df["bb_lower_slope"] = df["bb_lower"].pct_change() * 100
    df["bb_upper_slope"] = df["bb_upper"].pct_change() * 100
    
    return df


def add_dmi_indicators(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """计算 DMI 指标：+DI、-DI、ADX."""
    high = df["high"]
    low = df["low"]
    close = df["close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr_components = pd.concat(
        [high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    )
    tr = tr_components.max(axis=1)
    tr_series = pd.Series(tr, index=df.index)
    atr = tr_series.ewm(alpha=1 / period, adjust=False).mean()

    plus_smoothed = pd.Series(plus_dm, index=df.index).ewm(alpha=1 / period, adjust=False).mean()
    minus_smoothed = pd.Series(minus_dm, index=df.index).ewm(alpha=1 / period, adjust=False).mean()
    atr_safe = atr.replace(0, np.nan)
    plus_di = 100 * plus_smoothed / atr_safe
    minus_di = 100 * minus_smoothed / atr_safe

    dm_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / dm_sum
    adx = dx.ewm(alpha=1 / period, adjust=False).mean()

    df["+di"] = plus_di
    df["-di"] = minus_di
    df["adx"] = adx
    return df


def add_price_percentile(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """计算收盘价在近 window 天内的百分位."""
    close = df["close"]
    percentile = close.rolling(window=window, min_periods=window).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1],
        raw=False,
    )
    df[f"price_percentile_{window}"] = percentile
    return df


def add_volume_indicators(df: pd.DataFrame, ma_window: int = 20) -> pd.DataFrame:
    """计算成交量的移动均值及占比."""
    ma_col = f"volume_ma_{ma_window}"
    df[ma_col] = df["volume"].rolling(window=ma_window, min_periods=ma_window).mean()
    df[f"volume_ratio_ma_{ma_window}"] = df["volume"] / df[ma_col]
    return df


def add_price_moving_averages(df: pd.DataFrame, windows: list) -> pd.DataFrame:
    """计算多条移动平均线."""
    for window in windows:
        df[f"ma_{window}"] = df["close"].rolling(window=window, min_periods=window).mean()
    return df


def compute_signal_stars(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算买卖星级信号:
    - Buy stars: RSI oversold + Low price & high vol + ADX down
    - Sell stars: RSI overbought + High price & high vol + ADX up
    """
    price_percentile = df.get("price_percentile_20")
    volume_ratio = df.get("volume_ratio_ma_20")

    # 低位放量 & 高位放量
    low_high_mask = ((price_percentile < 0.10) & (volume_ratio > 2.0)).fillna(False)
    high_high_mask = ((price_percentile > 0.90) & (volume_ratio > 2.0)).fillna(False)

    # RSI 超买超卖
    rsi_overbought = (df["rsi_14"] > 70).fillna(False)
    rsi_oversold = (df["rsi_14"] < 30).fillna(False)

    # ADX 趋势
    adx = df.get("adx")
    plus_di = df.get("+di")
    minus_di = df.get("-di")
    adx_threshold = 40
    adx_up = ((plus_di > minus_di) & (adx > adx_threshold)).fillna(False)
    adx_down = ((minus_di > plus_di) & (adx > adx_threshold)).fillna(False)

    # 星级计数
    buy_stars = rsi_oversold.astype(int) + low_high_mask.astype(int) + adx_down.astype(int)
    sell_stars = rsi_overbought.astype(int) + high_high_mask.astype(int) + adx_up.astype(int)

    df["buy_stars"] = buy_stars
    df["sell_stars"] = sell_stars

    return df


def calc_position_size(atr_pct: float) -> float:
    """
    根据波动率（ATR%）动态调整仓位:
    - ATR% < 2%: 100% 仓位（低波动）
    - ATR% 2-4%: 70% 仓位（中波动）
    - ATR% > 4%: 50% 仓位（高波动）
    """
    if atr_pct is None or np.isnan(atr_pct):
        return 0.7  # 默认中等仓位
    
    if atr_pct < 2.0:
        return 1.0
    elif atr_pct < 4.0:
        return 0.7
    else:
        return 0.5


def run_backtest():
    """
    优化版 Custom Signal V2:
    - Entry: 同V2（月度信号 + MA20站稳 + BB下轨平缓）
    - Position Sizing: 根据ATR%动态调整（50%-100%）
    - Stop Loss: Entry - 2.5*ATR
    - Partial Profit: 利润>20%时减仓30%，>40%时再减30%
    - Exit: 同V2（月度卖出信号 + BB上轨平缓 + MA5下穿MA20）
    """
    # 读取 4H 数据
    df = pd.read_csv(SIGNAL_DIR / "ETHUSDT_4h_signals.csv")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["ret"] = df["close"].pct_change().fillna(0.0)

    # 添加指标
    df = add_bollinger_bands(df)
    df["rsi_14"] = compute_rsi(df["close"], 14)
    df = add_dmi_indicators(df)
    df = add_price_percentile(df, window=20)
    df = add_volume_indicators(df, ma_window=20)
    ma_windows = [5, 20, 60]
    df = add_price_moving_averages(df, windows=ma_windows)

    # 计算信号星级
    df = compute_signal_stars(df)

    # 计算"站稳20日线"：连续3天收盘价 > MA20
    df["above_ma20"] = (df["close"] > df["ma_20"]).fillna(False)
    df["stood_above_ma20_3d"] = (
        df["above_ma20"] & 
        df["above_ma20"].shift(1).fillna(False) & 
        df["above_ma20"].shift(2).fillna(False)
    )

    # 计算5日线下穿20日线
    df["ma5_below_ma20"] = (df["ma_5"] < df["ma_20"]).fillna(False)
    df["ma5_cross_below_ma20"] = (
        df["ma5_below_ma20"] & 
        ~df["ma5_below_ma20"].shift(1).fillna(True)
    )

    positions = []
    strategy_equity = 1.0
    buy_hold_equity = 1.0
    equity_series = []

    prev_pos = 0.0

    # 交易跟踪
    in_trade = False
    entry_equity = None
    entry_price = None
    initial_position_size = None
    current_position_size = None
    trades_pnl = []
    profit_taken_20 = False  # 是否已在20%利润处减仓
    profit_taken_40 = False  # 是否已在40%利润处减仓

    # 4H数据，30天 = 30*24/4 = 180根K线
    lookback_window = 180

    for i, row in df.iterrows():
        price = row["close"]
        low_price = row["low"]

        buy_stars = row.get("buy_stars", 0)
        sell_stars = row.get("sell_stars", 0)
        stood_above_ma20_3d = row.get("stood_above_ma20_3d", False)
        bb_lower_slope = row.get("bb_lower_slope", None)
        bb_upper_slope = row.get("bb_upper_slope", None)
        ma5_cross_below_ma20 = row.get("ma5_cross_below_ma20", False)
        atr = row.get("atr_14", None)

        pos = prev_pos

        # 检查过去30天内是否有2星级以上信号
        start_idx = max(0, i - lookback_window)
        buy_signal_in_month = df.loc[start_idx:i, "buy_stars"].max() >= 2
        sell_signal_in_month = df.loc[start_idx:i, "sell_stars"].max() >= 2

        # =============================
        # 1. 平仓逻辑
        # =============================
        if prev_pos > 0.0:
            # 计算当前利润
            current_profit = (price / entry_price - 1.0) if entry_price else 0.0

            # 分批止盈逻辑
            if current_profit > 0.20 and not profit_taken_20 and current_position_size is not None:
                # 利润>20%，减仓30%
                reduction = current_position_size * 0.3
                current_position_size -= reduction
                pos = current_position_size
                profit_taken_20 = True
            
            elif current_profit > 0.40 and not profit_taken_40 and current_position_size is not None:
                # 利润>40%，再减仓30%
                reduction = current_position_size * 0.3
                current_position_size -= reduction
                pos = current_position_size
                profit_taken_40 = True

            # 止损逻辑
            hit_stop_loss = False
            if entry_price is not None and atr is not None:
                stop_loss_price = entry_price - 2.5 * atr
                if low_price <= stop_loss_price:
                    hit_stop_loss = True

            # 原始退出条件
            cond_sell_signal = sell_signal_in_month
            cond_bb_upper_flat = (bb_upper_slope is not None and bb_upper_slope <= 0.1)
            cond_ma_cross = ma5_cross_below_ma20

            # 完全平仓
            if hit_stop_loss or (cond_sell_signal and cond_bb_upper_flat and cond_ma_cross):
                pos = 0.0
                if in_trade:
                    exit_equity = strategy_equity * (1 + prev_pos * row["ret"])
                    pnl = (exit_equity / entry_equity) - 1.0
                    trades_pnl.append(pnl)

                    in_trade = False
                    entry_price = None
                    entry_equity = None
                    initial_position_size = None
                    current_position_size = None
                    profit_taken_20 = False
                    profit_taken_40 = False

        # =============================
        # 2. 开仓逻辑
        # =============================
        if pos == 0.0:
            cond_buy_signal = buy_signal_in_month
            cond_stood_ma20 = stood_above_ma20_3d
            cond_bb_lower_flat_up = (bb_lower_slope is not None and bb_lower_slope >= -0.1)

            if cond_buy_signal and cond_stood_ma20 and cond_bb_lower_flat_up:
                # 根据ATR%计算仓位
                atr_pct = (atr / price * 100) if atr and price else None
                position_size = calc_position_size(atr_pct)
                
                pos = position_size
                in_trade = True
                entry_price = price
                entry_equity = strategy_equity
                initial_position_size = position_size
                current_position_size = position_size
                profit_taken_20 = False
                profit_taken_40 = False

        # =============================
        # 3. 更新净值
        # =============================
        buy_hold_equity *= (1 + row["ret"])
        strategy_equity *= (1 + prev_pos * row["ret"])

        positions.append(pos)
        equity_series.append(strategy_equity)
        prev_pos = pos

    # 收尾
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

    out_path = OUT_DIR / "ETH_4h_custom_signal_v2_optimized_backtest.csv"
    df.to_csv(out_path, index=False)

    print(f"Backtest saved -> {out_path}")
    print(f"Buy & Hold Final Equity:  {df['buy_hold_equity'].iloc[-1]:.4f}")
    print(f"Strategy Final Equity:    {df['strategy_equity'].iloc[-1]:.4f}")
    print(f"Strategy Max Drawdown:    {max_drawdown:.2%}")
    print(f"Total Trades: {len(trades_pnl)}, Win Rate: {win_rate:.2%}")
    print(f"\nOptimization Features:")
    print(f"  - Dynamic position sizing (50%-100% based on ATR%)")
    print(f"  - Stop loss at Entry - 2.5*ATR")
    print(f"  - Partial profit taking at +20% and +40%")


if __name__ == "__main__":
    run_backtest()

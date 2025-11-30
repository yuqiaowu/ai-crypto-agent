import pandas as pd
import numpy as np
import os

def calculate_rsi(series, window=14):
    """Calculate Relative Strength Index (RSI)"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(series, fast=12, slow=26, signal=9):
    """Calculate MACD, Signal line, and Histogram"""
    exp1 = series.ewm(span=fast, adjust=False).mean()
    exp2 = series.ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histogram = macd - signal_line
    return macd, signal_line, histogram

def calculate_atr(df, window=14):
    """Calculate Average True Range (ATR)"""
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    atr = true_range.rolling(window=window).mean()
    return atr

def calculate_bollinger_bands(series, window=20, window_dev=2):
    """Calculate Bollinger Bands"""
    rolling_mean = series.rolling(window=window).mean()
    rolling_std = series.rolling(window=window).std()
    
    upper_band = rolling_mean + (rolling_std * window_dev)
    lower_band = rolling_mean - (rolling_std * window_dev)
    return upper_band, lower_band

def build_factors(df: pd.DataFrame):
    """
    Generates technical factors for the given DataFrame.
    """
    # Ensure date is datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Sort by date just in case
    df = df.sort_values('date').reset_index(drop=True)

    # ========== Trend / Momentum ==========
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    df["ma_cross"] = np.where(df["ma20"] > df["ma60"], 1, -1)
    df["momentum_12"] = df["close"].pct_change(12)
    
    # MACD
    df["macd"], df["macd_signal"], df["macd_hist"] = calculate_macd(df["close"])

    # Trend Strength: price slope (simple linear regression approximation via pct change)
    df["trend_strength"] = df["close"].pct_change(5)

    # ========== Volatility ==========
    # ATR
    df["atr_14"] = calculate_atr(df)

    # Bollinger Bands
    bb_upper, bb_lower = calculate_bollinger_bands(df["close"])
    df["bb_width_20"] = (bb_upper - bb_lower) / df["close"]
    # Avoid division by zero if bandwidth is 0 (highly unlikely but safe)
    denom = bb_upper - bb_lower
    df["bb_pos_20"] = np.where(denom == 0, 0.5, (df["close"] - bb_lower) / denom)

    # ========== Volume Structure ==========
    # Relative Volume
    df["rel_volume_20"] = df["volume"] / df["volume"].rolling(20).mean()

    # ========== Price Location ==========
    rolling_low = df["low"].rolling(20).min()
    rolling_high = df["high"].rolling(20).max()
    
    channel_range = rolling_high - rolling_low
    df["price_position_20"] = np.where(channel_range == 0, 0.5, (df["close"] - rolling_low) / channel_range)
    df["channel_range_20"] = np.where(df["close"] == 0, 0, channel_range / df["close"])
    
    # ========== Oscillators ==========
    # RSI
    df["rsi_14"] = calculate_rsi(df["close"])
    
    # Log Returns (often used in ML)
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))

    # Drop NaN values created by rolling windows (max window is 60)
    df = df.dropna()
    
    return df

def process_file(input_path, output_path):
    if not os.path.exists(input_path):
        print(f"File not found: {input_path}")
        return

    print(f"Processing {input_path}...")
    df = pd.read_csv(input_path)
    df_factors = build_factors(df)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df_factors.to_csv(output_path, index=False)
    print(f"Saved factors to {output_path} ({len(df_factors)} rows)")

def main():
    # Process 1H data
    process_file('csv_data/ETH_1h.csv', 'csv_data/ETH_1h_factors.csv')
    
    # Process 4H data
    process_file('csv_data/ETH_4h.csv', 'csv_data/ETH_4h_factors.csv')

if __name__ == "__main__":
    main()

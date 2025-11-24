"""
Multi-Coin Signal Generator
Generates technical indicators for BTC, ETH, BNB, DOGE, SOL
"""
import pandas as pd
import numpy as np
from pathlib import Path

COINS = ['BTC', 'ETH', 'BNB', 'DOGE', 'SOL']
CSV_DIR = Path("csv_data")
SIGNALS_DIR = Path("signals")
SIGNALS_DIR.mkdir(exist_ok=True)

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Calculate RSI using Wilder's smoothing"""
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add technical indicators to OHLCV data"""
    
    # Moving Averages
    df['ma_5'] = df['close'].rolling(window=5, min_periods=5).mean()
    df['ma_20'] = df['close'].rolling(window=20, min_periods=20).mean()
    df['ma_60'] = df['close'].rolling(window=60, min_periods=60).mean()
    
    # MA Cross
    df['ma_cross'] = (df['ma_5'] > df['ma_20']).astype(int)
    
    # Momentum
    df['momentum_12'] = df['close'].pct_change(periods=12)
    
    # MACD
    ema_12 = df['close'].ewm(span=12, adjust=False).mean()
    ema_26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema_12 - ema_26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']
    
    # ATR (Average True Range)
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr_14'] = tr.ewm(alpha=1/14, adjust=False).mean()
    
    # Bollinger Bands
    rolling_mean = df['close'].rolling(window=20, min_periods=20).mean()
    rolling_std = df['close'].rolling(window=20, min_periods=20).std(ddof=0)
    df['bb_mid'] = rolling_mean
    df['bb_upper'] = rolling_mean + 2 * rolling_std
    df['bb_lower'] = rolling_mean - 2 * rolling_std
    df['bb_width_20'] = (df['bb_upper'] - df['bb_lower']) / df['bb_mid']
    df['bb_pos_20'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
    
    # RSI
    df['rsi_14'] = compute_rsi(df['close'], 14)
    
    # Volume indicators
    df['volume_ma_20'] = df['volume'].rolling(window=20, min_periods=20).mean()
    df['rel_volume_20'] = df['volume'] / df['volume_ma_20']
    
    # Price position
    df['price_position_20'] = df['close'].rolling(window=20, min_periods=20).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    )
    
    # Returns
    df['log_return'] = np.log(df['close'] / df['close'].shift(1))
    df['ret'] = df['close'].pct_change()
    
    # Volatility
    df['volatility_20'] = df['ret'].rolling(window=20, min_periods=20).std()
    
    # Sentiment Factors (if available)
    if 'funding_rate' in df.columns:
        # Funding Rate Z-Score (20 periods)
        fr_mean = df['funding_rate'].rolling(window=20, min_periods=20).mean()
        fr_std = df['funding_rate'].rolling(window=20, min_periods=20).std()
        df['funding_rate_zscore'] = (df['funding_rate'] - fr_mean) / (fr_std + 1e-8)
        
    if 'open_interest' in df.columns:
        # OI Change
        df['oi_change'] = df['open_interest'].pct_change()
        # OI RSI (is money flowing in too fast?)
        df['oi_rsi'] = compute_rsi(df['open_interest'], 14)
    
    return df

def process_coin(coin: str):
    """Process a single coin"""
    print(f"\n📊 Processing {coin}...")
    
    input_path = CSV_DIR / f"{coin}_4h.csv"
    if not input_path.exists():
        print(f"   ⚠️ File not found: {input_path}")
        return
    
    # Load data
    df = pd.read_csv(input_path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    
    print(f"   Loaded {len(df)} rows from {df['date'].min()} to {df['date'].max()}")
    
    # Add indicators
    df = add_technical_indicators(df)
    
    # Add instrument column
    df['instrument'] = coin
    
    # Save
    output_path = SIGNALS_DIR / f"{coin}_4h_signals.csv"
    df.to_csv(output_path, index=False)
    print(f"   ✅ Saved to {output_path}")
    
    # Print summary
    print(f"   Features: {len(df.columns)} columns")
    print(f"   Sample RSI: {df['rsi_14'].iloc[-1]:.2f}")
    print(f"   Sample MACD: {df['macd_hist'].iloc[-1]:.4f}")

def main():
    print("🚀 Multi-Coin Signal Generator")
    print(f"Processing {len(COINS)} coins: {', '.join(COINS)}\n")
    
    for coin in COINS:
        process_coin(coin)
    
    print(f"\n✅ All done! Signals saved to {SIGNALS_DIR}/")

if __name__ == "__main__":
    main()

"""
Prepare Multi-Coin Data for Qlib
- Load signals for BTC, ETH, BNB, DOGE, SOL
- Combine into long format (instrument column)
- Calculate target: future 4H/24H return
- Save to Qlib-compatible CSV
"""
import pandas as pd
import numpy as np
from pathlib import Path

SIGNALS_DIR = Path("signals")
QLIB_DATA_DIR = Path("qlib_data")
QLIB_DATA_DIR.mkdir(exist_ok=True)

COINS = ['BTC', 'ETH', 'BNB', 'DOGE', 'SOL']
OUT_PATH = QLIB_DATA_DIR / "multi_coin_features.csv"

def prepare_qlib_data():
    print("🚀 Preparing Multi-Coin Data for Qlib...\n")
    
    all_dfs = []
    
    for coin in COINS:
        signal_path = SIGNALS_DIR / f"{coin}_4h_signals.csv"
        
        if not signal_path.exists():
            print(f"⚠️ Skipping {coin}: file not found")
            continue
        
        print(f"📥 Loading {coin}...")
        df = pd.read_csv(signal_path)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        # Add instrument column
        df['instrument'] = coin
        
        # Calculate future returns (targets)
        # Future 4H return (next candle)
        df['future_4h_ret'] = df['ret'].shift(-1)
        
        # Future 24H return (sum of next 6 candles)
        # Correct way: sum of next 6 individual returns
        future_rets = []
        for i in range(len(df)):
            if i + 6 <= len(df):
                future_rets.append(df['ret'].iloc[i+1:i+7].sum())
            else:
                future_rets.append(np.nan)
        df['future_24h_ret'] = future_rets
        
        # Select features for Qlib
        feature_cols = [
            'date', 'instrument',
            # Price & Volume
            'open', 'high', 'low', 'close', 'volume',
            # Returns
            'ret', 'log_return',
            # Moving Averages
            'ma_5', 'ma_20', 'ma_60', 'ma_cross',
            # Momentum
            'momentum_12', 'macd', 'macd_signal', 'macd_hist',
            # Volatility
            'atr_14', 'bb_width_20', 'bb_pos_20', 'volatility_20',
            # RSI
            'rsi_14',
            # Volume
            'volume_ma_20', 'rel_volume_20',
            # Price Position
            'price_position_20',
            # Sentiment
            'funding_rate', 'funding_rate_zscore',
            'open_interest', 'oi_change', 'oi_rsi',
            # Targets
            'future_4h_ret', 'future_24h_ret'
        ]
        
        # Filter to available columns
        available = [c for c in feature_cols if c in df.columns]
        df = df[available]
        
        # Fill NaN features PER COIN (safe)
        # Exclude targets from filling
        targets = ['future_4h_ret', 'future_24h_ret']
        features = [c for c in df.columns if c not in targets]
        df[features] = df[features].fillna(method='ffill').fillna(0)
        
        print(f"   {len(df)} rows, {len(available)} features")
        
        all_dfs.append(df)
    
    if not all_dfs:
        print("❌ No data loaded!")
        return
    
    # Combine all coins
    combined = pd.concat(all_dfs, ignore_index=True)
    combined = combined.rename(columns={'date': 'datetime'})
    
    # Convert to timezone-naive (Qlib doesn't handle timezones well)
    combined['datetime'] = pd.to_datetime(combined['datetime']).dt.tz_localize(None)
    
    combined = combined.sort_values(['datetime', 'instrument'])
    
    # Drop warmup period (first 60 rows per instrument where ma_60 is still 0)
    print(f"\n🔍 Filtering warmup period...")
    print(f"   Before: {len(combined)} rows")
    combined = combined[combined['ma_60'] > 0]
    print(f"   After: {len(combined)} rows (removed warmup period)")
    
    # Drop rows with NaN targets (CRITICAL: Do not fill them!)
    combined = combined.dropna(subset=['future_4h_ret', 'future_24h_ret'])
    
    # Save
    combined.to_csv(OUT_PATH, index=False)
    
    print(f"\n✅ Saved to {OUT_PATH}")
    print(f"   Total rows: {len(combined)}")
    print(f"   Coins: {combined['instrument'].unique().tolist()}")
    print(f"   Time range: {combined['datetime'].min()} to {combined['datetime'].max()}")
    print(f"   Features: {len(combined.columns)}")
    
    # Summary stats
    print(f"\n📊 Target Variable Stats:")
    print(f"   future_4h_ret mean: {combined['future_4h_ret'].mean():.6f}")
    print(f"   future_4h_ret std: {combined['future_4h_ret'].std():.6f}")
    print(f"   future_24h_ret mean: {combined['future_24h_ret'].mean():.6f}")
    print(f"   future_24h_ret std: {combined['future_24h_ret'].std():.6f}")
    
    print(f"\n📈 Sample by Coin:")
    for coin in COINS:
        count = (combined['instrument'] == coin).sum()
        if count > 0:
            print(f"   {coin}: {count} rows")

if __name__ == "__main__":
    prepare_qlib_data()

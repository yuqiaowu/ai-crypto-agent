import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
QLIB_DATA_DIR = BASE_DIR / "qlib_data"
SIGNALS_DIR = BASE_DIR / "signals"

STRATEGY_FACTORS_PATH = QLIB_DATA_DIR / "ETH_4h_strategy_factors.csv"
MARKET_FACTORS_PATH = QLIB_DATA_DIR / "eth_daily_market_factors.csv"
SIGNALS_PATH = SIGNALS_DIR / "ETHUSDT_4h_signals.csv"
OUT_PATH = QLIB_DATA_DIR / "strategy_pool_features_v2.csv"

# Only keep 3 diverse strategies
SELECTED_STRATEGIES = ['custom_v2', 'flowchart', 'regime']

def prepare_data():
    print("🚀 Starting Strategy Pool Data Preparation (V2 - Diverse Strategies)...")
    
    # 1. Load Strategy Factors (Wide Format)
    if not STRATEGY_FACTORS_PATH.exists():
        print(f"❌ File not found: {STRATEGY_FACTORS_PATH}")
        return
    
    print("📥 Loading strategy factors...")
    wide_df = pd.read_csv(STRATEGY_FACTORS_PATH)
    wide_df['date'] = pd.to_datetime(wide_df['date'])
    
    # 2. Load Market Signals (4H granularity, has technical indicators)
    print("📥 Loading market signals (4H)...")
    signals_df = pd.DataFrame()
    if SIGNALS_PATH.exists():
        signals_df = pd.read_csv(SIGNALS_PATH)
        signals_df['date'] = pd.to_datetime(signals_df['date'])
        
        # Select market regime features
        market_regime_cols = [
            'atr_14', 'bb_width_20', 'bb_pos_20',  # Volatility
            'adx', '+di', '-di',  # Trend
            'rsi_14', 'macd_hist', 'momentum_12',  # Momentum
            'rel_volume_20', 'price_position_20',  # Volume & Structure
        ]
        
        # Filter to available columns
        available_cols = [c for c in market_regime_cols if c in signals_df.columns]
        signals_df = signals_df[['date'] + available_cols]
        
        # Calculate derived features
        # Volatility Regime (based on BB width percentile)
        if 'bb_width_20' in signals_df.columns:
            signals_df['volatility_regime'] = pd.qcut(
                signals_df['bb_width_20'], 
                q=3, 
                labels=[0, 1, 2],  # 0=Low, 1=Medium, 2=High
                duplicates='drop'
            ).astype(float)
        
        # Trend Regime (based on ADX and DI)
        if 'adx' in signals_df.columns and '+di' in signals_df.columns:
            conditions = [
                (signals_df['adx'] > 25) & (signals_df['+di'] > signals_df['-di']),  # Strong Up
                (signals_df['adx'] <= 25) & (signals_df['+di'] > signals_df['-di']), # Weak Up
                (signals_df['adx'] <= 25) & (signals_df['+di'] <= signals_df['-di']), # Weak Down
                (signals_df['adx'] > 25) & (signals_df['+di'] <= signals_df['-di']),  # Strong Down
            ]
            choices = [2, 1, -1, -2]
            signals_df['trend_regime'] = np.select(conditions, choices, default=0)
        
        # ATR as percentage of price
        if 'atr_14' in signals_df.columns:
            # Need to merge with price data temporarily
            price_df = pd.read_csv(SIGNALS_PATH)[['date', 'close']]
            price_df['date'] = pd.to_datetime(price_df['date'])
            signals_df = signals_df.merge(price_df, on='date', how='left')
            signals_df['atr_pct'] = (signals_df['atr_14'] / signals_df['close'] * 100).fillna(0)
            signals_df = signals_df.drop(columns=['close'])
        
        print(f"   Loaded {len(available_cols)} market regime features")
    else:
        print("⚠️ Signals file not found, proceeding without 4H market features.")
    
    # 3. Load Daily Market Factors
    print("📥 Loading daily market factors...")
    daily_market_df = pd.DataFrame()
    if MARKET_FACTORS_PATH.exists():
        daily_market_df = pd.read_csv(MARKET_FACTORS_PATH)
        daily_market_df['datetime'] = pd.to_datetime(daily_market_df['datetime'])
        if daily_market_df['datetime'].dt.tz is not None:
            daily_market_df['datetime'] = daily_market_df['datetime'].dt.tz_localize(None)
        daily_market_df = daily_market_df.sort_values('datetime')
        
        # Select key daily factors
        daily_cols = [
            'funding_rate', 'funding_rate_zscore_60',
            'open_interest_usd_zscore_60', 'open_interest_usd_change_pct_3d',
            'volatility_daily', 'liq_imbalance', 'liq_total_usd_zscore_60',
            'ret_1d', 'ret_5d'
        ]
        available_daily = [c for c in daily_cols if c in daily_market_df.columns]
        daily_market_df = daily_market_df[['datetime'] + available_daily]
    else:
        print("⚠️ Daily market factors not found.")
    
    # 4. Convert to Long Format (Only selected strategies)
    print(f"📊 Converting to long format (keeping only {SELECTED_STRATEGIES})...")
    long_dfs = []
    
    for strat in SELECTED_STRATEGIES:
        # Check if strategy exists
        if f"{strat}_ret" not in wide_df.columns:
            print(f"   ⚠️ Strategy {strat} not found, skipping")
            continue
        
        # Extract columns for this strategy
        cols = [c for c in wide_df.columns if c.startswith(f"{strat}_")]
        strat_df = wide_df[['date'] + cols].copy()
        
        # Rename columns to generic names
        rename_map = {c: c.replace(f"{strat}_", "") for c in cols}
        strat_df = strat_df.rename(columns=rename_map)
        
        # Add instrument identifier
        strat_df['instrument'] = strat
        
        # Calculate Target: Future 24h Return (Sum of next 6 periods)
        strat_df['future_24h_ret'] = strat_df['ret'].rolling(window=6).sum().shift(-6)
        
        long_dfs.append(strat_df)
        
    if not long_dfs:
        print("❌ No strategies found!")
        return
        
    long_df = pd.concat(long_dfs, ignore_index=True)
    long_df = long_df.rename(columns={'date': 'datetime'})
    long_df = long_df.sort_values(['datetime', 'instrument'])
    
    print(f"   Long format rows: {len(long_df)} ({len(SELECTED_STRATEGIES)} strategies)")
    
    # 5. Merge with 4H Market Signals
    if not signals_df.empty:
        print("🔗 Merging 4H market signals...")
        long_df = long_df.merge(signals_df, left_on='datetime', right_on='date', how='left')
        long_df = long_df.drop(columns=['date'])
    
    # 6. Merge with Daily Market Factors
    if not daily_market_df.empty:
        print("🔗 Merging daily market factors...")
        long_df = long_df.sort_values('datetime')
        daily_market_df = daily_market_df.sort_values('datetime')
        
        merged = pd.merge_asof(
            long_df, 
            daily_market_df, 
            on='datetime', 
            direction='backward',
            tolerance=pd.Timedelta(days=2)
        )
        long_df = merged
    
    # 7. Fill NaNs
    long_df = long_df.fillna(method='ffill').fillna(0)
    
    # 8. Filter to active timestamps (at least 2 strategies with position > 0)
    print(f"\n🔍 Filtering to active timestamps...")
    print(f"   Before filtering: {len(long_df)} rows")
    
    timestamps = long_df['datetime'].unique()
    active_timestamps = []
    
    for ts in timestamps:
        subset = long_df[long_df['datetime'] == ts]
        active_count = (subset['position'] > 0).sum()
        if active_count >= 2:  # At least 2 strategies have positions
            active_timestamps.append(ts)
    
    long_df = long_df[long_df['datetime'].isin(active_timestamps)]
    print(f"   After filtering: {len(long_df)} rows ({len(active_timestamps)}/{len(timestamps)} timestamps kept, {len(active_timestamps)/len(timestamps)*100:.1f}%)")
    
    # 9. Filter out rows where target is NaN
    long_df = long_df.dropna(subset=['future_24h_ret'])
    
    # 9. Save
    long_df.to_csv(OUT_PATH, index=False)
    print(f"\n✅ Saved strategy pool features to: {OUT_PATH}")
    print(f"   Total rows: {len(long_df)}")
    print(f"   Strategies: {SELECTED_STRATEGIES}")
    print(f"   Total columns: {len(long_df.columns)}")
    
    # Print column summary
    print(f"\n📋 Column Categories:")
    strategy_cols = [c for c in long_df.columns if c in ['ret', 'equity', 'position', 'sharpe_30', 'vol_10', 'max_dd_30', 'ret_5', 'ret_20']]
    market_4h_cols = [c for c in long_df.columns if c in ['atr_14', 'bb_width_20', 'adx', 'rsi_14', 'volatility_regime', 'trend_regime', 'atr_pct']]
    market_daily_cols = [c for c in long_df.columns if c in ['funding_rate', 'open_interest_usd_zscore_60', 'liq_imbalance', 'ret_1d']]
    
    print(f"   Strategy Features: {len(strategy_cols)}")
    print(f"   Market 4H Features: {len(market_4h_cols)}")
    print(f"   Market Daily Features: {len(market_daily_cols)}")

if __name__ == "__main__":
    prepare_data()

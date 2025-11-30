import pandas as pd
import numpy as np
from pathlib import Path
import os

DATA_DIR = Path("csv_data")
OUT_DIR = Path("signals")

FILES = [
    ("ETH_1h_factors.csv", "ETHUSDT_1h"),
    ("ETH_4h_factors.csv", "ETHUSDT_4h"),
]

def compute_trend_score_vectorized(df):
    """
    Vectorized calculation of trend score.
    """
    score = np.zeros(len(df))
    
    # ma_cross: +0.4 / -0.4
    # Assuming ma_cross is 1 or -1. If it's boolean or other, adjust accordingly.
    # Based on previous script: np.where(df["ma20"] > df["ma60"], 1, -1)
    score += np.where(df["ma_cross"] == 1, 0.4, 0.0)
    score += np.where(df["ma_cross"] == -1, -0.4, 0.0)
    
    # momentum_12: >0.5% (+0.3), <-0.5% (-0.3)
    mom = df.get("momentum_12", pd.Series(np.zeros(len(df))))
    score += np.where(mom > 0.005, 0.3, 0.0)
    score += np.where(mom < -0.005, -0.3, 0.0)
    
    # macd_hist: >0 (+0.3), <0 (-0.3)
    hist = df.get("macd_hist", pd.Series(np.zeros(len(df))))
    # Handle NaNs in hist (though input shouldn't have them if cleaned, but safe to check)
    hist = hist.fillna(0)
    score += np.where(hist > 0, 0.3, 0.0)
    score += np.where(hist < 0, -0.3, 0.0)
    
    # Clip to [-1, 1]
    return np.clip(score, -1.0, 1.0)

def label_trend_vectorized(trend_scores, up_thresh=0.3, down_thresh=-0.3):
    """
    Vectorized trend labeling.
    """
    conditions = [
        trend_scores >= up_thresh,
        trend_scores <= down_thresh
    ]
    choices = ["up", "down"]
    return np.select(conditions, choices, default="sideways")

def label_volatility_vectorized(series):
    """
    Vectorized volatility labeling.
    """
    med = series.rolling(50).median()
    vol_score = series / med - 1.0
    
    # Handle NaNs in vol_score (start of rolling window)
    vol_score = vol_score.fillna(0)
    
    conditions = [
        vol_score > 0.3,
        vol_score < -0.3
    ]
    choices = ["high", "low"]
    labels = np.select(conditions, choices, default="normal")
    
    return vol_score, labels

def label_exhaustion_vectorized(rsi, bb_pos):
    """
    Vectorized exhaustion labeling.
    """
    rsi = rsi.fillna(50) # Default neutral
    bb_pos = bb_pos.fillna(0.5) # Default neutral
    
    # Oversold: RSI < 30 or BB% < 0.1
    # Overbought: RSI > 70 or BB% > 0.9
    
    is_oversold = (rsi < 30) | (bb_pos < 0.1)
    is_overbought = (rsi > 70) | (bb_pos > 0.9)
    
    conditions = [is_oversold, is_overbought]
    score_choices = [-1.0, 1.0]
    label_choices = ["oversold", "overbought"]
    
    scores = np.select(conditions, score_choices, default=0.0)
    labels = np.select(conditions, label_choices, default="neutral")
    
    return scores, labels

def label_volume_vectorized(rel_vol):
    """
    Vectorized volume labeling.
    """
    vol_score = rel_vol - 1.0
    vol_score = vol_score.fillna(0)
    
    conditions = [
        rel_vol > 1.5,
        rel_vol < 0.7
    ]
    choices = ["high", "low"]
    labels = np.select(conditions, choices, default="normal")
    
    return vol_score, labels

def build_signals(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure date is datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values("date").reset_index(drop=True)

    # 1) Trend
    df["trend_score"] = compute_trend_score_vectorized(df)
    df["trend_label"] = label_trend_vectorized(df["trend_score"])

    # 2) Volatility
    if "bb_width_20" in df.columns:
        vol_base = df["bb_width_20"]
    elif "atr_14" in df.columns:
        vol_base = df["atr_14"]
    else:
        vol_base = df["close"].pct_change().abs()

    df["vol_score"], df["vol_label"] = label_volatility_vectorized(vol_base)

    # 3) Exhaustion
    rsi = df.get("rsi_14", pd.Series(np.full(len(df), np.nan)))
    bb_pos = df.get("bb_pos_20", pd.Series(np.full(len(df), np.nan)))

    df["exhaustion_score"], df["exhaustion_label"] = label_exhaustion_vectorized(rsi, bb_pos)

    # 4) Volume
    if "rel_volume_20" in df.columns:
        df["volume_score"], df["volume_label"] = label_volume_vectorized(df["rel_volume_20"])
    else:
        df["volume_score"] = 0.0
        df["volume_label"] = "normal"

    return df

def main():
    # Create output directory
    os.makedirs(OUT_DIR, exist_ok=True)

    for file_name, symbol in FILES:
        in_path = DATA_DIR / file_name
        if not in_path.exists():
            print(f"File not found: {in_path}")
            continue
            
        print(f"Processing {in_path}...")
        df = pd.read_csv(in_path)

        df = build_signals(df)

        out_path = OUT_DIR / f"{symbol}_signals.csv"
        df.to_csv(out_path, index=False)
        print(f"Saved signals -> {out_path} ({len(df)} rows)")

if __name__ == "__main__":
    main()

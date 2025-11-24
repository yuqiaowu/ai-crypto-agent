"""
Split multi-coin CSV into separate files (Qlib standard format)
"""
import pandas as pd
from pathlib import Path

QLIB_DATA_DIR = Path("qlib_data")
INPUT_PATH = QLIB_DATA_DIR / "multi_coin_features.csv"
OUTPUT_DIR = QLIB_DATA_DIR / "instruments"
OUTPUT_DIR.mkdir(exist_ok=True)

def split_by_instrument():
    print("🔄 Splitting multi-coin CSV into separate instrument files...")
    
    df = pd.read_csv(INPUT_PATH)
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    instruments = df['instrument'].unique()
    
    for inst in instruments:
        inst_df = df[df['instrument'] == inst].copy()
        inst_df = inst_df.drop(columns=['instrument'])
        inst_df = inst_df.sort_values('datetime')
        
        output_path = OUTPUT_DIR / f"{inst}.csv"
        inst_df.to_csv(output_path, index=False)
        print(f"   ✅ {inst}: {len(inst_df)} rows → {output_path}")
    
    print(f"\n✅ Split complete! {len(instruments)} instruments")

if __name__ == "__main__":
    split_by_instrument()

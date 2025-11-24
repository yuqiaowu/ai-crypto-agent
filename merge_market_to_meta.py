# merge_market_to_meta.py
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
QLIB_DATA_DIR = BASE_DIR / "qlib_data"

META_FEATURES_PATH = QLIB_DATA_DIR / "meta_features_eth_4h.csv"
DAILY_MARKET_PATH = QLIB_DATA_DIR / "eth_daily_market_factors.csv"
OUT_PATH = QLIB_DATA_DIR / "meta_features_eth_4h_v2.csv"


def main():
    print("🚀 Starting Merge Process...")
    
    # 1. 加载 4H Meta Features
    if not META_FEATURES_PATH.exists():
        print(f"❌ Meta features file not found: {META_FEATURES_PATH}")
        return
        
    print("📥 Loading 4H meta features...")
    meta_df = pd.read_csv(META_FEATURES_PATH)
    meta_df["datetime"] = pd.to_datetime(meta_df["datetime"])
    # 移除时区信息，确保是 naive datetime
    if meta_df["datetime"].dt.tz is not None:
        meta_df["datetime"] = meta_df["datetime"].dt.tz_localize(None)
    meta_df = meta_df.sort_values("datetime")
    print(f"   - Rows: {len(meta_df)}")

    # 2. 加载 Daily Market Factors
    if not DAILY_MARKET_PATH.exists():
        print(f"❌ Daily market factors file not found: {DAILY_MARKET_PATH}")
        print("   Please run sync_market_factors.py first.")
        return
        
    print("📥 Loading daily market factors...")
    daily_df = pd.read_csv(DAILY_MARKET_PATH)
    daily_df["datetime"] = pd.to_datetime(daily_df["datetime"])
    # 移除时区信息
    if daily_df["datetime"].dt.tz is not None:
        daily_df["datetime"] = daily_df["datetime"].dt.tz_localize(None)
    daily_df = daily_df.sort_values("datetime")
    print(f"   - Rows: {len(daily_df)}")

    # 3. 对齐数据 (Daily -> 4H)
    print("🔄 Aligning daily data to 4H timeframe...")
    
    # 把日线转成 index
    daily_df = daily_df.set_index("datetime")
    
    # 关键步骤：使用 reindex + ffill 将日线数据广播到 4H
    # method='ffill' 意味着 4H K线会使用最近的一个日线数据（即当天的日线数据）
    # 注意：这可能引入未来函数（如果日线是收盘后才有的）。
    # 更严谨的做法是 shift(1)，即用昨天的日线数据预测今天。
    # 这里我们假设日线数据在当天 00:00 之后可用（对于前一天的统计），或者我们接受当天的实时数据。
    # 为了安全起见，我们通常 shift(1) 日线数据，确保只使用过去的信息。
    
    daily_shifted = daily_df.shift(1) # 使用昨天的数据
    
    # 对齐到 4H 时间轴
    aligned_daily = daily_shifted.reindex(meta_df["datetime"], method="ffill")
    aligned_daily.reset_index(drop=True, inplace=True)

    # 4. 合并数据
    print("🔗 Merging datasets...")
    
    # 识别新列（排除 meta_df 已有的列，如 datetime, instrument）
    base_cols = set(meta_df.columns)
    new_cols = [c for c in aligned_daily.columns if c not in base_cols]
    
    print(f"   - Adding {len(new_cols)} new market features")

    merged = pd.concat(
        [meta_df.reset_index(drop=True), aligned_daily[new_cols].reset_index(drop=True)],
        axis=1,
    )
    
    # 清理 NaN (前向填充 + 0填充)
    merged = merged.fillna(method='ffill').fillna(0)

    # 5. 保存结果
    merged.to_csv(OUT_PATH, index=False)
    print(f"\n✅ Saved merged meta features (v2) to: {OUT_PATH}")
    print("   Total rows:", len(merged))
    print("   Total cols:", len(merged.columns))
    print("\n📝 Next Steps:")
    print("   1. Update run_qlib_full.py to use: meta_features_eth_4h_v2.csv")
    print("   2. Re-run Qlib workflow")


if __name__ == "__main__":
    main()

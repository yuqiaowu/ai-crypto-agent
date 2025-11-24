import pandas as pd
from pathlib import Path

SIGNAL_DIR = Path("signals")

def main():
    # === 1. 读取 4H & 1H 信号 ===
    path_4h = SIGNAL_DIR / "ETHUSDT_4h_signals.csv"
    path_1h = SIGNAL_DIR / "ETHUSDT_1h_signals.csv"

    if not path_4h.exists() or not path_1h.exists():
        print("Error: Signal files not found.")
        return

    df_4h = pd.read_csv(path_4h)
    df_1h = pd.read_csv(path_1h)

    # 确保时间列是 datetime，按时间排序
    df_4h["date"] = pd.to_datetime(df_4h["date"])
    df_1h["date"] = pd.to_datetime(df_1h["date"])

    df_4h = df_4h.sort_values("date").reset_index(drop=True)
    df_1h = df_1h.sort_values("date").reset_index(drop=True)

    # === 2. 选择想要从 1H 拿来的字段 ===
    # 排除掉一些不需要重复合并的基础列（如 open, high, low, volume 等，除非你想比较）
    # 这里只保留 1H 的特有因子和信号
    one_hour_cols = [
        "date",              # 必须保留，用来对齐
        "close",             # 保留 close 可以用来校验对齐是否合理（比如 4H 的 close 应该接近 1H 的 close）
        "rsi_14",
        "macd_hist",
        "price_position_20",
        "trend_score",
        "trend_label",
        "vol_score",
        "vol_label",
        "exhaustion_score",
        "exhaustion_label",
        "volume_score",
        "volume_label",
    ]

    # 只保留存在的列
    one_hour_cols = [c for c in one_hour_cols if c in df_1h.columns]
    df_1h_subset = df_1h[one_hour_cols].copy()

    # 给 1H 特征加前缀
    rename_map = {col: f"h1_{col}" for col in df_1h_subset.columns if col != "date"}
    df_1h_subset = df_1h_subset.rename(columns=rename_map)

    # === 3. 使用 merge_asof 按时间对齐（向后对齐/backward） ===
    # direction='backward': 寻找 1H date <= 4H date 的最近一条
    # tolerance: 可选，限制最大时间差（例如 pd.Timedelta("1h")），防止匹配到太久以前的数据
    merged = pd.merge_asof(
        df_4h.sort_values("date"),
        df_1h_subset.sort_values("date"),
        on="date",
        direction="backward",
        tolerance=pd.Timedelta("1h") # 严格一点，只匹配最近 1 小时内的
    )

    # === 4. 检查合并质量 ===
    # 比如检查 h1_close 和 close 的差异，或者检查有多少 NaN
    print(f"Merged shape: {merged.shape}")
    nan_count = merged["h1_trend_score"].isna().sum()
    print(f"Rows with missing 1H data: {nan_count} (due to tolerance or missing history)")

    # === 5. 保存结果 ===
    out_path = SIGNAL_DIR / "ETHUSDT_4h_1h_merged.csv"
    merged.to_csv(out_path, index=False)
    print(f"Saved merged multi-TF signals -> {out_path}")

if __name__ == "__main__":
    main()

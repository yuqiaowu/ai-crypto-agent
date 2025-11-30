import pandas as pd
from pathlib import Path

# 设置路径
BASE_DIR = Path(__file__).resolve().parent  # 当前脚本所在目录
QLIB_DATA_DIR = BASE_DIR / "qlib_data"

FACTOR_CSV = QLIB_DATA_DIR / "ETH_4h_strategy_factors.csv"
RET_CSV = QLIB_DATA_DIR / "ETH_4h_strategy_returns_simple.csv"

OUT_FEATURES = QLIB_DATA_DIR / "meta_features_eth_4h.csv"
OUT_LABELS = QLIB_DATA_DIR / "meta_labels_eth_4h.csv"


def process_dataframe(df: pd.DataFrame, instrument_name: str = "ETH4H") -> pd.DataFrame:
    """
    通用处理函数：
    1. 统一时间列名为 datetime
    2. 添加 instrument 列
    3. 排序并重置索引
    4. 调整列顺序
    """
    # 1. 统一时间列名
    time_cols = ["date", "time", "timestamp"]
    for col in time_cols:
        if col in df.columns:
            df.rename(columns={col: "datetime"}, inplace=True)
            break
    
    if "datetime" not in df.columns:
        raise ValueError("❌ 数据中找不到时间列 (date/time/datetime)")

    # 确保是 datetime 类型
    df["datetime"] = pd.to_datetime(df["datetime"])

    # 2. 添加 instrument 列
    df["instrument"] = instrument_name

    # 3. 排序
    df = df.sort_values(["instrument", "datetime"]).reset_index(drop=True)

    # 4. 调整列顺序: instrument, datetime, ...others
    cols = ["instrument", "datetime"] + [c for c in df.columns if c not in ["instrument", "datetime"]]
    df = df[cols]

    return df


def prepare_features():
    print(f"📥 读取因子文件: {FACTOR_CSV}")
    if not FACTOR_CSV.exists():
        raise FileNotFoundError(f"❌ 找不到因子文件: {FACTOR_CSV}")

    df = pd.read_csv(FACTOR_CSV)
    
    # 处理数据
    df = process_dataframe(df)

    # 检查 NaN
    nan_count = df.isna().sum().sum()
    if nan_count > 0:
        print(f"⚠️  警告: 特征数据中包含 {nan_count} 个 NaN 值，正在填充为 0...")
        df = df.fillna(0)

    # 保存
    OUT_FEATURES.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_FEATURES, index=False)
    
    print(f"✅ 特征因子已保存: {OUT_FEATURES}")
    print(f"  - 形状: {df.shape}")
    print(f"  - 时间范围: {df['datetime'].min()} 至 {df['datetime'].max()}")
    print(f"  - 包含因子数: {len(df.columns) - 2}")  # 减去 instrument 和 datetime


def prepare_labels():
    print(f"\n📥 读取收益文件: {RET_CSV}")
    if not RET_CSV.exists():
        print(f"⚠️  没找到收益文件: {RET_CSV}，跳过标签生成。")
        return

    df = pd.read_csv(RET_CSV)
    
    # 处理数据
    df = process_dataframe(df)

    # 保存
    df.to_csv(OUT_LABELS, index=False)
    
    print(f"✅ 标签/收益数据已保存: {OUT_LABELS}")
    print(f"  - 形状: {df.shape}")
    print(f"  - 包含列数: {len(df.columns) - 2}")


def main():
    print("=== 🚀 准备 Qlib 元策略数据集 (ETH 4H) ===")
    print(f"工作目录: {QLIB_DATA_DIR}\n")
    
    try:
        prepare_features()
        prepare_labels()
        print("\n✨ 全部完成！")
        print("后续步骤：")
        print("1. 使用 Qlib 的 dump_bin 将 CSV 转换为 Qlib BIN 格式")
        print("2. 编写 Qlib 配置文件 (yaml) 进行训练")
        
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")


if __name__ == "__main__":
    main()

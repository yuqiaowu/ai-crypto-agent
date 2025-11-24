import pandas as pd
import numpy as np
from pathlib import Path
from lightgbm import LGBMRegressor
import matplotlib.pyplot as plt
import joblib

# 设置路径
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "qlib_data"

FEATURE_PATH = DATA_DIR / "meta_features_eth_4h.csv"
MODEL_PATH = DATA_DIR / "meta_lightgbm.pkl"


def load_dataset():
    print("📥 Loading dataset...")

    if not FEATURE_PATH.exists():
        raise FileNotFoundError("❌ 找不到特征文件，请先运行 prepare_qlib_data.py")

    # 直接读取特征文件，它已经包含了所有列（包括收益率）
    df = pd.read_csv(FEATURE_PATH)

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)

    # 🎯 定义预测目标 (Label)
    # 我们预测 "Custom Signal V2" 策略的未来收益
    target_col = "custom_v2_ret" 
    
    if target_col not in df.columns:
        print(f"⚠️  未找到 {target_col}，尝试查找其他收益列...")
        ret_cols = [c for c in df.columns if c.endswith("_ret")]
        if ret_cols:
            target_col = ret_cols[0]
            print(f"👉 使用 {target_col} 作为预测目标")
        else:
            raise ValueError("❌ 无法找到收益率列作为预测目标")

    print(f"🎯 预测目标: {target_col} (Next Bar Return)")
    
    # 构造 Label: 未来 1 根 K 线的收益
    df["label"] = df[target_col].shift(-1)
    
    # 移除最后一行（因为没有 label）
    df = df.dropna(subset=["label"])

    # 特征列：排除 instrument, datetime, label 以及所有的 _ret, _equity, _position 列（避免未来函数）
    # 注意：我们只使用 "过去" 的信息作为特征。
    # 排除所有包含 "ret", "equity", "position" 的列，除非它们是滚动指标（如 ret_5, ret_20 等，这些是过去发生的，可以作为特征）
    # 但是，ret_5 是 "过去5根K线的收益"，在T时刻是已知的，所以可以用。
    # 只有当期的 "ret" (单根K线收益) 是我们需要预测的目标（的滞后值）。
    
    # 严格来说，T时刻的 ret 是已知的。但是为了避免直接泄漏（比如 label = ret.shift(-1)），我们要小心。
    # 这里的特征工程是在 export_strategy_factors.py 里做的，ret_5 是 rolling sum。
    
    exclude_keywords = ["_equity", "_position", "instrument", "datetime", "label"]
    # 排除当期收益率列 (以 _ret 结尾，且不是 _ret_5, _ret_20 等)
    # 简单的做法：排除所有以 _ret 结尾的列，只保留 _ret_X
    
    feature_cols = []
    for c in df.columns:
        if c in exclude_keywords or c == target_col:
            continue
        if any(k in c for k in exclude_keywords):
            continue
        
        # 处理 _ret 列
        if c.endswith("_ret"):
            continue # 排除当期收益
            
        feature_cols.append(c)
    
    # 简单清洗：移除包含 infinite 的行
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    print(f"📊 Features: {len(feature_cols)} columns")
    print(f"📈 Samples: {len(df)} rows")

    return df, feature_cols


def train_model(df, feature_cols):
    X = df[feature_cols]
    y = df["label"]

    # 时间序列分割 (前 80% 训练，后 20% 测试)
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    
    # 记录测试集的时间范围
    test_start = df["datetime"].iloc[split_idx]
    test_end = df["datetime"].iloc[-1]
    print(f"📅 Test Period: {test_start} to {test_end}")

    print("🚀 Training LightGBM model...")

    model = LGBMRegressor(
        n_estimators=1000,
        learning_rate=0.005,
        max_depth=5,
        num_leaves=31,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        eval_metric="rmse",
        callbacks=[
            # lightgbm.early_stopping(stopping_rounds=50)
        ]
    )

    # 预测
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    # 评估 IC (Information Coefficient)
    train_ic = np.corrcoef(train_pred, y_train)[0, 1]
    test_ic = np.corrcoef(test_pred, y_test)[0, 1]
    
    # 评估 Rank IC
    test_rank_ic = pd.Series(test_pred).corr(pd.Series(y_test.values), method="spearman")

    print(f"\n📊 Model Performance:")
    print(f"  Train IC: {train_ic:.4f}")
    print(f"  Test IC:  {test_ic:.4f}")
    print(f"  Rank IC:  {test_rank_ic:.4f}")

    return model, X_test, y_test, test_pred


def plot_feature_importance(model, feature_cols):
    print("\n🎨 Plotting feature importance...")

    importance = model.feature_importances_
    # 获取前 20 个重要特征
    indices = np.argsort(importance)[-20:]
    
    plt.figure(figsize=(10, 8))
    plt.title("Top 20 Feature Importance (LightGBM)")
    plt.barh(range(len(indices)), importance[indices], align="center")
    plt.yticks(range(len(indices)), [feature_cols[i] for i in indices])
    plt.xlabel("Feature Importance")
    plt.tight_layout()
    
    # 保存图片
    plt.savefig(DATA_DIR / "feature_importance.png")
    print(f"🖼️  Feature importance saved to {DATA_DIR / 'feature_importance.png'}")


def backtest_strategy(y_test, test_pred, df_test):
    """简单的策略回测"""
    print("\n💰 Simple Backtest on Test Set:")
    
    # 策略：如果预测收益 > 0，做多；否则空仓
    signals = pd.Series(np.where(test_pred > 0, 1.0, 0.0), index=y_test.index)
    
    # 计算策略收益
    strategy_ret = signals * y_test
    
    # 计算累计净值
    cum_ret = (1 + strategy_ret).cumprod()
    benchmark_cum_ret = (1 + y_test).cumprod()
    
    final_ret = cum_ret.iloc[-1] - 1
    bench_ret = benchmark_cum_ret.iloc[-1] - 1
    
    print(f"  Strategy Return: {final_ret:.2%}")
    print(f"  Benchmark Return: {bench_ret:.2%}")
    
    # 简单的夏普
    sharpe = strategy_ret.mean() / strategy_ret.std() * np.sqrt(365 * 6)
    print(f"  Strategy Sharpe: {sharpe:.2f}")


def main():
    try:
        df, feature_cols = load_dataset()
        model, X_test, y_test, test_pred = train_model(df, feature_cols)

        # 保存模型
        joblib.dump(model, MODEL_PATH)
        print(f"\n💾 Model saved to: {MODEL_PATH}")

        # 特征重要性
        plot_feature_importance(model, feature_cols)
        
        # 简单回测
        split_idx = int(len(df) * 0.8)
        df_test = df.iloc[split_idx:]
        backtest_strategy(y_test, test_pred, df_test)

        print("\n🔥 Meta-strategy training completed!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

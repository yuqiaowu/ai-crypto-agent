"""
Qlib Multi-Coin Price Prediction
Predict future 4H/24H returns for BTC, ETH, BNB, DOGE, SOL
"""
import sys
import subprocess
import shutil
import pandas as pd
import qlib
from qlib.constant import REG_CN
from qlib.utils import init_instance_by_config
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
QLIB_DATA_DIR = BASE_DIR / "qlib_data"

def _needs_redump(bin_dir: Path) -> bool:
    """Detect legacy BIN data with tz-aware calendar that can trigger NaN->int errors."""
    if not bin_dir.exists():
        return True
    day_path = bin_dir / "calendars" / "day.txt"
    if not day_path.exists():
        return True
    try:
        first_line = day_path.read_text().splitlines()[0]
    except Exception:
        return True
    if "+00:00" in first_line or "NaT" in first_line:
        print(f"Calendar at {day_path} is tz-aware or invalid; rebuilding BIN data to keep timestamps clean.")
        shutil.rmtree(bin_dir)
        return True
    return False

def run_workflow():
    print("\n🚀 启动 Qlib 多币种预测工作流...\n")
    
    # ================= 配置区域 =================
    CSV_PATH = QLIB_DATA_DIR / "multi_coin_features.csv"
    BIN_DIR = QLIB_DATA_DIR / "bin_multi_coin"
    
    # 自动计算时间范围
    df_temp = pd.read_csv(CSV_PATH)
    START_TIME = df_temp['datetime'].min()
    END_TIME = df_temp['datetime'].max()
    
    # 训练/测试切分 (留最后 20% 做测试)
    dates = sorted(df_temp['datetime'].unique())
    split_idx = int(len(dates) * 0.8)
    TRAIN_PERIOD = (dates[0], dates[split_idx])
    TEST_PERIOD = (dates[split_idx+1], dates[-1])
    
    print(f"📅 训练集: {TRAIN_PERIOD}")
    print(f"📅 测试集: {TEST_PERIOD}")

    # ================= Qlib 初始化 =================
    provider_uri = str(BIN_DIR)
    qlib.init(provider_uri=provider_uri, region=REG_CN)

    # ================= 数据转换 (CSV -> Qlib BIN) =================
    if _needs_redump(BIN_DIR):
        print("🔄 转换 CSV 到 Qlib BIN 格式...")
        cmd = [
            sys.executable, "dump_bin.py",
            "--csv_path", str(CSV_PATH),
            "--qlib_dir", str(BIN_DIR),
            "--date_field_name", "datetime",
            "--symbol_field_name", "instrument",
        ]
        subprocess.check_call(cmd)
    else:
        print("✅ Qlib BIN 数据已存在，跳过转换。")

    # ================= 定义特征与标签 =================
    # Predict future 24H return
    label_expr = "$future_24h_ret"
    
    # Use simpler, more robust features (avoid division by zero issues)
    feature_cols = [
        # Price momentum (safe)
        "ret", "momentum_12",
        # MACD (safe)
        "macd_hist",
        # Volatility (safe - already calculated)
        "atr_14", "bb_width_20",
        # RSI (safe)
        "rsi_14",
        # Volume (safe)
        "rel_volume_20",
        # Price position (safe)
        "price_position_20",
        # Sentiment (New!)
        "funding_rate", "funding_rate_zscore",
        "oi_change", "oi_rsi",
    ]
    
    feature_exprs = [f"${col}" for col in feature_cols]
    
    # ================= 市场定义 =================
    market = "all"  # All coins
    
    # ================= DataHandler 配置 =================
    data_handler_config = {
        "class": "DataHandlerLP",
        "module_path": "qlib.data.dataset.handler",
        "kwargs": {
            "start_time": TRAIN_PERIOD[0],
            "end_time": TEST_PERIOD[1],
            "instruments": market,
            "infer_processors": [
                {"class": "RobustZScoreNorm", "kwargs": {"fields_group": "feature", "clip_outlier": True, "fit_start_time": TRAIN_PERIOD[0], "fit_end_time": TRAIN_PERIOD[1]}},
                {"class": "Fillna", "kwargs": {"fields_group": "feature"}},
            ],
            "learn_processors": [
                {"class": "DropnaLabel"},
                {"class": "CSRankNorm", "kwargs": {"fields_group": "label"}},
            ],
            "data_loader": {
                "class": "QlibDataLoader",
                "kwargs": {
                    "config": {
                        "feature": feature_exprs,
                        "label": [label_expr],
                    },
                },
            },
        },
    }
    
    # ================= 模型配置 (LightGBM) =================
    model_config = {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": {
            "loss": "mse",
            "colsample_bytree": 0.8,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "lambda_l1": 10,
            "lambda_l2": 10,
            "max_depth": 6,
            "num_leaves": 64,
            "num_threads": 8,
            "n_estimators": 200,
            "early_stopping_rounds": 50,
            "verbose": -1,
        },
    }
    
    # ================= Dataset 配置 =================
    dataset_config = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": data_handler_config,
            "segments": {
                "train": TRAIN_PERIOD,
                "valid": TRAIN_PERIOD,
                "test": TEST_PERIOD,
            },
        },
    }
    
    # ================= 完整配置 =================
    config = {
        "task": {
            "model": model_config,
            "dataset": dataset_config,
            "record": [
                {
                    "class": "SignalRecord",
                    "module_path": "qlib.workflow.record_temp",
                    "kwargs": {},
                },
            ],
        },
    }
    
    # ================= 运行工作流 =================
    print("\n🏋️ 开始训练模型 (Multi-Coin Price Prediction)...")
    
    with R.start(experiment_name="multi_coin_prediction"):
        # 初始化 dataset
        dataset = init_instance_by_config(config["task"]["dataset"])
        
        # 初始化 model
        model = init_instance_by_config(config["task"]["model"])
        
        # 训练
        model.fit(dataset)
        
        # 预测
        print("🔮 生成预测...")
        pred = model.predict(dataset)
        
        # 保存预测结果
        pred_path = QLIB_DATA_DIR / "multi_coin_pred.csv"
        if isinstance(pred, pd.Series):
            pred_df = pred.reset_index()
            pred_df.columns = ['datetime', 'instrument', 'score']
        else:
            pred_df = pred
        pred_df.to_csv(pred_path, index=False)
        print(f"💾 预测结果已保存: {pred_path}")
        
        # 记录结果
        # R.log_metrics(l2=model.score(dataset))  # LGBModel doesn't have score method
        
        # 保存模型 (Qlib Artifact)
        R.save_objects(model=model)
        
        # Explicitly save to local file for easy inference
        import pickle
        model_path = QLIB_DATA_DIR / "model_latest.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)
        print(f"💾 Model explicitly saved to: {model_path}")
        
        # Feature Importance
        try:
            # LightGBM Booster feature importance
            importance = model.model.feature_importance(importance_type='gain')
            feature_names = model.model.feature_name()
            fi_df = pd.DataFrame({'feature': feature_names, 'importance': importance})
            fi_df = fi_df.sort_values('importance', ascending=False)
            print("\n🌟 Feature Importance (Gain):")
            print(fi_df.head(15).to_string(index=False))
        except Exception as e:
            print(f"⚠️ Could not print feature importance: {e}")
        
        print("\n📊 实验结果已记录到 Qlib Recorder")
        
        # 简单评估
        print("\n📈 预测完成！查看结果:")
        print(f"   预测文件: {pred_path}")
        print(f"   预测行数: {len(pred_df)}")

if __name__ == "__main__":
    run_workflow()

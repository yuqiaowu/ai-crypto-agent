import sys
import shutil
import os
import pandas as pd
import subprocess
from pathlib import Path

if os.getenv("USE_LOCAL_QLIB", "1") == "1":
    sys.path.insert(0, str(Path(__file__).resolve().parent / "qlib"))

import qlib
from qlib.constant import REG_CN
from qlib.utils import init_instance_by_config
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, SigAnaRecord

# 路径设置
BASE_DIR = Path(__file__).resolve().parent
QLIB_DATA_DIR = BASE_DIR / "qlib_data"
# 1. 转换数据 (CSV -> Qlib BIN)
CSV_PATH = BASE_DIR / "qlib_data" / "meta_features_eth_4h_v2.csv"  # 使用 v2 版本
BIN_DIR = BASE_DIR / "qlib_data" / "bin"

def run_workflow():
    """运行 Qlib 工作流"""
    print("\n🚀 启动 Qlib 工作流...")
    
    # ================= 配置区域 =================
    CSV_PATH = QLIB_DATA_DIR / "multi_coin_features.csv"  # Multi-coin dataset
    BIN_DIR = QLIB_DATA_DIR / "bin_multi_coin_v2"
    
    # 自动计算时间范围
    df_temp = pd.read_csv(CSV_PATH)
    START_TIME = df_temp['datetime'].min()
    END_TIME = df_temp['datetime'].max()
    
    # 训练/验证/测试切分：70% 训练，10% 验证，20% 测试
    dates = sorted(df_temp['datetime'].unique())
    n = len(dates)
    train_end = max(0, int(n * 0.7) - 1)
    valid_end = max(train_end + 1, int(n * 0.9) - 1)
    TRAIN_PERIOD = (dates[0], dates[train_end])
    VALID_PERIOD = (dates[train_end + 1], dates[valid_end])
    TEST_PERIOD = (dates[valid_end + 1], dates[-1])
    
    print(f"📅 训练集: {TRAIN_PERIOD}")
    print(f"📅 验证集: {VALID_PERIOD}")
    print(f"📅 测试集: {TEST_PERIOD}")

    # ================= Qlib 初始化 =================
    provider_uri = str(BIN_DIR)
    qlib.init(provider_uri=provider_uri, region=REG_CN)

    # ================= 数据转换 (CSV -> Qlib BIN) =================
    if not BIN_DIR.exists():
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
    label_expr = "$future_24h_ret"
    
    feature_cols = [
        # Price momentum
        "ret", "momentum_12",
        # MACD
        "macd_hist",
        # Volatility
        "atr_14", "bb_width_20",
        # RSI
        "rsi_14",
        # Volume
        "rel_volume_20",
        # Price position
        "price_position_20",
    ]
    
    feature_exprs = [f"${col}" for col in feature_cols]
    
    # ================= 模型训练配置 =================
    market = "all"
    
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

    config = {
        "task": {
            "model": {
                "class": "LGBModel",
                "module_path": "qlib.contrib.model.gbdt",
                "kwargs": {
                    "loss": "mse",
                    "n_estimators": 200,
                    "learning_rate": 0.02,
                    "max_depth": 5,
                    "num_leaves": 15,
                    "colsample_bytree": 0.6,
                    "subsample": 0.8,
                    "subsample_freq": 1,
                    "lambda_l1": 0.1,
                    "lambda_l2": 0.1,
                    "min_child_samples": 20,
                    "num_threads": 20,
                    "verbose": -1,
                    "early_stopping_rounds": 1000,
                },
            },
            "dataset": {
                "class": "DatasetH",
                "module_path": "qlib.data.dataset",
                "kwargs": {
                    "handler": data_handler_config,
                    "segments": {
                        "train": TRAIN_PERIOD,
                        "valid": VALID_PERIOD,
                        "test": TEST_PERIOD,
                    },
                },
            },
            "record": [
                {
                    "class": "SignalRecord",
                    "module_path": "qlib.workflow.record_temp",
                    "kwargs": {"model": "<MODEL>", "dataset": "<DATASET>"},
                },
                {
                    "class": "SigAnaRecord",
                    "module_path": "qlib.workflow.record_temp",
                    "kwargs": {"ana_long_short": False, "ann_scaler": 252*6},
                },
            ],
        },
    }

    # 3. 运行实验
    with R.start(experiment_name="multi_coin_prediction"):
        print("🏋️ 开始训练模型 (Strategy Ranking)...")
        model = init_instance_by_config(config["task"]["model"])
        dataset = init_instance_by_config(config["task"]["dataset"])
        
        model.fit(dataset)
        
        print("🔮 生成预测...")
        pred = model.predict(dataset)
        if isinstance(pred, pd.Series):
            pred = pred.to_frame("score")
        
        # 保存预测结果
        pred_path = QLIB_DATA_DIR / "multi_coin_pred.csv"
        pred.to_csv(pred_path)
        print(f"💾 预测结果已保存: {pred_path}")
        
        # 记录 Metrics 与完整 Recorder 工件
        recorder = R.get_recorder()
        sig_rec = SignalRecord(recorder=recorder, model=model, dataset=dataset)
        sig_rec.generate()
        sar = SigAnaRecord(recorder=recorder, ana_long_short=True, ann_scaler=252*6)
        sar.generate()
        
        print("\n📊 实验结果已记录到 Qlib Recorder")
        
        # 打印 IC
        metrics = recorder.list_metrics()
        print("\n📈 核心指标:")
        for key in ["IC", "ICIR", "Rank IC", "Rank ICIR"]:
            if key in metrics:
                print(f"   {key}: {metrics[key]:.4f}")
        for key, value in metrics.items():
            if key not in {"IC", "ICIR", "Rank IC", "Rank ICIR"} and isinstance(value, (int, float)):
                print(f"   {key}: {value:.4f}")


if __name__ == "__main__":
    run_workflow()

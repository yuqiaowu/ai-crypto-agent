"""
Daily Trading Cycle Orchestrator
Runs the full pipeline: Data -> Signals -> Qlib -> News -> Agent -> Decision
"""
import os
import sys
import time
import subprocess
from datetime import datetime
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "trading_cycle.log"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {message}"
    print(msg)
    with open(LOG_FILE, "a") as f:
        f.write(msg + "\n")

def run_script(script_name, description):
    log(f"🚀 Starting: {description} ({script_name})...")
    start_time = time.time()
    
    try:
        # Run script as a subprocess to ensure clean state
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=BASE_DIR,
            capture_output=True,
            text=True
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            log(f"✅ Completed: {description} in {duration:.2f}s")
            # Optional: Log stdout if needed, or just keep it clean
            # log(f"Output:\n{result.stdout}")
            return True
        else:
            log(f"❌ Failed: {description}")
            log(f"Error Output:\n{result.stderr}")
            return False
            
    except Exception as e:
        log(f"❌ Exception running {script_name}: {e}")
        return False

def main():
    log("="*50)
    log("🤖 Starting New Trading Cycle")
    log("="*50)
    
    # 1. Fetch Market Data (OKX OHLCV + Sentiment)
    if not run_script("fetch_okx_data.py", "Fetch Market Data"):
        log("⛔ Stopping cycle due to data fetch failure.")
        return

    # 2. Generate Technical Signals
    if not run_script("generate_multi_coin_signals.py", "Generate Signals"):
        log("⛔ Stopping cycle due to signal generation failure.")
        return

    # 3. Prepare Data for Qlib (Format & Clean)
    if not run_script("prepare_multi_coin_qlib.py", "Prepare Qlib Data"):
        log("⛔ Stopping cycle due to Qlib preparation failure.")
        return

    # 4. Run Qlib Inference (Predict Scores)
    # Note: We don't need to rebuild BIN every time if we use CSV mode or update incrementally,
    # but for simplicity and correctness, we might need to ensure Qlib sees new data.
    # If prepare_multi_coin_qlib.py updates the CSV that Qlib reads, we are good.
    # However, Qlib usually needs `dump_bin` if using BIN format.
    # Let's check if inference uses CSV or BIN. It uses BIN.
    # So we MUST update BIN data.
    
    # 3.5 Update Qlib BIN Data
    log("🔄 Updating Qlib BIN Data...")
    
    # 3.5 Update Qlib BIN Data
    log("🔄 Updating Qlib BIN Data...")
    
    dump_bin_script = BASE_DIR / "dump_bin.py"
    
    if not dump_bin_script.exists():
        log(f"❌ dump_bin.py not found at {dump_bin_script}")
        # Try to find it in qlib package as fallback? No, let's just fail if missing.
        return

    bin_cmd = [
        sys.executable, str(dump_bin_script),
        "--csv_path", str(BASE_DIR / "qlib_data/multi_coin_features.csv"),
        "--qlib_dir", str(BASE_DIR / "qlib_data/bin_multi_coin"),
        "--symbol_field_name", "instrument",
        "--date_field_name", "datetime",
        "--include_fields", "open,high,low,close,volume,funding_rate,oi_change,funding_rate_zscore,oi_rsi,rsi_14,macd_hist,atr_14,bb_width_20,momentum_12,ret,future_4h_ret,future_24h_ret"
    ]
    
    try:
        subprocess.run(bin_cmd, check=True, capture_output=True)
        log("✅ Qlib BIN Data Updated")
    except subprocess.CalledProcessError as e:
        log(f"❌ Failed to update Qlib BIN: {e}")
        # If we can't update data, inference will use old data. 
        # We should probably stop, but for resilience we can continue with a warning.
        # However, for a trading bot, stale data is dangerous.
        return


    if not run_script("inference_qlib_model.py", "Qlib Inference"):
        log("⛔ Stopping cycle due to inference failure.")
        return

    # 5. Fetch News & On-Chain Data
    if not run_script("fetch_onchain_and_news.py", "Fetch News & On-Chain"):
        log("⚠️ News fetch failed, continuing with limited context...")
        # Don't stop, just continue

    # 6. Run Agent Decision
    if not run_script("DeepSeek_Agent.py", "Agent Decision"):
        log("❌ Agent failed to generate decision.")
        return

    # 7. Execute Trades (Mock)
    if not run_script("mock_trade_executor.py", "Execute Trades (Mock)"):
        log("❌ Trade execution failed.")
        return

    log("="*50)
    log("🎉 Trading Cycle Completed Successfully")
    log("="*50)

if __name__ == "__main__":
    main()

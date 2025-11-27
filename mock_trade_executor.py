"""
Mock Trade Executor for Dolores Agent

- Reads:
    - portfolio_state.json   (Current Portfolio)
    - qlib_data/deepseek_payload.json  (Current Market Prices)
    - agent_decision_log.json (Agent Decisions)

- Applies:
    - open_long / open_short
    - close_position
    - adjust_sl
    - hold

- Writes:
    - Updated portfolio_state.json
    - Appends to trade_log.csv
"""

import json
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
QLIB_DATA_DIR = BASE_DIR / "qlib_data"

PORTFOLIO_PATH = BASE_DIR / "portfolio_state.json"
PAYLOAD_PATH = QLIB_DATA_DIR / "deepseek_payload.json"
DECISION_PATH = BASE_DIR / "agent_decision_log.json"
TRADE_LOG_PATH = BASE_DIR / "trade_log.csv"

# Simulation Settings
FEE_RATE = 0.001  # 0.1% Taker Fee

# ---------------------------
# Helper Functions
# ---------------------------

def load_json(path, default=None):
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def init_portfolio():
    """Initialize portfolio if not exists"""
    if PORTFOLIO_PATH.exists():
        return load_json(PORTFOLIO_PATH)

    portfolio = {
        "nav": 10000.0,
        "cash": 10000.0,
        "positions": [],
        "last_update": datetime.now().isoformat()
    }
    save_json(PORTFOLIO_PATH, portfolio)
    return portfolio


def get_market_data_map(payload):
    """
    Extract current market data from deepseek_payload.json
    Returns: { "BTC": {"close": 98000.0, "high": 99000.0, "low": 97000.0}, ... }
    """
    data_map = {}
    for coin in payload.get("coins", []):
        symbol = coin.get("symbol")
        market_data = coin.get("market_data", {})
        close = market_data.get("close")
        
        if symbol is None or close is None:
            continue
            
        # Parse high/low if available, otherwise fallback to close
        high = market_data.get("high")
        low = market_data.get("low")
        
        # Handle string formatting if necessary (though inference script saves as float/int usually, 
        # but let's be safe if they are strings like "123.45%")
        # Actually inference script saves raw values for these fields.
        
        data_map[symbol] = {
            "close": float(close),
            "high": float(high) if high is not None else float(close),
            "low": float(low) if low is not None else float(close)
        }
    return data_map


def compute_nav(portfolio, market_map):
    """
    Calculate NAV = Available Cash + Sum(Position Margin + Unrealized PnL)
    """
    cash = float(portfolio.get("cash", 0.0))
    positions = portfolio.get("positions", [])

    total_equity = cash
    
    for pos in positions:
        symbol = pos["symbol"]
        qty = float(pos["quantity"])
        entry_price = float(pos["entry_price"])
        margin = float(pos.get("margin", 0.0))
        
        # Get current price or fallback to entry
        market_data = market_map.get(symbol, {})
        current_price = market_data.get("close", entry_price)
        pos["current_price"] = current_price

        # PnL Calculation
        # Long (qty > 0): (Curr - Entry) * qty
        # Short (qty < 0): (Curr - Entry) * qty = (Entry - Curr) * abs(qty)
        pnl = (current_price - entry_price) * qty
        pos["unrealized_pnl"] = round(pnl, 2)

        # Equity = Margin + PnL
        total_equity += margin + pnl

    portfolio["nav"] = round(total_equity, 2)
    portfolio["last_update"] = datetime.now().isoformat()
    return portfolio


def append_trade_log(record: dict):
    """Append trade record to CSV"""
    df_new = pd.DataFrame([record])

    if TRADE_LOG_PATH.exists():
        df_old = pd.read_csv(TRADE_LOG_PATH)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new

    df_all.to_csv(TRADE_LOG_PATH, index=False)


# ---------------------------
# Core Execution Logic
# ---------------------------

def apply_actions():
    print("💸 Starting Mock Execution...")
    
    # 1. Load Data
    portfolio = init_portfolio()

    payload = load_json(PAYLOAD_PATH, default=None)
    if payload is None:
        print(f"❌ Market payload not found: {PAYLOAD_PATH}")
        return

    decision_data = load_json(DECISION_PATH, default=None)
    if decision_data is None:
        print(f"❌ Agent decision not found: {DECISION_PATH}")
        return

    # Handle history list format (take latest)
    if isinstance(decision_data, list):
        if not decision_data:
            print(f"⚠️ Agent decision log is empty list")
            return
        decision = decision_data[0]
    else:
        decision = decision_data

    market_map = get_market_data_map(payload)
    as_of = payload.get("as_of", datetime.now().isoformat())
    
    # Update NAV before trading (mark-to-market)
    portfolio = compute_nav(portfolio, market_map)
    nav_before = portfolio.get("nav", 0.0)
    cash_before = portfolio.get("cash", 0.0)

    # ---------------------------
    # Check TP/SL Hits (Intra-period)
    # ---------------------------
    positions = portfolio.get("positions", [])
    remaining_positions = []
    
    for pos in positions:
        symbol = pos["symbol"]
        market_data = market_map.get(symbol)
        if not market_data:
            remaining_positions.append(pos)
            continue
            
        high = market_data["high"]
        low = market_data["low"]
        close = market_data["close"]
        
        exit_plan = pos.get("exit_plan", {})
        tp = exit_plan.get("take_profit")
        sl = exit_plan.get("stop_loss")
        
        triggered = False
        exit_price = close
        exit_reason = ""
        
        if pos["side"] == "long":
            # Check SL first (conservative)
            if sl and low <= sl:
                triggered = True
                exit_price = sl
                # If SL is above entry price, it's a trailing stop (profit protection)
                if sl > entry_price:
                    exit_reason = "trailing_stop"
                else:
                    exit_reason = "stop_loss"
            elif tp and high >= tp:
                triggered = True
                exit_price = tp
                exit_reason = "take_profit"
        else: # Short
            # Check SL first (conservative)
            if sl and high >= sl:
                triggered = True
                exit_price = sl
                # If SL is below entry price, it's a trailing stop (profit protection)
                if sl < entry_price:
                    exit_reason = "trailing_stop"
                else:
                    exit_reason = "stop_loss"
            elif tp and low <= tp:
                triggered = True
                exit_price = tp
                exit_reason = "take_profit"
                
        if triggered:
            qty = float(pos["quantity"])
            entry_price = float(pos["entry_price"])
            margin = float(pos.get("margin", 0.0))
            
            # PnL
            pnl = (exit_price - entry_price) * qty
            
            # Fee
            notional_exit = abs(qty) * exit_price
            fee = notional_exit * FEE_RATE
            
            # Return to Cash
            net_return = margin + pnl - fee
            portfolio["cash"] += net_return
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            trade_rec = {
                "time": timestamp,
                "symbol": symbol,
                "action": "close_position",
                "side": pos["side"],
                "qty": qty,
                "price": exit_price,
                "notional": notional_exit,
                "margin": margin,
                "fee": fee,
                "realized_pnl": pnl - fee,
                "nav_after": None,
                "reason": exit_reason
            }
            append_trade_log(trade_rec)
            print(f"⚡ {exit_reason.upper()} TRIGGERED for {symbol} | Price: {exit_price} | PnL: ${pnl:.2f}")
        else:
            remaining_positions.append(pos)
            
    # Update positions list after TP/SL checks
    portfolio["positions"] = remaining_positions
    positions = remaining_positions # Update local var for next steps

    actions = decision.get("actions", [])
    print(f"📌 Market Time: {as_of}")
    print(f"💰 NAV: ${nav_before:,.2f} | Cash: ${cash_before:,.2f}")
    print(f"🧾 Actions: {len(actions)}")

    for act in actions:
        symbol = act.get("symbol")
        action_type = act.get("action")
        leverage = float(act.get("leverage", 1.0) or 1.0)
        size_usd = float(act.get("position_size_usd", 0.0) or 0.0)
        exit_plan = act.get("exit_plan", {})

        if not symbol or not action_type:
            continue

        current_price = market_map.get(symbol, {}).get("close")
        if current_price is None:
            print(f"⚠️ No price for {symbol}, skipping.")
            continue

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # -------------------
        # OPEN LONG / SHORT
        # -------------------
        if action_type in ("open_long", "open_short"):
            if size_usd <= 0:
                print(f"⚠️ {symbol}: Invalid size ${size_usd}")
                continue

            # Calculate Fee
            notional = size_usd * leverage
            fee = notional * FEE_RATE
            cost = size_usd + fee

            if cost > portfolio["cash"]:
                print(f"⚠️ {symbol}: Insufficient cash. Need ${cost:.2f}, Have ${portfolio['cash']:.2f}")
                continue

            side = "long" if action_type == "open_long" else "short"
            qty = notional / current_price
            if side == "short":
                qty = -qty

            # Deduct Cash
            portfolio["cash"] -= cost

            pos = {
                "symbol": symbol,
                "side": side,
                "quantity": qty,
                "entry_price": current_price,
                "leverage": leverage,
                "margin": size_usd,
                "notional": notional,
                "current_price": current_price,
                "unrealized_pnl": -fee, # Start with loss due to fee
                "exit_plan": exit_plan,
                "opened_at": timestamp
            }
            positions.append(pos)

            trade_rec = {
                "time": timestamp,
                "symbol": symbol,
                "action": action_type,
                "side": side,
                "qty": qty,
                "price": current_price,
                "notional": notional,
                "margin": size_usd,
                "fee": fee,
                "realized_pnl": -fee,
                "nav_after": None
            }
            append_trade_log(trade_rec)
            print(f"✅ OPEN {side.upper()} {symbol} | Size: ${size_usd} | Price: {current_price} | Fee: ${fee:.2f}")

        # -------------------
        # CLOSE POSITION
        # -------------------
        elif action_type == "close_position":
            remaining_positions = []
            
            for pos in positions:
                if pos["symbol"] != symbol:
                    remaining_positions.append(pos)
                    continue

                qty = float(pos["quantity"])
                entry_price = float(pos["entry_price"])
                margin = float(pos.get("margin", 0.0))
                
                # PnL
                pnl = (current_price - entry_price) * qty
                
                # Fee
                notional_exit = abs(qty) * current_price
                fee = notional_exit * FEE_RATE
                
                # Return to Cash: Margin + PnL - Fee
                net_return = margin + pnl - fee
                portfolio["cash"] += net_return

                trade_rec = {
                    "time": timestamp,
                    "symbol": symbol,
                    "action": "close_position",
                    "side": pos["side"],
                    "qty": qty,
                    "price": current_price,
                    "notional": notional_exit,
                    "margin": margin,
                    "fee": fee,
                    "realized_pnl": pnl - fee,
                    "nav_after": None
                }
                append_trade_log(trade_rec)
                print(f"🔁 CLOSE {pos['side'].upper()} {symbol} | PnL: ${pnl:.2f} | Fee: ${fee:.2f} | Net: ${pnl-fee:.2f}")

            positions = remaining_positions

        # -------------------
        # ADJUST SL/TP
        # -------------------
        elif action_type == "adjust_sl":
            updated = False
            for pos in positions:
                if pos["symbol"] == symbol:
                    pos.setdefault("exit_plan", {}).update(exit_plan or {})
                    updated = True
            if updated:
                print(f"✏️ UPDATE {symbol} SL/TP: {exit_plan}")
            else:
                print(f"⚠️ {symbol}: No position found to update.")

        else:
            print(f"ℹ️ {symbol}: {action_type} (No execution needed)")

    # Update Positions & NAV
    portfolio["positions"] = positions
    portfolio = compute_nav(portfolio, market_map)

    print(f"\n✅ Execution Complete")
    print(f"💰 New NAV: ${portfolio['nav']:,.2f} | Cash: ${portfolio['cash']:,.2f}")
    save_json(PORTFOLIO_PATH, portfolio)

    # Append to NAV History CSV
    nav_history_path = BASE_DIR / "nav_history.csv"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Check if file exists to write header
    file_exists = nav_history_path.exists()
    
    with open(nav_history_path, "a", encoding="utf-8") as f:
        if not file_exists:
            f.write("timestamp,nav\n")
        f.write(f"{timestamp},{portfolio['nav']:.2f}\n")
    
    print(f"📈 Updated NAV history: {portfolio['nav']:.2f}")


if __name__ == "__main__":
    apply_actions()

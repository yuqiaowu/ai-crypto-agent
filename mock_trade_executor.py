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


def get_price_map_from_payload(payload):
    """
    Extract current prices from deepseek_payload.json
    Returns: { "BTC": 98000.0, ... }
    """
    price_map = {}
    for coin in payload.get("coins", []):
        symbol = coin.get("symbol")
        close = coin.get("market_data", {}).get("close")
        if symbol is None or close is None:
            continue
        price_map[symbol] = float(close)
    return price_map


def compute_nav(portfolio, price_map):
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
        current_price = price_map.get(symbol, entry_price)
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

    decision = load_json(DECISION_PATH, default=None)
    if decision is None:
        print(f"❌ Agent decision not found: {DECISION_PATH}")
        return

    price_map = get_price_map_from_payload(payload)
    as_of = payload.get("as_of", datetime.now().isoformat())
    
    # Update NAV before trading (mark-to-market)
    portfolio = compute_nav(portfolio, price_map)
    nav_before = portfolio.get("nav", 0.0)
    cash_before = portfolio.get("cash", 0.0)

    actions = decision.get("actions", [])
    print(f"📌 Market Time: {as_of}")
    print(f"💰 NAV: ${nav_before:,.2f} | Cash: ${cash_before:,.2f}")
    print(f"🧾 Actions: {len(actions)}")

    positions = portfolio.get("positions", [])

    for act in actions:
        symbol = act.get("symbol")
        action_type = act.get("action")
        leverage = float(act.get("leverage", 1.0) or 1.0)
        size_usd = float(act.get("position_size_usd", 0.0) or 0.0)
        exit_plan = act.get("exit_plan", {})

        if not symbol or not action_type:
            continue

        current_price = price_map.get(symbol)
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
    portfolio = compute_nav(portfolio, price_map)

    print(f"\n✅ Execution Complete")
    print(f"💰 New NAV: ${portfolio['nav']:,.2f} | Cash: ${portfolio['cash']:,.2f}")
    save_json(PORTFOLIO_PATH, portfolio)


if __name__ == "__main__":
    apply_actions()


from flask import Flask, jsonify, request
import json
import csv
import os
from pathlib import Path
from datetime import datetime

app = Flask(__name__)

BASE_DIR = Path(__file__).parent
PORTFOLIO_PATH = BASE_DIR / "portfolio_state.json"
TRADE_LOG_PATH = BASE_DIR / "trade_log.csv"
AGENT_LOG_PATH = BASE_DIR / "agent_decision_log.json"

# Map symbols to names (optional)
COIN_NAMES = {
    "BTC": "Bitcoin",
    "ETH": "Ethereum",
    "SOL": "Solana",
    "BNB": "BNB",
    "DOGE": "Dogecoin"
}

@app.route('/api/agent-decision', methods=['GET'])
def get_agent_decision():
    if not AGENT_LOG_PATH.exists():
        return jsonify({"analysis_summary": "暂无决策记录", "actions": []})
        
    try:
        with open(AGENT_LOG_PATH, 'r') as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    return response

@app.after_request
def after_request(response):
    return add_cors_headers(response)

@app.route('/api/positions', methods=['GET'])
def get_positions():
    if not PORTFOLIO_PATH.exists():
        return jsonify([])

    try:
        with open(PORTFOLIO_PATH, 'r') as f:
            state = json.load(f)
        
        positions = []
        for pos in state.get("positions", []):
            symbol = pos["symbol"]
            entry_price = pos["entry_price"]
            current_price = pos["current_price"]
            quantity = pos["quantity"]
            
            # Calculate PnL
            if pos["side"] == "long":
                pnl = (current_price - entry_price) * quantity
                pnl_percent = ((current_price - entry_price) / entry_price) * 100
            else:
                pnl = (entry_price - current_price) * quantity
                pnl_percent = ((entry_price - current_price) / entry_price) * 100

            positions.append({
                "symbol": symbol,
                "name": COIN_NAMES.get(symbol, symbol),
                "entryPrice": entry_price,
                "currentPrice": current_price,
                "stopLoss": pos["exit_plan"]["stop_loss"],
                "takeProfit": pos["exit_plan"]["take_profit"],
                "amount": round(quantity, 4),
                "pnl": round(pnl, 2),
                "pnlPercent": round(pnl_percent, 2)
            })
            
        return jsonify(positions)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    if not TRADE_LOG_PATH.exists():
        return jsonify([])

    try:
        history = []
        with open(TRADE_LOG_PATH, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Only include closed trades or relevant actions
                # Assuming trade_log has: timestamp, symbol, action, price, quantity, fee
                # We need to reconstruct "trades" from log actions, which is complex.
                # For now, let's just return raw log or try to parse 'close_position' events?
                # A better way is if mock_trade_executor saved a 'closed_trades.json'.
                # But let's look at trade_log.csv structure first.
                pass
                
        # Since parsing CSV log to reconstruct trades is hard without state,
        # let's just return a mock history or empty for now, 
        # OR better: update mock_trade_executor to save closed trades to a JSON!
        
        return jsonify([]) 
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/summary', methods=['GET'])
def get_summary():
    # Return total NAV and PnL
    if not PORTFOLIO_PATH.exists():
        return jsonify({"nav": 10000, "pnl": 0})
        
    with open(PORTFOLIO_PATH, 'r') as f:
        state = json.load(f)
        
    nav = state.get("nav", 10000)
    initial_nav = 10000 # Hardcoded for now
    total_pnl = nav - initial_nav
    
    return jsonify({
        "nav": round(nav, 2),
        "totalPnl": round(total_pnl, 2),
        "pnlPercent": round((total_pnl / initial_nav) * 100, 2)
    })

if __name__ == '__main__':
    print("🚀 Starting CryptoQuant API Server on port 5001...")
    app.run(host='0.0.0.0', port=5001, debug=True)

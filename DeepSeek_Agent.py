"""
DeepSeek Trading Agent (Dolores)
Integrates Qlib Multi-Coin Model, Market Data, and LLM Reasoning.
"""
import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

# Load environment variables
load_dotenv()
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
BASE_URL = "https://api.deepseek.com"

# Paths
BASE_DIR = Path(__file__).resolve().parent
QLIB_DATA_DIR = BASE_DIR / "qlib_data"
PAYLOAD_PATH = QLIB_DATA_DIR / "deepseek_payload.json"
PORTFOLIO_PATH = BASE_DIR / "portfolio_state.json" # Mock portfolio for now

# ------------------------------------------------------------------------
# 1. System Prompt (Optimized)
# ------------------------------------------------------------------------
SYSTEM_PROMPT = """
🟩 0. YOU ARE “AI TRADING AGENT – DOLORES”

Role: Professional Crypto Trading AI.
Capabilities:
- Analyze Multi-Coin Market Structure (Price, Volume, Trend).
- Interpret Sentiment Data (Funding Rate, Open Interest, Z-Scores).
- Integrate Quantitative Signals (Qlib Model Scores).
- Process News & On-Chain Context (if provided).
- Detect Pain Trades (Squeezes, Crowded Trades).
- Manage Risk (Position Sizing, Stop Loss, Portfolio Heat).

Goal: Achieve stable risk-adjusted returns. Avoid ruin.

🟧 1. CURRENT TIME
Current Timestamp: {{CURRENT_TIMESTAMP}}

🟦 2. MARKET INPUTS (QLIB + SENTIMENT)
You will receive a JSON payload containing:
- `qlib_score`: Relative strength prediction (Higher = Stronger).
- `rank`: 1 (Best) to 5 (Worst).
- `market_data`: Raw metrics (RSI, Funding Rate, OI Change, etc.).
- `market_summary`: Overall trend/volatility assessment.
- `recommend_top`: Baseline quantitative recommendations.

{{QLIB_JSON_PAYLOAD}}

🟨 3. NEWS & ON-CHAIN CONTEXT (OPTIONAL)
If available, use this to validate or reject quantitative signals.
- Look for: "Impulse" (New Driver) vs "Priced In" (Old News).
- Check for: Divergence (Good News + Bad Price = Distribution).

{{NEWS_CONTEXT}}

🟥 4. ANALYSIS LOGIC (The "Dolores" Method)

A. Trend & Regime Check
- Compare `qlib_score` with `momentum_12` and `macd_hist`.
- If Score > 0.5 but Momentum < 0: Potential Reversal or Dip Buy?
- If Score < 0 but Momentum > 0: Top Formation?

B. Sentiment & Pain Trade Detection
- **Long Squeeze Risk**: Funding > 0.03% + RSI > 70 + High OI. -> DANGER for Longs.
- **Short Squeeze Opportunity**: Funding < -0.03% + RSI < 30 + High OI. -> OPPORTUNITY for Longs.
- **Apathy**: Low Volatility + Low Volume + Neutral Funding. -> NO TRADE.

C. Alpha Hypotheses
- **Trend Following**: High Score + Positive Momentum + Normal Funding.
- **Mean Reversion**: Extreme RSI + Extreme Funding + Reversal Candle.

🟧 5. PORTFOLIO & RISK MANAGEMENT
Current State:
{{PORTFOLIO_STATE_JSON}}

**IMPORTANT: Review Existing Positions First!**
Before opening new positions:
1. Check if you already have open positions (see "positions" array above)
2. For each existing position, decide:
   - **HOLD**: If still valid (price within range, thesis intact)
   - **ADJUST_SL**: If need to move stop-loss (e.g., trail profits)
   - **CLOSE_POSITION**: If invalidated (stop hit, thesis broken, or take profit)
3. Only open NEW positions if:
   - Current position count < 3
   - You have strong conviction
   - Risk budget allows (check available cash)

Constraints:
- Max Open Positions: 3 (including existing ones!)
- Max Risk Per Trade: 2% of NAV (Stop Loss distance * Position Size).
- Max Leverage: 3x.

Hard Safety Rules (ALWAYS OBEY):
- If market is extremely volatile OR portfolio NAV drawdown > 20%, prefer "actions": [].
- Never open new positions if there are already 3 open positions.
- Never allocate more than 50% of NAV in total across all new actions in a single decision.

🟫 6. OUTPUT FORMAT (JSON ONLY)
You must output a single valid JSON object. No markdown, no conversational text.

Structure:
{
  "analysis_summary": "Brief market view (e.g., 'Bullish trend with overheating signs in SOL').",
  "actions": [
    {
      "symbol": "BTC",
      "action": "open_long",  // open_long, open_short, close_position, adjust_sl, hold
      "leverage": 2,
      "position_size_usd": 1000,
      "entry_reason": "Qlib Rank #1, Funding negative (short squeeze potential), RSI neutral.",
      "exit_plan": {
        "take_profit": 99000,
        "stop_loss": 95000,
        "invalidation": "Funding flips positive > 0.01%"
      }
    }
  ]
}

If no trade is suitable, return "actions": [] with a summary explaining why.
"""

# ------------------------------------------------------------------------
# 2. Helper Functions
# ------------------------------------------------------------------------

def get_portfolio_state():
    """Load or create mock portfolio state"""
    if PORTFOLIO_PATH.exists():
        with open(PORTFOLIO_PATH, "r") as f:
            return f.read()
    else:
        # Default mock state
        mock_state = {
            "nav": 10000.0,
            "cash": 10000.0,
            "positions": []
        }
        return json.dumps(mock_state, indent=2)

def get_news_context():
    """
    Fetch news and on-chain context from the global snapshot.
    """
    snapshot_path = BASE_DIR / "global_onchain_news_snapshot.json"
    if not snapshot_path.exists():
        return "No news data available."
        
    try:
        with open(snapshot_path, "r") as f:
            data = json.load(f)
            
        # 1. News
        news_items = data.get("news", {}).get("items", [])[:5]
        news_str = "Latest News:\n"
        for item in news_items:
            news_str += f"- {item.get('title')} ({item.get('source')})\n"
            
        # 2. Liquidations (Derivatives)
        derivs = data.get("derivatives", {}).get("okx", {})
        liqs = derivs.get("eth_liquidations", {}).get("totals", {})
        long_liq = liqs.get("long_usd", 0)
        short_liq = liqs.get("short_usd", 0)
        
        liq_str = f"\nLiquidation Context (48h):\n- Long Liquidations: ${long_liq:,.2f}\n- Short Liquidations: ${short_liq:,.2f}\n"
        
        if long_liq > short_liq * 2:
            liq_str += "-> Longs have been flushed. Potential bounce?\n"
        elif short_liq > long_liq * 2:
            liq_str += "-> Shorts have been squeezed. Potential correction?\n"
            
        # 3. Fear & Greed
        fng = data.get("fear_greed", {}).get("latest", {})
        fng_str = f"\nFear & Greed Index: {fng.get('value')} ({fng.get('classification')})\n"
        
        return news_str + liq_str + fng_str
        
    except Exception as e:
        return f"Error reading news data: {e}"

def run_agent():
    print("🤖 Activating Agent Dolores...")
    
    # 1. Load Qlib Payload
    if not PAYLOAD_PATH.exists():
        print(f"❌ Payload not found at {PAYLOAD_PATH}. Run inference first.")
        return
    
    with open(PAYLOAD_PATH, "r") as f:
        qlib_payload = f.read()
        
    # 2. Prepare Prompt
    portfolio_state = get_portfolio_state()
    news_context = get_news_context()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    final_prompt = SYSTEM_PROMPT.replace("{{CURRENT_TIMESTAMP}}", current_time)
    final_prompt = final_prompt.replace("{{QLIB_JSON_PAYLOAD}}", qlib_payload)
    final_prompt = final_prompt.replace("{{PORTFOLIO_STATE_JSON}}", portfolio_state)
    final_prompt = final_prompt.replace("{{NEWS_CONTEXT}}", news_context)
    
    # 3. Call DeepSeek API
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
    }
    
    data = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": final_prompt},
            {"role": "user", "content": "Analyze the market and generate trading actions based on the latest data."}
        ],
        "temperature": 0.1,  # Low temp for strict JSON output
        "response_format": {"type": "json_object"}  # 🔥 Force JSON output
    }
    
    try:
        print("🤔 Dolores is thinking...")
        response = requests.post(f"{BASE_URL}/chat/completions", headers=headers, json=data)
        response.raise_for_status()
        
        result = response.json()
        content = result['choices'][0]['message']['content']
        
        # Clean output (remove markdown code blocks if present)
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
            
        # Parse JSON
        decision = json.loads(content)
        
        # Validate Decision
        validate_decision(decision)
        
        print("\n💡 Dolores' Decision:")
        print(json.dumps(decision, indent=2, ensure_ascii=False))
        
        # Save decision log (Append mode)
        log_path = BASE_DIR / "agent_decision_log.json"
        history = []
        if log_path.exists():
            try:
                with open(log_path, "r") as f:
                    content = json.load(f)
                    if isinstance(content, list):
                        history = content
                    else:
                        history = [content]
            except:
                history = []
        
        # Add timestamp if missing
        if "timestamp" not in decision:
            decision["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        # Prepend new decision (newest first)
        history.insert(0, decision)
        
        # Keep last 50 records to avoid huge file
        history = history[:50]
        
        with open(log_path, "w") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
            
    except Exception as e:
        print(f"❌ Error calling DeepSeek: {e}")
        if 'response' in locals():
            print(response.text)
            
        # Write error to log so frontend shows something
        error_decision = {
            "analysis_summary": f"模型调用失败: {str(e)}",
            "actions": [],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        log_path = BASE_DIR / "agent_decision_log.json"
        history = []
        if log_path.exists():
            try:
                with open(log_path, "r") as f:
                    content = json.load(f)
                    if isinstance(content, list):
                        history = content
                    else:
                        history = [content]
            except:
                history = []
                
        history.insert(0, error_decision)
        history = history[:50]
        
        with open(log_path, "w") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)

ALLOWED_ACTIONS = {"open_long", "open_short", "close_position", "adjust_sl", "hold"}

def validate_decision(decision):
    """
    Sanity check for agent decisions.
    Prints warnings instead of raising errors for now.
    """
    actions = decision.get("actions", [])
    if not actions:
        print("⚠️ No actions generated.")
        return

    # Load portfolio state for NAV check
    try:
        portfolio = json.loads(get_portfolio_state())
        nav = float(portfolio.get("nav", 10000))
    except:
        nav = 10000.0 # Fallback
        
    total_new_risk = 0.0
    
    print("\n🔍 Validating Decision...")
    
    for i, act in enumerate(actions):
        symbol = act.get("symbol", "UNKNOWN")
        action_type = act.get("action")
        
        # 1. Check Action Type
        if action_type not in ALLOWED_ACTIONS:
            print(f"  ⚠️ [Action #{i+1} {symbol}] Invalid action: '{action_type}'")
            
        # 2. Check Size & Leverage
        size = float(act.get("position_size_usd", 0) or 0)
        lev = float(act.get("leverage", 1) or 1)
        
        if size < 0:
            print(f"  ⚠️ [Action #{i+1} {symbol}] Negative position size: ${size}")
        
        if lev > 3:
            print(f"  ⚠️ [Action #{i+1} {symbol}] Leverage too high: {lev}x (Max 3x)")
            
        if action_type in ["open_long", "open_short"]:
            total_new_risk += size

    # 3. Check Total Risk
    if total_new_risk > nav * 0.5:
        print(f"  ⚠️ Total new position size (${total_new_risk:,.2f}) exceeds 50% of NAV (${nav:,.2f})")
    else:
        print(f"  ✅ Validation passed. Total new exposure: ${total_new_risk:,.2f}")

if __name__ == "__main__":
    run_agent()

"""
DeepSeek Trading Agent (Dolores)
Integrates Qlib Multi-Coin Model, Market Data, and LLM Reasoning.
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from openai import OpenAI  # NEW IMPORT
import time  # FOR RETRIES

# Load environment variables
load_dotenv()
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
BASE_URL = "https://api.deepseek.com"

# Initialize Client
client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=BASE_URL)

# Paths
BASE_DIR = Path(__file__).resolve().parent
QLIB_DATA_DIR = BASE_DIR / "qlib_data"
PAYLOAD_PATH = QLIB_DATA_DIR / "deepseek_payload.json"
PORTFOLIO_PATH = BASE_DIR / "portfolio_state.json" # Mock portfolio for now

# ... [omitted constants validation] ...


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
- `market_data`: 
    - **Technical**: RSI (14), MACD Hist, ATR, Bollinger Width, Momentum.
    - **Sentiment**: Funding Rate, Funding Z-Score, OI Change, OI RSI.
    - **Correlation**: BTC Correlation (btc_corr_24h).
    - **Volatility**: Normalized ATR (natr_14).
- `market_summary`: Overall trend/volatility assessment.


{{QLIB_JSON_PAYLOAD}}

🟦 2.1 MACRO TREND (1D TIMEFRAME)
Use this daily context to filter 4H signals.
- **Trend**: Price vs SMA50 (Bullish if Price > SMA50).
- **Momentum**: Daily RSI (Overbought > 70, Oversold < 30).
- **Structure**: Recent Highs/Lows.

{{DAILY_CONTEXT}}


🟨 3. NEWS & ON-CHAIN CONTEXT (OPTIONAL)
If available, use this to validate or reject quantitative signals.

{{NEWS_CONTEXT}}

🟦 3.1. SELF-REFLECTION & HISTORY (CRITICAL)
Learn from your recent performance. If you are losing, adjust strategy.
{{REFLECTION_CONTEXT}}

🟥 4. ANALYSIS LOGIC (The "Dolores" Method)


A. NARRATIVE VS REALITY CHECK (Crucial Step)
For each major news item or market move, ask:
- **Impulse**: Is this a NEW driver that changes the thesis? (Price moves WITH news).
- **Priced In**: Is this old news? (Price fades or ignores good news).
- **Divergence**: Good News + Bad Price = Distribution (Bearish). Bad News + Good Price = Accumulation (Bullish).

B. THE PAIN TRADE (Liquidity Hunting)
Identify where the crowd is trapped:
- **Long Squeeze Risk**: Funding > 0.03% (Crowded Longs) + Price Stalling + High OI. -> DANGER for Longs.
- **Short Squeeze Opportunity**: Funding < -0.03% (Crowded Shorts) + Price Holding Support + High OI. -> OPPORTUNITY for Longs.
- **Liquidity Trap**: Late chasers entering at resistance (High Funding + High RSI).

C. HYPOTHESIS MENU (Generate 3 Scenarios)
For top candidates, evaluate:
1.  **Trend Following**: High Qlib Score + Positive Momentum + Normal Funding. (Go with the flow).
2.  **Mean Reversion**: Extreme RSI (>75 or <25) + Extreme Funding + Reversal Candle. (Fade the move).
3.  **Microstructure/Squeeze**: Negative Funding + Price Resilience. (Bet on short covering).

Select the hypothesis with the highest probability.

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
- Max Open Positions: 3 (Strictly enforced).
- Max Risk Per Trade: 2% of NAV (Stop Loss distance * Position Size).
- Max Leverage: 5x (Normal), 1x (High Volatility).

Hard Safety Rules (ALWAYS OBEY):
- **3-Position Limit**: Never exceed 3 open positions.
- **Dynamic Exposure**:
    - **Bear Market (BTC < SMA50)**: Max Total Exposure = 50% NAV.
    - **Bull Market (BTC > SMA50)**: Max Total Exposure = 100% NAV.
- **Volatility Guard**: If Volatility is "High", FORCE Leverage = 1x.
- If market is extremely volatile OR portfolio NAV drawdown > 20%, prefer "actions": [].

🟫 6. OUTPUT FORMAT (JSON ONLY)
You must output a single valid JSON object. No markdown, no conversational text.
**IMPORTANT: All narrative fields (analysis_summary, entry_reason, invalidation) MUST be objects with "zh" (Chinese) and "en" (English) keys.**

Structure:
{
  "analysis_summary": {
    "zh": "必须是中文，综合叙述（3-4句话）。1. 首先进行【叙事校验】（Section 4A），判断当前宏观/新闻是Impulse还是Priced In。2. 结合日线趋势（Section 2.1）和【痛苦交易】检测（Section 4B），指出市场是否存在轧空/轧多风险。3. 阐述你选择的【假设剧本】（Section 4C）。例如：'尽管有ETF利好，但日线RSI超买且费率过高，显示利好已兑现（Priced In），存在轧多风险。我选择均值回归剧本，做空BTC...'",
    "en": "English translation of the above Chinese summary."
  },
  "context_analysis": {
    "technical_signal": {
      "zh": "简评技术面（Qlib分/日线趋势/主要指标）。",
      "en": "Brief review of technicals (Qlib score, trend, key indicators)."
    },
    "macro_onchain": {
      "zh": "简评宏观与链上（新闻/恐慌指数/资金流向）。",
      "en": "Brief review of macro & on-chain (News, Fear Index, Fund Flow)."
    },
    "portfolio_status": {
      "zh": "简评当前持仓与风险敞口。",
      "en": "Current portfolio exposure & risk status."
    },
    "reflection": {
      "zh": "简评历史反思对本次决策的影响。",
      "en": "How historical reflection influenced this decision."
    }
  },
  "actions": [
    {
      "symbol": "BTC",
      "action": "open_long",  // open_long, open_short, close_position, adjust_sl, hold
      "leverage": 2,
      "position_size_usd": 1000,
      "entry_reason": {
        "zh": "中文理由。例如：符合【微观结构挤压】剧本...",
        "en": "English reason. E.g., Matches [Microstructure Squeeze] scenario..."
      },
      "exit_plan": {
        "take_profit": 99000,
        "stop_loss": 95000,
        "invalidation": {
          "zh": "中文失效条件...",
          "en": "English invalidation condition..."
        }
      }
    }
  ]
}

If you have existing positions, you MUST output an action for EACH of them (e.g., "action": "hold").
If no NEW trade is suitable and you just want to hold existing positions, return the list of "hold" actions.
Only return "actions": [] if you have NO open positions and NO new trades.
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

        # Merge external Whale Analysis (Telegram Source) if available to align with Frontend
        whale_path = BASE_DIR.parent / "frontend/data/whale_analysis.json"
        if whale_path.exists():
            try:
                with open(whale_path, "r") as wf:
                    w_data = json.load(wf)
                    # Merge 'eth', 'sol' stats into data if present
                    for chain in ["eth", "sol"]:
                        if chain in w_data:
                            # Ensure structure exists in snapshot data
                            if chain not in data: data[chain] = {}
                            # Overwrite stats_24h with high quality data from whale_analysis
                            if "stats_24h" in w_data[chain]:
                                data[chain]["stats_24h"] = w_data[chain]["stats_24h"]
            except Exception as we:
                print(f"⚠️ Failed to merge whale analysis for context: {we}")
            
        # 1. News - Collect from all available sources
        news_dict = data.get("news", {})
        all_news = []
        
        # Calendar (Economic Data) - High Priority
        calendar_news = news_dict.get("calendar", {}).get("items", [])
        calendar_str = ""
        if calendar_news:
            calendar_str = "Economic Calendar (This Week):\n"
            for item in calendar_news[:5]:
                calendar_str += f"- {item.get('title')} [{item.get('published')}]\n"
        
        # General News
        for source_key in ["macro", "bitcoin", "ethereum", "general"]:
            source_news = news_dict.get(source_key, {}).get("items", [])
            all_news.extend(source_news[:3])  # Take top 3 from each source
        
        news_str = "Latest News:\n"
        if all_news:
            for item in all_news[:8]:  # Show max 8 total general news
                news_str += f"- {item.get('title')} ({item.get('published', 'N/A')})\n"
        else:
            news_str += "No recent news available.\n"
            
        # Combine Calendar + News
        final_news_context = f"{calendar_str}\n{news_str}" if calendar_str else news_str
            
        # 2. On-Chain Liquidity & Whale Activity (ETH & SOL)
        # These act as proxies for broad market sentiment/liquidity
        onchain_str = "\n=== On-Chain Liquidity & Whale Activity (24h) ===\n"
        onchain_str += "*Note: Positive flow always indicates bullish interest: Stablecoin Flow (+) means Buying Power; Token Flow (+) means Net Accumulation (moving to wallets).* \n"
        
        for chain in ["eth", "sol"]:
            chain_data = data.get(chain, {}).get("stats_24h", {})
            if chain_data:
                whale_cnt = chain_data.get("whale_count", "N/A")
                stable_flow = chain_data.get("stablecoin_net_flow", 0)
                token_flow = chain_data.get("token_net_flow", 0)
                
                # Format numbers millions/thousands
                def fmt_num(n):
                    if not isinstance(n, (int, float)): return str(n)
                    if abs(n) >= 1_000_000: return f"${n/1_000_000:.1f}M"
                    if abs(n) >= 1_000: return f"${n/1_000:.1f}K"
                    return f"${n:.0f}"

                onchain_str += f"- [{chain.upper()}] Active Whales: {whale_cnt} | Stablecoin Flow: {fmt_num(stable_flow)} | Token Flow: {fmt_num(token_flow)}\n"
        
        # 3. Liquidations (Derivatives)
        derivs = data.get("derivatives", {}).get("okx", {})
        liqs = derivs.get("eth_liquidations", {}).get("totals", {})
        long_liq = liqs.get("long_usd", 0)
        short_liq = liqs.get("short_usd", 0)
        
        liq_str = f"\n=== Derivatives Market Context ===\nLiquidation Context (48h):\n- Long Liquidations: ${long_liq:,.2f}\n- Short Liquidations: ${short_liq:,.2f}\n"
        
        if long_liq > short_liq * 2:
            liq_str += "-> Longs have been flushed. Potential bounce?\n"
        elif short_liq > long_liq * 2:
            liq_str += "-> Shorts have been squeezed. Potential correction?\n"
            
        # 4. Fear & Greed
        fng = data.get("fear_greed", {}).get("latest") or {}
        fng_str = f"Fear & Greed Index: {fng.get('value')} ({fng.get('classification')})\n"
        
        # 5. Fed Rate Probability
        fed_futures = data.get("fed_futures", {})
        fed_rate_str = ""
        if fed_futures and not fed_futures.get("error"):
            rate = fed_futures.get("implied_rate")
            change = fed_futures.get("change_5d_bps")
            trend = fed_futures.get("trend", "Neutral")
            
            fed_rate_str = f"Fed Rate Expectations: {rate}% (Trend: {trend})\n"
        
        return final_news_context + "\n" + onchain_str + liq_str + fng_str + fed_rate_str
        
    except Exception as e:
        return f"Error reading news data: {e}"

def get_daily_context_summary():
    """
    Read 1D CSVs and generate a summary string for the agent.
    """
    csv_dir = BASE_DIR / "csv_data"
    summary = ""
    
    # List of coins to check (hardcoded or derived)
    coins = ["BTC", "ETH", "SOL", "BNB", "DOGE"]
    
    for symbol in coins:
        file_path = csv_dir / f"{symbol}_1d.csv"
        if not file_path.exists():
            continue
            
        try:
            df = pd.read_csv(file_path)
            if df.empty or len(df) < 50:
                continue
                
            # Calculate Indicators
            close = df["close"].iloc[-1]
            sma50 = df["close"].rolling(50).mean().iloc[-1]
            
            # RSI 14
            delta = df["close"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs)).iloc[-1]
            
            trend = "BULLISH" if close > sma50 else "BEARISH"
            
            summary += f"- **{symbol}**: Trend={trend} (Price ${close:.2f} vs SMA50 ${sma50:.2f}), RSI={rsi:.1f}\n"
            
        except Exception as e:
            print(f"⚠️ Error processing 1D data for {symbol}: {e}")
            
    if not summary:
        return "No daily data available."
        
    return summary

def run_agent():
    print("🤖 Activating Agent Dolores...")
    
    # 1. Load Qlib Payload
    if not PAYLOAD_PATH.exists():
        print(f"❌ Payload not found at {PAYLOAD_PATH}. Run inference first.")
        return
    
    with open(PAYLOAD_PATH, "r") as f:
        qlib_payload = f.read()
        
    # Parse payload for validation context
    try:
        payload_json = json.loads(qlib_payload)
        market_summary = payload_json.get("market_summary", {})
    except:
        market_summary = {}

    # Load Fear & Greed Index for Validation
    fear_index = 50 # Default Neutral
    try:
        snapshot_path = BASE_DIR / "global_onchain_news_snapshot.json"
        if snapshot_path.exists():
            with open(snapshot_path, "r") as f:
                snap_data = json.load(f)
                fng_val = snap_data.get("fear_greed", {}).get("latest", {}).get("value")
                if fng_val is not None:
                    fear_index = float(fng_val)
    except Exception as e:
        print(f"⚠️ Failed to load Fear Index: {e}")
        
    # 2. Prepare Prompt
    portfolio_state = get_portfolio_state()
    news_context = get_news_context()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    final_prompt = SYSTEM_PROMPT.replace("{{CURRENT_TIMESTAMP}}", current_time)
    final_prompt = final_prompt.replace("{{QLIB_JSON_PAYLOAD}}", qlib_payload)
    
    # Add Daily Context
    daily_context = get_daily_context_summary()
    final_prompt = final_prompt.replace("{{DAILY_CONTEXT}}", daily_context)
    
    final_prompt = final_prompt.replace("{{PORTFOLIO_STATE_JSON}}", portfolio_state)
    
    # Load Analyst Report (Crypto Brain) to facilitate collaboration
    analyst_report_str = ""
    try:
        whale_path = BASE_DIR.parent / "frontend/data/whale_analysis.json"
        if whale_path.exists():
            with open(whale_path, "r") as f:
                w_data = json.load(f)
                ai_summary = w_data.get("ai_summary", {})
                # Prefer Chinese
                report_text = ""
                if isinstance(ai_summary, dict):
                    report_text = ai_summary.get("zh") or ai_summary.get("en") or ""
                elif isinstance(ai_summary, str):
                    report_text = ai_summary
                
                if report_text:
                    analyst_report_str = f"\n\n🔍 **SENIOR ANALYST REPORT (Input for Decision)**:\n{report_text}\n"
                    print("✅ Loaded Senior Analyst Report for Context.")
    except Exception as e:
        print(f"⚠️ Failed to load analyst report: {e}")

    final_prompt = final_prompt.replace("{{NEWS_CONTEXT}}", news_context + analyst_report_str)

    # Add Reflection (History)
    try:
        from reflection import get_reflection_context
        reflection_str = get_reflection_context()
        print(f"\n🧠 Reflection Loaded:\n{reflection_str[:100]}...")
    except Exception as e:
        print(f"⚠️ Failed to load reflection: {e}")
        reflection_str = "No reflection data available."
    
    final_prompt = final_prompt.replace("{{REFLECTION_CONTEXT}}", reflection_str)
    
    # 3. Call DeepSeek API with OpenAI SDK (with manual retry loop)
    try:
        MAX_RETRIES = 5
        RETRY_DELAY = 5 # base seconds
        
        decision = None
        content = None
        
        for attempt in range(MAX_RETRIES):
            try:
                print(f"🤔 Dolores is thinking... (Attempt {attempt+1}/{MAX_RETRIES}, Timeout: 120s)")
                
                response = client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": final_prompt},
                        {"role": "user", "content": "Analyze the market and generate trading actions based on the latest data."}
                    ],
                    temperature=0.1,
                    response_format={"type": "json_object"},
                    timeout=120
                )
                
                content = response.choices[0].message.content
                # Parse JSON
                decision = json.loads(content)
                break # Success!
                
            except Exception as e:
                wait_time = RETRY_DELAY * (2 ** attempt) # Exponential backoff: 5, 10, 20, 40, 80...
                print(f"⚠️ Attempt {attempt+1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    print(f"🔄 Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise e # Final attempt failed
        
        # Validate & Enforce
        decision = validate_and_enforce_decision(decision, market_summary, daily_context, fear_index)
            
        print("\n💡 Dolores' Decision:")
        print(json.dumps(decision, indent=2, ensure_ascii=False))
        
        # Save decision log
        log_path = BASE_DIR / "agent_decision_log.json"
        
        # Load History
        history = []
        if log_path.exists():
            try:
                with open(log_path, "r") as f:
                    content_json = json.load(f)
                    if isinstance(content_json, list):
                        history = content_json
                    else:
                        history = [content_json]
            except:
                history = []
        
        # Add metadata and indicators
        import datetime as dt
        utc_now = dt.datetime.utcnow()
        beijing_time = utc_now + dt.timedelta(hours=8)
        decision["timestamp"] = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
        decision["market_indicators_at_time"] = market_summary # Save structured indicators
            
        history.insert(0, decision)
        history = history[:50]
        
        with open(log_path, "w") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)

        # 8. Send Notifications (Telegram & Discord)
        try:
            from notifier import send_notifications
            print("📢 Sending notifications...")
            send_notifications(decision)
        except Exception as e:
            print(f"⚠️ Notification failed: {e}")
                
    except Exception as e:
        print(f"❌ Error calling DeepSeek (OpenAI SDK): {e}")
            
        # Write error to log so frontend shows something
        error_decision = {
            "analysis_summary": f"模型调用失败: {str(e)}",
            "actions": [],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Send error notification
        try:
            from notifier import send_notifications
            send_notifications(error_decision)
        except:
            pass
        
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

def enforce_risk_limits(decision, portfolio, market_summary, daily_context_str, fear_index):
    """
    Strictly enforce risk management rules.
    Modifies decision in-place.
    """
    actions = decision.get("actions", [])
    if not actions:
        return decision

    # 1. Volatility Guard
    # If volatility is high, force leverage to 1x
    volatility = market_summary.get("volatility", "medium")
    if volatility == "high":
        print("⚠️ High Volatility Detected! Forcing Leverage = 1x")
        for act in actions:
            if act.get("action") in ["open_long", "open_short"]:
                if float(act.get("leverage", 1)) > 1:
                    act["leverage"] = 1
                    print(f"  🔻 {act.get('symbol')} leverage reduced to 1x")

    # Initialize status
    for act in actions:
        if "status" not in act:
            act["status"] = "executed"

    # 2. 3-Position Limit
    existing_positions = portfolio.get("positions", [])
    existing_symbols = {p["symbol"] for p in existing_positions}
    
    # Filter for valid open actions only (ignore holds/closes for this limit)
    open_actions = [a for a in actions if a.get("action") in ["open_long", "open_short"]]
    
    # We work on a copy/list to determine WHICH to reject, but we modify the objects in place
    active_open_actions = list(open_actions) # copy
    
    # Sort actions so we process new symbols last (easier to reject new ones if full)
    # But wait, we want to prioritize existing symbols? No, usually first-come first-served or based on score.
    # Let's just process linearly.
    
    # Calculate projected symbol set
    projected_symbols = existing_symbols.copy()
    
    valid_actions = []
    
    for act in active_open_actions:
        sym = act.get("symbol")
        
        # If symbol already in projected set, it's a "Merge Order" (Add to position)
        # This is ALWAYS allowed regarding the *count* limit (assuming exposure allows)
        if sym in projected_symbols:
            valid_actions.append(act)
        else:
            # New Symbol
            if len(projected_symbols) < 3:
                projected_symbols.add(sym)
                valid_actions.append(act)
            else:
                print(f"⛔ 3-Position Limit Exceeded! Rejecting new symbol {sym}")
                act["status"] = "rejected"
                act["rejection_reason"] = "Position Limit Exceeded (Max 3 Unique Symbols)"
                
    # Update active_open_actions to only be the valid ones for the NEXT check (Exposure)
    active_open_actions = [a for a in valid_actions if a.get("status") != "rejected"]
            
    # 3. Dynamic Exposure Cap
    # Check BTC Trend from daily_context_str AND Fear Index
    btc_bullish_trend = "BTC: Trend=BULLISH" in daily_context_str
    sentiment_ok = fear_index > 40 # Not in Extreme Fear
    
    # Strict Bull Mode: Must be Uptrend AND Not Panic
    is_bull_mode = btc_bullish_trend and sentiment_ok
    
    max_exposure_ratio = 1.0 if is_bull_mode else 0.5
    
    nav = float(portfolio.get("nav", 10000))
    max_exposure = nav * max_exposure_ratio
    
    mode_str = "BULL (Max 100%)" if is_bull_mode else "BEAR/DEFENSIVE (Max 50%)"
    reason_str = ""
    if not is_bull_mode:
        if not btc_bullish_trend: reason_str = "[Trend is Bearish]"
        elif not sentiment_ok: reason_str = f"[Extreme Fear: {fear_index}]"
    
    print(f"🛡️ Risk Mode: {mode_str} {reason_str} | Max Exposure: ${max_exposure:,.2f}")
    
    # Calculate current exposure (Margin * Leverage)
    current_exposure = sum([float(p.get("margin", 0)) * float(p.get("leverage", 1)) for p in existing_positions])
    
    # Calculate new exposure
    # Only iterate actions that haven't been rejected yet
    for act in active_open_actions:
        size = float(act.get("position_size_usd", 0))
        lev = float(act.get("leverage", 1))
        exposure = size * lev
        
        if current_exposure + exposure > max_exposure:
            # Calculate remaining available exposure
            available = max_exposure - current_exposure
            
            if available <= 0:
                print(f"⛔ Max Exposure Reached! Rejecting {act.get('symbol')}")
                act["status"] = "rejected"
                act["rejection_reason"] = f"Max Exposure Limit Reached ({mode_str})"
                continue
            
            # Reduce size to fit
            # New Exposure = New Size * Lev = Available
            # New Size = Available / Lev
            new_size = available / lev
            
            # If new size is too small (e.g. < $10), just reject it
            if new_size < 10:
                print(f"⛔ Insufficient room for {act.get('symbol')}. Rejecting.")
                act["status"] = "rejected"
                act["rejection_reason"] = "Insufficient Exposure Room"
                continue
                
            print(f"⚠️ Exposure Limit! Reducing {act.get('symbol')} size from ${size:.2f} to ${new_size:.2f}")
            act["position_size_usd"] = round(new_size, 2)
            act["original_size_usd"] = size # Optional: track original
            act["rejection_reason"] = "Size Reduced due to Exposure Limit" # Warning but not rejection
            current_exposure += available # Now full
        else:
            current_exposure += exposure
            
    decision["actions"] = actions
    return decision

def validate_and_enforce_decision(decision, market_summary, daily_context_str, fear_index=50):
    """
    Sanity check AND strict enforcement.
    """
    actions = decision.get("actions", [])
    if not actions:
        print("⚠️ No actions generated.")
        return decision

    # Load portfolio state
    try:
        portfolio = json.loads(get_portfolio_state())
    except:
        portfolio = {"nav": 10000.0, "positions": []}
        
    print("\n🔍 Validating & Enforcing Rules...")
    
    # Enforce Limits
    decision = enforce_risk_limits(decision, portfolio, market_summary, daily_context_str, fear_index)
    
    # Final Sanity Print
    actions = decision.get("actions", [])
    for i, act in enumerate(actions):
        symbol = act.get("symbol", "UNKNOWN")
        action_type = act.get("action")
        size = float(act.get("position_size_usd", 0) or 0)
        lev = float(act.get("leverage", 1) or 1)
        
        if action_type in ["open_long", "open_short"]:
            print(f"  ✅ [Action #{i+1} {symbol}] {action_type} | Size: ${size} | Lev: {lev}x")
        
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

            
    return decision

if __name__ == "__main__":
    run_agent()

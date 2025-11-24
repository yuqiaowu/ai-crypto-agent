# Dolores - AI Crypto Trading Agent

Dolores is an autonomous AI trading agent powered by **DeepSeek-V3**, **Qlib** (Microsoft AI), and **LightGBM**. She analyzes multi-coin market structures, sentiment data (Funding Rate, OI), and news to make risk-managed trading decisions.

## 🚀 Features

- **Multi-Coin Prediction**: Uses Qlib to predict relative strength (Ranking) of BTC, ETH, SOL, BNB, DOGE.
- **Sentiment Analysis**: Integrates Funding Rate Z-Scores, Open Interest changes, and Fear & Greed Index.
- **AI Reasoning**: DeepSeek LLM analyzes quantitative data + news context to generate trading plans.
- **Risk Management**:
  - Max 3 open positions.
  - Max 50% NAV exposure per decision.
  - Hard stop-loss and take-profit logic.
- **Automated Cycle**: `run_daily_cycle.py` orchestrates the full pipeline.

## 🛠️ Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   *(Note: Qlib requires specific installation steps, refer to [Qlib Docs](https://github.com/microsoft/qlib))*

2. **Environment Variables**:
   Create a `.env` file:
   ```ini
   DEEPSEEK_API_KEY=your_key_here
   OKX_API_KEY=your_key (optional for public data)
   OKX_SECRET_KEY=your_key
   OKX_PASSPHRASE=your_key
   ```

3. **Run the Cycle**:
   ```bash
   python3 run_daily_cycle.py
   ```

## 📂 Architecture

- `fetch_okx_data.py`: Fetches OHLCV & Sentiment data.
- `generate_multi_coin_signals.py`: Computes technical indicators.
- `prepare_multi_coin_qlib.py`: Formats data for Qlib.
- `inference_qlib_model.py`: Runs LightGBM model inference.
- `fetch_onchain_and_news.py`: Aggregates news & on-chain metrics.
- `DeepSeek_Agent.py`: The "Brain" - generates decisions.
- `run_daily_cycle.py`: The "Heartbeat" - runs the loop.

## 📊 Data Storage

- **Market Data**: Local CSVs in `data_features/` and `qlib_data/`.
- **State**: `portfolio_state.json` (Track positions).
- **Logs**: `agent_decision_log.json` (Latest decision).

## ⚠️ Disclaimer

This is experimental software. Use at your own risk.

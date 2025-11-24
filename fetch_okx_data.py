"""
Fetch multi-coin 4H OHLCV data from OKX using direct API calls
Coins: BTC, ETH, BNB, DOGE, SOL
"""
import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

CSV_DIR = Path("csv_data")
CSV_DIR.mkdir(exist_ok=True)

HTTP_TIMEOUT = 30

def resolve_proxy():
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    if not proxy and os.environ.get("USE_LOCAL_PROXY", "0").lower() in {"1", "true", "yes"}:
        proxy = "http://127.0.0.1:7890"
    if proxy:
        return {"http": proxy, "https": proxy}
    return None

PROXIES = resolve_proxy()
OKX_BASE = "https://www.okx.com"

def okx_get(path: str, params: dict) -> dict:
    """Make OKX API request"""
    url = OKX_BASE + path
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT, proxies=PROXIES)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("code") not in (None, "0"):
            print(f"⚠️ OKX API Error {data.get('code')}: {data.get('msg')}")
            return {}
        return data
    except Exception as e:
        print(f"⚠️ Request failed: {e}")
        return {}

def fetch_okx_candles(symbol: str, bar: str = "4H", days: int = 730) -> pd.DataFrame:
    """
    Fetch OHLCV candles from OKX
    API: GET /api/v5/market/candles?instId=BTC-USDT&bar=4H&limit=300
    """
    print(f"Fetching {symbol} {bar} data for {days} days...")
    
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days + 5)
    
    all_records = []
    after = None
    
    for iteration in range(50):  # Max 50 iterations
        params = {
            "instId": symbol,
            "bar": bar,
            "limit": "300",  # Max per request
        }
        
        if after:
            params["after"] = after
        
        payload = okx_get("/api/v5/market/candles", params)
        rows = payload.get("data", [])
        
        if not rows:
            break
        
        for row in rows:
            # [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
            ts = int(row[0])
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            
            if dt < start_dt:
                break
            
            all_records.append({
                "datetime": dt,
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            })
        
        if rows:
            after = rows[-1][0]  # Timestamp of last candle
        else:
            break
        
        # Check if we've gone back far enough
        oldest_ts = int(rows[-1][0])
        oldest_dt = datetime.fromtimestamp(oldest_ts / 1000, tz=timezone.utc)
        if oldest_dt < start_dt or len(rows) < 300:
            break
        
        print(f"  Fetched {len(rows)} candles, oldest: {oldest_dt.strftime('%Y-%m-%d %H:%M')}")
        time.sleep(0.2)  # Rate limiting
    
    if not all_records:
        print("⚠️ No data fetched")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_records)
    df = df.sort_values("datetime").reset_index(drop=True)
    
    # Convert to date column (Qlib format)
    df['date'] = df['datetime']
    df = df[['date', 'datetime', 'open', 'high', 'low', 'close', 'volume']]
    
    print(f"  ✅ Total {len(df)} candles from {df['date'].min()} to {df['date'].max()}")
    
    return df
def fetch_funding_rate(symbol: str, days: int = 730) -> pd.DataFrame:
    """
    Fetch funding rate history
    API: GET /api/v5/public/funding-rate-history?instId=BTC-USDT-SWAP&limit=100
    """
    swap_symbol = symbol.replace("-USDT", "-USDT-SWAP")
    print(f"Fetching funding rate for {swap_symbol}...")
    
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days + 5)
    
    all_records = []
    after = None
    
    for _ in range(100):
        params = {"instId": swap_symbol, "limit": "100"}
        if after:
            params["after"] = after
            
        payload = okx_get("/api/v5/public/funding-rate-history", params)
        rows = payload.get("data", [])
        
        if not rows:
            break
            
        for row in rows:
            # Response is list of dicts: {'instId': '...', 'fundingRate': '...', 'fundingTime': '...'}
            try:
                ts = int(row.get('fundingTime', 0))
                rate = float(row.get('fundingRate', 0))
            except (ValueError, AttributeError):
                # Fallback for list format if API changes
                ts = int(row[4])
                rate = float(row[2])
                
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            
            if dt < start_dt:
                break
                
            all_records.append({
                "datetime": dt,
                "funding_rate": rate,
            })
            
        if rows:
            # Get last timestamp for pagination
            try:
                last_ts = int(rows[-1].get('fundingTime', 0))
                after = str(last_ts)
            except:
                last_ts = int(rows[-1][4])
                after = str(last_ts)
                
            if datetime.fromtimestamp(last_ts/1000, tz=timezone.utc) < start_dt:
                break
        else:
            break
        time.sleep(0.1)
        
    if not all_records:
        return pd.DataFrame()
        
    df = pd.DataFrame(all_records)
    df = df.sort_values("datetime").reset_index(drop=True)
    print(f"  ✅ Fetched {len(df)} funding rate records")
    return df

def fetch_open_interest(symbol: str, bar: str = "4H", days: int = 730) -> pd.DataFrame:
    """
    Fetch open interest history
    API: GET /api/v5/rubik/stat/contracts/open-interest-history?instId=BTC-USDT-SWAP&period=4H
    """
    swap_symbol = symbol.replace("-USDT", "-USDT-SWAP")
    print(f"Fetching open interest for {swap_symbol}...")
    
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days + 5)
    
    all_records = []
    end_ts = None
    
    for _ in range(50):
        params = {"instId": swap_symbol, "period": bar, "limit": "100"}
        if end_ts:
            params["end"] = end_ts
            
        payload = okx_get("/api/v5/rubik/stat/contracts/open-interest-history", params)
        rows = payload.get("data", [])
        
        if not rows:
            break
            
        for row in rows:
            # Response is list of dicts: {'ts': '...', 'oi': '...', 'oiCcy': '...'}
            try:
                ts = int(row.get('ts', 0))
                oi = float(row.get('oi', 0))
            except (ValueError, AttributeError):
                # Fallback
                ts = int(row[0])
                oi = float(row[1])
                
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            
            if dt < start_dt:
                break
                
            all_records.append({
                "datetime": dt,
                "open_interest": oi,
            })
            
        if rows:
            try:
                last_ts = int(rows[-1].get('ts', 0))
                end_ts = str(last_ts)
            except:
                last_ts = int(rows[-1][0])
                end_ts = str(last_ts)
                
            if datetime.fromtimestamp(last_ts/1000, tz=timezone.utc) < start_dt:
                break
        else:
            break
        time.sleep(0.1)
        
    if not all_records:
        return pd.DataFrame()
        
    df = pd.DataFrame(all_records)
    df = df.sort_values("datetime").reset_index(drop=True)
    print(f"  ✅ Fetched {len(df)} open interest records")
    return df
def main():
    """Fetch 4H data for multiple coins"""
    symbols = {
        'BTC-USDT': 'BTC',
        'ETH-USDT': 'ETH',
        'BNB-USDT': 'BNB',
        'DOGE-USDT': 'DOGE',
        'SOL-USDT': 'SOL',
    }
    
    print("🚀 Fetching Multi-Coin 4H Data from OKX\n")
    
    for symbol, coin_name in symbols.items():
        print(f"\n{'='*60}")
        print(f"Processing {symbol} ({coin_name})")
        print(f"{'='*60}")
        
        df = fetch_okx_candles(symbol, bar="4H", days=730)
        
        if df.empty:
            print(f"❌ Failed to fetch {symbol}")
            continue
            
        # Fetch Sentiment Data
        fr_df = fetch_funding_rate(symbol, days=730)
        oi_df = fetch_open_interest(symbol, bar="4H", days=730)
        
        # Merge Funding Rate (ffill as it's 8H)
        if not fr_df.empty:
            df = pd.merge_asof(df, fr_df, on='datetime', direction='backward')
            df['funding_rate'] = df['funding_rate'].fillna(method='ffill')
            
        # Merge Open Interest
        if not oi_df.empty:
            df = pd.merge_asof(df, oi_df, on='datetime', direction='nearest', tolerance=pd.Timedelta(hours=1))
            
        # Save
        output_path = CSV_DIR / f"{coin_name}_4h.csv"
        df.to_csv(output_path, index=False)
        print(f"💾 Saved to {output_path} (with sentiment data)")
        
        time.sleep(1)  # Pause between coins
    
    print(f"\n✅ All done! Data saved to {CSV_DIR}/")

if __name__ == "__main__":
    main()

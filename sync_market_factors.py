# sync_market_factors.py
import os
import time
import math
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# 加载 .env
load_dotenv()

# =====================
# 路径 & 基础配置
# =====================
BASE_DIR = Path(__file__).resolve().parent
QLIB_DATA_DIR = BASE_DIR / "qlib_data"
QLIB_DATA_DIR.mkdir(exist_ok=True)
OUT_PATH = QLIB_DATA_DIR / "eth_daily_market_factors.csv"

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

# 可选代理（如果你本地有 Clash / 代理的话）
def resolve_proxy() -> dict | None:
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    if not proxy and os.environ.get("USE_LOCAL_PROXY", "0").lower() in {"1", "true", "yes"}:
        proxy = "http://127.0.0.1:7890"
    if proxy:
        return {"http": proxy, "https": proxy}
    return None

PROXIES = resolve_proxy()


# =====================
# OKX 相关（现货 + 合约）
# =====================

OKX_BASE = "https://www.okx.com"


def okx_get(path: str, params: dict) -> dict:
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


def fetch_okx_spot_daily_ohlcv(symbol: str = "ETH-USDT", days: int = 730) -> pd.DataFrame:
    """
    OKX k线接口:
      GET /api/v5/market/candles?instId=ETH-USDT&bar=1D&limit=...
    返回: ts, o, h, l, c, vol, volCcy, ...
    """
    print(f"Fetching OKX Spot Daily OHLCV for {symbol}...")
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days + 5)
    # OKX k线不支持 since，直接多拉一点再裁剪
    params = {
        "instId": symbol,
        "bar": "1D",
        "limit": "300",  # 分页拉取，一次300
    }
    
    all_records = []
    after = None
    
    for _ in range(5): # 最多拉取 5 页 (1500天)
        p = params.copy()
        if after:
            p["after"] = after
            
        payload = okx_get("/api/v5/market/candles", p)
        rows = payload.get("data", [])
        if not rows:
            break
            
        for row in rows:
            # ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm
            ts = int(row[0])
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            
            if dt < start_dt:
                continue
            
            all_records.append(
                {
                    "datetime": dt,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                }
            )
            
        if rows:
            after = rows[-1][0] # 下一页从最后一条时间戳开始
        else:
            break

    if not all_records:
        print("⚠️ No daily OHLCV data from OKX")
        return pd.DataFrame()
        
    df = pd.DataFrame(all_records).set_index("datetime").sort_index()
    # 转上海时间, 再归一到日期
    df.index = df.index.tz_convert("Asia/Shanghai")
    df = df[~df.index.duplicated(keep="last")]
    return df


def fetch_okx_oi_and_perp_volume(
    ccy: str = "ETH",
    inst_type: str = "SWAP",
    period: str = "1D",
    limit: int = 365,
) -> pd.DataFrame:
    """
    OKX Rubik 开仓量 & 永续成交额:
    GET /api/v5/rubik/stat/contracts/open-interest-volume
    """
    print(f"Fetching OKX OI & Volume for {ccy}...")
    params = {
        "ccy": ccy,
        "instType": inst_type,
        "period": period,
        "limit": str(limit),
    }
    payload = okx_get("/api/v5/rubik/stat/contracts/open-interest-volume", params)
    rows = payload.get("data", [])
    records = []
    for row in rows:
        # ts, oiUsd, volUsd, oi, vol
        ts = int(row[0])
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(
            timezone(timedelta(hours=8))
        )
        records.append(
            {
                "datetime": dt,
                "open_interest_usd": float(row[1]),
                "perp_volume_usd": float(row[2]),
            }
        )
    if not records:
        return pd.DataFrame(columns=["open_interest_usd", "perp_volume_usd"])
    df = pd.DataFrame(records).set_index("datetime").sort_index()
    return df


def fetch_okx_funding_rate_history(
    inst_id: str = "ETH-USDT-SWAP",
    days: int = 365,
) -> pd.DataFrame:
    """
    资金费率历史:
      GET /api/v5/public/funding-rate-history?instId=ETH-USDT-SWAP&limit=...
    返回每8小时一条，聚合成日均值。
    """
    print(f"Fetching OKX Funding Rate for {inst_id}...")
    limit = 100
    end_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days + 5)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    all_rows = []
    after = None
    for _ in range(20): # 限制循环次数
        params = {"instId": inst_id, "limit": str(limit)}
        if after:
            params["before"] = after
        payload = okx_get("/api/v5/public/funding-rate-history", params)
        rows = payload.get("data", [])
        if not rows:
            break
        for row in rows:
            ts = int(row["fundingTime"])
            if ts < cutoff_ms:
                continue
            rate = float(row["fundingRate"])
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(
                timezone(timedelta(hours=8))
            )
            all_rows.append({"datetime": dt, "funding_rate": rate})
        
        if not rows:
            break
            
        oldest = min(int(r["fundingTime"]) for r in rows)
        if oldest <= cutoff_ms or len(rows) < limit:
            break
        after = str(oldest)

    if not all_rows:
        return pd.DataFrame(columns=["funding_rate"])
    df = pd.DataFrame(all_rows)
    df["date"] = df["datetime"].dt.normalize()
    daily = df.groupby("date")["funding_rate"].mean().to_frame()
    # daily.index = daily.index.tz_localize("Asia/Shanghai") # 已经是带时区的了，不需要再次localize
    return daily


def fetch_okx_liquidations_daily(
    uly: str = "ETH-USDT",
    inst_type: str = "SWAP",
    days: int = 90,
) -> pd.DataFrame:
    """
    使用你之前类似的逻辑：/api/v5/public/liquidation-orders
    聚合为日度多空爆仓规模.
    """
    print(f"Fetching OKX Liquidations for {uly}...")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    records = []
    after = None
    seen = set()

    for _ in range(50): # 限制循环次数
        params = {
            "instType": inst_type,
            "uly": uly,
            "state": "filled",
            "limit": "100",
        }
        if after:
            params["after"] = after
        payload = okx_get("/api/v5/public/liquidation-orders", params)
        data_entries = payload.get("data", [])
        details = []
        for entry in data_entries:
            d = entry.get("details")
            if isinstance(d, list):
                details.extend(d)
        if not details:
            break

        oldest_ts = None
        for d in details:
            ts_raw = d.get("ts") or d.get("time")
            try:
                ts_int = int(ts_raw)
            except Exception:
                continue
            if ts_int in seen:
                continue
            seen.add(ts_int)
            if oldest_ts is None or ts_int < oldest_ts:
                oldest_ts = ts_int
            if ts_int < cutoff_ms:
                continue
            pos_side = (d.get("posSide") or d.get("side") or "").lower()
            if pos_side not in {"long", "short"}:
                continue
            try:
                sz = float(d.get("sz", "0"))
            except Exception:
                continue
            price = None
            try:
                price = float(d.get("bkPx"))
            except Exception:
                pass
            notional = sz * price if price is not None else None
            dt = datetime.fromtimestamp(ts_int / 1000, tz=timezone.utc).astimezone(
                timezone(timedelta(hours=8))
            )
            records.append(
                {
                    "datetime": dt,
                    "pos_side": pos_side,
                    "size": sz,
                    "notional_usd": notional or 0.0,
                }
            )
        if not oldest_ts:
            break
        after = str(oldest_ts - 1)
        if len(details) < 100:
            break

    if not records:
        return pd.DataFrame(columns=["liquidation_long_usd", "liquidation_short_usd"])
    df = pd.DataFrame(records)
    df["date"] = df["datetime"].dt.normalize()
    grouped = (
        df.groupby(["date", "pos_side"])["notional_usd"]
        .sum()
        .unstack(fill_value=0.0)
        .rename(columns={"long": "liquidation_long_usd", "short": "liquidation_short_usd"})
    )
    # grouped.index = grouped.index.tz_localize("Asia/Shanghai")
    return grouped


# =====================
# Etherscan 相关
# =====================

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")


def etherscan_get(params: dict) -> dict:
    url = "https://api.etherscan.io/api"
    if ETHERSCAN_API_KEY:
        params["apikey"] = ETHERSCAN_API_KEY
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT, proxies=PROXIES)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "0" and data.get("message") != "No data found":
            print("Etherscan warning:", data.get("message"))
        return data
    except Exception as e:
        print(f"⚠️ Etherscan request failed: {e}")
        return {}


def fetch_daily_tx_and_gas(
    start_date: str = "2023-01-01",
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Etherscan stats:
      - dailytx
      - dailyavggasprice
    注意：免费 key 有频率限制，第一次可能慢一点。
    """
    print("Fetching Etherscan Data...")
    if end_date is None:
        end_date = datetime.utcnow().strftime("%Y-%m-%d")

    tx_params = {
        "module": "stats",
        "action": "dailytx",
        "startdate": start_date,
        "enddate": end_date,
        "sort": "asc",
    }
    gas_params = {
        "module": "stats",
        "action": "dailyavggasprice",
        "startdate": start_date,
        "enddate": end_date,
        "sort": "asc",
    }

    tx_data = etherscan_get(tx_params)
    # 避免频率限制，sleep一下
    time.sleep(1) 
    gas_data = etherscan_get(gas_params)

    tx_rows = tx_data.get("result", [])
    gas_rows = gas_data.get("result", [])

    # 检查 result 是否为列表（有时候错误信息会直接返回字符串）
    if not isinstance(tx_rows, list):
        print(f"⚠️ Etherscan tx data invalid: {tx_rows}")
        tx_rows = []
    if not isinstance(gas_rows, list):
        print(f"⚠️ Etherscan gas data invalid: {gas_rows}")
        gas_rows = []

    if not tx_rows and not gas_rows:
        return pd.DataFrame()

    tx_df = pd.DataFrame(tx_rows)
    gas_df = pd.DataFrame(gas_rows)

    if not tx_df.empty:
        tx_df["datetime"] = pd.to_datetime(tx_df["UTCDate"])
        tx_df["tx_count"] = tx_df["transactionCount"].astype(float)
        tx_df = tx_df[["datetime", "tx_count"]]
    else:
        tx_df = pd.DataFrame(columns=["datetime", "tx_count"])

    if not gas_df.empty:
        gas_df["datetime"] = pd.to_datetime(gas_df["UTCDate"])
        # gasPrice 出来的单位一般是 Wei，这里转 Gwei
        gas_df["gas_price_gwei"] = gas_df["gasPrice"].astype(float) / 1e9
        gas_df = gas_df[["datetime", "gas_price_gwei"]]
    else:
        gas_df = pd.DataFrame(columns=["datetime", "gas_price_gwei"])

    merged = pd.merge(tx_df, gas_df, on="datetime", how="outer").sort_values("datetime")
    if not merged.empty:
        merged["datetime"] = merged["datetime"].dt.tz_localize("UTC").dt.tz_convert(
            "Asia/Shanghai"
        )
        merged = merged.set_index("datetime")
    return merged


# =====================
# 恐慌与贪婪指数 (alternative.me)
# =====================

def fetch_fear_greed_index(days: int = 730) -> pd.DataFrame:
    """
    Crypto Fear & Greed Index:
      https://api.alternative.me/fng/?limit=0
    返回大量历史数据，我们取最近 days 天。
    """
    print("Fetching Fear & Greed Index...")
    url = "https://api.alternative.me/fng/"
    params = {"limit": 0, "format": "json"}
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT, proxies=PROXIES)
        r.raise_for_status()
        data = r.json()
        rows = data.get("data", []) or []
        records = []
        cutoff = datetime.utcnow() - timedelta(days=days + 5)
        for row in rows:
            # {"value": "30", "timestamp": "1710460800", ...}
            try:
                ts = int(row["timestamp"])
                dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(
                    timezone(timedelta(hours=8))
                )
                if dt < cutoff:
                    continue
                value = float(row["value"])
                records.append({"datetime": dt, "fear_greed_index": value})
            except Exception:
                continue
        if not records:
            return pd.DataFrame(columns=["fear_greed_index"])
        df = pd.DataFrame(records).set_index("datetime").sort_index()
        # 同一天可能多条（不同时间），取最后一条
        df = df[~df.index.duplicated(keep="last")]
        return df
    except Exception as e:
        print(f"⚠️ Fear & Greed request failed: {e}")
        return pd.DataFrame(columns=["fear_greed_index"])


# =====================
# 因子工程 & 汇总
# =====================

def zscore(series: pd.Series, window: int) -> pd.Series:
    rolling = series.rolling(window)
    mean = rolling.mean()
    std = rolling.std(ddof=0)
    return (series - mean) / std.replace(0, np.nan)


def build_daily_market_factors() -> pd.DataFrame:
    # 1. 获取所有数据源
    price_df = fetch_okx_spot_daily_ohlcv()
    if price_df.empty:
        print("❌ Critical: No price data. Aborting.")
        return pd.DataFrame()
        
    # 归一到 date index
    price_df["date"] = price_df.index.normalize()
    price_df = price_df.set_index("date")

    oi_df = fetch_okx_oi_and_perp_volume()
    if not oi_df.empty:
        oi_df["date"] = oi_df.index.normalize()
        oi_df = oi_df.set_index("date")

    fr_df = fetch_okx_funding_rate_history()
    if not fr_df.empty:
        # fr_df 已经是 date index
        pass

    liq_df = fetch_okx_liquidations_daily()
    if not liq_df.empty:
        # liq_df 已经是 date index
        pass

    # ethscan_df = fetch_daily_tx_and_gas(start_date="2023-01-01")
    # if not ethscan_df.empty:
    #     ethscan_df["date"] = ethscan_df.index.normalize()
    #     ethscan_df = ethscan_df.set_index("date")
    ethscan_df = pd.DataFrame() # Skip Etherscan due to Pro API requirement

    fng_df = fetch_fear_greed_index()
    if not fng_df.empty:
        fng_df["date"] = fng_df.index.normalize()
        fng_df = fng_df.set_index("date")

    # 2. 合并数据
    print("\nMerging all data sources...")
    dfs = [price_df, oi_df, fr_df, liq_df, ethscan_df, fng_df]
    base = price_df.copy()
    
    for d in dfs[1:]:
        if d is not None and not d.empty:
            # 使用 join 合并，自动对齐 index (date)
            # rsuffix 处理重复列名
            base = base.join(d, how="left", rsuffix="_dup")

    # 清理重复列
    base = base[[c for c in base.columns if not c.endswith("_dup")]]
    
    base = base.sort_index()
    # 只保留从 2023-01-01 之后的
    base = base[base.index >= pd.Timestamp("2023-01-01").tz_localize("Asia/Shanghai")]

    # ====== 特征工程 ======
    print("Generating derived features...")
    
    # 1) 收盘价 & 日收益
    base["ret_1d"] = base["close"].pct_change()
    base["ret_5d"] = base["close"].pct_change(5)
    base["ret_20d"] = base["close"].pct_change(20)
    
    # 波动率 (ATR 简化版: High-Low / Close)
    base["volatility_daily"] = (base["high"] - base["low"]) / base["close"]
    base["volatility_ma_7"] = base["volatility_daily"].rolling(7).mean()

    # 2) OI 相关
    if "open_interest_usd" in base:
        base["open_interest_usd_ma_7"] = base["open_interest_usd"].rolling(7).mean()
        base["open_interest_usd_ma_30"] = base["open_interest_usd"].rolling(30).mean()
        base["open_interest_usd_zscore_60"] = zscore(base["open_interest_usd"], 60)
        base["open_interest_usd_change_pct_3d"] = (
            base["open_interest_usd"].pct_change(3) * 100
        )

    # 3) 爆仓相关
    if "liquidation_long_usd" in base and "liquidation_short_usd" in base:
        base["liq_total_usd"] = (
            base["liquidation_long_usd"].fillna(0)
            + base["liquidation_short_usd"].fillna(0)
        )
        base["liq_imbalance"] = (
            (base["liquidation_long_usd"] - base["liquidation_short_usd"])
            / base["liq_total_usd"].replace(0, np.nan)
        )
        base["liq_total_usd_zscore_60"] = zscore(base["liq_total_usd"], 60)

    # 4) 资金费率
    if "funding_rate" in base:
        base["funding_rate_ma_7"] = base["funding_rate"].rolling(7).mean()
        base["funding_rate_ma_30"] = base["funding_rate"].rolling(30).mean()
        base["funding_rate_zscore_60"] = zscore(base["funding_rate"], 60)

    # 5) Etherscan 链上活跃 & Gas
    if "tx_count" in base:
        base["tx_count_ma_7"] = base["tx_count"].rolling(7).mean()
        base["tx_count_ma_30"] = base["tx_count"].rolling(30).mean()
        base["tx_count_zscore_60"] = zscore(base["tx_count"], 60)

    if "gas_price_gwei" in base:
        base["gas_price_ma_7"] = base["gas_price_gwei"].rolling(7).mean()
        base["gas_price_zscore_60"] = zscore(base["gas_price_gwei"], 60)

    # 6) 恐慌贪婪
    if "fear_greed_index" in base:
        base["fg_index_ma_7"] = base["fear_greed_index"].rolling(7).mean()
        base["fg_extreme_greed"] = (base["fear_greed_index"] > 75).astype(int)
        base["fg_extreme_fear"] = (base["fear_greed_index"] < 25).astype(int)

    # 填补部分 NA（但保留前期完全没有数据的行可以删掉）
    base = base.dropna(subset=["close"])
    base = base.sort_index()
    
    # 前向填充，处理周末/假期缺失数据
    base = base.fillna(method='ffill')
    
    return base


def main():
    print("🚀 Starting Market Factor Sync...")
    df = build_daily_market_factors()
    
    if df.empty:
        print("❌ Failed to generate market factors.")
        return
        
    df_reset = df.reset_index().rename(columns={"index": "date"})
    df_reset = df_reset.rename(columns={"date": "datetime"})
    df_reset.to_csv(OUT_PATH, index=False)
    print(f"\n✅ Saved daily market factors to: {OUT_PATH}")
    print("   rows:", len(df_reset))
    print("   cols:", len(df_reset.columns))
    print("   range:", df_reset["datetime"].min(), "to", df_reset["datetime"].max())


if __name__ == "__main__":
    main()

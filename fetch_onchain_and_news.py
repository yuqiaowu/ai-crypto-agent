from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree

import requests

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
DEFAULT_OUTPUT = Path("global_onchain_news_snapshot.json")

# Cache for previous snapshot
_prev_snapshot_cache: Optional[Dict[str, Any]] = None

def _load_previous_snapshot() -> Optional[Dict[str, Any]]:
    global _prev_snapshot_cache
    if _prev_snapshot_cache is not None:
        return _prev_snapshot_cache
    try:
        if DEFAULT_OUTPUT.exists():
            with DEFAULT_OUTPUT.open("r", encoding="utf-8") as f:
                _prev_snapshot_cache = json.load(f)
                return _prev_snapshot_cache
    except Exception:
        return None
    return None

BRIDGES_DATASET_URLS = [
    "https://bridges.llama.fi/bridges",
]

STABLECOIN_CHAIN_DATASET_URLS = []


def _resolve_proxy() -> Optional[str]:
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    if proxy:
        return proxy.strip() or None
    use_local_proxy = os.environ.get("USE_LOCAL_PROXY", "1").lower()
    if use_local_proxy in {"1", "true", "yes"}:
        return "http://127.0.0.1:7890"
    return None


def _build_session() -> requests.Session:
    session = requests.Session()
    proxy = _resolve_proxy()
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    session.headers.update(
        {
            "User-Agent": os.environ.get(
                "HTTP_USER_AGENT",
                "CodexDataFetcher/1.0 (+https://defillama.com)",
            )
        }
    )
    return session


def _fetch_json(
    session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    try:
        resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        return {"error": str(exc), "url": url, "params": params}
    try:
        return resp.json()
    except ValueError:
        preview = resp.text[:400]
        return {"error": "invalid_json", "url": url, "params": params, "preview": preview}


def _fetch_rss_items(session: requests.Session, url: str, limit: int = 5) -> Dict[str, Any]:
    try:
        resp = session.get(url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        return {"error": str(exc), "url": url}
    try:
        root = ElementTree.fromstring(resp.content)
    except ElementTree.ParseError as exc:
        preview = resp.text[:400]
        return {"error": f"rss_parse_error: {exc}", "url": url, "preview": preview}

    items: List[Dict[str, Any]] = []
    for item in root.findall(".//item")[:limit]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or item.findtext("{http://purl.org/dc/elements/1.1/}date") or "").strip()
        summary = (item.findtext("description") or "").strip()
        items.append(
            {
                "title": title,
                "link": link,
                "published": pub_date,
                "summary": summary,
            }
        )
    return {"items": items, "source": url}


def _fetch_cryptocompare_news(
    session: requests.Session,
    categories: str = "BTC,ETH,SOL,BNB,DOGE",
    lang: str = "EN",
    limit: int = 50,
) -> Dict[str, Any]:
    params = {
        "categories": categories,
        "lang": lang.upper(),
        "sortOrder": "latest",
    }
    data = _fetch_json(session, "https://min-api.cryptocompare.com/data/v2/news/", params)
    if isinstance(data, dict) and data.get("error"):
        return {"error": data.get("error"), "url": "cryptocompare", "params": params}
    items: List[Dict[str, Any]] = []
    entries = []
    if isinstance(data, dict):
        if isinstance(data.get("Data"), list):
            entries = data.get("Data")
        elif isinstance(data.get("data"), list):
            entries = data.get("data")
    for entry in entries[:limit]:
        if not isinstance(entry, dict):
            continue
        ts = entry.get("published_on")
        if ts:
            try:
                published = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
            except (ValueError, TypeError):
                published = ts
        else:
            published = None
        items.append(
            {
                "title": entry.get("title"),
                "link": entry.get("url"),
                "published": published,
                "source": entry.get("source"),
                "tags": entry.get("categories"),
                "summary": entry.get("body"),
            }
        )
    if not items:
        return {
            "items": items,
            "source": "CryptoCompare",
            "params": params,
            "note": "No news items returned; may be rate limited or category empty.",
            "raw": data,
        }
    return {"items": items, "source": "CryptoCompare", "params": params}


def _numeric_change(current: Optional[float], previous: Optional[float]) -> Dict[str, Optional[float]]:
    if current is None or previous is None:
        return {"current": current, "previous": previous, "abs_change": None, "pct_change": None}
    abs_change = current - previous
    pct_change = (abs_change / previous * 100) if previous else None
    return {
        "current": round(current, 2),
        "previous": round(previous, 2),
        "abs_change": round(abs_change, 2),
        "pct_change": round(pct_change, 2) if pct_change is not None else None,
    }


def _extract_series_value(entry: Dict[str, Any]) -> Optional[float]:
    def _extract_numeric(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.replace(",", ""))
            except (TypeError, ValueError):
                return None
        if isinstance(value, dict):
            # common nested keys for stablecoin stats
            for nested_key in (
                "peggedUSD",
                "totalCirculating",
                "totalCirculatingUSD",
                "total",
                "value",
                "current",
            ):
                if nested_key in value:
                    nested_val = _extract_numeric(value[nested_key])
                    if nested_val is not None:
                        return nested_val
            for nested_val in value.values():
                num = _extract_numeric(nested_val)
                if num is not None:
                    return num
        if isinstance(value, (list, tuple)):
            for item in value:
                num = _extract_numeric(item)
                if num is not None:
                    return num
        return None

    for key in (
        "totalCirculating",
        "totalCirculatingUSD",
        "totalLiquidityUSD",
        "totalLiquidity",
        "value",
        "circulating",
        "circulatingUSD",
    ):
        if key in entry:
            num = _extract_numeric(entry[key])
            if num is not None:
                return num
    return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def fetch_okx_open_interest_volume(
    session: requests.Session,
    ccy: str = "ETH",
    inst_type: str = "SWAP",
    period: str = "1D",
    limit: int = 90,
) -> Dict[str, Any]:
    """
    拉取 OKX Rubik 永续合约开仓量与成交额历史。
    """
    url = "https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume"
    params = {
        "ccy": ccy,
        "instType": inst_type,
        "period": period,
        "limit": str(limit),
    }
    try:
        resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        return {"error": str(exc), "url": url, "params": params}
    try:
        payload = resp.json()
    except ValueError:
        return {"error": "invalid_json", "url": url, "params": params}
    if isinstance(payload, dict) and payload.get("code") not in (None, "0"):
        return {"error": payload.get("msg") or payload.get("error_message"), "url": url, "params": params, "raw": payload}

    data_rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data_rows, list):
        return {"error": "missing_data", "url": url, "params": params, "raw": payload}

    series: List[Dict[str, Any]] = []
    for row in data_rows:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        ts_raw, oi_raw, vol_raw = row[:3]
        try:
            ts = int(ts_raw)
            oi_val = float(oi_raw)
            vol_val = float(vol_raw)
        except (TypeError, ValueError):
            continue
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        series.append(
            {
                "timestamp": dt.isoformat(),
                "date_cn": dt.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d"),
                "open_interest_usd": round(oi_val, 2),
                "perp_volume_usd": round(vol_val, 2),
            }
        )
    series.sort(key=lambda x: x["timestamp"])

    latest = series[-1] if series else None
    prev = series[-2] if len(series) >= 2 else None
    change_pct = None
    if latest and prev and prev.get("open_interest_usd"):
        try:
            change_pct = round(
                (latest["open_interest_usd"] - prev["open_interest_usd"]) / prev["open_interest_usd"] * 100,
                2,
            )
        except ZeroDivisionError:
            change_pct = None

    return {
        "series": series,
        "latest": latest,
        "previous": prev,
        "change_pct": change_pct,
        "params": params,
    }


def fetch_okx_liquidation_summary(
    session: requests.Session,
    uly: str = "ETH-USDT",
    inst_type: str = "SWAP",
    hours: int = 48,
    batch_limit: int = 100,
) -> Dict[str, Any]:
    """
    拉取 OKX 永续合约爆仓订单并按日聚合多空名义金额。
    """
    url = "https://www.okx.com/api/v5/public/liquidation-orders"
    cutoff_ts = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_ms = int(cutoff_ts.timestamp() * 1000)
    after: Optional[str] = None
    collected: List[Dict[str, Any]] = []
    seen: set[int] = set()

    for _ in range(300):
        params: Dict[str, Any] = {
            "instType": inst_type,
            "uly": uly,
            "state": "filled",
            "limit": str(batch_limit),
        }
        if after:
            params["after"] = after
        try:
            resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            return {"error": str(exc), "url": url, "params": params}
        try:
            payload = resp.json()
        except ValueError:
            return {"error": "invalid_json", "url": url, "params": params}
        if isinstance(payload, dict) and payload.get("code") not in (None, "0"):
            return {"error": payload.get("msg") or payload.get("error_message"), "url": url, "params": params, "raw": payload}

        data_entries = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data_entries, list):
            break
        details: List[Dict[str, Any]] = []
        for entry in data_entries:
            entry_details = entry.get("details") if isinstance(entry, dict) else None
            if isinstance(entry_details, list):
                details.extend(entry_details)
        if not details:
            break
        oldest_ts: Optional[int] = None
        for detail in details:
            ts_raw = detail.get("ts") or detail.get("time")
            try:
                ts = int(ts_raw)
            except (TypeError, ValueError):
                continue
            if ts in seen:
                continue
            seen.add(ts)
            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts
            if ts < cutoff_ms:
                continue
            side = (detail.get("posSide") or detail.get("side") or "").strip().lower()
            if side not in {"long", "short"}:
                continue
            try:
                size = float(detail.get("sz", 0))
            except (TypeError, ValueError):
                size = 0.0
            bk_px = detail.get("bkPx")
            try:
                price = float(bk_px)
            except (TypeError, ValueError):
                price = None
            notional = size * price if price is not None else None
            collected.append(
                {
                    "ts": ts,
                    "side": side,
                    "size": size,
                    "notional_usd": notional,
                }
            )
        if oldest_ts is None or len(details) < batch_limit:
            break
        after = str(oldest_ts - 1)

    aggregated: Dict[str, Dict[str, float]] = defaultdict(lambda: {"long": 0.0, "short": 0.0})
    for item in collected:
        ts = item["ts"]
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(timezone(timedelta(hours=8)))
        day_key = dt.strftime("%Y-%m-%d")
        side = item["side"]
        value = float(item.get("notional_usd") or 0.0)
        aggregated[day_key][side] += value

    series = []
    for day, values in sorted(aggregated.items()):
        series.append(
            {
                "date": day,
                "long_liquidations_usd": round(values.get("long", 0.0), 2),
                "short_liquidations_usd": round(values.get("short", 0.0), 2),
            }
        )

    totals = {
        "long_usd": round(sum(v["long_liquidations_usd"] for v in series), 2),
        "short_usd": round(sum(v["short_liquidations_usd"] for v in series), 2),
    }

    return {
        "series": series,
        "totals": totals,
        "params": {
            "uly": uly,
            "instType": inst_type,
            "hours": hours,
            "batch_limit": batch_limit,
        },
        "records": len(collected),
    }


def fetch_blockchair_eth_overview(session: requests.Session) -> Dict[str, Any]:
    """
    采用 gemini_advisor 中的方式，拉取 Blockchair Ethereum stats，提炼核心字段。
    """
    url = os.getenv("BLOCKCHAIR_STATS_URL", "https://api.blockchair.com/ethereum/stats")
    headers = {
        "User-Agent": os.getenv("BLOCKCHAIR_USER_AGENT", "eth-daily-report/0.1"),
        "Accept": "application/json",
    }
    try:
        resp = session.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as exc:
        return {"error": str(exc)}
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return {"error": "invalid_response"}

    result: Dict[str, Any] = {}
    circulation = _safe_decimal(data.get("circulation_approximate"))
    burned_total = _safe_decimal(data.get("burned"))
    burned_24h = _safe_decimal(data.get("burned_24h"))
    mempool_value = _safe_decimal(data.get("mempool_total_value_approximate"))

    if circulation is not None:
        result["circulation_eth"] = float(circulation / Decimal(10) ** 18)
    if burned_total is not None:
        result["burned_total_eth"] = float(burned_total / Decimal(10) ** 18)
    if burned_24h is not None:
        result["burned_24h_eth"] = float(burned_24h / Decimal(10) ** 18)
    if mempool_value is not None:
        result["mempool_value_eth"] = float(mempool_value / Decimal(10) ** 18)

    def to_float(val: Any) -> Optional[float]:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    for key in (
        "market_price_usd",
        "market_price_usd_change_24h_percentage",
        "market_cap_usd",
        "mempool_tps",
    ):
        value = to_float(data.get(key))
        if value is not None:
            result[key] = value

    transactions = data.get("mempool_transactions")
    if isinstance(transactions, int):
        result["mempool_transactions"] = transactions

    fees = data.get("suggested_transaction_fee_gwei_options")
    if isinstance(fees, dict):
        result["suggested_fees_gwei"] = fees

    return result


def fetch_defillama_bridge_flows_simple(session: requests.Session) -> Dict[str, Any]:
    """
    采用 gemini_advisor 中的方式，统计最近 24h 的跨链体量及 Top ETH bridge。
    """
    url = os.getenv("DEFILLAMA_BRIDGES_URL", "https://bridges.llama.fi/bridges")
    try:
        resp = session.get(url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as exc:
        return {"error": str(exc)}

    bridges = payload.get("bridges") if isinstance(payload, dict) else None
    if not isinstance(bridges, list):
        return {"error": "invalid_response"}

    eth_total = 0.0
    btc_total = 0.0
    eth_bridges: List[Dict[str, Any]] = []

    for item in bridges:
        if not isinstance(item, dict):
            continue
        volume_24h = item.get("last24hVolume")
        try:
            volume = float(volume_24h)
        except (TypeError, ValueError):
            continue
        chains = item.get("chains") or []
        chains_lower = {str(c).lower() for c in chains}
        if "ethereum" in chains_lower:
            eth_total += volume
            eth_bridges.append(
                {
                    "name": item.get("displayName") or item.get("name"),
                    "volume_24h_usd": volume,
                    "chains": chains,
                }
            )
        if "bitcoin" in chains_lower or "btc" in chains_lower:
            btc_total += volume

    eth_bridges.sort(key=lambda b: b["volume_24h_usd"], reverse=True)
    return {
        "eth_volume_24h_usd": eth_total if eth_total else None,
        "btc_volume_24h_usd": btc_total if btc_total else None,
        "top_eth_bridges": eth_bridges[:5],
    }


def _match_stablecoin_chain_entry(data: Dict[str, Any], chain_cap: str) -> Optional[Dict[str, Any]]:
    chain_lower = chain_cap.lower()
    candidates: List[Dict[str, Any]] = []
    for key in ("chains", "data", "entries", "list"):
        val = data.get(key)
        if isinstance(val, dict):
            for name, entry in val.items():
                if isinstance(entry, dict):
                    entry_copy = dict(entry)
                    entry_copy.setdefault("name", name)
                    candidates.append(entry_copy)
        elif isinstance(val, list):
            for entry in val:
                if isinstance(entry, dict):
                    candidates.append(entry)
    if not candidates and all(isinstance(k, str) for k in data.keys()):
        for name, entry in data.items():
            if isinstance(entry, dict):
                entry_copy = dict(entry)
                entry_copy.setdefault("name", name)
                candidates.append(entry_copy)
    for entry in candidates:
        name = str(entry.get("name") or entry.get("chain") or "").lower()
        if not name:
            continue
        if name == chain_lower:
            return entry
        if name.replace(" ", "") == chain_lower.replace(" ", ""):
            return entry
    return None


def _normalize_bridge_protocol(entry: Dict[str, Any]) -> Dict[str, Any]:
    net_field = entry.get("netflow") or entry.get("netFlow") or {}
    if not isinstance(net_field, dict):
        net_field = {}
    return {
        "name": entry.get("displayName") or entry.get("name"),
        "category": entry.get("category"),
        "tvl": entry.get("totalLiquidity") or entry.get("tvl"),
        "net_flow_1d": net_field.get("1d"),
        "net_flow_7d": net_field.get("7d"),
        "net_flow_1m": net_field.get("1m"),
        "raw": entry,
    }


def _fallback_bridge_protocols(session: requests.Session, chain_param: str) -> Dict[str, Any]:
    errors: List[Dict[str, Any]] = []
    collected: List[Dict[str, Any]] = []

    for url in BRIDGES_DATASET_URLS:
        data = _fetch_json(session, url)
        if isinstance(data, dict) and data.get("error"):
            errors.append({"url": url, "detail": data["error"]})
            continue
        candidates: List[Dict[str, Any]] = []
        if isinstance(data, list):
            candidates = [d for d in data if isinstance(d, dict)]
        elif isinstance(data, dict):
            for key in ("bridges", "data", "protocols"):
                val = data.get(key)
                if isinstance(val, list):
                    candidates = [d for d in val if isinstance(d, dict)]
                    break
        if not candidates:
            errors.append({"url": url, "detail": "no_candidates"})
            continue
        filtered = []
        for c in candidates:
            chains_field = c.get("chains")
            matches_chain = False
            if isinstance(chains_field, list):
                matches_chain = chain_param in chains_field or chain_param.lower() in [str(x).lower() for x in chains_field]
            elif isinstance(chains_field, dict):
                key_list = list(chains_field.keys())
                matches_chain = chain_param in key_list or chain_param.lower() in [str(k).lower() for k in key_list]
            dest_chain = c.get("destinationChain") or c.get("chain")
            if not matches_chain and isinstance(dest_chain, str):
                matches_chain = dest_chain.lower() == chain_param.lower()
            if not matches_chain and isinstance(c.get("name"), str):
                matches_chain = chain_param.lower() in c["name"].lower()
            if matches_chain:
                metrics_source = c.get("stats") or c
                filtered.append(
                    {
                        "name": c.get("displayName") or c.get("name"),
                        "category": c.get("category"),
                        "tvl": c.get("tvl") or c.get("totalLiquidity"),
                        "chains": c.get("chains"),
                        "destination": dest_chain,
                        "volume_1d": _safe_float(
                            metrics_source.get("volumePrevDay")
                            or metrics_source.get("volume_1d")
                            or metrics_source.get("last24hVolume")
                            or metrics_source.get("dailyVolume")
                        ),
                        "volume_7d": _safe_float(
                            metrics_source.get("volume_7d") or metrics_source.get("weeklyVolume")
                        ),
                        "volume_30d": _safe_float(
                            metrics_source.get("volume_30d") or metrics_source.get("monthlyVolume")
                        ),
                        "net_flow": metrics_source.get("netFlow") or metrics_source.get("netflow"),
                        "raw": c,
                    }
                )
        if filtered:
            collected.extend(filtered)
        else:
            errors.append({"url": url, "detail": "no_matching_chain"})
    collected.sort(key=lambda x: (x.get("volume_1d") or 0), reverse=True)
    top = collected[:10]
    summary: Dict[str, Any] = {}
    if collected:
        for key in ("volume_1d", "volume_7d", "volume_30d"):
            vals = [item.get(key) for item in collected if isinstance(item.get(key), (int, float))]
            if vals:
                summary[key] = sum(vals)
        nets: Dict[str, float] = {}
        for item in collected:
            net = item.get("net_flow")
            if isinstance(net, dict):
                for period, value in net.items():
                    fv = _safe_float(value)
                    if fv is None:
                        continue
                    nets[period] = nets.get(period, 0.0) + fv
        if nets:
            summary["net_flow"] = nets
    return {"protocols": top, "errors": errors, "summary": summary if summary else None}


def _extract_series_from_payload(payload: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return None
    for key in ("data", "chainBalances", "chart", "series"):
        val = payload.get(key)
        if isinstance(val, list):
            return [item for item in val if isinstance(item, dict)]
    return None


def _summarize_stablecoin_series(series: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    def _coerce_order(ts: Any) -> Optional[float]:
        if isinstance(ts, (int, float)):
            return float(ts)
        if isinstance(ts, str):
            try:
                return float(ts)
            except (TypeError, ValueError):
                try:
                    normalized = ts.replace("Z", "+00:00") if "Z" in ts and "+" not in ts else ts
                    return datetime.fromisoformat(normalized).timestamp()
                except (TypeError, ValueError):
                    return None
        return None

    for entry in series:
        value = _extract_series_value(entry)
        if value is None:
            continue
        ts = (
            entry.get("date")
            or entry.get("timestamp")
            or entry.get("time")
            or entry.get("ts")
            or entry.get("dateUTC")
        )
        cleaned.append({"timestamp": ts, "value": float(value), "order": _coerce_order(ts)})
    if not cleaned:
        return None
    cleaned.sort(key=lambda item: item.get("order") if item.get("order") is not None else float("inf"))
    latest = cleaned[-1]
    previous = cleaned[-2] if len(cleaned) > 1 else None
    summary: Dict[str, Any] = {
        "latest": {
            "timestamp": latest["timestamp"],
            "value": round(latest["value"], 2),
        },
        "previous": None,
        "change": None,
    }
    if previous:
        summary["previous"] = {
            "timestamp": previous["timestamp"],
            "value": round(previous["value"], 2),
        }
        summary["change"] = _numeric_change(latest["value"], previous["value"])
    for item in cleaned:
        item.pop("order", None)
    return summary


def _fill_stablecoin_change_from_previous(chain_key: str, summary: Optional[Dict[str, Any]], prev_snapshot: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(summary, dict):
        return summary
    if not prev_snapshot or not isinstance(prev_snapshot, dict):
        return summary
    try:
        prev_defi = prev_snapshot.get("defillama") or {}
        prev_chain = prev_defi.get(chain_key) or {}
        prev_stable = prev_chain.get("stablecoin") or {}
        prev_summary = prev_stable.get("summary") or {}
        prev_latest = prev_summary.get("latest") or {}
        prev_val = prev_latest.get("value")
        cur_latest = summary.get("latest") or {}
        cur_val = cur_latest.get("value")
        if isinstance(cur_val, (int, float)) and isinstance(prev_val, (int, float)):
            # attach previous and change if missing
            if not summary.get("previous"):
                summary["previous"] = {"timestamp": prev_latest.get("timestamp"), "value": prev_val}
            if not summary.get("change"):
                summary["change"] = _numeric_change(cur_val, prev_val)
    except Exception:
        return summary
    return summary


def _fetch_stablecoin_history(session: requests.Session, chain: str, prev_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    chain_slug = chain.lower()
    if chain_slug == "bitcoin":
        return {"source": "skipped", "summary": None, "note": "Bitcoin has no stablecoin chain data on DefiLlama"}
    chain_cap = chain.capitalize()
    endpoints = [
        (f"https://stablecoins.llama.fi/stablecoincharts/{chain_cap}", None),
        (f"https://stablecoins.llama.fi/stablecoincharts/{chain_slug}", None),
        (f"https://stablecoins.llama.fi/historicalChain/{chain_slug}", None),
        (f"https://stablecoins.llama.fi/chain/{chain_slug}", None),
        ("https://stablecoins.llama.fi/stablecoins", {"includeChains": "true"}),
    ]
    attempts: List[Dict[str, Any]] = []
    for url, params in endpoints:
        data = _fetch_json(session, url, params)
        if isinstance(data, dict) and data.get("error"):
            attempts.append({"url": url, "params": params, "detail": data.get("error")})
            continue
        if url.endswith("/stablecoins") and params and params.get("includeChains") == "true":
            stablecoins = data.get("stablecoins") or data.get("data")
            if isinstance(stablecoins, list):
                total = 0.0
                for coin in stablecoins:
                    chains = coin.get("chains")
                    if isinstance(chains, dict):
                        try:
                            total += float(
                                chains.get(chain.capitalize())
                                or chains.get(chain)
                                or chains.get(chain_slug)
                                or 0.0
                            )
                        except (TypeError, ValueError):
                            continue
                summary = {
                    "latest": {"value": round(total, 2), "timestamp": None},
                    "previous": None,
                    "change": None,
                }
                # fill change from previous snapshot if available
                summary = _fill_stablecoin_change_from_previous(chain_slug, summary, prev_snapshot)
                return {
                    "source": url,
                    "params": params,
                    "raw": data,
                    "summary": summary,
                }
            attempts.append({"url": url, "params": params, "detail": "no_stablecoins_list"})
            continue
        series = _extract_series_from_payload(data)
        if not series and isinstance(data, list):
            series = [d for d in data if isinstance(d, dict)]
        if series:
            summary = _summarize_stablecoin_series(series)
            if summary:
                summary = _fill_stablecoin_change_from_previous(chain_slug, summary, prev_snapshot)
                return {
                    "source": url,
                    "params": params,
                    "raw": data,
                    "summary": summary,
                }
            else:
                attempts.append({"url": url, "params": params, "detail": "no_summary"})
                continue
        attempts.append({"url": url, "params": params, "detail": "no_series"})
    chains_snapshot = _fetch_json(session, "https://stablecoins.llama.fi/stablecoinchains")
    data_list = chains_snapshot if isinstance(chains_snapshot, list) else chains_snapshot.get("data")
    if isinstance(data_list, list):
        match = next(
            (
                c
                for c in data_list
                if isinstance(c, dict)
                and (
                    c.get("name") == chain_cap
                    or c.get("chain") == chain_cap
                    or c.get("name") == chain_slug.capitalize()
                )
            ),
            None,
        )
        if match:
            value = match.get("value") or match.get("totalCirculating") or match.get("total")
            try:
                latest_value = round(float(value), 2)
            except (TypeError, ValueError):
                latest_value = value
            summary = {
                "latest": {"value": latest_value, "timestamp": datetime.now(timezone.utc).isoformat()},
                "previous": None,
                "change": None,
                "note": "Fell back to stablecoincharts; change metrics unavailable.",
            }
            summary = _fill_stablecoin_change_from_previous(chain_slug, summary, prev_snapshot)
            return {
                "source": "https://stablecoins.llama.fi/stablecoinchains",
                "raw": chains_snapshot,
                "summary": summary,
                "attempts": attempts,
            }
    for url in STABLECOIN_CHAIN_DATASET_URLS:
        dataset = _fetch_json(session, url)
        if not isinstance(dataset, dict) or dataset.get("error"):
            attempts.append({"url": url, "detail": dataset.get("error") if isinstance(dataset, dict) else "invalid"})
            continue
        match = _match_stablecoin_chain_entry(dataset, chain_cap)
        if match:
            latest_value = _safe_float(match.get("latestValue") or match.get("value") or match.get("total"))
            change_24h = _safe_float(match.get("change_24h") or match.get("change24h") or match.get("change1d"))
            timestamp = match.get("timestamp") or dataset.get("timestamp")
            summary = {
                "latest": {
                    "value": round(latest_value, 2) if isinstance(latest_value, float) else latest_value,
                    "timestamp": timestamp,
                },
                "previous": None,
                "change": change_24h,
            }
            if change_24h is None:
                summary["note"] = "Dataset snapshot provided without change metrics."
            summary = _fill_stablecoin_change_from_previous(chain_slug, summary, prev_snapshot)
            return {
                "source": url,
                "raw": dataset,
                "summary": summary,
                "attempts": attempts,
            }
        attempts.append({"url": url, "detail": "chain_not_found"})

    # Additional fallback: use stablecoins aggregated endpoint to compute chain totalCirculatingUSD
    sc_fallback = _fetch_json(session, "https://stablecoins.llama.fi/stablecoins")
    if isinstance(sc_fallback.get("chains"), list):
        # try exact name match (case-insensitive)
        entry = None
        for e in sc_fallback["chains"]:
            name = (e.get("name") or "").strip()
            if name.lower() == chain_slug:
                entry = e
                break
        if entry:
            tc = entry.get("totalCirculatingUSD")
            total = None
            if isinstance(tc, dict):
                nums = [v for v in tc.values() if isinstance(v, (int, float))]
                if nums:
                    total = sum(nums)
            if total is not None:
                summary = {
                    "latest": {"value": round(float(total), 2), "timestamp": datetime.now(timezone.utc).isoformat()},
                    "previous": None,
                    "change": None,
                    "note": "Computed from stablecoins.llama.fi/stablecoins (chain-level totalCirculatingUSD).",
                }
                summary = _fill_stablecoin_change_from_previous(chain_slug, summary, prev_snapshot)
                attempts.append({"url": "https://stablecoins.llama.fi/stablecoins", "detail": "used_chain_total"})
                return {
                    "source": "https://stablecoins.llama.fi/stablecoins",
                    "raw": sc_fallback,
                    "summary": summary,
                    "attempts": attempts,
                }
        else:
            attempts.append({"url": "https://stablecoins.llama.fi/stablecoins", "detail": "chain_not_found"})

    return {"error": "stablecoin_series_not_found", "attempts": attempts, "chains_snapshot": chains_snapshot}


def fetch_defillama_flows(session: requests.Session, chain: str = "Ethereum") -> Dict[str, Any]:
    chain_param = chain.capitalize()

    # Load previous snapshot once and pass into stablecoin to compute change if needed
    prev_snapshot = _load_previous_snapshot()

    overview = _fetch_json(
        session,
        "https://api.llama.fi/overview/bridges",
        params={
            "chains": chain_param,
            "excludeTotalDataChart": "true",
            "excludeTotalDataChartBreakdown": "true",
        },
    )

    highlights: List[Dict[str, Any]] = []
    chain_protocols = None
    if isinstance(overview.get("chainProtocols"), dict):
        chain_protocols = overview["chainProtocols"].get(chain_param)
    # Fallback: refetch without chain filter if empty or unavailable
    if not isinstance(chain_protocols, list) or len(chain_protocols) == 0:
        overview_no_filter = _fetch_json(
            session,
            "https://api.llama.fi/overview/bridges",
            params={
                "excludeTotalDataChart": "true",
                "excludeTotalDataChartBreakdown": "true",
            },
        )
        if isinstance(overview_no_filter.get("chainProtocols"), dict):
            chain_protocols = overview_no_filter["chainProtocols"].get(chain_param)
    if isinstance(chain_protocols, list):
        for entry in chain_protocols[:10]:
            highlights.append(_normalize_bridge_protocol(entry))
    fallback_protocols = None
    if not highlights:
        fallback_protocols = _fallback_bridge_protocols(session, chain_param)

    stablecoin_info = _fetch_stablecoin_history(session, chain_param, prev_snapshot=prev_snapshot)

    notes: List[str] = []
    if isinstance(overview, dict) and overview.get("error"):
        notes.append("overview endpoint returned error; fallback data may be limited.")
    if not highlights and fallback_protocols and not fallback_protocols.get("protocols"):
        notes.append("No bridge protocols matched the requested chain in fallback sources.")
    if isinstance(stablecoin_info, dict) and stablecoin_info.get("error"):
        notes.append("Stablecoin history unavailable; see attempts for details.")

    # Ensure bridge_summary exists by synthesizing from fallback protocols when summary empty
    bridge_summary = None
    if fallback_protocols:
        bridge_summary = fallback_protocols.get("summary")
        if not bridge_summary and isinstance(fallback_protocols.get("protocols"), list):
            # compute minimal summary from top protocols
            protos = fallback_protocols["protocols"]
            totals: Dict[str, float] = {}
            for key in ("volume_1d", "volume_7d", "volume_30d"):
                vals = [p.get(key) for p in protos if isinstance(p.get(key), (int, float))]
                if vals:
                    totals[key] = sum(vals)
            if totals:
                bridge_summary = totals

    return {
        "chain": chain_param,
        "bridge_overview_raw": overview,
        "bridge_top_protocols": highlights,
        "bridge_fallback": fallback_protocols,
        "bridge_summary": bridge_summary,
        "stablecoin": stablecoin_info,
        "notes": notes,
    }


def fetch_blockchair_metrics(session: requests.Session, chain: str) -> Dict[str, Any]:
    chain_slug = chain.lower()
    stats = _fetch_json(session, f"https://api.blockchair.com/{chain_slug}/stats")

    gas_snapshot: Optional[Dict[str, Optional[float]]] = None
    suggested_fee = None
    mempool_summary: Optional[Dict[str, Any]] = None
    if isinstance(stats.get("data"), dict):
        data = stats["data"]
        gas_fields = [
            "suggested_transaction_fee_per_gas_wei",
            "gas_price",
            "avg_gas_price_10m",
            "avg_gas_price_24h",
            "mempool_median_gas_price_wei",
            "mempool_median_gas_price",
        ]
        gas_price_raw = None
        for field in gas_fields:
            if data.get(field) is not None:
                gas_price_raw = data[field]
                break
        base_fields = [
            "suggested_base_fee_per_gas_wei",
            "base_fee_per_gas_wei",
            "base_fee_24h",
            "avg_base_fee_24h",
        ]
        base_fee_raw = None
        for field in base_fields:
            if data.get(field) is not None:
                base_fee_raw = data[field]
                break
        if gas_price_raw is not None or base_fee_raw is not None:
            try:
                gas_price_gwei = float(gas_price_raw) / 1e9 if gas_price_raw is not None else None
            except (TypeError, ValueError):
                gas_price_gwei = None
            try:
                base_fee_gwei = float(base_fee_raw) / 1e9 if base_fee_raw is not None else None
            except (TypeError, ValueError):
                base_fee_gwei = None
            gas_snapshot = {
                "suggested_gas_price_gwei": round(gas_price_gwei, 4) if gas_price_gwei is not None else None,
                "base_fee_gwei": round(base_fee_gwei, 4) if base_fee_gwei is not None else None,
            }
        suggested_fee_fields = [
            "suggested_transaction_fee_per_byte_sat",
            "suggested_transaction_fee_per_byte_wei",
            "mempool_median_transaction_fee_sat",
            "median_transaction_fee_24h",
        ]
        for field in suggested_fee_fields:
            if data.get(field) is not None:
                suggested_fee = data[field]
                break
        mempool_fields = {
            "transactions": [
                "mempool_txs",
                "mempool_transactions",
                "mempool_total_transactions",
            ],
            "size_kb": [
                "mempool_total_size_kb",
                "mempool_size_kb",
            ],
            "size_bytes": [
                "mempool_total_size",
                "mempool_size",
            ],
            "unconfirmed_value_usd": [
                "mempool_outputs_total_value_usd",
                "mempool_total_value_usd",
            ],
        }
        mempool_summary = {}
        for key, candidates in mempool_fields.items():
            for field in candidates:
                if data.get(field) is not None:
                    mempool_summary[key] = data[field]
                    break
        if not mempool_summary:
            mempool_summary = None

    return {
        "chain": chain_slug,
        "stats": stats,
        "gas_snapshot": gas_snapshot,
        "suggested_fee_rate": suggested_fee,
        "mempool_summary": mempool_summary,
        "notes": [
            f"Blockchair /{chain_slug}/mempool endpoint currently 404; stats-based summary provided instead."
        ],
    }


def fetch_bitcoin_mempool(session: requests.Session) -> Dict[str, Any]:
    """
    汇总 mempool.space 的 BTC 排队情况与建议手续费。
    """
    overview = _fetch_json(session, "https://mempool.space/api/mempool")
    recommended = _fetch_json(session, "https://mempool.space/api/v1/fees/recommended")
    queue_metrics = None
    if isinstance(overview, dict) and not overview.get("error"):
        queue_metrics = {
            "count": overview.get("count"),
            "vsize": overview.get("vsize"),
            "total_fee": overview.get("total_fee"),
        }
    combined = {
        "overview": overview,
        "recommended_fees": recommended,
        "queue_metrics": queue_metrics,
    }
    return combined


def fetch_eth_gas_etherscan(session: requests.Session, api_key: Optional[str]) -> Dict[str, Any]:
    """
    使用 Etherscan 免费 API 获取 ETH Gas 速率与历史。
    如果缺少 API Key，则只返回错误提示。
    """
    base_params = {"module": "gastracker"}
    if not api_key:
        return {"error": "missing_api_key", "detail": "ETHERSCAN_API_KEY 未设置"}

    gas_oracle = _fetch_json(
        session,
        "https://api.etherscan.io/api",
        params={
            **base_params,
            "action": "gasoracle",
            "apikey": api_key,
        },
    )

    fee_history = _fetch_json(
        session,
        "https://api.etherscan.io/api",
        params={
            **base_params,
            "action": "gasfeeHistory",
            "blockCount": "12",
            "rewardPercentiles": "10,50,90",
            "apikey": api_key,
        },
    )

    # Cloudflare JSON-RPC fallback (Etherscan V1 deprecated)
    cf_fallback = None
    if not isinstance(fee_history.get("result"), dict):
        try:
            cf_req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_feeHistory",
                "params": [10, "latest", [10, 50, 90]],
            }
            cf_resp = session.post("https://cloudflare-eth.com", json=cf_req, timeout=HTTP_TIMEOUT)
            cf = cf_resp.json()
            if isinstance(cf.get("result"), dict):
                cf_fallback = cf["result"]
            else:
                cf_fallback = {"error": cf.get("error"), "raw": cf}
        except Exception as e:
            cf_fallback = {"error": str(e)}

    gas_summary: Optional[Dict[str, Any]] = None
    if isinstance(gas_oracle.get("result"), dict):
        result = gas_oracle["result"]
        try:
            gas_summary = {
                "safe_gwei": float(result.get("SafeGasPrice")),
                "propose_gwei": float(result.get("ProposeGasPrice")),
                "fast_gwei": float(result.get("FastGasPrice")),
                "suggest_base_fee": float(result.get("suggestBaseFee")),
                "gas_used_ratio": float(result.get("gasUsedRatio")),
            }
        except (TypeError, ValueError):
            gas_summary = {
                "safe_gwei": result.get("SafeGasPrice"),
                "propose_gwei": result.get("ProposeGasPrice"),
                "fast_gwei": result.get("FastGasPrice"),
                "suggest_base_fee": result.get("suggestBaseFee"),
                "gas_used_ratio": result.get("gasUsedRatio"),
            }
    elif isinstance(cf_fallback, dict) and cf_fallback and "baseFeePerGas" in cf_fallback:
        base_fees = cf_fallback.get("baseFeePerGas", [])
        gas_ratios = cf_fallback.get("gasUsedRatio", [])
        rewards = cf_fallback.get("reward", [])
        # last block metrics
        last_base_hex = base_fees[-1] if base_fees else None
        try:
            base_gwei = int(last_base_hex, 16) / 1e9 if isinstance(last_base_hex, str) else None
        except Exception:
            base_gwei = None
        last_reward = rewards[-1] if rewards else []
        def hex_to_gwei(h):
            try:
                return int(h, 16) / 1e9
            except Exception:
                return None
        pr10 = hex_to_gwei(last_reward[0]) if len(last_reward) > 0 else None
        pr50 = hex_to_gwei(last_reward[1]) if len(last_reward) > 1 else None
        pr90 = hex_to_gwei(last_reward[2]) if len(last_reward) > 2 else None
        gas_summary = {
            "safe_gwei": round((base_gwei or 0) + (pr10 or 0), 4) if base_gwei is not None else None,
            "propose_gwei": round((base_gwei or 0) + (pr50 or 0), 4) if base_gwei is not None else None,
            "fast_gwei": round((base_gwei or 0) + (pr90 or 0), 4) if base_gwei is not None else None,
            "suggest_base_fee": base_gwei,
            "gas_used_ratio": gas_ratios[-1] if gas_ratios else None,
        }

    fee_points: List[Dict[str, Any]] = []
    if isinstance(fee_history.get("result"), dict):
        fh = fee_history["result"]
        base_fees = fh.get("baseFeePerGas", [])
        gas_ratios = fh.get("gasUsedRatio", [])
        rewards = fh.get("reward", [])
        newest_block = fh.get("latestBlock")
        for idx, base_fee in enumerate(base_fees):
            try:
                base_fee_gwei = int(base_fee, 16) / 1e9
            except (TypeError, ValueError):
                base_fee_gwei = base_fee
            reward_set = rewards[idx] if idx < len(rewards) else []
            fee_points.append(
                {
                    "base_fee_gwei": round(base_fee_gwei, 4) if isinstance(base_fee_gwei, float) else base_fee_gwei,
                    "gas_used_ratio": gas_ratios[idx] if idx < len(gas_ratios) else None,
                    "priority_fee_percentiles": reward_set,
                }
            )
        history_meta = {
            "block_count": fh.get("blockCount"),
            "last_block": newest_block,
            "points": fee_points,
        }
    elif isinstance(cf_fallback, dict) and cf_fallback and "baseFeePerGas" in cf_fallback:
        base_fees = cf_fallback.get("baseFeePerGas", [])
        gas_ratios = cf_fallback.get("gasUsedRatio", [])
        rewards = cf_fallback.get("reward", [])
        newest_block = cf_fallback.get("oldestBlock")
        for idx, base_fee in enumerate(base_fees):
            try:
                base_fee_gwei = int(base_fee, 16) / 1e9
            except (TypeError, ValueError):
                base_fee_gwei = base_fee
            reward_set = rewards[idx] if idx < len(rewards) else []
            fee_points.append(
                {
                    "base_fee_gwei": round(base_fee_gwei, 4) if isinstance(base_fee_gwei, float) else base_fee_gwei,
                    "gas_used_ratio": gas_ratios[idx] if idx < len(gas_ratios) else None,
                    "priority_fee_percentiles": reward_set,
                }
            )
        history_meta = {
            "block_count": len(base_fees),
            "last_block": newest_block,
            "points": fee_points,
        }
    else:
        history_meta = {"error": fee_history.get("error"), "raw": fee_history, "cf_fallback": cf_fallback}

    return {
        "gas_oracle_raw": gas_oracle,
        "gas_oracle_summary": gas_summary,
        "fee_history_raw": fee_history,
        "fee_history_summary": history_meta,
    }


def gather_news(session: requests.Session) -> Dict[str, Any]:
    feeds = {
        "bitcoin": [
            "https://www.coindesk.com/tag/bitcoin/rss/",
            "https://cointelegraph.com/rss/tag/bitcoin",
        ],
        "ethereum": [
            "https://www.coindesk.com/tag/ethereum/rss/",
            "https://cointelegraph.com/rss/tag/ethereum",
        ],
        "general": [
            "https://decrypt.co/feed",
            "https://news.bitcoin.com/feed/",
        ],
    }
    news: Dict[str, Any] = {}
    crypto_compare_cache = _fetch_cryptocompare_news(session, categories="BTC,ETH", lang="EN", limit=30)
    for topic, urls in feeds.items():
        topic_items: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        for url in urls:
            result = _fetch_rss_items(session, url, limit=5)
            if "items" in result:
                topic_items.extend(result["items"])
            else:
                errors.append(result)
        topic_items.sort(key=lambda x: x.get("published", ""), reverse=True)
        if topic == "general" and "items" in crypto_compare_cache:
            topic_items.extend(crypto_compare_cache["items"])
        topic_items.sort(key=lambda x: x.get("published", ""), reverse=True)
        note = None
        if not topic_items and errors:
            note = "所有新闻源均拉取失败，可能需要代理或额外认证。"
        extra_errors: List[Dict[str, Any]] = []
        if topic == "general" and crypto_compare_cache.get("error"):
            extra_errors.append(crypto_compare_cache)
        news[topic] = {
            "items": topic_items[:10],
            "errors": errors + extra_errors,
            "note": note,
        }
    if "items" in crypto_compare_cache and crypto_compare_cache["items"]:
        news["cryptocompare"] = crypto_compare_cache
    else:
        news["cryptocompare"] = crypto_compare_cache
    return news


def fetch_fear_greed_index(session: requests.Session, limit: int = 15) -> Dict[str, Any]:
    resp = _fetch_json(session, "https://api.alternative.me/fng/", params={"limit": str(limit)})
    series: List[Dict[str, Any]] = []
    data = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(data, list):
        for item in data:
            val = item.get("value")
            try:
                val_f = float(val) if val is not None else None
            except (TypeError, ValueError):
                val_f = None
            series.append({
                "timestamp": item.get("timestamp"),
                "value": val_f,
                "classification": item.get("value_classification"),
            })
    latest = series[0] if series else None
    return {"source": "https://api.alternative.me/fng/", "raw": resp, "latest": latest, "series": series}


def _bridge_topN(flows: Dict[str, Any], top_n: int = 5) -> List[Dict[str, Any]]:
    protos: List[Dict[str, Any]] = []
    if isinstance(flows.get("bridge_top_protocols"), list) and flows["bridge_top_protocols"]:
        protos = flows["bridge_top_protocols"]
    elif isinstance(flows.get("bridge_fallback"), dict) and isinstance(flows["bridge_fallback"].get("protocols"), list):
        protos = flows["bridge_fallback"]["protocols"]
    def vol1d(p: Dict[str, Any]) -> float:
        v = p.get("volume_1d")
        return float(v) if isinstance(v, (int, float)) else 0.0
    protos_sorted = sorted(protos, key=vol1d, reverse=True)
    return [{
        "name": p.get("name"),
        "volume_1d": p.get("volume_1d"),
        "volume_7d": p.get("volume_7d"),
        "volume_30d": p.get("volume_30d"),
    } for p in protos_sorted[:top_n]]


def build_daily_report(
    defi_eth: Dict[str, Any],
    defi_btc: Dict[str, Any],
    fear: Dict[str, Any],
    eth_gas: Dict[str, Any],
    btc_mempool: Dict[str, Any],
    news: Dict[str, Any],
    bridge_simple: Dict[str, Any] | None,
    blockchair_overview: Dict[str, Any] | None,
    top_n: int = 5,
) -> Dict[str, Any]:
    def fmt_sc(sc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        summ = sc.get("summary") if isinstance(sc, dict) else None
        latest = summ.get("latest") if isinstance(summ, dict) else None
        change = summ.get("change") if isinstance(summ, dict) else None
        previous = summ.get("previous") if isinstance(summ, dict) else None
        note = summ.get("note") if isinstance(summ, dict) else None
        return {"latest": latest, "previous": previous, "change": change, "note": note}
    eth_sc = fmt_sc(defi_eth.get("stablecoin"))
    btc_sc = fmt_sc(defi_btc.get("stablecoin"))
    eth_top = _bridge_topN(defi_eth, top_n)
    btc_top = _bridge_topN(defi_btc, top_n)
    eth_sum = defi_eth.get("bridge_summary")
    btc_sum = defi_btc.get("bridge_summary")
    fear_latest = fear.get("latest")
    eth_latest_val = eth_sc.get("latest")
    eth_latest_val = eth_latest_val.get("value") if isinstance(eth_latest_val, dict) else None
    eth_prev_val = eth_sc.get("previous")
    eth_prev_val = eth_prev_val.get("value") if isinstance(eth_prev_val, dict) else None

    def _fmt_usd(value: Any) -> str:
        if value is None:
            return "N/A"
        try:
            return f"{float(value):,.2f} USD"
        except (TypeError, ValueError):
            return str(value)

    def _fmt_change(change: Any) -> Optional[str]:
        if isinstance(change, dict):
            abs_change = change.get("abs_change")
            pct_change = change.get("pct_change")
            parts: List[str] = []
            if abs_change is not None:
                try:
                    parts.append(f"{abs_change:+,.2f} USD")
                except (TypeError, ValueError):
                    parts.append(str(abs_change))
            if pct_change is not None:
                try:
                    parts.append(f"{pct_change:+.2f}%")
                except (TypeError, ValueError):
                    parts.append(str(pct_change))
            if parts:
                return " / ".join(parts)
            current = change.get("current")
            previous = change.get("previous")
            if isinstance(current, (int, float)) and isinstance(previous, (int, float)) and previous:
                delta_pct = (current - previous) / previous * 100
                return f"{delta_pct:+.2f}%"
        elif change is not None:
            return str(change)
        return None

    change_display = _fmt_change(eth_sc.get("change"))
    change_parts: List[str] = []
    if change_display:
        change_parts.append(change_display)
    if eth_prev_val is not None:
        change_parts.append(f"前值 {_fmt_usd(eth_prev_val)}")

    sc_para = f"稳定币 — ETH 最新: {_fmt_usd(eth_latest_val)}"
    if change_parts:
        sc_para += f"，环比: {'； '.join(change_parts)}"
    sc_para += "。"
    bridge_para_parts: List[str] = []
    if isinstance(bridge_simple, dict) and not bridge_simple.get("error"):
        eth_24h = bridge_simple.get("eth_volume_24h_usd")
        btc_24h = bridge_simple.get("btc_volume_24h_usd")
        if eth_24h is not None or btc_24h is not None:
            bridge_para_parts.append(
                "24h 跨链资金流 — "
                + ", ".join(
                    part
                    for part in (
                        f"ETH {eth_24h:,.0f} USD" if isinstance(eth_24h, (int, float)) else None,
                        f"BTC {btc_24h:,.0f} USD" if isinstance(btc_24h, (int, float)) else None,
                    )
                    if part
                )
            )
        top_eth = bridge_simple.get("top_eth_bridges") or []
        if top_eth:
            top_str = ", ".join(
                f"{item.get('name')}: {item.get('volume_24h_usd'):,}"
                for item in top_eth
                if isinstance(item, dict) and item.get("volume_24h_usd") is not None
            )
            if top_str:
                bridge_para_parts.append(f"ETH Top Bridges: {top_str}")
    else:
        eth_top_str = ", ".join([f"{p.get('name')}: {p.get('volume_1d')}" for p in eth_top])
        btc_top_str = ", ".join([f"{p.get('name')}: {p.get('volume_1d')}" for p in btc_top])
        bridge_para_parts.append(
            f"桥接 — ETH 量 (1d/7d/30d): {tuple((eth_sum or {}).get(k) for k in ('volume_1d','volume_7d','volume_30d'))}; "
            f"Top {top_n}: {eth_top_str}。 "
            f"BTC 量 (1d/7d/30d): {tuple((btc_sum or {}).get(k) for k in ('volume_1d','volume_7d','volume_30d'))}; "
            f"Top {top_n}: {btc_top_str}."
        )
    bridges_para = "； ".join(bridge_para_parts)
    fear_para = (
        f"BTC 恐慌指数 — 最新值: {fear_latest.get('value') if isinstance(fear_latest, dict) else None}, "
        f"分类: {fear_latest.get('classification') if isinstance(fear_latest, dict) else None} (近 {len(fear.get('series') or [])} 天)。"
    )
    eth_gas_summary = eth_gas.get("gas_oracle_summary") if isinstance(eth_gas, dict) else None
    btc_fee = btc_mempool.get("recommended_fees") if isinstance(btc_mempool, dict) else None
    queue_metrics = btc_mempool.get("queue_metrics") if isinstance(btc_mempool, dict) else None
    gas_para_items: List[str] = []
    if isinstance(eth_gas_summary, dict):
        gas_para_items.append(
            "ETH Gas: "
            + ", ".join(
                [
                    f"安全 {eth_gas_summary.get('safe_gwei')} gwei",
                    f"提议 {eth_gas_summary.get('propose_gwei')} gwei",
                    f"快速 {eth_gas_summary.get('fast_gwei')} gwei",
                    f"基础费 {eth_gas_summary.get('suggest_base_fee')} gwei",
                ]
            )
        )
    if isinstance(btc_fee, dict):
        gas_para_items.append(
            "BTC 推荐费率: "
            + ", ".join(
                [
                    f"最低 {btc_fee.get('minimumFee')} sat/vB",
                    f"经济 {btc_fee.get('economyFee')} sat/vB",
                    f"正常 {btc_fee.get('normalFee')} sat/vB",
                    f"优先 {btc_fee.get('priorityFee')} sat/vB",
                ]
            )
        )
    if isinstance(queue_metrics, dict):
        gas_para_items.append(
            "BTC Mempool 队列: "
            + ", ".join(
                [
                    f"交易数 {queue_metrics.get('count')}",
                    f"体积 {queue_metrics.get('vsize')} vBytes",
                    f"累计费 {queue_metrics.get('total_fee')} sat",
                ]
            )
        )
    if isinstance(blockchair_overview, dict) and not blockchair_overview.get("error"):
        mempool_value = blockchair_overview.get("mempool_value_eth")
        burned_24h = blockchair_overview.get("burned_24h_eth")
        price_change = blockchair_overview.get("market_price_usd_change_24h_percentage")
        mempool_tx = blockchair_overview.get("mempool_transactions")
        fees = blockchair_overview.get("suggested_fees_gwei")
        parts = []
        if isinstance(mempool_value, (int, float)):
            parts.append(f"ETH Mempool 价值 {mempool_value:,.0f} ETH")
        if isinstance(burned_24h, (int, float)):
            parts.append(f"近24h 烧毁 {burned_24h:,.1f} ETH")
        if isinstance(price_change, (int, float)):
            parts.append(f"24h 价格变动 {price_change:+.2f}%")
        if isinstance(mempool_tx, int):
            parts.append(f"待处理交易 {mempool_tx}")
        if isinstance(fees, dict):
            priority = fees.get("priority")
            if priority is not None:
                parts.append(f"Blockchair 建议费 {priority} gwei")
        if parts:
            gas_para_items.append("Blockchair 概览: " + ", ".join(parts))
    gas_para = "；".join(gas_para_items) if gas_para_items else None

    return {
        "stablecoins": {"ethereum": eth_sc, "paragraph": sc_para},
        "bridges": {
            "ethereum": {"summary": eth_sum, "top": eth_top},
            "bitcoin": {"summary": btc_sum, "top": btc_top},
            "paragraph": bridges_para,
        },
        "fear_greed": {"series": fear.get("series"), "latest": fear_latest, "paragraph": fear_para},
        "gas": {
            "ethereum": eth_gas_summary,
            "bitcoin": {"recommended": btc_fee, "queue": queue_metrics},
            "paragraph": gas_para,
        },
        "news": news,
    }


def aggregate_snapshot(session: requests.Session) -> Dict[str, Any]:
    timestamp = datetime.now(timezone.utc).isoformat()
    ethereum_flows = fetch_defillama_flows(session, "Ethereum")
    bitcoin_flows = fetch_defillama_flows(session, "Bitcoin")
    ethereum_metrics = fetch_blockchair_metrics(session, "ethereum")
    bitcoin_metrics = fetch_blockchair_metrics(session, "bitcoin")
    btc_mempool = fetch_bitcoin_mempool(session)
    eth_gas = fetch_eth_gas_etherscan(session, os.environ.get("ETHERSCAN_API_KEY"))
    news = gather_news(session)
    fear_greed = fetch_fear_greed_index(session, limit=15)
    blockchair_overview = fetch_blockchair_eth_overview(session)
    eth_open_interest = fetch_okx_open_interest_volume(session, ccy="ETH", inst_type="SWAP", period="1D", limit=120)
    eth_liquidations = fetch_okx_liquidation_summary(session, uly="ETH-USDT", inst_type="SWAP", hours=72)
    bridge_simple = fetch_defillama_bridge_flows_simple(session)
    bridge_top_n = int(os.getenv("BRIDGE_TOP_N", "5"))
    daily_report = build_daily_report(
        ethereum_flows,
        bitcoin_flows,
        fear_greed,
        eth_gas,
        btc_mempool,
        news,
        bridge_simple,
        blockchair_overview,
        top_n=bridge_top_n,
    )

    return {
        "generated_at": timestamp,
        "defillama": {
            "ethereum": ethereum_flows,
            "bitcoin": bitcoin_flows,
        },
        "blockchair": {
            "ethereum": ethereum_metrics,
            "bitcoin": bitcoin_metrics,
        },
        "btc_mempool": btc_mempool,
        "eth_gas": eth_gas,
        "news": news,
        "fear_greed": fear_greed,
        "blockchair_overview": blockchair_overview,
        "bridge_summary_24h": bridge_simple,
        "derivatives": {
            "okx": {
                "eth_open_interest_volume": eth_open_interest,
                "eth_liquidations": eth_liquidations,
            }
        },
        "daily_report": daily_report,
    }


def save_snapshot(data: Dict[str, Any], output_path: Path = DEFAULT_OUTPUT) -> Path:
    output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def main() -> None:
    session = _build_session()
    snapshot = aggregate_snapshot(session)
    path = save_snapshot(snapshot, DEFAULT_OUTPUT)
    print(f"快照已生成：{path}（UTC {snapshot['generated_at']}）")


if __name__ == "__main__":
    main()


def fetch_fear_greed_index(session: requests.Session, limit: int = 15) -> Dict[str, Any]:
    resp = _fetch_json(session, "https://api.alternative.me/fng/", params={"limit": str(limit)})
    series: List[Dict[str, Any]] = []
    data = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(data, list):
        for item in data:
            val = item.get("value")
            try:
                val_f = float(val) if val is not None else None
            except (TypeError, ValueError):
                val_f = None
            series.append({
                "timestamp": item.get("timestamp"),
                "value": val_f,
                "classification": item.get("value_classification"),
            })
    latest = series[0] if series else None
    return {"source": "https://api.alternative.me/fng/", "raw": resp, "latest": latest, "series": series}


def _bridge_topN(flows: Dict[str, Any], top_n: int = 5) -> List[Dict[str, Any]]:
    protos: List[Dict[str, Any]] = []
    if isinstance(flows.get("bridge_top_protocols"), list) and flows["bridge_top_protocols"]:
        protos = flows["bridge_top_protocols"]
    elif isinstance(flows.get("bridge_fallback"), dict) and isinstance(flows["bridge_fallback"].get("protocols"), list):
        protos = flows["bridge_fallback"]["protocols"]
    def vol1d(p: Dict[str, Any]) -> float:
        v = p.get("volume_1d")
        return float(v) if isinstance(v, (int, float)) else 0.0
    protos_sorted = sorted(protos, key=vol1d, reverse=True)
    return [{
        "name": p.get("name"),
        "volume_1d": p.get("volume_1d"),
        "volume_7d": p.get("volume_7d"),
        "volume_30d": p.get("volume_30d"),
    } for p in protos_sorted[:top_n]]


def build_daily_report(defi_eth: Dict[str, Any], defi_btc: Dict[str, Any], fear: Dict[str, Any], top_n: int = 5) -> Dict[str, Any]:
    def fmt_sc(sc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        summ = sc.get("summary") if isinstance(sc, dict) else None
        latest = summ.get("latest") if isinstance(summ, dict) else None
        change = summ.get("change") if isinstance(summ, dict) else None
        previous = summ.get("previous") if isinstance(summ, dict) else None
        note = summ.get("note") if isinstance(summ, dict) else None
        return {"latest": latest, "previous": previous, "change": change, "note": note}
    eth_sc = fmt_sc(defi_eth.get("stablecoin"))
    btc_sc = fmt_sc(defi_btc.get("stablecoin"))
    eth_top = _bridge_topN(defi_eth, top_n)
    btc_top = _bridge_topN(defi_btc, top_n)
    eth_sum = defi_eth.get("bridge_summary")
    btc_sum = defi_btc.get("bridge_summary")
    fear_latest = fear.get("latest")
    eth_latest_val = eth_sc.get('latest')
    eth_latest_val = eth_latest_val.get('value') if isinstance(eth_latest_val, dict) else None
    btc_latest_val = btc_sc.get('latest') if isinstance(btc_sc, dict) else None
    btc_latest_val = btc_latest_val.get('value') if isinstance(btc_latest_val, dict) else None
    sc_para = f"稳定币 — ETH 最新: {eth_latest_val}, 环比: {eth_sc.get('change')}。"
    eth_top_str = ", ".join([f"{p.get('name')}: {p.get('volume_1d')}" for p in eth_top])
    btc_top_str = ", ".join([f"{p.get('name')}: {p.get('volume_1d')}" for p in btc_top])
    bridges_para = (
        f"桥接 — ETH 量 (1d/7d/30d): {tuple((eth_sum or {}).get(k) for k in ('volume_1d','volume_7d','volume_30d'))}; "
        f"Top {top_n}: {eth_top_str}。 "
        f"BTC 量 (1d/7d/30d): {tuple((btc_sum or {}).get(k) for k in ('volume_1d','volume_7d','volume_30d'))}; "
        f"Top {top_n}: {btc_top_str}."
    )
    fear_para = (
        f"BTC 恐慌指数 — 最新值: {fear_latest.get('value') if isinstance(fear_latest, dict) else None}, "
        f"分类: {fear_latest.get('classification') if isinstance(fear_latest, dict) else None} (近 {len(fear.get('series') or [])} 天)。"
    )
    return {
        "stablecoins": {"ethereum": eth_sc, "bitcoin": btc_sc, "paragraph": sc_para},
        "bridges": {
            "ethereum": {"summary": eth_sum, "top": eth_top},
            "bitcoin": {"summary": btc_sum, "top": btc_top},
            "paragraph": bridges_para,
        },
        "fear_greed": {"series": fear.get("series"), "latest": fear_latest, "paragraph": fear_para},
    }

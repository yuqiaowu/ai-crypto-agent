import requests
import time

def test_url(name, url):
    print(f"Testing {name} ({url})...")
    try:
        start = time.time()
        response = requests.get(url, timeout=10)
        duration = time.time() - start
        print(f"  ✅ Status: {response.status_code}, Time: {duration:.2f}s")
        if response.status_code != 200:
            print(f"  ⚠️ Response: {response.text[:200]}")
    except Exception as e:
        print(f"  ❌ Failed: {e}")

if __name__ == "__main__":
    print("--- Connectivity Test ---")
    test_url("OKX Public API", "https://www.okx.com/api/v5/public/instruments?instType=SPOT")
    test_url("Binance Public API", "https://api.binance.com/api/v3/ping")
    test_url("Yahoo Finance", "https://query2.finance.yahoo.com/v8/finance/chart/BTC-USD")
    print("-----------------------")

!pip install aiohttp nest_asyncio pandas

import aiohttp
import asyncio
import pandas as pd
import nest_asyncio
from datetime import datetime, timedelta
from IPython.display import display  # for Colab

nest_asyncio.apply()

API_KEY = "cXcAYHG065BCC9xr6iTMMyhFlhZ2M7Uh"
OORVOL_THRESHOLD = 1.2  # Show all OORVOLs for now
MIN_PRICE = 2
MIN_AVG_VOLUME = 1_000_000
OOH_PRICE_THRESHOLD = 2  # % change vs yesterday's close

TODAY = datetime.today().strftime('%Y-%m-%d')
YESTERDAY = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
TWO_DAYS_AGO = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')
START_DATE = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

async def fetch(session, url):
    try:
        async with session.get(url, timeout=10) as response:
            return await response.json()
    except Exception as e:
        print(f"Request error: {e}")
        return {}

async def get_grouped_data_with_metadata(session):
    group_url = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{YESTERDAY}?adjusted=true&apiKey={API_KEY}"
    )
    prev_url = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{TWO_DAYS_AGO}?adjusted=true&apiKey={API_KEY}"
    )

    today_data = await fetch(session, group_url)
    prev_data = await fetch(session, prev_url)

    today_results = {item["T"]: item["c"] for item in today_data.get("results", [])}
    prev_results = {item["T"]: item["c"] for item in prev_data.get("results", [])}

    metadata = {}
    for ticker, today_close in today_results.items():
        prev_close = prev_results.get(ticker)
        if prev_close and today_close >= MIN_PRICE:
            pct_change = (today_close - prev_close) / prev_close * 100
            metadata[ticker] = {
                "close": round(today_close, 2),
                "pct_change": round(pct_change, 2),
                "prev_close": round(prev_close, 2),
            }
    return metadata

async def fetch_21d_avg_volume(session, ticker):
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
        f"{START_DATE}/{YESTERDAY}?adjusted=true&sort=desc&limit=30&apiKey={API_KEY}"
    )
    data = await fetch(session, url)
    volumes = [d['v'] for d in data.get("results", [])][-21:]
    if len(volumes) >= 21:
        avg_vol = sum(volumes) / 21
        if avg_vol >= MIN_AVG_VOLUME:
            return (ticker, avg_vol)
    return None

async def fetch_ooh_volume(session, ticker):
    url_yesterday = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/"
        f"{YESTERDAY}/{YESTERDAY}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
    )
    url_today = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/"
        f"{TODAY}/{TODAY}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
    )

    dy = await fetch(session, url_yesterday)
    dt = await fetch(session, url_today)

    post, pre = 0, 0
    pre_prices, post_prices = [], []
    pre_times, post_times = [], []

    for c in dy.get("results", []):
        ts = c["t"] / 1000
        dtm = datetime.fromtimestamp(ts)
        if dtm.hour >= 16:
            post += c["v"]
            post_times.append(dtm)
            post_prices.append(c["c"])

    for c in dt.get("results", []):
        ts = c["t"] / 1000
        dtm = datetime.fromtimestamp(ts)
        if dtm.hour < 9 or (dtm.hour == 9 and dtm.minute < 30):
            pre += c["v"]
            pre_times.append(dtm)
            pre_prices.append(c["c"])

    return (
        ticker,
        pre + post,
        pre_times[0] if pre_times else None,
        pre_times[-1] if pre_times else None,
        post_times[0] if post_times else None,
        post_times[-1] if post_times else None,
        pre_prices[0] if pre_prices else None,
        post_prices[-1] if post_prices else None,
    )

async def main_async():
    async with aiohttp.ClientSession() as session:
        print("🔄 Fetching grouped data...")
        metadata_map = await get_grouped_data_with_metadata(session)
        tickers = list(metadata_map.keys())

        print(f"🔎 Checking {len(tickers)} tickers for avg volume...")
        volume_tasks = [fetch_21d_avg_volume(session, t) for t in tickers]
        volume_results = await asyncio.gather(*volume_tasks)
        volume_map = {t: v for result in volume_results if result for t, v in [result]}

        print(f"📉 {len(volume_map)} tickers passed volume filter. Fetching OOH volume...")
        ooh_tasks = [fetch_ooh_volume(session, t) for t in volume_map]
        ooh_results = await asyncio.gather(*ooh_tasks)

        print("✅ Calculating OORVOL and applying filters...")
        results = []
        for (t, ooh_vol, pre_start, pre_end, post_start, post_end, pre_price, post_price) in ooh_results:
            if not pre_price or not post_price:
                continue
            avg_vol = volume_map[t]
            oorvol = ooh_vol / avg_vol
            ooh_pct = (pre_price - metadata_map[t]["prev_close"]) / metadata_map[t]["prev_close"] * 100

            if oorvol > OORVOL_THRESHOLD and ooh_pct > OOH_PRICE_THRESHOLD:
                results.append({
                    "Ticker": t,
                    "21D Avg Volume": int(avg_vol),
                    "OOH Volume": int(ooh_vol),
                    "OORVOL": round(oorvol, 2),
                    "OOH % Change": round(ooh_pct, 2),
                    "Last Close": metadata_map[t]["prev_close"],
                    "Daily % Change": metadata_map[t]["pct_change"],
                    "Pre Start": pre_start,
                    "Pre End": pre_end,
                    "Post Start": post_start,
                    "Post End": post_end,
                })

        df = pd.DataFrame(results)
        if not df.empty:
            df.sort_values("OORVOL", ascending=False, inplace=True)
            df.to_csv("oorvol_scan_results.csv", index=False)
            print(f"📁 Results saved to 'oorvol_scan_results.csv'")
            print(f"✅ Found {len(df)} qualifying stocks.")
            display(df)
        else:
            print("⚠️ No qualifying stocks. Check filters or run at a different time.")

if __name__ == "__main__":
    asyncio.run(main_async())

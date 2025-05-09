import aiohttp
import asyncio
import pandas as pd
import nest_asyncio
from datetime import datetime, timedelta

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
    group_url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{YESTERDAY}?adjusted=true&apiKey={API_KEY}"
    prev_url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{TWO_DAYS_AGO}?adjusted=true&apiKey={API_KEY}"

    today_data = await fetch(session, group_url)
    prev_data = await fetch(session, prev_url)

    today_results = {item["T"]: item["c"] for item in today_data.get("results", [])}
    prev_results = {item["T"]: item["c"] for item in prev_data.get("results", [])}

    metadata = {}
    for ticker, today_close in today_results.items():
        if today_close and ticker in prev_results:
            prev_close = prev_results[ticker]
            if prev_close and today_close >= MIN_PRICE:
                pct_change = ((today_close - prev_close) / prev_close) * 100
                metadata[ticker] = {
                    "close": round(today_close, 2),
                    "pct_change": round(pct_change, 2),
                    "prev_close": round(prev_close, 2)
                }
    return metadata

async def fetch_21d_avg_volume(session, ticker):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{START_DATE}/{YESTERDAY}?adjusted=true&sort=desc&limit=30&apiKey={API_KEY}"
    data = await fetch(session, url)
    volumes = [d['v'] for d in data.get("results", [])][-21:]
    if len(volumes) >= 21:
        avg_vol = sum(volumes) / 21
        if avg_vol >= MIN_AVG_VOLUME:
            return (ticker, avg_vol)
    return None

async def fetch_ooh_volume(session, ticker):
    url_yesterday = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{YESTERDAY}/{YESTERDAY}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
    url_today = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{TODAY}/{TODAY}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"

    data_yesterday = await fetch(session, url_yesterday)
    data_today = await fetch(session, url_today)

    post, pre = 0, 0
    pre_prices, post_prices = [], []
    pre_times, post_times = [], []

    if "results" in data_yesterday:
        for c in data_yesterday["results"]:
            ts = c["t"] / 1000
            dt = datetime.fromtimestamp(ts)
            if dt.hour >= 16:
                post += c["v"]
                post_times.append(dt)
                post_prices.append(c["c"])

    if "results" in data_today:
        for c in data_today["results"]:
            ts = c["t"] / 1000
            dt = datetime.fromtimestamp(ts)
            if dt.hour < 9 or (dt.hour == 9 and dt.minute < 30):
                pre += c["v"]
                pre_times.append(dt)
                pre_prices.append(c["c"])

    return (
        ticker,
        pre + post,
        pre_times[0] if pre_times else None,
        pre_times[-1] if pre_times else None,
        post_times[0] if post_times else None,
        post_times[-1] if post_times else None,
        pre_prices[0] if pre_prices else None,
        post_prices[-1] if post_prices else None
    )

async def main_async():
    async with aiohttp.ClientSession() as session:
        print("🔄 Fetching grouped data...")
        metadata_map = await get_grouped_data_with_metadata(session)
        tickers = list(metadata_map.keys())

        print(f"🔎 Checking {len(tickers)} tickers for avg volume...")
        tasks_volume = [fetch_21d_avg_volume(session, t) for t in tickers]
        volume_results = await asyncio.gather(*tasks_volume)
        volume_map = {t: v for result in volume_results if result is not None for t, v in [result]}

        print(f"📉 {len(volume_map)} tickers passed volume filter. Checking OOH volume...")
        tasks_ooh = [fetch_ooh_volume(session, t) for t in volume_map]
        ooh_results = await asyncio.gather(*tasks_ooh)
        ooh_map = {
            t: {
                "volume": vol,
                "pre_start": pre_start,
                "pre_end": pre_end,
                "post_start": post_start,
                "post_end": post_end,
                "pre_price": pre_price,
                "post_price": post_price
            }
            for t, vol, pre_start, pre_end, post_start, post_end, pre_price, post_price in ooh_results
        }

        print("✅ Calculating OORVOL and applying filters...")
        results = []
        for ticker, avg_vol in volume_map.items():
            ooh_data = ooh_map.get(ticker, {})
            meta = metadata_map.get(ticker, {})
            ooh_vol = ooh_data.get("volume", 0)
            pre_price = ooh_data.get("pre_price")
            post_price = ooh_data.get("post_price")
            last_close = meta.get("close")

            if not pre_price or not post_price or not last_close:
                continue

            ooh_pct_change = ((pre_price - last_close) / last_close) * 100
            oorvol = ooh_vol / avg_vol if avg_vol else 0

            if ooh_pct_change < OOH_PRICE_THRESHOLD:
                continue
            if oorvol < OORVOL_THRESHOLD:
                continue

            results.append({
                "Ticker": ticker,
                "21D Avg Volume": int(avg_vol),
                "OOH Volume": int(ooh_vol),
                "OORVOL": round(oorvol, 2),
                "OOH Price Change": round(pre_price - last_close, 2),
                "OOH % Change": round(ooh_pct_change, 2),
                "Last Close": last_close,
                "Daily % Change": meta.get("pct_change"),
                "Pre Start": ooh_data.get("pre_start"),
                "Pre End": ooh_data.get("pre_end"),
                "Post Start": ooh_data.get("post_start"),
                "Post End": ooh_data.get("post_end")
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

await main_async()

import streamlit as st
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import nest_asyncio

nest_asyncio.apply()

# ——— CONFIG ———
API_KEY = st.secrets["API_KEY"]         # Make sure you’ve set this in Streamlit Cloud secrets
OORVOL_THRESHOLD = 1.2
MIN_AVG_VOLUME = 1_000_000
MIN_PRICE = 2.0

TODAY = datetime.today().strftime("%Y-%m-%d")
YESTERDAY = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
TWO_DAYS_AGO = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

st.set_page_config(page_title="OOH Volume Scanner", layout="wide")
st.title("📊 Out-of-Hours Volume & Price Breakout Scanner")

# ——— HELPER FUNCTIONS ———
async def fetch(session, url):
    try:
        async with session.get(url, timeout=10) as resp:
            return await resp.json()
    except Exception as e:
        st.error(f"Fetch error: {e}")
        return {}

async def get_grouped_data_with_metadata(session):
    url_today = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{YESTERDAY}?adjusted=true&apiKey={API_KEY}"
    )
    url_prev = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{TWO_DAYS_AGO}?adjusted=true&apiKey={API_KEY}"
    )
    today_data = await fetch(session, url_today)
    prev_data = await fetch(session, url_prev)

    today_map = {r["T"]: r["c"] for r in today_data.get("results", [])}
    prev_map = {r["T"]: r["c"] for r in prev_data.get("results", [])}

    md = {}
    for ticker, close in today_map.items():
        prev_close = prev_map.get(ticker)
        if prev_close and close >= MIN_PRICE:
            pct = ((close - prev_close) / prev_close) * 100
            md[ticker] = {
                "close": round(close, 2),
                "pct_change": round(pct, 2),
                "prev_close": round(prev_close, 2),
            }
    return md

async def fetch_21d_avg_volume(session, ticker):
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
        f"{START_DATE}/{YESTERDAY}?adjusted=true&sort=desc&limit=30&apiKey={API_KEY}"
    )
    data = await fetch(session, url)
    vols = [d["v"] for d in data.get("results", [])][-21:]
    if len(vols) >= 21:
        avg = sum(vols) / 21
        if avg >= MIN_AVG_VOLUME:
            return ticker, avg
    return None

async def fetch_ooh_volume(session, ticker):
    url_y = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/"
        f"{YESTERDAY}/{YESTERDAY}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
    )
    url_t = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/"
        f"{TODAY}/{TODAY}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
    )
    dy = await fetch(session, url_y)
    dt = await fetch(session, url_t)

    post_v = pre_v = 0
    pre_prices = []
    post_prices = []
    pre_times = []
    post_times = []

    for c in dy.get("results", []):
        tm = datetime.fromtimestamp(c["t"] / 1000)
        if tm.hour >= 16:
            post_v += c["v"]
            post_times.append(tm)
            post_prices.append(c["c"])
    for c in dt.get("results", []):
        tm = datetime.fromtimestamp(c["t"] / 1000)
        if tm.hour < 9 or (tm.hour == 9 and tm.minute < 30):
            pre_v += c["v"]
            pre_times.append(tm)
            pre_prices.append(c["c"])

    return (
        ticker,
        pre_v + post_v,
        pre_times[0] if pre_times else None,
        pre_times[-1] if pre_times else None,
        post_times[0] if post_times else None,
        post_times[-1] if post_times else None,
        pre_prices[0] if pre_prices else None,
        post_prices[-1] if post_prices else None,
    )

# ——— MAIN ASYNC WORKFLOW ———
async def main_async():
    async with aiohttp.ClientSession() as s:
        md = await get_grouped_data_with_metadata(s)
        tickers = list(md.keys())

        # 21-day volume filter
        vols = await asyncio.gather(*(fetch_21d_avg_volume(s, t) for t in tickers))
        volume_map = {t: v for t, v in vols if t}

        # OOH volume and price breakout filter
        oohs = await asyncio.gather(*(fetch_ooh_volume(s, t) for t in volume_map))
        results = []
        for t, avg in volume_map.items():
            # find matching tuple
            rec = next(x for x in oohs if x[0] == t)
            _, ooh_vol, pre_start, pre_end, post_start, post_end, pre_price, post_price = rec

            if not pre_price or not post_price: 
                continue

            oorvol = ooh_vol / avg if avg else 0
            ooh_pct = (pre_price - md[t]["prev_close"]) / md[t]["prev_close"] * 100

            if oorvol > OORVOL_THRESHOLD and ooh_pct > 2:
                results.append({
                    "Ticker": t,
                    "21D Avg Volume": int(avg),
                    "OOH Volume": int(ooh_vol),
                    "OORVOL": round(oorvol, 2),
                    "OOH % Change": round(ooh_pct, 2),
                    "Last Close": md[t]["prev_close"],
                    "Daily % Change": md[t]["pct_change"],
                    "Pre Start": pre_start,
                    "Pre End": pre_end,
                    "Post Start": post_start,
                    "Post End": post_end,
                })

        return pd.DataFrame(results).sort_values("OORVOL", ascending=False)

# ——— RUN & DISPLAY ———
with st.spinner("Running scan... this may take a minute"):
    df = asyncio.run(main_async())

if not df.empty:
    st.success(f"✅ Found {len(df)} qualifying stocks")
    st.dataframe(df, use_container_width=True)
else:
    st.warning("⚠️ No qualifying stocks met the criteria today.")

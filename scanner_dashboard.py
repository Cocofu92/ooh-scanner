import streamlit as st
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import nest_asyncio

nest_asyncio.apply()

API_KEY = st.secrets["API_KEY"]

st.set_page_config(page_title="OOH Volume Scanner", layout="wide")
st.title("📊 Out-of-Hours Volume & Price Breakout Scanner")

# ← This is the single line you requested:
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

OORVOL_THRESHOLD = st.sidebar.slider("Min OORVOL", 0.0, 5.0, 1.2, 0.1)
MIN_AVG_VOLUME   = st.sidebar.number_input("Min 21-Day Avg Volume", value=1_000_000)
MIN_PRICE        = st.sidebar.number_input("Min Price ($)", value=2.0)
OOH_PRICE_THRESHOLD = st.sidebar.slider("OOH Price Change vs Close (%)", 0, 20, 2)
REFRESH_MINUTES  = st.sidebar.slider("Refresh every X minutes", 1, 60, 5)

TODAY      = datetime.today().strftime('%Y-%m-%d')
YESTERDAY  = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
TWO_DAYS_AGO = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')
START_DATE = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

async def fetch(_session, url):
    try:
        async with _session.get(url, timeout=10) as resp:
            return await resp.json()
    except Exception as e:
        st.error(f"Request error: {e}")
        return {}

async def get_grouped_data_with_metadata(session):
    today_url = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{YESTERDAY}?adjusted=true&apiKey={API_KEY}"
    )
    prev_url = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{TWO_DAYS_AGO}?adjusted=true&apiKey={API_KEY}"
    )
    today_data = await fetch(session, today_url)
    prev_data  = await fetch(session, prev_url)

    today_map = {r["T"]: r["c"] for r in today_data.get("results", [])}
    prev_map  = {r["T"]: r["c"] for r in prev_data.get("results", [])}

    metadata = {}
    for t, close in today_map.items():
        prev_close = prev_map.get(t)
        if prev_close and close >= MIN_PRICE:
            pct = (close - prev_close) / prev_close * 100
            metadata[t] = {
                "close": round(close, 2),
                "pct_change": round(pct, 2),
                "prev_close": round(prev_close, 2),
            }
    return metadata

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
    pre_times  = []
    post_times = []

    for c in dy.get("results", []):
        tm = datetime.fromtimestamp(c["t"] / 1000)
        if tm.hour >= 16:
            post_v += c["v"]
            post_prices.append(c["c"])
    for c in dt.get("results", []):
        tm = datetime.fromtimestamp(c["t"] / 1000)
        if tm.hour < 9 or (tm.hour == 9 and tm.minute < 30):
            pre_v += c["v"]
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

async def run_scanner():
    async with aiohttp.ClientSession() as session:
        meta = await get_grouped_data_with_metadata(session)
        tickers = list(meta.keys())

        vols = await asyncio.gather(*(fetch_21d_avg_volume(session, t) for t in tickers))
        vol_map = {t: v for t, v in vols if t}

        oohs = await asyncio.gather(*(fetch_ooh_volume(session, t) for t in vol_map))
        df_rows = []
        for (t, ooh_vol, _, _, _, _, pre_price, _) in oohs:
            avg_vol = vol_map[t]
            oorvol = ooh_vol / avg_vol
            ooh_pct = (pre_price - meta[t]["prev_close"]) / meta[t]["prev_close"] * 100
            if oorvol > OORVOL_THRESHOLD and ooh_pct > OOH_PRICE_THRESHOLD:
                df_rows.append({
                    "Ticker": t,
                    "21D Avg Volume": int(avg_vol),
                    "OOH Volume": int(ooh_vol),
                    "OORVOL": round(oorvol, 2),
                    "OOH % Change": round(ooh_pct, 2),
                    "Last Close": meta[t]["prev_close"],
                    "Daily % Change": meta[t]["pct_change"],
                })
        return pd.DataFrame(df_rows).sort_values("OORVOL", ascending=False)

# ——— RUN & DISPLAY ———
with st.spinner("Running scan... this may take 1–2 minutes"):
    df = asyncio.run(run_scanner())

if not df.empty:
    st.success(f"✅ Found {len(df)} qualifying stocks")
    st.dataframe(df, use_container_width=True)
else:
    st.warning("⚠️ No qualifying stocks met the criteria today.")

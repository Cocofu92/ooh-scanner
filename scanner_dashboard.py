import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta

# ─── CONFIG ───
API_KEY        = st.secrets["API_KEY"]
OORVOL_THRESH  = 1.2
MIN_AVG_VOLUME = 1_000_000
MIN_PRICE      = 2.0
OOH_PCT_THRESH = 2.0

TODAY     = datetime.today().strftime("%Y-%m-%d")
YESTERDAY = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

st.set_page_config(page_title="OOH Volume Scanner", layout="wide")
st.title("📊 Out-of-Hours Volume & Price Breakout Scanner")
st.caption(f"Last updated: {datetime.now():%Y-%m-%d %H:%M:%S}")

# ─── 1) Bulk snapshot of today’s volume ───
@st.cache_data(ttl=300)
def get_volume_snapshot():
    url = (
        "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        f"?apiKey={API_KEY}"
    )
    data = requests.get(url, timeout=15).json().get("tickers", [])
    return {
        t["ticker"]: t["day"]["v"]
        for t in data
        if t.get("day", {}).get("v", 0) >= MIN_AVG_VOLUME
    }

# ─── 2) Yesterday’s close filtered by MIN_PRICE ───
@st.cache_data(ttl=3600)
def get_metadata():
    url = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{YESTERDAY}?adjusted=true&apiKey={API_KEY}"
    )
    res = requests.get(url, timeout=15).json().get("results", [])
    return {
        r["T"]: r["c"]
        for r in res
        if r["c"] >= MIN_PRICE
    }

# ─── 3) Scan OOH minute bars and apply filters ───
def scan_ooh(vol_map, meta_map):
    rows = []
    for ticker, today_vol in vol_map.items():
        prev_close = meta_map.get(ticker)
        if prev_close is None:
            continue

        # Fetch post-market (yesterday) and pre-market (today) minute bars
        url_y = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/"
            f"{YESTERDAY}/{YESTERDAY}"
            f"?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
        )
        url_t = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/"
            f"{TODAY}/{TODAY}"
            f"?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
        )

        dy = requests.get(url_y, timeout=10).json().get("results", [])
        dt = requests.get(url_t, timeout=10).json().get("results", [])

        post_vol = pre_vol = 0
        pre_prices = []; post_prices = []

        for bar in dy:
            tm = datetime.fromtimestamp(bar["t"]/1000)
            if tm.hour >= 16:
                post_vol += bar["v"]
                post_prices.append(bar["c"])
        for bar in dt:
            tm = datetime.fromtimestamp(bar["t"]/1000)
            if tm.hour < 9 or (tm.hour == 9 and tm.minute < 30):
                pre_vol += bar["v"]
                pre_prices.append(bar["c"])

        if not pre_prices or not post_prices:
            continue

        total_ooh = pre_vol + post_vol
        oorvol    = total_ooh / today_vol
        ooh_pct   = (pre_prices[-1] - prev_close) / prev_close * 100

        if oorvol > OORVOL_THRESH and ooh_pct > OOH_PCT_THRESH:
            rows.append({
                "Ticker":        ticker,
                "Today's Vol":   int(today_vol),
                "OOH Volume":    int(total_ooh),
                "OORVOL":        round(oorvol, 2),
                "OOH % Change":  round(ooh_pct, 2),
                "Prev Close":    round(prev_close, 2),
            })

    # Build DataFrame and only sort if column exists
    df = pd.DataFrame(rows)
    if "OORVOL" in df.columns:
        df = df.sort_values("OORVOL", ascending=False)
    return df

# ─── MAIN ───
with st.spinner("Running scan… this may take 30–60 seconds"):
    volume_map = get_volume_snapshot()
    metadata   = get_metadata()
    df         = scan_ooh(volume_map, metadata)

if not df.empty:
    st.success(f"✅ Found {len(df)} qualifying stocks")
    st.dataframe(df, use_container_width=True)
else:
    st.warning("⚠️ No qualifying stocks met the criteria today.")

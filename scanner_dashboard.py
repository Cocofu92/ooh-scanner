import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta

# ─── CONFIG ───
API_KEY        = st.secrets["API_KEY"]
OORVOL_THRESH  = 1.2
MIN_AVG_VOLUME = 1_000_000  # here using today’s volume as a proxy
MIN_PRICE      = 2.0
OOH_PCT_THRESH = 2.0        # % price change

# Date strings
TODAY     = datetime.today().strftime("%Y-%m-%d")
YESTERDAY = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# ─── PAGE SETUP ───
st.set_page_config(page_title="OOH Volume Scanner", layout="wide")
st.title("📊 Out-of-Hours Volume & Price Breakout Scanner")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ─── 1) Fetch bulk snapshot of today’s volumes ───
@st.cache_data(ttl=300)
def get_volume_snapshot():
    url = (
        "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks"
        f"?apiKey={API_KEY}"
    )
    data = requests.get(url, timeout=15).json().get("tickers", [])
    # build { ticker: volume } only for tickers above threshold
    return {
        t["ticker"]: t["day"]["v"]
        for t in data
        if t.get("day", {}).get("v", 0) >= MIN_AVG_VOLUME
    }

# ─── 2) Fetch grouped metadata (yesterday close) ───
@st.cache_data(ttl=3600)
def get_metadata():
    url = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{YESTERDAY}?adjusted=true&apiKey={API_KEY}"
    )
    res = requests.get(url, timeout=15).json().get("results", [])
    return { r["T"]: r["c"] for r in res if r["c"] >= MIN_PRICE }

# ─── 3) Scan OOH minute bars and apply filters ───
def scan_ooh(vol_map, meta_map):
    rows = []
    for ticker, avg_vol in vol_map.items():
        prev_close = meta_map.get(ticker)
        if not prev_close:
            continue

        # fetch yesterday’s post-market minutes
        url_y = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/"
            f"{YESTERDAY}/{YESTERDAY}"
            f"?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
        )
        # fetch today’s pre-market minutes
        url_t = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/"
            f"{TODAY}/{TODAY}"
            f"?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
        )
        dyn = requests.get(url_y, timeout=10).json().get("results", [])
        dtn = requests.get(url_t, timeout=10).json().get("results", [])

        post_vol = pre_vol = 0
        pre_prices = []; post_prices = []
        for bar in dyn:
            dtm = datetime.fromtimestamp(bar["t"] / 1000)
            if dtm.hour >= 16:
                post_vol += bar["v"]
                post_prices.append(bar["c"])
        for bar in dtn:
            dtm = datetime.fromtimestamp(bar["t"] / 1000)
            if dtm.hour < 9 or (dtm.hour == 9 and dtm.minute < 30):
                pre_vol += bar["v"]
                pre_prices.append(bar["c"])

        if not pre_prices or not post_prices:
            continue

        total_ooh = pre_vol + post_vol
        oorvol    = total_ooh / avg_vol
        ooh_pct   = (pre_prices[-1] - prev_close) / prev_close * 100

        if oorvol > OORVOL_THRESH and ooh_pct > OOH_PCT_THRESH:
            rows.append({
                "Ticker":          ticker,
                "Today's Vol":     int(avg_vol),
                "OOH Volume":      int(total_ooh),
                "OORVOL":          round(oorvol, 2),
                "OOH % Change":    round(ooh_pct, 2),
                "Prev Close":      round(prev_close, 2),
            })

    return pd.DataFrame(rows).sort_values("OORVOL", ascending=False)

# ─── MAIN ───
with st.spinner("Running scan… this may take 30–60 seconds"):
    volume_map = get_volume_snapshot()
    meta_map   = get_metadata()
    df         = scan_ooh(volume_map, meta_map)

if not df.empty:
    st.success(f"✅ Found {len(df)} qualifying stocks")
    st.dataframe(df, use_container_width=True)
else:
    st.warning("⚠️ No qualifying stocks met the criteria today.")

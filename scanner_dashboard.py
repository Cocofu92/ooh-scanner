import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta

# ——— CONFIG ———
API_KEY = st.secrets["API_KEY"]
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
@st.cache_data(ttl=3600)
def get_metadata():
    """Fetch grouped close prices for yesterday vs two days ago."""
    urls = {
        "today": f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{YESTERDAY}"
                 f"?adjusted=true&apiKey={API_KEY}",
        "prev":  f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{TWO_DAYS_AGO}"
                 f"?adjusted=true&apiKey={API_KEY}"
    }
    md = {}
    today = requests.get(urls["today"], timeout=10).json().get("results", [])
    prev  = requests.get(urls["prev"],  timeout=10).json().get("results", [])
    prev_map = {r["T"]: r["c"] for r in prev}

    for r in today:
        t = r["T"]; close = r["c"]
        prev_close = prev_map.get(t)
        if prev_close and close >= MIN_PRICE:
            pct = (close - prev_close) / prev_close * 100
            md[t] = {
                "close": round(close,2),
                "prev_close": round(prev_close,2),
                "pct_change": round(pct,2)
            }
    return md

@st.cache_data(ttl=3600)
def get_21d_avg_volume(ticker_list):
    """Fetch and filter by 21-day avg volume."""
    vol_map = {}
    for t in ticker_list:
        url = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/"
               f"{START_DATE}/{YESTERDAY}?adjusted=true&sort=desc&limit=30&apiKey={API_KEY}")
        data = requests.get(url, timeout=10).json().get("results", [])
        vols = [d["v"] for d in data][-21:]
        if len(vols)==21:
            avg = sum(vols)/21
            if avg >= MIN_AVG_VOLUME:
                vol_map[t] = avg
    return vol_map

def get_ooh_data(vol_map):
    """Fetch post-market (yesterday) + pre-market (today) minute bars."""
    results = []
    for t, avg in vol_map.items():
        # minute bars
        url_y = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/minute/"
                 f"{YESTERDAY}/{YESTERDAY}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}")
        url_t = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/minute/"
                 f"{TODAY}/{TODAY}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}")
        dy = requests.get(url_y, timeout=10).json().get("results", [])
        dt = requests.get(url_t, timeout=10).json().get("results", [])

        post_vol = pre_vol = 0
        pre_prices = []; post_prices = []
        for c in dy:
            tm = datetime.fromtimestamp(c["t"]/1000)
            if tm.hour>=16:
                post_vol += c["v"]
                post_prices.append(c["c"])
        for c in dt:
            tm = datetime.fromtimestamp(c["t"]/1000)
            if tm.hour<9 or (tm.hour==9 and tm.minute<30):
                pre_vol += c["v"]
                pre_prices.append(c["c"])

        if not pre_prices or not post_prices:
            continue

        oorvol = (pre_vol+post_vol)/avg
        ooh_pct = (pre_prices[-1] - metadata[t]["prev_close"]) / metadata[t]["prev_close"] * 100

        if oorvol > OORVOL_THRESHOLD and ooh_pct>2:
            results.append({
                "Ticker": t,
                "21D Avg Volume": int(avg),
                "OOH Volume": int(pre_vol+post_vol),
                "OORVOL": round(oorvol,2),
                "OOH % Change": round(ooh_pct,2),
                "Last Close": metadata[t]["prev_close"],
                "Daily % Change": metadata[t]["pct_change"],
            })

    return pd.DataFrame(results).sort_values("OORVOL", ascending=False)

# ——— MAIN & DISPLAY ———
with st.spinner("Running scan..."):
    metadata = get_metadata()
    vol_map = get_21d_avg_volume(list(metadata.keys()))
    df = get_ooh_data(vol_map)

if not df.empty:
    st.success(f"✅ Found {len(df)} qualifying stocks")
    st.dataframe(df, use_container_width=True)
else:
    st.warning("⚠️ No qualifying stocks met the criteria today.")

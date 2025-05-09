import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta

# ——— CONFIGURATION ———
API_KEY       = st.secrets["API_KEY"]    # ← Set this in Streamlit Cloud Secrets
OORVOL_THRESH = 1.2
MIN_AVG_VOL   = 1_000_000
MIN_PRICE     = 2.0

TODAY      = datetime.today().strftime("%Y-%m-%d")
YESTERDAY  = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
TWO_DAYS   = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")

st.set_page_config(page_title="OOH Volume Scanner", layout="wide")
st.title("📊 Out-of-Hours Volume & Price Breakout Scanner")

# ——— CACHED METADATA ———
@st.cache_data(ttl=3600)
def get_metadata():
    url_today = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{YESTERDAY}?adjusted=true&apiKey={API_KEY}"
    )
    url_prev = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{TWO_DAYS}?adjusted=true&apiKey={API_KEY}"
    )
    today = requests.get(url_today, timeout=10).json().get("results", [])
    prev  = requests.get(url_prev,  timeout=10).json().get("results", [])
    prev_map = {r["T"]: r["c"] for r in prev}

    md = {}
    for r in today:
        t = r["T"]; close = r["c"]
        p = prev_map.get(t)
        if p and close >= MIN_PRICE:
            pct = (close - p) / p * 100
            md[t] = {
                "prev_close": round(p, 2),
                "close":      round(close, 2),
                "pct_change": round(pct, 2),
            }
    return md

# ——— CACHED 21-DAY VOLUME ———
@st.cache_data(ttl=3600)
def get_avg_volumes(tickers):
    vmap = {}
    for t in tickers:
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/"
            f"{START_DATE}/{YESTERDAY}"
            f"?adjusted=true&sort=desc&limit=30&apiKey={API_KEY}"
        )
        res = requests.get(url, timeout=10).json().get("results", [])
        vols = [d["v"] for d in res][-21:]
        if len(vols) == 21:
            avg = sum(vols) / 21
            if avg >= MIN_AVG_VOL:
                vmap[t] = avg
    return vmap

# ——— SCAN OOH ———
def scan_ooh(vmap, md):
    rows = []
    for t, avg in vmap.items():
        url_y = (
            f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/minute/"
            f"{YESTERDAY}/{YESTERDAY}"
            f"?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
        )
        url_t = (
            f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/minute/"
            f"{TODAY}/{TODAY}"
            f"?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
        )
        dy = requests.get(url_y, timeout=10).json().get("results", [])
        dt = requests.get(url_t, timeout=10).json().get("results", [])

        post_v = pre_v = 0
        pre_prices = []; post_prices = []

        for c in dy:
            tm = datetime.fromtimestamp(c["t"] / 1000)
            if tm.hour >= 16:
                post_v += c["v"]
                post_prices.append(c["c"])
        for c in dt:
            tm = datetime.fromtimestamp(c["t"] / 1000)
            if tm.hour < 9 or (tm.hour == 9 and tm.minute < 30):
                pre_v += c["v"]
                pre_prices.append(c["c"])

        if not pre_prices or not post_prices:
            continue

        oorvol = (pre_v + post_v) / avg
        ooh_pct = (pre_prices[-1] - md[t]["prev_close"]) / md[t]["prev_close"] * 100

        if oorvol > OORVOL_THRESH and ooh_pct > 2:
            rows.append({
                "Ticker":         t,
                "21D Avg Volume": int(avg),
                "OOH Volume":     int(pre_v + post_v),
                "OORVOL":         round(oorvol, 2),
                "OOH % Change":   round(ooh_pct, 2),
                "Last Close":     md[t]["prev_close"],
                "Daily % Change": md[t]["pct_change"],
            })

    return pd.DataFrame(rows).sort_values("OORVOL", ascending=False)

# ——— MAIN ———
with st.spinner("Running scan... this may take ~1 minute"):
    metadata   = get_metadata()
    avg_vols   = get_avg_volumes(list(metadata.keys()))
    df_results = scan_ooh(avg_vols, metadata)

if not df_results.empty:
    st.success(f"✅ Found {len(df_results)} qualifying stocks")
    st.dataframe(df_results, use_container_width=True)
else:
    st.warning("⚠️ No qualifying stocks met the criteria today.")

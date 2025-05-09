import streamlit as st
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import nest_asyncio

nest_asyncio.apply()

# Your Polygon API key is stored in Streamlit secrets
API_KEY = st.secrets["API_KEY"]

st.set_page_config(page_title="OOH Volume Scanner", layout="wide")
st.title("📊 Out-of-Hours Volume & Price Breakout Scanner")

# Sidebar filters
OORVOL_THRESHOLD = st.sidebar.slider("Min OORVOL", 0.0, 5.0, 1.2, 0.1)
MIN_AVG_VOLUME = st.sidebar.number_input("Min 21-Day Avg Volume", value=1_000_000)
MIN_PRICE = st.sidebar.number_input("Min Price ($)", value=2.0)
OOH_PRICE_THRESHOLD = st.sidebar.slider("OOH Price Change vs Close (%)", 0, 20, 2)

# Date variables
today = datetime.today().strftime('%Y-%m-%d')
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
two_days_ago = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')
start_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

async def fetch(session, url):
    """
    Fetch JSON data from the given URL using aiohttp.
    Returns an empty dict on error.
    """
    try:
        async with session.get(url, timeout=10) as resp:
            return await resp.json()
    except Exception as e:
        st.error(f"Error fetching {url}: {e}")
        return {}

async def get_grouped_data_with_metadata(session):
    # Use yesterday for metadata grouping
    url_today = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{yesterday}?adjusted=true&apiKey={API_KEY}"
    url_prev = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{two_days_ago}?adjusted=true&apiKey={API_KEY}"

    data_today = await fetch(session, url_today)
    data_prev = await fetch(session, url_prev)

    # Map ticker -> close price
    today_closes = {item['T']: item['c'] for item in data_today.get('results', [])}
    prev_closes = {item['T']: item['c'] for item in data_prev.get('results', [])}

    metadata = {}
    for ticker, close in today_closes.items():
        if close >= MIN_PRICE and ticker in prev_closes:
            prev = prev_closes[ticker]
            if prev:
                pct_change = (close - prev) / prev * 100
                metadata[ticker] = {
                    'close': round(close,2),
                    'pct_change': round(pct_change,2),
                    'prev_close': round(prev,2)
                }
    return metadata

async def fetch_21d_avg_volume(session, ticker):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{yesterday}?adjusted=true&sort=desc&limit=30&apiKey={API_KEY}"
    data = await fetch(session, url)
    vols = [d['v'] for d in data.get('results', [])][-21:]
    if len(vols) >= 21:
        avg = sum(vols)/21
        if avg >= MIN_AVG_VOLUME:
            return ticker, avg
    return None

async def fetch_ooh_volume(session, ticker):
    url_post = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{yesterday}/{yesterday}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"
    url_pre = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{today}/{today}?adjusted=true&sort=asc&limit=10000&apiKey={API_KEY}"

    data_post = await fetch(session, url_post)
    data_pre = await fetch(session, url_pre)

    post_vol = 0
    post_prices = []
    pre_vol = 0
    pre_prices = []

    # After-hours yesterday
    for bar in data_post.get('results', []):
        t = datetime.fromtimestamp(bar['t']/1000)
        if t.hour >= 16:
            post_vol += bar['v']
            post_prices.append(bar['c'])
    # Pre-market today
    for bar in data_pre.get('results', []):
        t = datetime.fromtimestamp(bar['t']/1000)
        if t.hour < 9 or (t.hour==9 and t.minute<30):
            pre_vol += bar['v']
            pre_prices.append(bar['c'])

    return ticker, post_vol+pre_vol, pre_prices[0] if pre_prices else None, post_prices[-1] if post_prices else None

async def main_async():
    async with aiohttp.ClientSession() as session:
        metadata = await get_grouped_data_with_metadata(session)
        tickers = list(metadata.keys())

        # Filter by 21-day avg volume
        vols = await asyncio.gather(*[fetch_21d_avg_volume(session,t) for t in tickers])
        vol_map = {t:v for t,v in vols if v is not None}

        # Fetch OOH volume and price
        ooh = await asyncio.gather(*[fetch_ooh_volume(session,t) for t in vol_map])

        results = []
        for ticker, tot_vol, pre_price, post_price in ooh:
            avg = vol_map.get(ticker,0)
            if pre_price is None or post_price is None:
                continue
            # Price change vs yesterday's close
            last_close = metadata[ticker]['close']
            pct = (pre_price - last_close)/last_close*100
            oor = tot_vol/avg if avg else 0
            if pct>OOH_PRICE_THRESHOLD and oor>OORVOL_THRESHOLD:
                results.append({
                    'Ticker':ticker,
                    'OOH % Change':round(pct,2),
                    'OORVOL':round(oor,2),
                    '21D Avg Volume':int(avg),
                    'OOH Volume':int(tot_vol),
                    'Last Close':last_close,
                    'Daily % Change':metadata[ticker]['pct_change']
                })
        df=pd.DataFrame(results)
        df.sort_values('OORVOL',ascending=False,inplace=True)
        return df

# Run and display
with st.spinner("Running scan... this may take 1–2 minutes"):
    df = asyncio.run(main_async())

if not df.empty:
    st.success(f"✅ Found {len(df)} qualifying stocks")
    st.dataframe(df, use_container_width=True)
else:
    st.warning("⚠️ No qualifying stocks met the criteria today.")

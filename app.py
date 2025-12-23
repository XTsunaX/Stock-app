import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import math
import time
import os
import itertools
import json
import re
from datetime import datetime, time as dt_time, timedelta
import pytz
from decimal import Decimal, ROUND_HALF_UP
import io
import twstock  # 必須安裝: pip install twstock

# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="當沖戰略室", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

# 1. 標題
st.title("⚡ 當沖戰略室 ⚡")

CONFIG_FILE = "config.json"
DATA_CACHE_FILE = "data_cache.json"
URL_CACHE_FILE = "url_cache.json"
SEARCH_CACHE_FILE = "search_cache.json"

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f: return json.load(f)
        except: return {}
    return {}

def save_config(font_size, limit_rows, auto_update, delay_sec):
    try:
        config = {
            "font_size": font_size, 
            "limit_rows": limit_rows,
            "auto_update": auto_update,
            "delay_sec": delay_sec
        }
        with open(CONFIG_FILE, "w") as f: json.dump(config, f)
        return True
    except: return False

def save_data_cache(df, ignored_set, candidates=[]):
    try:
        df_save = df.fillna("") 
        data_to_save = {
            "stock_data": df_save.to_dict(orient='records'),
            "ignored_stocks": list(ignored_set),
            "all_candidates": candidates
        }
        with open(DATA_CACHE_FILE, "w", encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=4)
    except: pass

def load_data_cache():
    if os.path.exists(DATA_CACHE_FILE):
        try:
            with open(DATA_CACHE_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
            df = pd.DataFrame(data.get('stock_data', []))
            ignored = set(data.get('ignored_stocks', []))
            candidates = data.get('all_candidates', [])
            return df, ignored, candidates
        except: return pd.DataFrame(), set(), []
    return pd.DataFrame(), set(), []

def load_url_history():
    if os.path.exists(URL_CACHE_FILE):
        try:
            with open(URL_CACHE_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
                if "url" in data and isinstance(data["url"], str) and data["url"]:
                    return [data["url"]]
                return data.get("urls", [])
        except: return []
    return []

def save_url_history(urls):
    try:
        unique_urls = []
        seen = set()
        for u in urls:
            u_clean = u.strip()
            if u_clean and u_clean not in seen:
                unique_urls.append(u_clean)
                seen.add(u_clean)
        
        with open(URL_CACHE_FILE, "w", encoding='utf-8') as f:
            json.dump({"urls": unique_urls}, f)
        return True
    except: return False

def load_search_cache():
    if os.path.exists(SEARCH_CACHE_FILE):
        try:
            with open(SEARCH_CACHE_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
            return data.get("selected", [])
        except: return []
    return []

def save_search_cache(selected_items):
    try:
        with open(SEARCH_CACHE_FILE, "w", encoding='utf-8') as f:
            json.dump({"selected": selected_items}, f, ensure_ascii=False)
    except: pass

# --- 初始化 Session State ---
if 'stock_data' not in st.session_state:
    cached_df, cached_ignored, cached_candidates = load_data_cache()
    st.session_state.stock_data = cached_df
    st.session_state.ignored_stocks = cached_ignored
    st.session_state.all_candidates = cached_candidates

if 'ignored_stocks' not in st.session_state:
    st.session_state.ignored_stocks = set()

if 'all_candidates' not in st.session_state:
    st.session_state.all_candidates = []

if 'calc_base_price' not in st.session_state:
    st.session_state.calc_base_price = 100.0

if 'calc_view_price' not in st.session_state:
    st.session_state.calc_view_price = 100.0

if 'url_history' not in st.session_state:
    st.session_state.url_history = load_url_history()

if 'cloud_url_input' not in st.session_state:
    st.session_state.cloud_url_input = st.session_state.url_history[0] if st.session_state.url_history else ""

if 'search_multiselect' not in st.session_state:
    st.session_state.search_multiselect = load_search_cache()

if 'saved_notes' not in st.session_state:
    st.session_state.saved_notes = {}

if 'futures_list' not in st.session_state:
    st.session_state.futures_list = set()

saved_config = load_config()

if 'font_size' not in st.session_state:
    st.session_state.font_size = saved_config.get('font_size', 15)

if 'limit_rows' not in st.session_state:
    st.session_state.limit_rows = saved_config.get('limit_rows', 5)

if 'auto_update_last_row' not in st.session_state:
    st.session_state.auto_update_last_row = saved_config.get('auto_update', True)

if 'update_delay_sec' not in st.session_state:
    st.session_state.update_delay_sec = saved_config.get('delay_sec', 1.0) 

# --- 側邊欄設定 ---
with st.sidebar:
    st.header("⚙️ 設定")
    
    current_font_size = st.slider(
        "字體大小 (表格)", 
        min_value=12, 
        max_value=72, 
        value=st.session_state.font_size,
        key='font_size_slider'
    )
    st.session_state.font_size = current_font_size
    
    hide_non_stock = st.checkbox("隱藏非個股 (ETF/權證/債券)", value=True)
    
    st.markdown("---")
    
    current_limit_rows = st.number_input(
        "顯示筆數 (檔案/雲端)", 
        min_value=1, 
        value=st.session_state.limit_rows,
        key='limit_rows_input',
        help="此設定限制「檔案/雲端」來源的股票數量。快速查詢的股票會額外顯示。"
    )
    st.session_state.limit_rows = current_limit_rows
    
    if st.button("💾 儲存設定"):
        if save_config(current_font_size, current_limit_rows, 
                      st.session_state.auto_update_last_row, 
                      st.session_state.update_delay_sec):
            st.toast("設定已儲存！", icon="✅")
            
    st.markdown("### 資料管理")
    st.write(f"🚫 已忽略 **{len(st.session_state.ignored_stocks)}** 檔")
    
    col_restore, col_clear = st.columns([1, 1])
    with col_restore:
        if st.button("♻️ 復原", use_container_width=True):
            st.session_state.ignored_stocks.clear()
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates)
            st.toast("已重置忽略名單。", icon="🔄")
            st.rerun()
    with col_clear:
        if st.button("🗑️ 清空", type="primary", use_container_width=True, help="清空所有分析資料 (不會刪除記憶的網址)"):
            st.session_state.stock_data = pd.DataFrame()
            st.session_state.ignored_stocks = set()
            st.session_state.all_candidates = []
            st.session_state.search_multiselect = []
            st.session_state.saved_notes = {} 
            save_search_cache([])
            if os.path.exists(DATA_CACHE_FILE):
                os.remove(DATA_CACHE_FILE)
            st.toast("資料已全部清空", icon="🗑️")
            st.rerun()
    
    if st.button("🧹 清除手動備註", use_container_width=True, help="清除所有記憶的戰略備註內容"):
        st.session_state.saved_notes = {}
        st.toast("手動備註已清除", icon="🧹")
        if not st.session_state.stock_data.empty:
             for idx in st.session_state.stock_data.index:
                 if '_auto_note' in st.session_state.stock_data.columns:
                     st.session_state.stock_data.at[idx, '戰略備註'] = st.session_state.stock_data.at[idx, '_auto_note']
        st.rerun()

    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n在表格左側勾選「刪除」框，資料將會立即移除並**自動遞補下一檔**。")
    
    st.markdown("---")
    st.markdown("### 🔗 外部資源")
    st.link_button("📥 Goodinfo 當日週轉率排行", "https://reurl.cc/Or9e37", use_container_width=True, help="點擊前往 Goodinfo 網站下載 CSV")

# --- 動態 CSS ---
font_px = f"{st.session_state.font_size}px"
zoom_level = current_font_size / 14.0

st.markdown(f"""
    <style>
    div[data-testid="stDataFrame"] {{
        width: 100%;
        zoom: {zoom_level};
    }}
    div[data-testid="stDataFrame"] table, 
    div[data-testid="stDataFrame"] thead, 
    div[data-testid="stDataFrame"] tbody, 
    div[data-testid="stDataFrame"] tr, 
    div[data-testid="stDataFrame"] th, 
    div[data-testid="stDataFrame"] td, 
    div[data-testid="stDataFrame"] div, 
    div[data-testid="stDataFrame"] span, 
    div[data-testid="stDataFrame"] p {{
        font-family: 'Microsoft JhengHei', sans-serif !important;
    }}
    div[data-testid="stDataFrame"] input {{
        font-family: 'Microsoft JhengHei', sans-serif !important;
        font-size: 0.9rem !important; 
    }}
    thead tr th:first-child {{ display:none }}
    tbody th {{ display:none }}
    .block-container {{ padding-top: 4.5rem; padding-bottom: 1rem; }}
    [data-testid="stMetricValue"] {{ font-size: 1.2em; }}
    div[data-testid="column"] {{
        padding-left: 0.1rem !important;
        padding-right: 0.1rem !important;
    }}
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 1. 資料庫與網路功能
# ==========================================

@st.cache_data
def load_local_stock_names():
    code_map = {}
    name_map = {}
    if os.path.exists("stock_names.csv"):
        try:
            df = pd.read_csv("stock_names.csv", header=None, names=["code", "name"], dtype=str)
            for _, row in df.iterrows():
                c = str(row['code']).strip()
                n = str(row['name']).strip()
                code_map[c] = n
                name_map[n] = c
        except: pass
    return code_map, name_map

@st.cache_data(ttl=86400)
def get_stock_name_online(code):
    code = str(code).strip()
    code_map, _ = load_local_stock_names()
    if code in code_map: return code_map[code]
    return code

@st.cache_data(ttl=86400)
def search_code_online(query):
    query = query.strip()
    if query.isdigit(): return query
    _, name_map = load_local_stock_names()
    if query in name_map: return name_map[query]
    return None

@st.cache_data(ttl=86400)
def fetch_futures_list():
    try:
        url = "https://www.taifex.com.tw/cht/2/stockLists"
        dfs = pd.read_html(url)
        if dfs:
            for df in dfs:
                if '證券代號' in df.columns:
                    return set(df['證券代號'].astype(str).str.strip().tolist())
                if 'Stock Code' in df.columns:
                    return set(df['Stock Code'].astype(str).str.strip().tolist())
    except: pass
    return set()

def get_live_price(code):
    try:
        realtime_data = twstock.realtime.get(code)
        if realtime_data and realtime_data.get('success'):
            price_str = realtime_data['realtime'].get('latest_trade_price')
            if price_str and price_str != '-' and float(price_str) > 0:
                return float(price_str)
            bids = realtime_data['realtime'].get('best_bid_price', [])
            if bids and bids[0] and bids[0] != '-':
                 return float(bids[0])
    except: pass
    try:
        ticker = yf.Ticker(f"{code}.TW")
        price = ticker.fast_info.get('last_price')
        if price and not math.isnan(price): return float(price)
        ticker = yf.Ticker(f"{code}.TWO")
        price = ticker.fast_info.get('last_price')
        if price and not math.isnan(price): return float(price)
    except: pass
    return None

def fetch_yahoo_web_backup(code):
    try:
        url = f"https://tw.stock.yahoo.com/quote/{code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        r = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        price_tag = soup.find('span', class_='Fz(32px)')
        if not price_tag: return None
        price = float(price_tag.text.replace(',', ''))
        
        change_tag = soup.find('span', class_='Fz(20px)')
        change = 0.0
        if change_tag:
             change_txt = change_tag.text.strip().replace('▲', '').replace('▼', '').replace('+', '').replace(',', '')
             parent = change_tag.parent
             if 'C($c-trend-down)' in str(parent):
                 change = -float(change_txt)
             else:
                 change = float(change_txt)
                 
        prev_close = price - change
        open_p = price
        high_p = price
        low_p = price
        
        details = soup.find_all('li', class_='price-detail-item')
        for item in details:
            label = item.find('span', class_='C(#6e7780)')
            val_tag = item.find('span', class_='Fw(600)')
            if label and val_tag:
                lbl = label.text.strip()
                val_txt = val_tag.text.strip().replace(',', '')
                if val_txt == '-': continue
                val = float(val_txt)
                if "開盤" in lbl: open_p = val
                elif "最高" in lbl: high_p = val
                elif "最低" in lbl: low_p = val

        today = datetime.now().date()
        data = {'Open': [open_p], 'High': [high_p], 'Low': [low_p], 'Close': [price], 'Volume': [0]}
        df = pd.DataFrame(data, index=[pd.to_datetime(today)])
        return df, prev_close
    except: return None, None

def fetch_finmind_backup(code):
    try:
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        url = f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockPrice&data_id={code}&start_date={start_date}"
        r = requests.get(url, timeout=5)
        data_json = r.json()
        if data_json.get('msg') == 'success' and data_json.get('data'):
            df = pd.DataFrame(data_json['data'])
            df['Date'] = pd.to_datetime(df['date'])
            df = df.set_index('Date')
            rename_map = {'open': 'Open', 'max': 'High', 'min': 'Low', 'close': 'Close', 'Trading_Volume': 'Volume'}
            df = df.rename(columns=rename_map)
            cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            for c in cols:
                if c not in df.columns:
                    if c.lower() in df.columns: df[c] = df[c.lower()]
                    else: df[c] = 0.0 
                df[c] = pd.to_numeric(df[c], errors='coerce')
            return df[cols]
    except: pass
    return None

# ==========================================
# 2. 核心計算邏輯
# ==========================================

def get_tick_size(price):
    try: price = float(price)
    except: return 0.01
    if pd.isna(price) or price <= 0: return 0.01
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def calculate_limits(price):
    try:
        p = float(price)
        if math.isnan(p) or p <= 0: return 0, 0
        raw_up = p * 1.10
        tick_up = get_tick_size(raw_up) 
        limit_up = math.floor(raw_up / tick_up) * tick_up
        raw_down = p * 0.90
        tick_down = get_tick_size(raw_down) 
        limit_down = math.ceil(raw_down / tick_down) * tick_down
        return float(f"{limit_up:.2f}"), float(f"{limit_down:.2f}")
    except: return 0, 0

def apply_tick_rules(price):
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        rounded = (Decimal(str(p)) / Decimal(str(tick))).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * Decimal(str(tick))
        return float(rounded)
    except: return price

def move_tick(price, steps):
    try:
        curr = float(price)
        if steps > 0:
            for _ in range(steps):
                tick = get_tick_size(curr)
                curr = round(curr + tick, 2)
        elif steps < 0:
            for _ in range(abs(steps)):
                tick = get_tick_size(curr - 0.0001)
                curr = round(curr - tick, 2)
        return curr
    except: return price

def apply_sr_rules(price, base_price):
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        d_val = Decimal(str(p))
        d_tick = Decimal(str(tick))
        if p < base_price: return float(math.ceil(d_val / d_tick) * d_tick)
        elif p > base_price: return float(math.floor(d_val / d_tick) * d_tick)
        else: return apply_tick_rules(p)
    except: return price

def fmt_price(v):
    try:
        if pd.isna(v) or v == "": return ""
        return f"{float(v):.2f}".rstrip('0').rstrip('.')
    except: return str(v)

def calculate_note_width(series, font_size):
    def get_width(s):
        w = 0
        for c in str(s): w += 2.0 if ord(c) > 127 else 1.0
        return w
    if series.empty: return 50
    max_w = series.apply(get_width).max()
    if pd.isna(max_w): max_w = 0
    pixel_width = int(max_w * (font_size * 0.44))
    return max(50, pixel_width)

def recalculate_row(row, points_map):
    custom_price = row.get('自訂價(可修)')
    code = row.get('代號')
    status = ""
    if pd.isna(custom_price) or str(custom_price).strip() == "": return status
    
    try:
        price = float(custom_price)
        limit_up = row.get('當日漲停價')
        limit_down = row.get('當日跌停價')
        
        l_up = float(limit_up) if limit_up and str(limit_up).replace('.','').isdigit() else None
        l_down = float(limit_down) if limit_down and str(limit_down).replace('.','').isdigit() else None
        
        strat_values = []
        points = points_map.get(code, [])
        if isinstance(points, list):
            for p in points: strat_values.append(p['val'])
        note_text = str(row.get('戰略備註', ''))
        found_prices = re.findall(r'\d+\.?\d*', note_text)
        for fp in found_prices:
            try: strat_values.append(float(fp))
            except: pass
            
        if l_up is not None and abs(price - l_up) < 0.01: status = "🔴 漲停"
        elif l_down is not None and abs(price - l_down) < 0.01: status = "🟢 跌停"
        elif strat_values:
            max_val = max(strat_values)
            min_val = min(strat_values)
            if price > max_val: status = "🔴 強"
            elif price < min_val: status = "🟢 弱"
            else:
                hit = False
                for v in strat_values:
                    if abs(v - price) < 0.01: hit = True; break
                if hit: status = "🟡 命中"
        return status
    except: return status

# [核心修正] 更新點位判定邏輯
def fetch_stock_data_raw(code, name_hint="", extra_data=None):
    code = str(code).strip()
    hist = pd.DataFrame()
    source_used = "none"

    def is_valid_data(df_check, code):
        if df_check is None or df_check.empty: return False
        try:
            last_row = df_check.iloc[-1]
            last_price = last_row['Close']
            if last_price <= 0: return False
            last_dt = df_check.index[-1]
            if last_dt.tzinfo is not None:
                last_dt = last_dt.astimezone(pytz.timezone('Asia/Taipei')).replace(tzinfo=None)
            now_dt = datetime.now().replace(tzinfo=None)
            if (now_dt - last_dt).days > 3: return False
            return True
        except: return False

    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist_yf = ticker.history(period="3mo")
        if hist_yf.empty or not is_valid_data(hist_yf, code):
            ticker = yf.Ticker(f"{code}.TWO")
            hist_yf = ticker.history(period="3mo")
        if not hist_yf.empty and is_valid_data(hist_yf, code):
            hist = hist_yf
            source_used = "yfinance"
    except: pass

    if hist.empty:
        try:
            stock = twstock.Stock(code)
            tw_data = stock.fetch_31()
            if tw_data:
                df_tw = pd.DataFrame(tw_data)
                df_tw['Date'] = pd.to_datetime(df_tw['date']); df_tw = df_tw.set_index('Date')
                rename_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'capacity': 'Volume'}
                df_tw = df_tw.rename(columns=rename_map)
                cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                for c in cols: df_tw[c] = pd.to_numeric(df_tw[c], errors='coerce')
                if not df_tw.empty and is_valid_data(df_tw, code):
                    hist = df_tw[cols]; source_used = "twstock"
        except: pass

    if hist.empty:
        df_fm = fetch_finmind_backup(code)
        if df_fm is not None and not df_fm.empty and is_valid_data(df_fm, code):
            hist = df_fm; source_used = "finmind"

    if hist.empty:
        df_web, _ = fetch_yahoo_web_backup(code)
        if df_web is not None and not df_web.empty:
            hist = df_web; source_used = "web_backup"

    if hist.empty: return None

    hist['High'] = hist[['High', 'Close']].max(axis=1)
    hist['Low'] = hist[['Low', 'Close']].min(axis=1)

    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    last_date = hist.index[-1].date()
    is_during_trading = (now.time() < dt_time(13, 30))
    
    hist_strat = hist.copy()
    if is_during_trading and last_date == now.date():
        hist_strat = hist_strat.iloc[:-1]

    if hist_strat.empty: return None

    # 基礎價格判定
    strategy_base_price = hist_strat.iloc[-1]['Close']
    if len(hist_strat) >= 2:
        prev_of_base = hist_strat.iloc[-2]['Close']
        pct_change = ((strategy_base_price - prev_of_base) / prev_of_base) * 100
    else: pct_change = 0.0

    limit_up_show, limit_down_show = calculate_limits(strategy_base_price)
    limit_up_T, limit_down_T = (None, None)
    if len(hist_strat) >= 2:
        limit_up_T, limit_down_T = calculate_limits(hist_strat.iloc[-2]['Close'])

    # +3% / -3% 目標與防守
    target_price = apply_sr_rules(strategy_base_price * 1.03, strategy_base_price)
    stop_price = apply_sr_rules(strategy_base_price * 0.97, strategy_base_price)
    
    points = []
    
    # 1. MA5
    if len(hist_strat) >= 5:
        ma5_raw = float(sum(hist_strat['Close'].tail(5)) / 5)
        ma5 = apply_sr_rules(ma5_raw, strategy_base_price)
        ma5_tag = "多" if ma5_raw < strategy_base_price else ("空" if ma5_raw > strategy_base_price else "平")
        points.append({"val": ma5, "tag": ma5_tag, "force": True})

    # 2. 歷史區間與當日高低 (90天)
    high_90_raw = hist_strat['High'].max()
    low_90_raw = hist_strat['Low'][hist_strat['Low'] > 0].min() if not hist_strat['Low'][hist_strat['Low'] > 0].empty else hist_strat['Low'].min()
    
    # 當前最後一根(昨日)的高低點標示
    last_candle = hist_strat.iloc[-1]
    last_high_v = apply_tick_rules(last_candle['High'])
    last_low_v = apply_tick_rules(last_candle['Low'])
    
    points.append({"val": last_high_v, "tag": "高"})
    points.append({"val": last_low_v, "tag": "低"})
    
    if abs(high_90_raw - last_candle['High']) > 0.01:
        points.append({"val": apply_tick_rules(high_90_raw), "tag": "高"})
    if abs(low_90_raw - last_candle['Low']) > 0.01:
        points.append({"val": apply_tick_rules(low_90_raw), "tag": "低"})

    # 3. [新邏輯] 新高/新低判定 +3% / -3%
    # 若昨日最高觸及新高，或收盤價距離新高 3% 內
    is_at_new_high = (last_candle['High'] >= high_90_raw - 0.01)
    close_near_high = (strategy_base_price >= high_90_raw * 0.97)
    # 若昨日最低觸及新低，或收盤價距離新低 3% 內
    is_at_new_low = (last_candle['Low'] <= low_90_raw + 0.01)
    close_near_low = (strategy_base_price <= low_90_raw * 1.03)

    if is_at_new_high or close_near_high: points.append({"val": target_price, "tag": ""})
    if is_at_new_low or close_near_low: points.append({"val": stop_price, "tag": ""})

    # 4. 其他盤中點位
    points.append({"val": apply_tick_rules(last_candle['Open']), "tag": ""})
    if limit_up_T and abs(last_candle['High'] - limit_up_T) < 0.01:
        points.append({"val": limit_up_T, "tag": "漲停高" if abs(limit_up_T - high_90_raw) < 0.05 else "漲停"})
    if limit_down_T and abs(last_candle['Low'] - limit_down_T) < 0.01:
        points.append({"val": limit_down_T, "tag": "跌停"})

    # 整理與排序
    display_candidates = [p for p in points if p.get('force', False) or (limit_down_show <= p['val'] <= limit_up_show)]
    display_candidates.sort(key=lambda x: x['val'])
    
    final_points = []
    for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
        tags = [x['tag'] for x in group if x['tag']]
        tag = ""
        if "漲停高" in tags: tag = "漲停高"
        elif "漲停" in tags: tag = "漲停"
        elif "跌停" in tags: tag = "跌停"
        elif "高" in tags: tag = "高"
        elif "低" in tags: tag = "低"
        elif "多" in tags: tag = "多"
        elif "空" in tags: tag = "空"
        final_points.append({"val": val, "tag": tag})
        
    note_parts = []
    seen_vals = set()
    for p in final_points:
        if p['val'] in seen_vals and p['tag'] == "": continue
        seen_vals.add(p['val']); v_str = fmt_price(p['val'])
        t = p['tag']
        item = f"{t}{v_str}" if t in ["漲停", "漲停高", "跌停", "高", "低"] else (f"{v_str}{t}" if t else v_str)
        note_parts.append(item)
    
    auto_note = "-".join(note_parts)
    manual_note = st.session_state.saved_notes.get(code, "")
    strategy_note = f"{auto_note} {manual_note}" if manual_note else auto_note

    final_name = name_hint if name_hint else get_stock_name_online(code)
    light = "🔴" if "多" in strategy_note else ("🟢" if "空" in strategy_note else "⚪")
    
    return {
        "代號": code, "名稱": f"{light} {final_name}", "收盤價": round(strategy_base_price, 2),
        "漲跌幅": pct_change, "期貨": "✅" if code in st.session_state.futures_list else "", 
        "當日漲停價": limit_up_show, "當日跌停價": limit_down_show,
        "自訂價(可修)": None, "+3%": target_price, "-3%": stop_price,   
        "戰略備註": strategy_note, "_points": final_points, "狀態": "", "_auto_note": auto_note
    }

# ==========================================
# 主介面與編輯邏輯 (維持不變)
# ==========================================

tab1, tab2 = st.tabs(["⚡ 當沖戰略室 ⚡", "💰 當沖損益室 💰"])

with tab1:
    col_search, col_file = st.columns([2, 1])
    with col_search:
        code_map, name_map = load_local_stock_names()
        stock_options = [f"{code} {name}" for code, name in sorted(code_map.items())]
        src_tab1, src_tab2 = st.tabs(["📂 本機", "☁️ 雲端"])
        with src_tab1:
            uploaded_file = st.file_uploader("上傳檔案", type=['xlsx', 'csv', 'html', 'xls'], label_visibility="collapsed")
            selected_sheet = 0
            if uploaded_file and not uploaded_file.name.endswith('.csv'):
                try:
                    xl = pd.ExcelFile(uploaded_file); sheet_options = xl.sheet_names
                    idx = sheet_options.index("週轉率") if "週轉率" in sheet_options else 0
                    selected_sheet = st.selectbox("選擇工作表", sheet_options, index=idx)
                except: pass
        with src_tab2:
            def on_history_change(): st.session_state.cloud_url_input = st.session_state.history_selected
            history_opts = st.session_state.url_history if st.session_state.url_history else ["(無紀錄)"]
            c_sel, c_del = st.columns([8, 1], gap="small")
            with c_sel: st.selectbox("📜 歷史紀錄", history_opts, key="history_selected", index=None, on_change=on_history_change, label_visibility="collapsed")
            with c_del:
                if st.button("🗑️") and st.session_state.history_selected in st.session_state.url_history:
                    st.session_state.url_history.remove(st.session_state.history_selected)
                    save_url_history(st.session_state.url_history); st.rerun()
            st.text_input("輸入連結", key="cloud_url_input")
        
        search_selection = st.multiselect("🔍 快速查詢", options=stock_options, key="search_multiselect", on_change=lambda: save_search_cache(st.session_state.search_multiselect))

    if st.button("🚀 執行分析"):
        if not st.session_state.futures_list: st.session_state.futures_list = fetch_futures_list()
        targets = []
        df_up = pd.DataFrame()
        curr_url = st.session_state.cloud_url_input.strip()
        if curr_url and curr_url not in st.session_state.url_history:
            st.session_state.url_history.insert(0, curr_url); save_url_history(st.session_state.url_history)
        
        try:
            if uploaded_file:
                uploaded_file.seek(0); fname = uploaded_file.name.lower()
                if fname.endswith('.csv'):
                    try: df_up = pd.read_csv(uploaded_file, dtype=str, encoding='cp950')
                    except: uploaded_file.seek(0); df_up = pd.read_csv(uploaded_file, dtype=str)
                elif fname.endswith('.xlsx'): df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet, dtype=str)
                else: 
                    dfs = pd.read_html(uploaded_file)
                    for d in dfs:
                        if d.apply(lambda r: r.astype(str).str.contains('代號').any(), axis=1).any():
                            df_up = d; break
            elif curr_url:
                if "docs.google.com" in curr_url: curr_url = curr_url.split("/edit")[0] + "/export?format=csv"
                try: df_up = pd.read_csv(curr_url, dtype=str)
                except: df_up = pd.read_excel(curr_url, dtype=str)
        except Exception as e: st.error(f"讀取失敗: {e}")

        if search_selection:
            for item in search_selection:
                p = item.split(' ', 1); targets.append((p[0], p[1] if len(p)>1 else "", 'search', 9999))
        if not df_up.empty:
            df_up.columns = df_up.columns.astype(str).str.strip()
            c_col = next((c for c in df_up.columns if "代號" in str(c)), None)
            n_col = next((c for c in df_up.columns if "名稱" in str(c)), None)
            if c_col:
                count = 0
                for _, row in df_up.iterrows():
                    c = str(row[c_col]).replace('=', '').replace('"', '').strip()
                    if not c or c.lower() == 'nan' or c in st.session_state.ignored_stocks: continue
                    if hide_non_stock and (c.startswith('00') or (len(c)>4 and c.isdigit())): continue
                    targets.append((c, str(row[n_col]) if n_col else "", 'upload', count))
                    count += 1
        
        st.session_state.all_candidates = targets
        seen = set(); existing_data = {}; bar = st.progress(0); status_text = st.empty()
        up_limit = st.session_state.limit_rows; up_cur = 0
        
        for i, (code, name, src, extra) in enumerate(targets):
            if src == 'upload' and up_cur >= up_limit: continue
            status_text.text(f"正在分析: {code} {name} ...")
            if (code, src) in seen: continue
            data = fetch_stock_data_raw(code, name, extra)
            if data:
                data.update({'_source': src, '_order': extra, '_source_rank': 1 if src == 'upload' else 2})
                existing_data[code] = data; seen.add((code, src))
                if src == 'upload': up_cur += 1
            bar.progress(min((i+1)/len(targets), 1.0))
        bar.empty(); status_text.empty()
        if existing_data:
            st.session_state.stock_data = pd.DataFrame(list(existing_data.values()))
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates)

    if not st.session_state.stock_data.empty:
        df_all = st.session_state.stock_data.copy()
        df_all['代號'] = df_all['代號'].astype(str)
        df_all = df_all[~df_all['代號'].isin(st.session_state.ignored_stocks)]
        if '_source_rank' in df_all.columns: df_all = df_all.sort_values(['_source_rank', '_order'])
        
        df_display = df_all.reset_index(drop=True)
        note_w = calculate_note_width(df_display['戰略備註'], current_font_size)
        df_display["移除"] = False
        points_map = df_display.set_index('代號')['_points'].to_dict()
        auto_notes_dict = df_display.set_index('代號')['_auto_note'].to_dict()
        
        cols_fmt = ["當日漲停價", "當日跌停價", "+3%", "-3%", "自訂價(可修)"]
        for c in cols_fmt: 
            if c in df_display.columns: df_display[c] = df_display[c].apply(fmt_price)
        
        input_cols = ["移除", "代號", "名稱", "戰略備註", "自訂價(可修)", "狀態", "當日漲停價", "當日跌停價", "+3%", "-3%", "收盤價", "漲跌幅", "期貨"]
        for i in range(len(df_display)):
            try:
                p, chg = float(df_display.at[i, "收盤價"]), float(df_display.at[i, "漲跌幅"])
                icon = "🔴" if chg > 0 else ("🟢" if chg < 0 else "⚪")
                df_display.at[i, "收盤價"] = f"{icon} {fmt_price(p)}"
                df_display.at[i, "漲跌幅"] = f"{icon} {chg:+.2f}%"
            except: pass

        st.markdown("""<style>[data-testid="stDataFrame"] td:nth-child(6):contains("🔴") { background-color: #ffecec !important; color: #ff4b4b !important; }
        [data-testid="stDataFrame"] td:nth-child(6):contains("🟢") { background-color: #ecffec !important; color: #00cc00 !important; }</style>""", unsafe_allow_html=True)

        edited_df = st.data_editor(df_display[input_cols], column_config={
            "移除": st.column_config.CheckboxColumn("刪除", width=40),
            "代號": st.column_config.TextColumn(disabled=True, width=50),
            "名稱": st.column_config.TextColumn(disabled=True, width="small"),
            "狀態": st.column_config.TextColumn(width=60, disabled=True),
            "戰略備註": st.column_config.TextColumn("戰略備註 ✏️", width=note_w),
            "自訂價(可修)": st.column_config.TextColumn("自訂價 ✏️", width=60),
        }, hide_index=True, use_container_width=False, key="main_editor")
        
        if not edited_df.empty:
            trigger = False
            if "移除" in edited_df.columns:
                to_rem = edited_df[edited_df["移除"] == True]
                if not to_rem.empty:
                    for c in to_rem["代號"].unique(): st.session_state.ignored_stocks.add(str(c))
                    st.session_state.stock_data = st.session_state.stock_data[~st.session_state.stock_data["代號"].isin(to_rem["代號"])]
                    trigger = True
            if not trigger and st.session_state.auto_update_last_row:
                l_idx = len(edited_df) - 1
                if l_idx >= 0:
                    l_code = edited_df.iloc[l_idx]['代號']; update_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
                    for i, row in st.session_state.stock_data.iterrows():
                        if row['代號'] == l_code and l_code in update_map:
                            if str(row['自訂價(可修)']) != str(update_map[l_code]['自訂價(可修)']):
                                if st.session_state.update_delay_sec > 0: time.sleep(st.session_state.update_delay_sec)
                                for j, r in st.session_state.stock_data.iterrows():
                                    c = r['代號']
                                    if c in update_map:
                                        st.session_state.stock_data.at[j, '自訂價(可修)'] = update_map[c]['自訂價(可修)']
                                        nn = update_map[c]['戰略備註']
                                        if str(r['戰略備註']) != str(nn):
                                            ba = auto_notes_dict.get(c, ""); pm = nn
                                            if ba and nn.startswith(ba): pm = nn[len(ba):].strip()
                                            st.session_state.stock_data.at[j, '戰略備註'] = nn; st.session_state.saved_notes[c] = pm
                                        st.session_state.stock_data.at[j, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[j], points_map)
                                trigger = True; break
            if trigger: st.rerun()

        st.markdown("---")
        btn_update = st.button("⚡ 執行更新", type="primary")
        auto_up = st.checkbox("☑️ 啟用最後一列自動更新", value=st.session_state.auto_update_last_row)
        st.session_state.auto_update_last_row = auto_up
        if auto_up: st.session_state.update_delay_sec = st.number_input("⏳ 緩衝秒數", 0.0, 5.0, 0.1, st.session_state.update_delay_sec)

        if btn_update:
            u_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
            for i, row in st.session_state.stock_data.iterrows():
                code = row['代號']
                if code in u_map:
                    st.session_state.stock_data.at[i, '自訂價(可修)'] = u_map[code]['自訂價(可修)']
                    nn = u_map[code]['戰略備註']
                    if str(row['戰略備註']) != str(nn):
                        ba = auto_notes_dict.get(code, ""); pm = nn
                        if ba and nn.startswith(ba): pm = nn[len(ba):].strip()
                        st.session_state.stock_data.at[i, '戰略備註'] = nn; st.session_state.saved_notes[code] = pm
                st.session_state.stock_data.at[i, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
            st.rerun()

with tab2:
    st.markdown("#### 💰 當沖損益室 💰")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: 
        cp = st.number_input("基準價格", value=float(st.session_state.calc_base_price), step=0.01, format="%.2f")
        if cp != st.session_state.calc_base_price: st.session_state.calc_base_price = cp; st.session_state.calc_view_price = apply_tick_rules(cp)
    with c2: shares = st.number_input("股數", 1000, 1000000, 1000, 1000)
    with c3: disc = st.number_input("手續費折扣 (折)", 0.1, 10.0, 2.8, 0.1)
    with c4: mf = st.number_input("最低手續費 (元)", 0, 100, 20, 1)
    with c5: tc = st.number_input("顯示檔數 (檔)", 1, 50, 5, 1)
    direct = st.radio("交易方向", ["當沖多 (先買後賣)", "當沖空 (先賣後買)"], horizontal=True)
    l_up, l_down = calculate_limits(st.session_state.calc_base_price)
    b1, b2, _ = st.columns([1, 1, 6])
    with b1:
        if st.button("🔼 向上", use_container_width=True): st.session_state.calc_view_price = min(l_up, move_tick(st.session_state.calc_view_price, tc)); st.rerun()
    with b2:
        if st.button("🔽 向下", use_container_width=True): st.session_state.calc_view_price = max(l_down, move_tick(st.session_state.calc_view_price, -tc)); st.rerun()
    
    calc_res = []
    base_p, view_p = st.session_state.calc_base_price, st.session_state.calc_view_price
    is_l = "多" in direct; fr, tr = 0.001425, 0.0015
    for i in range(tc, -(tc + 1), -1):
        p = move_tick(view_p, i)
        if p > l_up or p < l_down: continue
        if is_l:
            bf, sf = [max(mf, math.floor(v * shares * fr * (disc/10))) for v in [base_p, p]]
            tx = math.floor(p * shares * tr); profit = (p * shares - sf - tx) - (base_p * shares + bf)
        else:
            sf, bf = [max(mf, math.floor(v * shares * fr * (disc/10))) for v in [base_p, p]]
            tx = math.floor(base_p * shares * tr); profit = (base_p * shares - sf - tx) - (p * shares + bf)
        roi = (profit / (base_p * shares)) * 100 if base_p != 0 else 0
        diff = p - base_p; diff_s = ("+" if diff > 0 else "") + f"{diff:+.2f}".rstrip('0').rstrip('.')
        calc_res.append({"成交價": fmt_price(p), "漲跌": diff_s if diff != 0 else "0", "預估損益": int(profit), "報酬率%": f"{roi:+.2f}%", "手續費": bf+sf, "交易稅": tx, "_profit": profit, "_note": "up" if abs(p-l_up)<0.001 else ("down" if abs(p-l_down)<0.001 else ""), "_base": abs(p-base_p)<0.001})
    
    df_c = pd.DataFrame(calc_res)
    def style_c(r):
        if r['_base']: return ['background-color: #ffffcc; color: black; font-weight: bold; border: 2px solid #ffd700;'] * len(r)
        if r['_note'] == 'up': return ['background-color: #ff4b4b; color: white; font-weight: bold'] * len(r)
        if r['_note'] == 'down': return ['background-color: #00cc00; color: white; font-weight: bold'] * len(r)
        return ['color: #ff4b4b; font-weight: bold' if r['_profit'] > 0 else ('color: #00cc00; font-weight: bold' if r['_profit'] < 0 else 'color: gray')] * len(r)
    if not df_c.empty: st.dataframe(df_c.style.apply(style_c, axis=1), hide_index=True, height=(len(df_c)+1)*35, column_config={"_profit": None, "_note": None, "_base": None})

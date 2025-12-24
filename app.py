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

def save_data_cache(df, ignored_set, candidates=[], notes={}):
    try:
        df_save = df.fillna("") 
        data_to_save = {
            "stock_data": df_save.to_dict(orient='records'),
            "ignored_stocks": list(ignored_set),
            "all_candidates": candidates,
            "saved_notes": notes
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
            notes = data.get('saved_notes', {})
            return df, ignored, candidates, notes
        except: return pd.DataFrame(), set(), [], {}
    return pd.DataFrame(), set(), [], {}

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
    cached_df, cached_ignored, cached_candidates, cached_notes = load_data_cache()
    st.session_state.stock_data = cached_df
    st.session_state.ignored_stocks = cached_ignored
    st.session_state.all_candidates = cached_candidates
    st.session_state.saved_notes = cached_notes

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
    
    # [修正 3] 側邊欄新增選項
    include_3d_hl = st.checkbox("戰略備註包含近三日高低點", value=False, help="在備註中加入最近三個交易日的最高點與最低點參考")
    
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
    
    # [修正 2] 忽略名單管理介面
    if st.session_state.ignored_stocks:
        ignored_options = sorted(list(st.session_state.ignored_stocks))
        stocks_to_restore = st.multiselect("選取以復原股票:", options=ignored_options, placeholder="選擇代號...")
        if st.button("♻️ 復原選中股票", use_container_width=True):
            for c in stocks_to_restore:
                st.session_state.ignored_stocks.remove(c)
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast(f"已復原 {len(stocks_to_restore)} 檔股票", icon="🔄")
            st.rerun()

    col_restore_all, col_clear = st.columns([1, 1])
    with col_restore_all:
        if st.button("♻️ 全部復原", use_container_width=True):
            st.session_state.ignored_stocks.clear()
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("已重置所有忽略名單。", icon="🔄")
            st.rerun()
    with col_clear:
        if st.button("🗑️ 清空全部", type="primary", use_container_width=True, help="清空所有分析資料 (不含網址)"):
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
    except:
        pass
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
        data = {
            'Open': [open_p], 'High': [high_p], 'Low': [low_p], 'Close': [price], 'Volume': [0]
        }
        df = pd.DataFrame(data, index=[pd.to_datetime(today)])
        
        return df, prev_close
    except:
        return None, None

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
            rename_map = {
                'open': 'Open', 'max': 'High', 'min': 'Low', 'close': 'Close', 'Trading_Volume': 'Volume'
            }
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
            
        if l_up is not None and abs(price - l_up) < 0.01: 
            status = "🔴 漲停"
        elif l_down is not None and abs(price - l_down) < 0.01: 
            status = "🟢 跌停"
        elif strat_values:
            max_val = max(strat_values)
            min_val = min(strat_values)
            
            if price > max_val:
                status = "🔴 強"
            elif price < min_val:
                status = "🟢 弱"
            else:
                hit = False
                for v in strat_values:
                    if abs(v - price) < 0.01: hit = True; break
                if hit: status = "🟡 命中"
        
        return status
    except: return status

# [修正 3] 參數增加 include_3d_hl
def fetch_stock_data_raw(code, name_hint="", extra_data=None, include_3d_hl=False):
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
                df_tw['Date'] = pd.to_datetime(df_tw['date'])
                df_tw = df_tw.set_index('Date')
                rename_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'capacity': 'Volume'}
                df_tw = df_tw.rename(columns=rename_map)
                cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                for c in cols: df_tw[c] = pd.to_numeric(df_tw[c], errors='coerce')
                if not df_tw.empty and is_valid_data(df_tw, code):
                    hist = df_tw[cols]
                    source_used = "twstock"
        except: pass

    if hist.empty:
        df_fm = fetch_finmind_backup(code)
        if df_fm is not None and not df_fm.empty and is_valid_data(df_fm, code):
            hist = df_fm
            source_used = "finmind"

    if hist.empty:
        df_web, web_prev_close = fetch_yahoo_web_backup(code)
        if df_web is not None and not df_web.empty:
            hist = df_web
            source_used = "web_backup"

    if hist.empty: return None

    hist['High'] = hist[['High', 'Close']].max(axis=1)
    hist['Low'] = hist[['Low', 'Close']].min(axis=1)

    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    last_date = hist.index[-1].date()
    is_today_in_hist = (last_date == now.date())
    is_during_trading = (now.time() < dt_time(13, 30))
    
    hist_strat = hist.copy()
    if is_during_trading and is_today_in_hist:
        hist_strat = hist_strat.iloc[:-1]
    elif not is_today_in_hist and source_used != "web_backup":
        live = get_live_price(code)
        if live:
            new_row = pd.DataFrame({'Open': live, 'High': live, 'Low': live, 'Close': live, 'Volume': 0}, index=[pd.to_datetime(now.date())])
            hist_strat = pd.concat([hist_strat, new_row])

    if hist_strat.empty: return None

    strategy_base_price = hist_strat.iloc[-1]['Close']
    if len(hist_strat) >= 2:
        prev_of_base = hist_strat.iloc[-2]['Close']
    else:
        prev_of_base = strategy_base_price 

    pct_change = ((strategy_base_price - prev_of_base) / prev_of_base) * 100 if prev_of_base > 0 else 0.0
    limit_up_show, limit_down_show = calculate_limits(strategy_base_price)
    
    limit_up_T = None
    limit_down_T = None
    if len(hist_strat) >= 2:
        limit_up_T, limit_down_T = calculate_limits(hist_strat.iloc[-2]['Close'])

    points = []
    
    # [修正 3] 加入近三日高低點邏輯
    if include_3d_hl and len(hist_strat) >= 1:
        last_3_days = hist_strat.tail(3)
        h3_raw = last_3_days['High'].max()
        l3_raw = last_3_days['Low'].min()
        points.append({"val": apply_tick_rules(h3_raw), "tag": "3高"})
        points.append({"val": apply_tick_rules(l3_raw), "tag": "3低"})

    if len(hist_strat) >= 5:
        ma5_raw = float((sum(Decimal(str(x)) for x in hist_strat['Close'].tail(5).values) / Decimal("5")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        ma5 = apply_sr_rules(ma5_raw, strategy_base_price)
        ma5_tag = "多" if ma5_raw < strategy_base_price else ("空" if ma5_raw > strategy_base_price else "平")
        points.append({"val": ma5, "tag": ma5_tag, "force": True})

    if len(hist_strat) >= 2:
        last_candle = hist_strat.iloc[-1]
        p_open = apply_tick_rules(last_candle['Open'])
        if limit_down_show <= p_open <= limit_up_show: points.append({"val": p_open, "tag": ""})
        p_high = apply_tick_rules(last_candle['High'])
        p_low = apply_tick_rules(last_candle['Low'])
        if limit_down_show <= p_high <= limit_up_show: points.append({"val": p_high, "tag": ""})
        if limit_down_show <= p_low <= limit_up_show: 
             tag_low = "跌停" if limit_down_T and abs(p_low - limit_down_T) < 0.01 else ""
             points.append({"val": p_low, "tag": tag_low})

    if len(hist_strat) >= 3:
        pre_prev_candle = hist_strat.iloc[-2]
        pp_high = apply_tick_rules(pre_prev_candle['High'])
        pp_low = apply_tick_rules(pre_prev_candle['Low'])
        if limit_down_show <= pp_high <= limit_up_show: points.append({"val": pp_high, "tag": ""})
        if limit_down_show <= pp_low <= limit_up_show: points.append({"val": pp_low, "tag": ""})

    high_90_raw = hist_strat['High'].max()
    low_90_raw = hist_strat['Low'][hist_strat['Low'] > 0].min() if not hist_strat['Low'][hist_strat['Low'] > 0].empty else hist_strat['Low'].min()
    points.append({"val": apply_tick_rules(high_90_raw), "tag": "高"})
    points.append({"val": apply_tick_rules(low_90_raw), "tag": "低"})
    
    if len(hist_strat) >= 2:
        today_high = hist_strat.iloc[-1]['High']
        if limit_up_T and abs(today_high - limit_up_T) < 0.01:
            tag_label = "漲停高" if abs(limit_up_T - high_90_raw) < 0.05 else "漲停"
            if limit_down_show <= limit_up_T <= limit_up_show: points.append({"val": limit_up_T, "tag": tag_label})

    target_price = apply_sr_rules(strategy_base_price * 1.03, strategy_base_price)
    stop_price = apply_sr_rules(strategy_base_price * 0.97, strategy_base_price)
    
    if len(hist_strat) >= 2:
        high_T, low_T, close_T = hist_strat.iloc[-1]['High'], hist_strat.iloc[-1]['Low'], hist_strat.iloc[-1]['Close']
        if (limit_up_T and high_T >= limit_up_T - 0.01) and (limit_up_T and close_T >= limit_up_T * 0.97):
            points.append({"val": target_price, "tag": ""})
        if (limit_down_T and low_T <= limit_down_T + 0.01) and (limit_down_T and close_T <= limit_down_T * 1.03):
            points.append({"val": stop_price, "tag": ""})

    display_candidates = sorted([p for p in points if p.get('force') or (limit_down_show <= p['val'] <= limit_up_show)], key=lambda x: x['val'])
    
    final_display_points = []
    for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
        tags = [x['tag'] for x in list(group) if x['tag']]
        final_tag = ""
        if "漲停高" in tags: final_tag = "漲停高"
        elif "漲停" in tags: final_tag = "漲停"
        elif "跌停" in tags: final_tag = "跌停"
        elif "高" in tags: final_tag = "高"
        elif "低" in tags: final_tag = "低"
        elif "3高" in tags: final_tag = "3高"
        elif "3低" in tags: final_tag = "3低"
        elif "多" in tags: final_tag = "多"
        elif "空" in tags: final_tag = "空"
        elif "平" in tags: final_tag = "平"
        final_display_points.append({"val": val, "tag": final_tag})
        
    note_parts = []
    seen_vals = set() 
    for p in final_display_points:
        if p['val'] in seen_vals and p['tag'] == "": continue
        seen_vals.add(p['val'])
        v_str = fmt_price(p['val'])
        t = p['tag']
        note_parts.append(f"{t}{v_str}" if t in ["漲停", "漲停高", "跌停", "高", "低", "3高", "3低"] else (f"{v_str}{t}" if t else v_str))
    
    auto_note = "-".join(note_parts)
    manual_note = st.session_state.saved_notes.get(code, "")
    strategy_note = f"{auto_note} {manual_note}" if manual_note else auto_note

    final_name = name_hint if name_hint else get_stock_name_online(code)
    light = "🔴" if "多" in strategy_note else ("🟢" if "空" in strategy_note else "⚪")
    
    return {
        "代號": code, "名稱": f"{light} {final_name}", "收盤價": round(strategy_base_price, 2),
        "漲跌幅": pct_change, "期貨": "✅" if code in st.session_state.futures_list else "", 
        "當日漲停價": limit_up_show, "當日跌停價": limit_down_show,
        "自訂價(可修)": None, "獲利目標": target_price, "防守停損": stop_price,   
        "戰略備註": strategy_note, "_points": final_display_points, "狀態": "",
        "_auto_note": auto_note
    }

# ==========================================
# 主介面 (Tabs)
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
                    xl_file = pd.ExcelFile(uploaded_file)
                    sheet_options = xl_file.sheet_names
                    selected_sheet = st.selectbox("選擇工作表", sheet_options, index=sheet_options.index("週轉率") if "週轉率" in sheet_options else 0)
                except: pass
        with src_tab2:
            st.selectbox("📜 歷史紀錄", options=st.session_state.url_history if st.session_state.url_history else ["(無紀錄)"], key="history_selected", index=None, on_change=lambda: setattr(st.session_state, 'cloud_url_input', st.session_state.history_selected), label_visibility="collapsed")
            st.text_input("輸入連結", key="cloud_url_input", placeholder="https://...")
        
        search_selection = st.multiselect("🔍 快速查詢", options=stock_options, key="search_multiselect", on_change=lambda: save_search_cache(st.session_state.search_multiselect), placeholder="輸入 2330 或 台積電...")

    if st.button("🚀 執行分析"):
        if not st.session_state.futures_list: st.session_state.futures_list = fetch_futures_list()
        targets = []
        df_up = pd.DataFrame()
        if st.session_state.cloud_url_input.strip():
            u = st.session_state.cloud_url_input.strip()
            if u not in st.session_state.url_history: st.session_state.url_history.insert(0, u); save_url_history(st.session_state.url_history)
        
        try:
            if uploaded_file:
                uploaded_file.seek(0); fname = uploaded_file.name.lower()
                if fname.endswith('.csv'):
                    try: df_up = pd.read_csv(uploaded_file, dtype=str, encoding='cp950')
                    except: uploaded_file.seek(0); df_up = pd.read_csv(uploaded_file, dtype=str)
                elif fname.endswith(('.html', '.htm', '.xls')):
                    try: dfs = pd.read_html(uploaded_file, encoding='cp950')
                    except: uploaded_file.seek(0); dfs = pd.read_html(uploaded_file, encoding='utf-8')
                    if dfs: df_up = dfs[0]
                elif fname.endswith('.xlsx'): df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet, dtype=str)
            elif st.session_state.cloud_url_input:
                url = st.session_state.cloud_url_input
                if "docs.google.com" in url: url = url.split("/edit")[0] + "/export?format=csv"
                try: df_up = pd.read_csv(url, dtype=str)
                except: df_up = pd.read_excel(url, dtype=str)
        except Exception as e: st.error(f"讀取失敗: {e}")

        if search_selection:
            for item in search_selection:
                parts = item.split(' ', 1); targets.append((parts[0], parts[1] if len(parts) > 1 else "", 'search', 9999))
        if not df_up.empty:
            df_up.columns = df_up.columns.astype(str).str.strip()
            c_col = next((c for c in df_up.columns if "代號" in str(c)), None)
            n_col = next((c for c in df_up.columns if "名稱" in str(c)), None)
            if c_col:
                count = 0
                for _, row in df_up.iterrows():
                    c_raw = str(row[c_col]).replace('=', '').replace('"', '').strip()
                    if not c_raw or c_raw.lower() == 'nan' or c_raw in st.session_state.ignored_stocks: continue
                    if hide_non_stock and (c_raw.startswith('00') or (len(c_raw) > 4 and c_raw.isdigit())): continue
                    targets.append((c_raw, str(row[n_col]) if n_col else "", 'upload', count)); count += 1

        st.session_state.all_candidates = targets
        existing_data = {}
        old_data_backup = st.session_state.stock_data.set_index('代號').to_dict('index') if not st.session_state.stock_data.empty else {}
        st.session_state.stock_data = pd.DataFrame()
        
        status_text = st.empty(); bar = st.progress(0)
        upload_limit = st.session_state.limit_rows; upload_current = 0; total_fetched = 0
        
        for i, (code, name, source, extra) in enumerate(targets):
            if source == 'upload' and upload_current >= upload_limit: continue
            status_text.text(f"正在分析: {code} {name} ...")
            if code in st.session_state.ignored_stocks: continue
            time.sleep(0.05)
            # [修正 3] 執行分析時傳入選項
            data = fetch_stock_data_raw(code, name, extra, include_3d_hl=include_3d_hl)
            if not data and code in old_data_backup: data = old_data_backup[code]
            if data:
                data.update({'_source': source, '_order': extra, '_source_rank': 1 if source == 'upload' else 2})
                existing_data[code] = data
                total_fetched += 1
                if source == 'upload': upload_current += 1
            bar.progress(min(total_fetched / (len(search_selection) + upload_limit if search_selection else upload_limit), 1.0))
        
        bar.empty(); status_text.empty()
        if existing_data:
            st.session_state.stock_data = pd.DataFrame(list(existing_data.values()))
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)

    if not st.session_state.stock_data.empty:
        df_all = st.session_state.stock_data.copy()
        if hide_non_stock:
            df_all = df_all[~(df_all['代號'].str.startswith('00') | ((df_all['代號'].str.len() > 4) & df_all['代號'].str.isdigit()))]
        
        df_display = df_all.sort_values(by=['_source_rank', '_order']).reset_index(drop=True)
        note_width_px = calculate_note_width(df_display['戰略備註'], current_font_size)
        df_display["移除"] = False
        
        points_map = df_display.set_index('代號')['_points'].to_dict() if '_points' in df_display.columns else {}
        auto_notes_dict = df_display.set_index('代號')['_auto_note'].to_dict() if '_auto_note' in df_display.columns else {}

        input_cols = ["移除", "代號", "名稱", "戰略備註", "自訂價(可修)", "狀態", "當日漲停價", "當日跌停價", "獲利目標", "防守停損", "收盤價", "漲跌幅", "期貨"]
        for c in ["當日漲停價", "當日跌停價", "獲利目標", "防守停損", "自訂價(可修)"]:
            if c in df_display.columns: df_display[c] = df_display[c].apply(fmt_price)

        if "收盤價" in df_display.columns:
            for i in range(len(df_display)):
                p, chg = float(df_display.at[i, "收盤價"]), float(df_display.at[i, "漲跌幅"])
                icon = "🔴" if chg > 0 else ("🟢" if chg < 0 else "⚪")
                df_display.at[i, "收盤價"] = f"{icon} {fmt_price(p)}"
                df_display.at[i, "漲跌幅"] = f"{icon} {chg:+.2f}%"

        edited_df = st.data_editor(
            df_display[input_cols],
            column_config={
                "移除": st.column_config.CheckboxColumn("刪除", width=40),
                "代號": st.column_config.TextColumn(disabled=True, width=50), 
                "名稱": st.column_config.TextColumn(disabled=True, width="small"),
                "收盤價": st.column_config.TextColumn(width="small", disabled=True),
                "漲跌幅": st.column_config.TextColumn(disabled=True, width="small"),
                "自訂價(可修)": st.column_config.TextColumn("自訂價 ✏️", width=60), 
                "戰略備註": st.column_config.TextColumn("戰略備註 ✏️", width=note_width_px),
                "狀態": st.column_config.TextColumn(width=60, disabled=True),
            },
            hide_index=True, key="main_editor"
        )
        
        # [修正 1] 新增儲存與刪除備註按鈕
        col_btn1, col_btn2, col_btn3, _ = st.columns([1.5, 1.2, 1.2, 6.1])
        with col_btn1:
            btn_update = st.button("⚡ 執行更新", type="primary", use_container_width=True)
        with col_btn2:
            btn_save_notes = st.button("💾 儲存備註", use_container_width=True, help="立即將表格中的手動備註永久儲存")
        with col_btn3:
            btn_del_notes = st.button("🗑️ 刪除備註", use_container_width=True, help="清空所有手動新增的備註文字")

        # 邏輯處理
        trigger_rerun = False
        if not edited_df.empty:
            # 處理刪除
            to_remove = edited_df[edited_df["移除"] == True]
            if not to_remove.empty:
                for c in to_remove["代號"].unique(): st.session_state.ignored_stocks.add(str(c))
                st.session_state.stock_data = st.session_state.stock_data[~st.session_state.stock_data["代號"].isin(to_remove["代號"].unique())]
                save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                trigger_rerun = True

        if btn_save_notes or btn_update:
            update_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
            for i, row in st.session_state.stock_data.iterrows():
                code = row['代號']
                if code in update_map:
                    new_val, new_note = update_map[code]['自訂價(可修)'], update_map[code]['戰略備註']
                    st.session_state.stock_data.at[i, '自訂價(可修)'] = new_val
                    # 提取手動部分
                    auto_part = auto_notes_dict.get(code, "")
                    manual_part = new_note[len(auto_part):].strip() if auto_part and new_note.startswith(auto_part) else new_note
                    st.session_state.saved_notes[code] = manual_part
                    st.session_state.stock_data.at[i, '戰略備註'] = f"{auto_part} {manual_part}".strip()
                    st.session_state.stock_data.at[i, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("備註與設定已儲存！", icon="💾")
            trigger_rerun = True

        if btn_del_notes:
            st.session_state.saved_notes = {}
            for i, row in st.session_state.stock_data.iterrows():
                auto_part = auto_notes_dict.get(row['代號'], "")
                st.session_state.stock_data.at[i, '戰略備註'] = auto_part
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("已清除所有手動備註", icon="🗑️")
            trigger_rerun = True

        if trigger_rerun: st.rerun()

        # 自動遞補邏輯
        upload_count = len(st.session_state.stock_data[st.session_state.stock_data['_source'] == 'upload']) if not st.session_state.stock_data.empty else 0
        if upload_count < st.session_state.limit_rows and st.session_state.all_candidates:
            needed = st.session_state.limit_rows - upload_count; replenished = 0
            existing = set(st.session_state.stock_data['代號'].astype(str))
            for cand in st.session_state.all_candidates:
                c_code = str(cand[0])
                if cand[2] == 'upload' and c_code not in st.session_state.ignored_stocks and c_code not in existing:
                    # [修正 3] 遞補時也傳入選項
                    data = fetch_stock_data_raw(c_code, cand[1], cand[3], include_3d_hl=include_3d_hl)
                    if data:
                        data.update({'_source': 'upload', '_order': cand[3], '_source_rank': 1})
                        st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame([data])], ignore_index=True)
                        existing.add(c_code); replenished += 1
                    if replenished >= needed: break
            if replenished > 0:
                save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                st.rerun()

        st.markdown("---")
        auto_update = st.checkbox("☑️ 啟用最後一列自動更新", value=st.session_state.auto_update_last_row, key="toggle_auto_update")
        st.session_state.auto_update_last_row = auto_update
        if auto_update:
            col_delay, _ = st.columns([2, 8])
            with col_delay:
                st.session_state.update_delay_sec = st.number_input("⏳ 緩衝秒數", 0.0, 5.0, 0.1, st.session_state.update_delay_sec)

with tab2:
    st.markdown("#### 💰 當沖損益室 💰")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        calc_price = st.number_input("基準價格", value=float(st.session_state.calc_base_price), step=0.01, format="%.2f")
        if calc_price != st.session_state.calc_base_price:
            st.session_state.calc_base_price = calc_price
            st.session_state.calc_view_price = apply_tick_rules(calc_price)
    with c2: shares = st.number_input("股數", 1000, step=1000)
    with c3: discount = st.number_input("手續費折扣 (折)", 2.8, step=0.1)
    with c4: min_fee = st.number_input("最低手續費 (元)", 20, step=1)
    with c5: tick_count = st.number_input("顯示檔數 (檔)", 5, 1, 50)
    
    direction = st.radio("交易方向", ["當沖多 (先買後賣)", "當沖空 (先賣後買)"], horizontal=True)
    l_up, l_down = calculate_limits(st.session_state.calc_base_price)
    
    b1, b2, _ = st.columns([1, 1, 6])
    with b1:
        if st.button("🔼 向上", use_container_width=True):
            st.session_state.calc_view_price = min(l_up, move_tick(st.session_state.calc_view_price, tick_count)); st.rerun()
    with b2:
        if st.button("🔽 向下", use_container_width=True):
            st.session_state.calc_view_price = max(l_down, move_tick(st.session_state.calc_view_price, -tick_count)); st.rerun()
    
    calc_data = []
    view_p = st.session_state.calc_view_price
    is_long = "多" in direction
    for i in range(tick_count, -(tick_count + 1), -1):
        p = move_tick(view_p, i)
        if p > l_up or p < l_down: continue
        b_p, s_p = (st.session_state.calc_base_price, p) if is_long else (p, st.session_state.calc_base_price)
        b_f = max(min_fee, math.floor(b_p * shares * 0.001425 * (discount/10)))
        s_f = max(min_fee, math.floor(s_p * shares * 0.001425 * (discount/10)))
        tax = math.floor(st.session_state.calc_base_price * shares * 0.0015) if not is_long else math.floor(s_p * shares * 0.0015)
        prof = (s_p * shares - s_f - tax) - (b_p * shares + b_f)
        diff = p - st.session_state.calc_base_price
        calc_data.append({
            "成交價": fmt_price(p), "漲跌": f"{diff:+.2f}".rstrip('0').rstrip('.') if diff != 0 else "0",
            "預估損益": int(prof), "報酬率%": f"{(prof/(st.session_state.calc_base_price*shares))*100:+.2f}%",
            "手續費": int(b_f + s_f), "交易稅": int(tax), "_prof": prof, "_is_base": abs(p-st.session_state.calc_base_price)<0.001
        })
        
    if calc_data:
        df_c = pd.DataFrame(calc_data)
        st.dataframe(df_c.style.apply(lambda r: ['background-color: #ffffcc; font-weight: bold'] * len(r) if r['_is_base'] else (['color: #ff4b4b'] * len(r) if r['_prof'] > 0 else (['color: #00cc00'] * len(r) if r['_prof'] < 0 else ['color: gray'] * len(r))), axis=1), hide_index=True, column_config={"_prof":None, "_is_base":None})

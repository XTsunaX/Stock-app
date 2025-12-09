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
st.set_page_config(page_title="當沖戰略室", page_icon="⚡", layout="wide")

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

saved_config = load_config()

if 'font_size' not in st.session_state:
    st.session_state.font_size = saved_config.get('font_size', 15)

if 'limit_rows' not in st.session_state:
    st.session_state.limit_rows = saved_config.get('limit_rows', 5)

if 'auto_update_last_row' not in st.session_state:
    st.session_state.auto_update_last_row = saved_config.get('auto_update', True)

if 'update_delay_sec' not in st.session_state:
    st.session_state.update_delay_sec = saved_config.get('delay_sec', 4.0)

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

def get_live_price(code):
    """
    抓取當下即時成交價 (Sanity Check 用)。
    """
    # 1. 嘗試 twstock
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

    # 2. 備援 yfinance fast_info
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
    """
    [備援] 爬取 Yahoo 股市網頁
    """
    try:
        url = f"https://tw.stock.yahoo.com/quote/{code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        r = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        price_tag = soup.find('span', class_='Fz(32px)')
        if not price_tag: return None
        price = float(price_tag.text.replace(',', ''))
        
        # 昨收
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
    """
    [新增] FinMind API 備援 (資料非常準確)
    """
    try:
        # 計算3個月前的日期
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        url = f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockPrice&data_id={code}&start_date={start_date}"
        r = requests.get(url, timeout=5)
        data_json = r.json()
        
        if data_json.get('msg') == 'success' and data_json.get('data'):
            df = pd.DataFrame(data_json['data'])
            df['Date'] = pd.to_datetime(df['date'])
            df = df.set_index('Date')
            # 轉換欄位名稱以符合程式邏輯
            rename_map = {
                'open': 'Open', 'max': 'High', 'min': 'Low', 'close': 'Close', 'Trading_Volume': 'Volume'
            }
            df = df.rename(columns=rename_map)
            cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            # 確保欄位存在 (FinMind 欄位名有時不同)
            for c in cols:
                if c not in df.columns:
                    # 嘗試其他大小寫
                    if c.lower() in df.columns: df[c] = df[c.lower()]
                    else: df[c] = 0.0 # 若真的沒有
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
        
        if l_up is not None and abs(price - l_up) < 0.01: 
            status = "🔴 漲停"
        elif l_down is not None and abs(price - l_down) < 0.01: 
            status = "🟢 跌停"
        else:
            # 1. 檢查後台計算點位
            points = points_map.get(code, [])
            hit = False
            if isinstance(points, list):
                for p in points:
                    if abs(p['val'] - price) < 0.01:
                        hit = True; break
            
            # 2. 檢查手動備註內的數字
            if not hit:
                note_text = str(row.get('戰略備註', ''))
                found_prices = re.findall(r'\d+\.?\d*', note_text)
                for fp in found_prices:
                    try:
                        if abs(float(fp) - price) < 0.01:
                            hit = True; break
                    except: pass
            
            if hit: status = "🟡 命中"
            
        return status
    except: return status

# [修正] 嚴格順序制 + 完全靜態化 + 條件式 +/-3%
def fetch_stock_data_raw(code, name_hint="", extra_data=None):
    code = str(code).strip()
    
    hist = pd.DataFrame()
    source_used = "none"
    backup_prev_close = None

    # 1. 第一順位: YFinance
    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist_yf = ticker.history(period="3mo")
        if hist_yf.empty:
            ticker = yf.Ticker(f"{code}.TWO")
            hist_yf = ticker.history(period="3mo")
        
        if not hist_yf.empty:
            hist = hist_yf
            source_used = "yfinance"
    except: pass

    # 2. 第二順位: TWStock
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
                
                if not df_tw.empty:
                    hist = df_tw[cols]
                    source_used = "twstock"
        except: pass

    # 3. 第三順位: FinMind
    if hist.empty:
        df_fm = fetch_finmind_backup(code)
        if df_fm is not None and not df_fm.empty:
            hist = df_fm
            source_used = "finmind"

    # 4. 第四順位: Yahoo 網頁爬蟲
    if hist.empty:
        df_web, web_prev_close = fetch_yahoo_web_backup(code)
        if df_web is not None:
            hist = df_web
            backup_prev_close = web_prev_close
            source_used = "web_backup"

    if hist.empty: return None

    # --- 時間與資料定位 ---
    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    
    last_date = hist.index[-1].date()
    is_today_in_hist = (last_date == now.date())
    
    # 盤中界線：13:30 (台股收盤)
    is_during_trading = (now.time() < dt_time(13, 30))
    
    # 決定戰略使用的 "History" (hist_strat)
    hist_strat = hist.copy()
    
    if is_during_trading:
        # 盤中模式：強制剔除今日資料
        if is_today_in_hist:
            hist_strat = hist_strat.iloc[:-1]
    else:
        # 盤後模式：必須包含今日資料
        if not is_today_in_hist and source_used != "web_backup":
            live = get_live_price(code)
            if live:
                new_row = pd.DataFrame(
                    {'Open': live, 'High': live, 'Low': live, 'Close': live, 'Volume': 0},
                    index=[pd.to_datetime(now.date())]
                )
                hist_strat = pd.concat([hist_strat, new_row])

    if hist_strat.empty: return None

    # 1. 決定 UI 顯示用的 "收盤價" (靜態)
    strategy_base_price = hist_strat.iloc[-1]['Close'] # 這是 T
    
    # 找 T-1 用於計算漲跌幅
    if len(hist_strat) >= 2:
        prev_of_base = hist_strat.iloc[-2]['Close']
    else:
        prev_of_base = strategy_base_price 

    # 計算靜態漲跌幅
    if prev_of_base > 0:
        pct_change = ((strategy_base_price - prev_of_base) / prev_of_base) * 100
    else:
        pct_change = 0.0

    # 2. 決定 Limit Base Price (漲跌停基準)
    base_price_for_limit = strategy_base_price
    limit_up_show, limit_down_show = calculate_limits(base_price_for_limit)

    # 3. 戰略備註計算 (Strategy Note)
    target_raw = strategy_base_price * 1.03
    stop_raw = strategy_base_price * 0.97
    target_price = apply_sr_rules(target_raw, strategy_base_price)
    stop_price = apply_sr_rules(stop_raw, strategy_base_price)
    
    points = []
    
    # 5MA
    if len(hist_strat) >= 5:
        ma5_raw = hist_strat['Close'].tail(5).mean()
        ma5 = apply_sr_rules(ma5_raw, strategy_base_price)
        ma5_tag = "多" if ma5_raw < strategy_base_price else ("空" if ma5_raw > strategy_base_price else "平")
        points.append({"val": ma5, "tag": ma5_tag, "force": True})

    # T-1 (相對於戰略基準的前一日)
    if len(hist_strat) >= 2:
        last_candle = hist_strat.iloc[-1] # T
        prev_candle = hist_strat.iloc[-2] # T-1
        
        # [NEW] 盤後 (T為今日) -> 將 T 的 Open 加入戰略
        if not is_during_trading:
             p_open = apply_tick_rules(last_candle['Open'])
             if limit_down_show <= p_open <= limit_up_show: 
                 points.append({"val": p_open, "tag": ""})

        # 加入 T (Last Candle) 的高低 (對盤中來說這是昨天，對盤後來說這是今天)
        p_high = apply_tick_rules(last_candle['High'])
        p_low = apply_tick_rules(last_candle['Low'])
        
        if limit_down_show <= p_high <= limit_up_show: points.append({"val": p_high, "tag": ""})
        if limit_down_show <= p_low <= limit_up_show: points.append({"val": p_low, "tag": ""})

    # T-2
    if len(hist_strat) >= 3:
        pre_prev_candle = hist_strat.iloc[-2] # T-1
        
        pp_high = apply_tick_rules(pre_prev_candle['High'])
        pp_low = apply_tick_rules(pre_prev_candle['Low'])
        
        if limit_down_show <= pp_high <= limit_up_show: points.append({"val": pp_high, "tag": ""})
        if limit_down_show <= pp_low <= limit_up_show: points.append({"val": pp_low, "tag": ""})

    # 近期高低 & +3/-3 顯示邏輯
    show_plus_3 = False
    show_minus_3 = False
    
    if not hist_strat.empty:
        high_90_raw = hist_strat['High'].max()
        low_90_raw = hist_strat['Low'].min()
        high_90 = apply_tick_rules(high_90_raw)
        low_90 = apply_tick_rules(low_90_raw)
        
        points.append({"val": high_90, "tag": "高"})
        points.append({"val": low_90, "tag": "低"})
        
        # 1. 檢查是否創新高 (Current ~ High90)
        is_new_high = abs(strategy_base_price - high_90_raw) < 0.05
        if is_new_high:
            show_plus_3 = True # 創新高，無壓力，顯示目標
            # 尋找次高作為支撐
            sorted_highs = hist_strat['High'].sort_values(ascending=False).unique()
            if len(sorted_highs) > 1:
                second_high = sorted_highs[1]
                points.append({"val": apply_tick_rules(second_high), "tag": ""})
        else:
            # 沒創新高，檢查 High90 是否在明日漲停內
            if high_90 > limit_up_show:
                show_plus_3 = True # 高點太遠，顯示 +3% 當目標
            else:
                show_plus_3 = False # 高點可及，不顯示 +3%

        # 2. 檢查是否創新低
        is_new_low = abs(strategy_base_price - low_90_raw) < 0.05
        if is_new_low:
            show_minus_3 = True
            sorted_lows = hist_strat['Low'].sort_values(ascending=True).unique()
            if len(sorted_lows) > 1:
                second_low = sorted_lows[1]
                points.append({"val": apply_tick_rules(second_low), "tag": ""})
        else:
            if low_90 < limit_down_show:
                show_minus_3 = True
            else:
                show_minus_3 = False

    if show_plus_3: points.append({"val": target_price, "tag": ""})
    if show_minus_3: points.append({"val": stop_price, "tag": ""})
        
    display_candidates = []
    for p in points:
        v = float(f"{p['val']:.2f}")
        is_force = p.get('force', False)
        if is_force or (limit_down_show <= v <= limit_up_show):
             display_candidates.append(p) 
        
    display_candidates.sort(key=lambda x: x['val'])
    
    final_display_points = []
    for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
        g_list = list(group)
        tags = [x['tag'] for x in g_list if x['tag']]
        final_tag = ""
        has_limit_up = "漲停" in tags
        has_limit_down = "跌停" in tags
        has_high = "高" in tags
        has_low = "低" in tags
        
        if has_limit_up and has_high: final_tag = "漲停高"
        elif has_limit_down and has_low: final_tag = "跌停低"
        elif has_limit_up: final_tag = "漲停"
        elif has_limit_down: final_tag = "跌停"
        else:
            if has_high: final_tag = "高"
            elif has_low: final_tag = "低"
            elif "多" in tags: final_tag = "多"
            elif "空" in tags: final_tag = "空"
            elif "平" in tags: final_tag = "平"
        
        if ("多" in tags or "空" in tags or "平" in tags) and final_tag not in ["漲停", "跌停", "漲停高", "跌停低"]:
            if "多" in tags: final_tag = "多"
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
        if t in ["漲停", "漲停高", "跌停", "跌停低", "高", "低"]: item = f"{t}{v_str}"
        elif t: item = f"{v_str}{t}"
        else: item = v_str
        note_parts.append(item)
    
    strategy_note = "-".join(note_parts)
    full_calc_points = final_display_points
    
    final_name = name_hint if name_hint else get_stock_name_online(code)
    light = "⚪"
    if "多" in strategy_note: light = "🔴"
    elif "空" in strategy_note: light = "🟢"
    final_name_display = f"{light} {final_name}"
    
    return {
        "代號": code, "名稱": final_name_display, "收盤價": round(strategy_base_price, 2),
        "漲跌幅": pct_change, "當日漲停價": limit_up_show, "當日跌停價": limit_down_show,
        "自訂價(可修)": None, "獲利目標": target_price, "防守停損": stop_price,   
        "戰略備註": strategy_note, "_points": full_calc_points, "狀態": ""
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
            uploaded_file = st.file_uploader("上傳檔案 (CSV/XLS/HTML)", type=['xlsx', 'csv', 'html', 'xls'], label_visibility="collapsed")
            selected_sheet = 0
            if uploaded_file:
                try:
                    if not uploaded_file.name.endswith('.csv'):
                        xl_file = pd.ExcelFile(uploaded_file)
                        sheet_options = xl_file.sheet_names
                        default_idx = 0
                        if "週轉率" in sheet_options: default_idx = sheet_options.index("週轉率")
                        selected_sheet = st.selectbox("選擇工作表", sheet_options, index=default_idx)
                except: pass

        with src_tab2:
            def on_history_change():
                st.session_state.cloud_url_input = st.session_state.history_selected

            history_opts = st.session_state.url_history if st.session_state.url_history else ["(無紀錄)"]
            
            c_sel, c_del = st.columns([8, 1], gap="small")
            
            with c_sel:
                selected = st.selectbox(
                    "📜 歷史紀錄 (選取自動填入)", 
                    options=history_opts,
                    key="history_selected",
                    index=None,
                    placeholder="請選擇...",
                    on_change=on_history_change,
                    label_visibility="collapsed"
                )
            
            with c_del:
                if st.button("🗑️", help="刪除選取的歷史紀錄"):
                    if st.session_state.history_selected and st.session_state.history_selected in st.session_state.url_history:
                        st.session_state.url_history.remove(st.session_state.history_selected)
                        save_url_history(st.session_state.url_history)
                        st.toast("已刪除。", icon="🗑️")
                        st.rerun()

            st.text_input(
                "輸入連結 (CSV/Excel/Google Sheet)", 
                key="cloud_url_input",
                placeholder="https://..."
            )
        
        def update_search_cache():
            save_search_cache(st.session_state.search_multiselect)

        search_selection = st.multiselect(
            "🔍 快速查詢 (中文/代號)", 
            options=stock_options, 
            key="search_multiselect", 
            on_change=update_search_cache, 
            placeholder="輸入 2330 或 台積電..."
        )

    if st.button("🚀 執行分析"):
        save_search_cache(st.session_state.search_multiselect)
        
        targets = []
        df_up = pd.DataFrame()
        
        current_url = st.session_state.cloud_url_input.strip()
        if current_url:
            if current_url not in st.session_state.url_history:
                st.session_state.url_history.insert(0, current_url) 
                save_url_history(st.session_state.url_history)
        
        try:
            if uploaded_file:
                uploaded_file.seek(0)
                fname = uploaded_file.name.lower()
                
                if fname.endswith('.csv'):
                    try: df_up = pd.read_csv(uploaded_file, dtype=str, encoding='cp950')
                    except: 
                        uploaded_file.seek(0)
                        df_up = pd.read_csv(uploaded_file, dtype=str)
                        
                elif fname.endswith('.html') or fname.endswith('.htm') or fname.endswith('.xls'):
                    try: dfs = pd.read_html(uploaded_file, encoding='cp950')
                    except:
                        uploaded_file.seek(0)
                        dfs = pd.read_html(uploaded_file, encoding='utf-8')
                    for df in dfs:
                        if df.apply(lambda r: r.astype(str).str.contains('代號').any(), axis=1).any():
                             df_up = df
                             for i, row in df.iterrows():
                                 if "代號" in row.values:
                                     df_up.columns = row
                                     df_up = df_up.iloc[i+1:]
                                     break
                             break
                    if df_up.empty and dfs: df_up = dfs[0]
                
                elif fname.endswith('.xlsx'):
                    df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet, dtype=str)

            elif st.session_state.cloud_url_input:
                url = st.session_state.cloud_url_input
                if "docs.google.com" in url and "/spreadsheets/" in url and "/edit" in url:
                    url = url.split("/edit")[0] + "/export?format=csv"
                try: df_up = pd.read_csv(url, dtype=str)
                except:
                    try: df_up = pd.read_excel(url, dtype=str)
                    except: st.error("❌ 無法讀取雲端檔案。")
        except Exception as e: st.error(f"讀取失敗: {e}")

        if search_selection:
            for item in search_selection:
                parts = item.split(' ', 1)
                targets.append((parts[0], parts[1] if len(parts) > 1 else "", 'search', 9999))

        if not df_up.empty:
            df_up.columns = df_up.columns.astype(str).str.strip()
            c_col = next((c for c in df_up.columns if "代號" in str(c)), None)
            n_col = next((c for c in df_up.columns if "名稱" in str(c)), None)
            
            if c_col:
                limit_rows = st.session_state.limit_rows
                count = 0
                
                for _, row in df_up.iterrows():
                    c_raw = str(row[c_col]).replace('=', '').replace('"', '').strip()
                    if not c_raw or c_raw.lower() == 'nan': continue
                    is_valid = False
                    if c_raw.isdigit() and len(c_raw) <= 4: is_valid = True
                    elif len(c_raw) > 0 and (c_raw[0].isdigit() or c_raw[0] in ['0','00']): is_valid = True
                    if not is_valid: continue
                    
                    if c_raw in st.session_state.ignored_stocks: continue
                    
                    if hide_non_stock:
                        is_etf = c_raw.startswith('00')
                        is_warrant = (len(c_raw) > 4) and c_raw.isdigit()
                        if is_etf or is_warrant: continue
                    
                    n = str(row[n_col]) if n_col else ""
                    if n.lower() == 'nan': n = ""
                    targets.append((c_raw, n, 'upload', count))
                    count += 1

        st.session_state.all_candidates = targets

        results = []
        seen = set()
        status_text = st.empty()
        bar = st.progress(0)
        
        upload_limit = st.session_state.limit_rows
        upload_current = 0
        total_fetched = 0
        
        total_for_bar = len(search_selection) if search_selection else 0
        total_for_bar += min(len([t for t in targets if t[2]=='upload']), upload_limit)
        if total_for_bar == 0: total_for_bar = 1
        
        existing_data = {}
        
        old_data_backup = {}
        if not st.session_state.stock_data.empty:
             old_data_backup = st.session_state.stock_data.set_index('代號').to_dict('index')

        st.session_state.stock_data = pd.DataFrame() 
        fetch_cache = {}
        
        for i, (code, name, source, extra) in enumerate(targets):
            
            if source == 'upload':
                if upload_current >= upload_limit:
                    continue 
            
            status_text.text(f"正在分析: {code} {name} ...")
            
            if code in st.session_state.ignored_stocks: continue
            if (code, source) in seen: continue
            
            time.sleep(0.1)
            
            if code in fetch_cache: data = fetch_cache[code]
            else:
                data = fetch_stock_data_raw(code, name, extra)
                if not data and code in old_data_backup:
                    data = old_data_backup[code]
                    
                if data: fetch_cache[code] = data
            
            if data:
                data['_source'] = source
                data['_order'] = extra
                data['_source_rank'] = 1 if source == 'upload' else 2
                existing_data[code] = data
                seen.add((code, source))
                
                total_fetched += 1
                if source == 'upload':
                    upload_current += 1
                
            bar.progress(min(total_fetched / total_for_bar, 1.0))
        
        bar.empty()
        status_text.empty()
        
        if existing_data:
            st.session_state.stock_data = pd.DataFrame(list(existing_data.values()))
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates)

    if not st.session_state.stock_data.empty:
        limit = st.session_state.limit_rows
        df_all = st.session_state.stock_data.copy()
        
        if '_source' not in df_all.columns:
            df_all['_source'] = 'upload'

        df_all = df_all.rename(columns={"漲停價": "當日漲停價", "跌停價": "當日跌停價", "獲利目標": "+3%", "防守停損": "-3%"})
        df_all['代號'] = df_all['代號'].astype(str)
        df_all = df_all[~df_all['代號'].isin(st.session_state.ignored_stocks)]
        
        if hide_non_stock:
             mask_etf = df_all['代號'].str.startswith('00')
             mask_warrant = (df_all['代號'].str.len() > 4) & df_all['代號'].str.isdigit()
             df_all = df_all[~(mask_etf | mask_warrant)]
        
        if '_source_rank' in df_all.columns:
            df_all = df_all.sort_values(by=['_source_rank', '_order'])
        
        df_display = df_all.reset_index(drop=True)
        note_width_px = calculate_note_width(df_display['戰略備註'], current_font_size)
        df_display["移除"] = False
        
        points_map = {}
        if '_points' in df_display.columns:
            points_map = df_display.set_index('代號')['_points'].to_dict()

        input_cols = ["移除", "代號", "名稱", "戰略備註", "自訂價(可修)", "狀態", "當日漲停價", "當日跌停價", "+3%", "-3%", "收盤價", "漲跌幅"]
        for col in input_cols:
            if col not in df_display.columns: df_display[col] = None

        cols_to_fmt = ["收盤價", "當日漲停價", "當日跌停價", "+3%", "-3%", "自訂價(可修)"]
        for c in cols_to_fmt:
            if c in df_display.columns: df_display[c] = df_display[c].apply(fmt_price)

        df_display = df_display.reset_index(drop=True)
        for col in input_cols:
             if col != "移除": df_display[col] = df_display[col].astype(str)

        edited_df = st.data_editor(
            df_display[input_cols],
            column_config={
                "移除": st.column_config.CheckboxColumn("刪除", width=40, help="勾選後刪除並自動遞補"),
                "代號": st.column_config.TextColumn(disabled=True, width="small"),
                "名稱": st.column_config.TextColumn(disabled=True, width="small"),
                "收盤價": st.column_config.TextColumn(width="small", disabled=True),
                "漲跌幅": st.column_config.NumberColumn(format="%.2f%%", disabled=True, width="small"),
                "自訂價(可修)": st.column_config.TextColumn("自訂價 ✏️", width=80),
                "當日漲停價": st.column_config.TextColumn(width="small", disabled=True),
                "當日跌停價": st.column_config.TextColumn(width="small", disabled=True),
                "+3%": st.column_config.TextColumn(width="small", disabled=True),
                "-3%": st.column_config.TextColumn(width="small", disabled=True),
                "狀態": st.column_config.TextColumn(width=60, disabled=True),
                "戰略備註": st.column_config.TextColumn("戰略備註 ✏️", width=note_width_px, disabled=False),
            },
            hide_index=True,
            use_container_width=False,
            num_rows="fixed",
            key="main_editor"
        )

        if not edited_df.empty and "移除" in edited_df.columns:
            to_remove = edited_df[edited_df["移除"] == True]
            if not to_remove.empty:
                remove_codes = to_remove["代號"].unique()
                for c in remove_codes:
                    st.session_state.ignored_stocks.add(str(c))
                
                st.session_state.stock_data = st.session_state.stock_data[
                    ~st.session_state.stock_data["代號"].isin(remove_codes)
                ]
                
        df_curr = st.session_state.stock_data
        if not df_curr.empty:
            if '_source' not in df_curr.columns:
                 upload_count = len(df_curr)
            else:
                 upload_count = len(df_curr[df_curr['_source'] == 'upload'])
            
            limit = st.session_state.limit_rows
            
            if upload_count < limit and st.session_state.all_candidates:
                needed = limit - upload_count
                replenished_count = 0
                
                existing_codes = set(st.session_state.stock_data['代號'].astype(str))
                
                with st.spinner("正在載入更多資料..."):
                    for cand in st.session_state.all_candidates:
                         c_code = str(cand[0])
                         c_name = cand[1]
                         c_source = cand[2]
                         c_extra = cand[3]
                         
                         if c_source != 'upload': continue
                         if c_code in st.session_state.ignored_stocks: continue
                         if c_code in existing_codes: continue
                         
                         data = fetch_stock_data_raw(c_code, c_name, c_extra)
                         if data:
                             data['_source'] = c_source
                             data['_order'] = c_extra
                             data['_source_rank'] = 1
                             
                             st.session_state.stock_data = pd.concat([
                                 st.session_state.stock_data, 
                                 pd.DataFrame([data])
                             ], ignore_index=True)
                             
                             existing_codes.add(c_code)
                             replenished_count += 1
                             
                         if replenished_count >= needed: break
                
                if replenished_count > 0:
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates)
                    st.toast(f"已更新顯示筆數，增加 {replenished_count} 檔。", icon="🔄")
                    st.rerun()

        need_update = False
        
        if st.session_state.auto_update_last_row and not edited_df.empty:
            last_idx = len(edited_df) - 1
            last_row_price = str(edited_df.iloc[last_idx]['自訂價(可修)']).strip()
            
            if last_row_price and last_row_price.lower() != 'nan' and last_row_price.lower() != 'none':
                current_code = edited_df.iloc[last_idx]['代號']
                original_row = st.session_state.stock_data[st.session_state.stock_data['代號'] == current_code]
                
                if not original_row.empty:
                    orig_status = str(original_row.iloc[0]['狀態']).strip()
                    orig_price = str(original_row.iloc[0]['自訂價(可修)']).strip()
                    
                    if (not orig_status or orig_status == 'nan') or (last_row_price != orig_price):
                        need_update = True
        
        if need_update:
            if st.session_state.update_delay_sec > 0:
                time.sleep(st.session_state.update_delay_sec)
                
            update_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
            for i, row in st.session_state.stock_data.iterrows():
                code = row['代號']
                if code in update_map:
                    st.session_state.stock_data.at[i, '自訂價(可修)'] = update_map[code]['自訂價(可修)']
                    st.session_state.stock_data.at[i, '戰略備註'] = update_map[code]['戰略備註']
                    
                    new_status = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
                    st.session_state.stock_data.at[i, '狀態'] = new_status
            st.rerun()

        st.markdown("---")
        
        col_btn, _ = st.columns([2, 8])
        with col_btn:
            btn_update = st.button("⚡ 執行更新", use_container_width=False, type="primary")
        
        auto_update = st.checkbox("☑️ 啟用最後一列自動更新", 
            value=st.session_state.auto_update_last_row,
            key="toggle_auto_update")
        st.session_state.auto_update_last_row = auto_update
        
        if auto_update:
            col_delay, _ = st.columns([2, 8])
            with col_delay:
                delay_val = st.number_input("⏳ 緩衝秒數", 
                    min_value=0.0, max_value=5.0, step=0.1, 
                    value=st.session_state.update_delay_sec)
                st.session_state.update_delay_sec = delay_val

        if btn_update:
             update_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
             for i, row in st.session_state.stock_data.iterrows():
                code = row['代號']
                if code in update_map:
                    st.session_state.stock_data.at[i, '自訂價(可修)'] = update_map[code]['自訂價(可修)']
                    st.session_state.stock_data.at[i, '戰略備註'] = update_map[code]['戰略備註']
                
                new_status = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
                st.session_state.stock_data.at[i, '狀態'] = new_status
             st.rerun()

with tab2:
    st.markdown("#### 💰 當沖損益室 💰")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        calc_price = st.number_input("基準價格", value=float(st.session_state.calc_base_price), step=0.01, format="%.2f", key="input_base_price")
        if calc_price != st.session_state.calc_base_price:
            st.session_state.calc_base_price = calc_price
            st.session_state.calc_view_price = apply_tick_rules(calc_price)
    with c2: shares = st.number_input("股數", value=1000, step=1000)
    with c3: discount = st.number_input("手續費折扣 (折)", value=2.8, step=0.1, min_value=0.1, max_value=10.0)
    with c4: min_fee = st.number_input("最低手續費 (元)", value=20, step=1)
    with c5: tick_count = st.number_input("顯示檔數 (檔)", value=5, min_value=1, max_value=50, step=1)
    direction = st.radio("交易方向", ["當沖多 (先買後賣)", "當沖空 (先賣後買)"], horizontal=True)
    limit_up, limit_down = calculate_limits(st.session_state.calc_base_price)
    b1, b2, _ = st.columns([1, 1, 6])
    with b1:
        if st.button("🔼 向上", use_container_width=True):
            if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
            st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, tick_count)
            if st.session_state.calc_view_price > limit_up: st.session_state.calc_view_price = limit_up
            st.rerun()
    with b2:
        if st.button("🔽 向下", use_container_width=True):
            if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
            st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, -tick_count)
            if st.session_state.calc_view_price < limit_down: st.session_state.calc_view_price = limit_down
            st.rerun()
    
    ticks_range = range(tick_count, -(tick_count + 1), -1)
    calc_data = []
    base_p = st.session_state.calc_base_price
    if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = base_p
    view_p = st.session_state.calc_view_price
    is_long = "多" in direction
    fee_rate = 0.001425; tax_rate = 0.0015 
    
    for i in ticks_range:
        p = move_tick(view_p, i)
        if p > limit_up or p < limit_down: continue
        
        if is_long:
            buy_price = base_p; sell_price = p
            buy_fee = max(min_fee, math.floor(buy_price * shares * fee_rate * (discount/10)))
            sell_fee = max(min_fee, math.floor(sell_price * shares * fee_rate * (discount/10)))
            tax = math.floor(sell_price * shares * tax_rate)
            cost = (buy_price * shares) + buy_fee
            income = (sell_price * shares) - sell_fee - tax
            profit = income - cost
            total_fee = buy_fee + sell_fee
        else: 
            sell_price = base_p; buy_price = p
            sell_fee = max(min_fee, math.floor(sell_price * shares * fee_rate * (discount/10)))
            buy_fee = max(min_fee, math.floor(buy_price * shares * fee_rate * (discount/10)))
            tax = math.floor(sell_price * shares * tax_rate)
            income = (sell_price * shares) - sell_fee - tax
            cost = (buy_price * shares) + buy_fee
            profit = income - cost
            total_fee = buy_fee + sell_fee
        roi = 0
        if (base_p * shares) != 0: roi = (profit / (base_p * shares)) * 100
        diff = p - base_p
        diff_str = f"{diff:+.2f}".rstrip('0').rstrip('.') if diff != 0 else "0"
        if diff > 0 and not diff_str.startswith('+'): diff_str = "+" + diff_str
        
        note_type = ""
        if abs(p - limit_up) < 0.001: note_type = "up"
        elif abs(p - limit_down) < 0.001: note_type = "down"
        is_base = (abs(p - base_p) < 0.001)
        
        calc_data.append({
            "成交價": fmt_price(p), "漲跌": diff_str, "預估損益": int(profit), "報酬率%": f"{roi:+.2f}%",
            "手續費": int(total_fee), "交易稅": int(tax), "_profit": profit, "_note_type": note_type, "_is_base": is_base
        })
        
    df_calc = pd.DataFrame(calc_data)
    def style_calc_row(row):
        if row['_is_base']: return ['background-color: #ffffcc; color: black; font-weight: bold; border: 2px solid #ffd700;'] * len(row)
        nt = row['_note_type']
        if nt == 'up': return ['background-color: #ff4b4b; color: white; font-weight: bold'] * len(row)
        elif nt == 'down': return ['background-color: #00cc00; color: white; font-weight: bold'] * len(row)
        prof = row['_profit']
        if prof > 0: return ['color: #ff4b4b; font-weight: bold'] * len(row) 
        elif prof < 0: return ['color: #00cc00; font-weight: bold'] * len(row) 
        else: return ['color: gray'] * len(row)

    if not df_calc.empty:
        table_height = (len(df_calc) + 1) * 35 
        st.dataframe(
            df_calc.style.apply(style_calc_row, axis=1), use_container_width=False, hide_index=True, height=table_height,
            column_config={"_profit": None, "_note_type": None, "_is_base": None}
        )

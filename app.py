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
from datetime import datetime, time as dt_time, timedelta, date
import pytz
from decimal import Decimal, ROUND_HALF_UP
import io
import twstock
from concurrent.futures import ThreadPoolExecutor, as_completed
import calendar
import random  # [修正] 確保 random 模組有被匯入
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="當沖戰略室", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

# CSS 優化
st.markdown("""
<style>
    /* 側邊欄按鈕文字不換行 */
    [data-testid="stSidebar"] button {
        white-space: nowrap !important;
        text-overflow: clip !important;
        padding-left: 5px !important;
        padding-right: 5px !important;
    }
    /* 調整按鈕高度使其垂直置中 */
    div.stButton > button {
        min-height: 45px;
        font-size: 20px;
    }
    /* Dataframe 與按鈕間距 */
    .stButton { margin-top: 5px; }
    
    /* 月曆標題樣式 */
    .calendar-header {
        font-size: 2.5em;
        font-weight: 900;
        text-align: center;
        color: #ff9800; /* 亮橘色 */
        margin-bottom: 10px;
        line-height: 1.5;
        font-family: 'Arial', sans-serif;
    }
    
    /* 月曆格子樣式 */
    .cal-box { 
        text-align: center; 
        padding: 5px; 
        border-radius: 4px; 
        margin: 2px; 
        min-height: 90px; 
        border: 1px solid #555;
        font-size: 0.9em;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .cal-open { 
        background-color: #000000 !important; 
        color: #ffffff !important; 
    }
    .cal-closed { 
        background-color: #d32f2f !important; 
        color: #ffffff !important; 
        font-weight: bold;
    }
    .cal-week {
        background-color: #f0f0f0;
        color: #333;
        font-weight: bold;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 0.8em;
    }
    .settle-m { color: #ffff00; font-weight: bold; font-size: 0.85em; margin-top: 2px; line-height: 1.2; } 
    .settle-w { color: #00e676; font-size: 0.8em; margin-top: 2px; } 
    .settle-f { color: #29b6f6; font-size: 0.8em; margin-top: 2px; } 
    .holiday-tag { font-size: 0.85em; margin-bottom: 2px; color: #ffeb3b; background-color: rgba(0,0,0,0.5); border-radius: 3px; padding: 1px;}
    .today-border { border: 3px solid #ffff00 !important; }
    
    /* 強制欄位內容置中 */
    div[data-testid="column"] {
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

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

def save_data_cache(df, ignored_set, candidates=[], saved_notes={}):
    try:
        df_save = df.fillna("") 
        data_to_save = {
            "stock_data": df_save.to_dict(orient='records'),
            "ignored_stocks": list(ignored_set),
            "all_candidates": candidates,
            "saved_notes": saved_notes
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
            saved_notes = data.get('saved_notes', {}) 
            return df, ignored, candidates, saved_notes
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

# 行事曆日期狀態初始化
tz_tw = pytz.timezone('Asia/Taipei')
now_tw = datetime.now(tz_tw)
if 'cal_year' not in st.session_state:
    st.session_state.cal_year = now_tw.year
if 'cal_month' not in st.session_state:
    st.session_state.cal_month = now_tw.month

saved_config = load_config()

if 'font_size' not in st.session_state:
    st.session_state.font_size = saved_config.get('font_size', 15)

if 'limit_rows' not in st.session_state:
    st.session_state.limit_rows = saved_config.get('limit_rows', 5)

if 'auto_update_last_row' not in st.session_state:
    st.session_state.auto_update_last_row = saved_config.get('auto_update', True)

if 'update_delay_sec' not in st.session_state:
    st.session_state.update_delay_sec = saved_config.get('delay_sec', 1.0) 

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
    
    # 近3日高低點選項
    show_3d_hilo = st.checkbox("近3日高低點 (戰略備註)", value=False, help="勾選後，將於戰略備註中加入前天、昨天、今天的最高與最低價 (僅顯示數值)")
    
    st.markdown("---")
    
    current_limit_rows = st.number_input(
        "顯示筆數 (檔案/雲端)", 
        min_value=1, 
        value=st.session_state.limit_rows,
        key='limit_rows_input',
    )
    st.session_state.limit_rows = current_limit_rows
    
    if st.button("💾 儲存設定"):
        if save_config(current_font_size, current_limit_rows, 
                      st.session_state.auto_update_last_row, 
                      st.session_state.update_delay_sec):
            st.toast("設定已儲存！", icon="✅")
            
    st.markdown("### 資料管理")
    if st.session_state.ignored_stocks:
        st.write(f"🚫 忽略名單 (取消勾選以復原):")
        ignored_list = sorted(list(st.session_state.ignored_stocks))
        options_map = {f"{c} {get_stock_name_online(c)}": c for c in ignored_list}
        options_display = list(options_map.keys())
        
        selected_ignored_display = st.multiselect(
            "管理忽略股票",
            options=options_display,
            default=options_display,
            label_visibility="collapsed",
        )
        
        current_selected_codes = set(options_map[opt] for opt in selected_ignored_display)
        if len(current_selected_codes) != len(st.session_state.ignored_stocks):
            st.session_state.ignored_stocks = current_selected_codes
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("已更新忽略名單。", icon="🔄")
            st.rerun()
    else:
        st.write("🚫 目前無忽略股票")
    
    col_restore, col_clear = st.columns([1, 1], gap="small")
    with col_restore:
        if st.button("♻️ 全部復原", use_container_width=True):
            st.session_state.ignored_stocks.clear()
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("已重置忽略名單。", icon="🔄")
            st.rerun()
    with col_clear:
        if st.button("🗑️ 全部清空", type="primary", use_container_width=True):
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
    st.link_button("🚨 證交所處置股公告", "https://www.twse.com.tw/zh/announcement/punish.html", use_container_width=True)

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
            if price > max_val: status = "🔴 強"
            elif price < min_val: status = "🟢 弱"
            else:
                hit = False
                for v in strat_values:
                    if abs(v - price) < 0.01: hit = True; break
                if hit: status = "🟡 命中"
        return status
    except: return status

# [修正] 戰略備註生成器 (穩定版)
def generate_note_from_points(points, manual_note, show_3d):
    display_candidates = []
    
    target_tags = ['前高', '前低', '昨高', '昨低', '今高', '今低']
    
    for p in points:
        t = p.get('tag', '')
        if t in target_tags and not show_3d:
            continue
        if p['val'] <= 0: continue
        display_candidates.append(p)
        
    display_candidates.sort(key=lambda x: x['val'])
    
    note_parts = []
    seen_vals = set() 
    
    for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
        if val in seen_vals: continue
        seen_vals.add(val)
        
        g_list = list(group)
        tags = [x['tag'] for x in g_list if x['tag']]
        
        final_tag = ""
        if "漲停高" in tags: final_tag = "漲停高"
        elif "跌停低" in tags: final_tag = "跌停低" 
        elif "漲停" in tags: final_tag = "漲停"
        elif "跌停" in tags: final_tag = "跌停"
        elif "多" in tags: final_tag = "多"
        elif "空" in tags: final_tag = "空"
        elif "平" in tags: final_tag = "平"
        elif "高" in tags: final_tag = "高"
        elif "低" in tags: final_tag = "低"
        elif "今高" in tags: final_tag = "今高"
        elif "今低" in tags: final_tag = "今低"
        elif "昨高" in tags: final_tag = "昨高"
        elif "昨低" in tags: final_tag = "昨低"
        elif "前高" in tags: final_tag = "前高"
        elif "前低" in tags: final_tag = "前低"
        
        v_str = fmt_price(val)
        suffix_tags = ["多", "空", "平"]
        prefix_tags = ["漲停", "漲停高", "跌停", "跌停低", "高", "低"]
        numeric_only_tags = ["前高", "前低", "昨高", "昨低", "今高", "今低"]
        
        if final_tag in suffix_tags: item = f"{v_str}{final_tag}" 
        elif final_tag in prefix_tags: item = f"{final_tag}{v_str}"
        elif final_tag in numeric_only_tags: item = v_str 
        elif final_tag: item = f"{v_str}{final_tag}" 
        else: item = v_str
        note_parts.append(item)
        
    auto_note = "-".join(note_parts)
    
    # 支援手動修改：若 manual_note 存在，直接顯示
    if manual_note:
        if manual_note.startswith("[M]"):
            return manual_note[3:], auto_note
        if auto_note and manual_note.strip().startswith(auto_note.strip()):
            return manual_note, auto_note
        return f"{auto_note}{manual_note}", auto_note
            
    return auto_note, auto_note

# [穩定還原] 移除所有複雜的補即時資料邏輯，避免資料錯亂
def fetch_stock_data_raw(code, name_hint="", extra_data=None, futures_set=None, saved_notes_dict=None, name_map_dict=None):
    code = str(code).strip()
    
    hist = pd.DataFrame()
    source_used = "none"
    
    # 1. 抓歷史資料 (yfinance history)
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

    if hist.empty:
        df_fm = fetch_finmind_backup(code)
        if df_fm is not None and not df_fm.empty:
            hist = df_fm
            source_used = "finmind"

    # [修正] 關鍵修正：確保「今天」的即時資料存在 (Realtime Patch)
    # 改進邏輯：優先判定日期，若歷史資料停留在昨天，且今日是交易日，強制補一筆
    try:
        rt_data = twstock.realtime.get(code)
        if rt_data['success'] and rt_data['realtime']['latest_trade_price'] not in ['-', None, '']:
            rt_price = float(rt_data['realtime']['latest_trade_price'])
            rt_open = float(rt_data['realtime']['open']) if rt_data['realtime']['open'] != '-' else rt_price
            rt_high = float(rt_data['realtime']['high']) if rt_data['realtime']['high'] != '-' else rt_price
            rt_low = float(rt_data['realtime']['low']) if rt_data['realtime']['low'] != '-' else rt_price
            rt_vol = float(rt_data['realtime']['accumulate_trade_volume']) if rt_data['realtime']['accumulate_trade_volume'] != '-' else 0.0
            
            # 解析 Realtime 回傳的日期 (備用)
            rt_time_str = rt_data['info']['time'] 
            rt_dt = datetime.strptime(rt_time_str, "%Y-%m-%d %H:%M:%S")
            rt_date_parsed = pd.Timestamp(rt_dt.date())

            # 取得系統時間 (台灣)
            tz_tw = pytz.timezone('Asia/Taipei')
            now_tw = datetime.now(tz_tw)
            today_date = pd.Timestamp(now_tw.date())

            if hist.empty:
                hist = pd.DataFrame([{
                    'Open': rt_open, 'High': rt_high, 'Low': rt_low, 
                    'Close': rt_price, 'Volume': rt_vol
                }], index=[today_date]) # 強制用今日日期
            else:
                if hist.index.tzinfo is not None:
                    hist.index = hist.index.tz_localize(None)
                
                last_hist_date = hist.index[-1]
                
                # 判定是否需要新增一筆 (Append)
                # 條件：最後一筆日期小於今日，且今日是平日(週一~週五)，且現在時間超過 09:00
                # 這樣可以避免週六日時誤補，也能解決 twstock 日期可能是舊的但價格是新的問題
                should_append = False
                
                if last_hist_date < today_date:
                    if now_tw.weekday() < 5 and now_tw.hour >= 9:
                        should_append = True
                    # 若 twstock 自身回傳的日期確實比歷史新 (例如補上班日)，也允許 append
                    elif rt_date_parsed > last_hist_date:
                        should_append = True
                
                if should_append:
                    # 補上新的一天 (使用 today_date 強制校正，解決日期滯後導致的覆蓋錯誤)
                    target_index = today_date if now_tw.weekday() < 5 else rt_date_parsed
                    
                    new_row = pd.DataFrame([{
                        'Open': rt_open, 'High': rt_high, 'Low': rt_low, 
                        'Close': rt_price, 'Volume': rt_vol
                    }], index=[target_index])
                    hist = pd.concat([hist, new_row])
                    hist.sort_index(inplace=True)
                else:
                    # 若日期相同 (或不符合新增條件)，則更新最後一筆
                    # 但需確保不是「用今日價格覆蓋了昨天」
                    # 只有當 last_hist_date 確實等於 today_date (或 rt_date_parsed) 時才更新
                    if last_hist_date == today_date or last_hist_date == rt_date_parsed:
                        hist.at[last_hist_date, 'Close'] = rt_price
                        hist.at[last_hist_date, 'High'] = max(hist.at[last_hist_date, 'High'], rt_high)
                        hist.at[last_hist_date, 'Low'] = min(hist.at[last_hist_date, 'Low'], rt_low)
                        hist.at[last_hist_date, 'Volume'] = rt_vol
    except:
        pass # 若即時資料抓取失敗，就維持原狀

    if hist.empty: return None

    # 確保資料結構整潔
    if hist.index.tzinfo is not None:
        hist.index = hist.index.tz_localize(None)

    hist['High'] = hist[['High', 'Close']].max(axis=1)
    hist['Low'] = hist[['Low', 'Close']].min(axis=1)

    hist_strat = hist.copy()
    
    if hist_strat.empty: return None

    strategy_base_price = hist_strat.iloc[-1]['Close']
    
    # [修正] 漲跌幅計算：確保基準是「倒數第二筆」(即昨天)
    if len(hist_strat) >= 2:
        prev_of_base = hist_strat.iloc[-2]['Close']
    else:
        prev_of_base = strategy_base_price 

    if prev_of_base > 0:
        pct_change = ((strategy_base_price - prev_of_base) / prev_of_base) * 100
    else:
        pct_change = 0.0

    base_price_for_limit = strategy_base_price
    limit_up_show, limit_down_show = calculate_limits(base_price_for_limit)

    limit_up_T = None
    limit_down_T = None
    if len(hist_strat) >= 2:
        prev_close_T = hist_strat.iloc[-2]['Close']
        limit_up_T, limit_down_T = calculate_limits(prev_close_T)

    target_raw = strategy_base_price * 1.03
    stop_raw = strategy_base_price * 0.97
    target_price = apply_sr_rules(target_raw, strategy_base_price)
    stop_price = apply_sr_rules(stop_raw, strategy_base_price)
    
    points = []
    
    recent_k = hist_strat.tail(3)
    days_map = {0: "今", 1: "昨", 2: "前"}
    recent_records = recent_k.to_dict('records')
    recent_records.reverse()
    
    for idx, row in enumerate(recent_records):
        if idx in days_map:
            prefix = days_map[idx]
            h_val = apply_tick_rules(row['High'])
            l_val = apply_tick_rules(row['Low'])
            
            if h_val > 0 and limit_down_show <= h_val <= limit_up_show:
                points.append({"val": h_val, "tag": f"{prefix}高"})
            if l_val > 0 and limit_down_show <= l_val <= limit_up_show:
                points.append({"val": l_val, "tag": f"{prefix}低"})

    if len(hist_strat) >= 5:
        last_5_closes = hist_strat['Close'].tail(5).values
        sum_val = sum(Decimal(str(x)) for x in last_5_closes)
        avg_val = sum_val / Decimal("5")
        ma5_raw = float(avg_val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        ma5 = apply_sr_rules(ma5_raw, strategy_base_price)
        ma5_tag = "多" if ma5_raw < strategy_base_price else ("空" if ma5_raw > strategy_base_price else "平")
        points.append({"val": ma5, "tag": ma5_tag, "force": True})

    if len(hist_strat) >= 2:
        last_candle = hist_strat.iloc[-1]
        p_open = apply_tick_rules(last_candle['Open'])
        if limit_down_show <= p_open <= limit_up_show: 
             points.append({"val": p_open, "tag": ""})

        p_high = apply_tick_rules(last_candle['High'])
        p_low = apply_tick_rules(last_candle['Low'])
        if limit_down_show <= p_high <= limit_up_show: points.append({"val": p_high, "tag": ""})
        
        if limit_down_show <= p_low <= limit_up_show: 
             tag_low = ""
             if limit_down_T and abs(p_low - limit_down_T) < 0.01:
                 tag_low = "跌停"
             points.append({"val": p_low, "tag": tag_low})

    if len(hist_strat) >= 3:
        pre_prev_candle = hist_strat.iloc[-2]
        pp_high = apply_tick_rules(pre_prev_candle['High'])
        pp_low = apply_tick_rules(pre_prev_candle['Low'])
        if limit_down_show <= pp_high <= limit_up_show: points.append({"val": pp_high, "tag": ""})
        if limit_down_show <= pp_low <= limit_up_show: points.append({"val": pp_low, "tag": ""})

    show_plus_3 = False
    show_minus_3 = False
    
    if not hist_strat.empty:
        high_90_raw = hist_strat['High'].max()
        low_vals = hist_strat['Low'][hist_strat['Low'] > 0]
        if not low_vals.empty:
            low_90_raw = low_vals.min()
        else:
            low_90_raw = hist_strat['Low'].min()
            
        high_90 = apply_tick_rules(high_90_raw)
        low_90 = apply_tick_rules(low_90_raw)
        
        points.append({"val": high_90, "tag": "高"})
        points.append({"val": low_90, "tag": "低"})
        
        if len(hist_strat) >= 2:
             today_high = hist_strat.iloc[-1]['High']
             if limit_up_T and abs(today_high - limit_up_T) < 0.01:
                 is_new_high = (abs(limit_up_T - high_90_raw) < 0.05)
                 tag_label = "漲停高" if is_new_high else "漲停"
                 if limit_down_show <= limit_up_T <= limit_up_show:
                     points.append({"val": limit_up_T, "tag": tag_label})

        if len(hist_strat) >= 2:
            high_T = hist_strat.iloc[-1]['High']
            low_T = hist_strat.iloc[-1]['Low']
            close_T = hist_strat.iloc[-1]['Close']
            touched_limit_up = (limit_up_T and high_T >= limit_up_T - 0.01) 
            touched_limit_down = (limit_down_T and low_T <= limit_down_T + 0.01)
            
            if touched_limit_up and (limit_up_T and close_T >= limit_up_T * 0.97):
                show_plus_3 = True
            else:
                show_plus_3 = False
            if touched_limit_down and (limit_down_T and close_T <= limit_down_T * 1.03):
                show_minus_3 = True
            else:
                show_minus_3 = False
        else:
            show_plus_3 = False
            show_minus_3 = False

    if show_plus_3: points.append({"val": target_price, "tag": ""})
    if show_minus_3: points.append({"val": stop_price, "tag": ""})
        
    full_calc_points = []
    threed_tags = ['前高', '前低', '昨高', '昨低', '今高', '今低']
    
    for p in points:
        v = float(f"{p['val']:.2f}")
        is_force = p.get('force', False)
        if is_force or p.get('tag') in threed_tags or (limit_down_show <= v <= limit_up_show):
             full_calc_points.append(p) 
    
    # [修正] 改用參數傳入的 saved_notes_dict
    manual_note = ""
    if saved_notes_dict:
        manual_note = saved_notes_dict.get(code, "")
    
    strategy_note, auto_note = generate_note_from_points(full_calc_points, manual_note, show_3d=False)
    
    # [修正] 改用參數傳入的 name_map_dict
    if name_hint:
        final_name = name_hint
    elif name_map_dict and code in name_map_dict:
        final_name = name_map_dict[code]
    else:
        final_name = code

    light = "⚪"
    if "多" in strategy_note: light = "🔴"
    elif "空" in strategy_note: light = "🟢"
    final_name_display = f"{light} {final_name}"
    
    # [修正] 改用參數傳入的 futures_set
    has_futures = "✅" if futures_set and code in futures_set else ""
    
    return {
        "代號": code, "名稱": final_name_display, "收盤價": round(strategy_base_price, 2),
        "漲跌幅": pct_change, "期貨": has_futures, 
        "當日漲停價": limit_up_show, "當日跌停價": limit_down_show,
        "自訂價(可修)": None, "獲利目標": target_price, "防守停損": stop_price,   
        "戰略備註": strategy_note, "_points": full_calc_points, "狀態": "",
        "_auto_note": auto_note 
    }

# ==========================================
# 主介面 (Tabs)
# ==========================================

# [修正] 新增 台股行事曆 分頁
tab1, tab2, tab3 = st.tabs(["⚡ 當沖戰略室 ⚡", "💰 當沖損益室 💰", "📅 台股行事曆"])

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

    # [修正] 主畫面按鈕並排 - 移除可能導致按鈕消失的 CSS
    c_run, c_space = st.columns([1.5, 5])
    
    with c_run:
        btn_run = st.button("🚀 執行分析", use_container_width=True)

    if btn_run:
        save_search_cache(st.session_state.search_multiselect)
        
        if not st.session_state.futures_list:
            st.session_state.futures_list = fetch_futures_list()
        
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
        
        # ------------------------------------------------------------------
        # [多執行緒平行處理核心]
        # ------------------------------------------------------------------
        
        # 1. 準備執行緒需要的靜態資料副本
        futures_copy = set(st.session_state.futures_list)
        notes_copy = dict(st.session_state.saved_notes)
        code_map_copy, _ = load_local_stock_names()

        # 2. 定義任務函式
        def process_stock_task(t_code, t_name, t_source, t_extra, f_set, n_dict, c_map):
            # [修正] 加入隨機延遲，避免瞬間大量請求導致被鎖 IP
            time.sleep(random.uniform(0.5, 1.5))
            try:
                data = fetch_stock_data_raw(t_code, t_name, t_extra, f_set, n_dict, c_map)
                return (t_code, t_source, t_extra, data)
            except Exception:
                return (t_code, t_source, t_extra, None)

        tasks_to_run = []
        for i, (code, name, source, extra) in enumerate(targets):
            if source == 'upload' and upload_current >= upload_limit: continue
            if code in st.session_state.ignored_stocks: continue
            if (code, source) in seen: continue
            
            # 將任務參數打包
            tasks_to_run.append((code, name, source, extra))
            
            if source == 'upload': 
                upload_current += 1
            seen.add((code, source))

        # 3. 開始執行
        # [修正] 將 max_workers 從 8 降為 4，減緩請求頻率
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_task = {}
            for t in tasks_to_run:
                future = executor.submit(process_stock_task, t[0], t[1], t[2], t[3], futures_copy, notes_copy, code_map_copy)
                future_to_task[future] = t
            
            completed_count = 0
            total_tasks = len(tasks_to_run)
            if total_tasks == 0: total_tasks = 1
            
            for future in as_completed(future_to_task):
                t_code, t_source, t_extra, data = future.result()
                
                completed_count += 1
                progress_val = min(completed_count / total_tasks, 1.0)
                bar.progress(progress_val)
                status_text.text(f"正在分析 ({completed_count}/{total_tasks}): {t_code} ...")
                
                if data:
                    data['_source'] = t_source
                    data['_order'] = t_extra
                    data['_source_rank'] = 1 if t_source == 'upload' else 2
                    existing_data[t_code] = data
        
        # ------------------------------------------------------------------
        
        bar.empty()
        status_text.empty()
        
        if existing_data:
            st.session_state.stock_data = pd.DataFrame(list(existing_data.values()))
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)

    # [修正] 確保當有資料時，下方按鈕區塊一定會顯示，不受 btn_run 狀態影響
    if not st.session_state.stock_data.empty:
        df_all = st.session_state.stock_data.copy()
        
        if '_source' not in df_all.columns: df_all['_source'] = 'upload'
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
        
        for i, row in df_display.iterrows():
            points = row.get('_points', [])
            manual = st.session_state.saved_notes.get(row['代號'], "")
            
            new_full_note, new_auto_note = generate_note_from_points(points, manual, show_3d_hilo)
            
            df_display.at[i, "戰略備註"] = new_full_note
            df_display.at[i, "_auto_note"] = new_auto_note
            
            light = "⚪"
            if "多" in new_full_note: light = "🔴"
            elif "空" in new_full_note: light = "🟢"
            
            raw_name = row['名稱'].split(' ', 1)[-1] 
            df_display.at[i, "名稱"] = f"{light} {raw_name}"

        note_width_px = calculate_note_width(df_display['戰略備註'], current_font_size)
        df_display["移除"] = False
        
        points_map = {}
        if '_points' in df_display.columns:
            points_map = df_display.set_index('代號')['_points'].to_dict()
        
        auto_notes_dict = {}
        if '_auto_note' in df_display.columns:
            auto_notes_dict = df_display.set_index('代號')['_auto_note'].to_dict()

        input_cols = ["移除", "代號", "名稱", "戰略備註", "自訂價(可修)", "狀態", "當日漲停價", "當日跌停價", "+3%", "-3%", "收盤價", "漲跌幅", "期貨"]
        for col in input_cols:
            if col not in df_display.columns: df_display[col] = None

        cols_to_fmt = ["當日漲停價", "當日跌停價", "+3%", "-3%", "自訂價(可修)"]
        for c in cols_to_fmt:
            if c in df_display.columns: df_display[c] = df_display[c].apply(fmt_price)

        if "收盤價" in df_display.columns and "漲跌幅" in df_display.columns:
            for i in range(len(df_display)):
                try:
                    p = float(df_display.at[i, "收盤價"])
                    chg = float(df_display.at[i, "漲跌幅"])
                    color_icon = "⚪"
                    if chg > 0: color_icon = "🔴"
                    elif chg < 0: color_icon = "🟢"
                    df_display.at[i, "收盤價"] = f"{color_icon} {fmt_price(p)}"
                    chg_str = f"{chg:+.2f}%"
                    df_display.at[i, "漲跌幅"] = f"{color_icon} {chg_str}"
                except:
                    df_display.at[i, "收盤價"] = fmt_price(df_display.at[i, "收盤價"])
                    df_display.at[i, "漲跌幅"] = f"{float(df_display.at[i, '漲跌幅']):.2f}%"

        df_display = df_display.reset_index(drop=True)
        for col in input_cols:
             if col != "移除": df_display[col] = df_display[col].astype(str)

        edited_df = st.data_editor(
            df_display[input_cols],
            column_config={
                "移除": st.column_config.CheckboxColumn("刪除", width=40, help="勾選後刪除並自動遞補"),
                "代號": st.column_config.TextColumn(disabled=True, width=50), 
                "名稱": st.column_config.TextColumn(disabled=True, width="small"),
                "收盤價": st.column_config.TextColumn(width="small", disabled=True),
                "漲跌幅": st.column_config.TextColumn(disabled=True, width="small"),
                "期貨": st.column_config.TextColumn(disabled=True, width=40), 
                "自訂價(可修)": st.column_config.TextColumn("自訂價 ✏️", width=60), 
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
        
        if not edited_df.empty:
            trigger_rerun = False
            
            # [修正] 刪除並遞補邏輯優化
            if "移除" in edited_df.columns:
                to_remove = edited_df[edited_df["移除"] == True]
                if not to_remove.empty:
                    # 1. 處理移除
                    remove_codes = to_remove["代號"].unique()
                    for c in remove_codes:
                        st.session_state.ignored_stocks.add(str(c))
                    
                    st.session_state.stock_data = st.session_state.stock_data[
                        ~st.session_state.stock_data["代號"].isin(remove_codes)
                    ]
                    
                    # 2. 立即遞補 (在存檔前)
                    upload_count = len(st.session_state.stock_data[st.session_state.stock_data['_source'] == 'upload'])
                    limit = st.session_state.limit_rows
                    needed = limit - upload_count
                    
                    if needed > 0 and st.session_state.all_candidates:
                        replenished_count = 0
                        existing_codes = set(st.session_state.stock_data['代號'].astype(str))
                        futures_copy = set(st.session_state.futures_list)
                        notes_copy = dict(st.session_state.saved_notes)
                        code_map_copy, _ = load_local_stock_names()
                        
                        for cand in st.session_state.all_candidates:
                             c_code = str(cand[0])
                             c_name = cand[1]
                             c_source = cand[2]
                             c_extra = cand[3]
                             if c_source != 'upload': continue
                             if c_code in st.session_state.ignored_stocks: continue
                             if c_code in existing_codes: continue
                             
                             # 抓取資料
                             data = fetch_stock_data_raw(c_code, c_name, c_extra, futures_copy, notes_copy, code_map_copy)
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
                    
                    # 3. 存檔並重整
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                    trigger_rerun = True

            # 自動更新價格邏輯 (僅在未觸發刪除重整時執行)
            if not trigger_rerun and st.session_state.auto_update_last_row:
                last_visible_idx = len(edited_df) - 1
                if last_visible_idx >= 0:
                    last_visible_code = edited_df.iloc[last_visible_idx]['代號']
                    update_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
                    
                    for i, row in st.session_state.stock_data.iterrows():
                        if row['代號'] == last_visible_code:
                            if last_visible_code in update_map:
                                new_price = update_map[last_visible_code]['自訂價(可修)']
                                old_price = str(row['自訂價(可修)'])
                                if old_price != str(new_price) and str(new_price).strip().lower() != 'nan':
                                    if st.session_state.update_delay_sec > 0:
                                        time.sleep(st.session_state.update_delay_sec)
                                    
                                    for j, r in st.session_state.stock_data.iterrows():
                                        c_code = r['代號']
                                        if c_code in update_map:
                                            np = update_map[c_code]['自訂價(可修)']
                                            nn = update_map[c_code]['戰略備註']
                                            st.session_state.stock_data.at[j, '自訂價(可修)'] = np
                                            if str(r['戰略備註']) != str(nn):
                                                base_auto = auto_notes_dict.get(c_code, "")
                                                pure_manual = ""
                                                b_auto = str(base_auto).strip()
                                                n_note = str(new_note).strip()
                                                
                                                if b_auto and n_note.startswith(b_auto):
                                                    pure_manual = n_note[len(b_auto):]
                                                else:
                                                    pure_manual = f"[M]{n_note}"
                                                    
                                                st.session_state.stock_data.at[j, '戰略備註'] = nn
                                                st.session_state.saved_notes[c_code] = pure_manual
                                        
                                        new_status = recalculate_row(st.session_state.stock_data.iloc[j], points_map)
                                        st.session_state.stock_data.at[j, '狀態'] = new_status
                                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                                    trigger_rerun = True
                            break

            if trigger_rerun:
                st.rerun()

        # 自動遞補邏輯 (針對非刪除動作導致的缺額)
        df_curr = st.session_state.stock_data
        if not df_curr.empty:
            if '_source' not in df_curr.columns: upload_count = len(df_curr)
            else: upload_count = len(df_curr[df_curr['_source'] == 'upload'])
            limit = st.session_state.limit_rows
            
            if upload_count < limit and st.session_state.all_candidates:
                needed = limit - upload_count
                replenished_count = 0
                existing_codes = set(st.session_state.stock_data['代號'].astype(str))
                
                futures_copy = set(st.session_state.futures_list)
                notes_copy = dict(st.session_state.saved_notes)
                code_map_copy, _ = load_local_stock_names()

                with st.spinner("正在載入更多資料..."):
                    for cand in st.session_state.all_candidates:
                         c_code = str(cand[0])
                         c_name = cand[1]
                         c_source = cand[2]
                         c_extra = cand[3]
                         if c_source != 'upload': continue
                         if c_code in st.session_state.ignored_stocks: continue
                         if c_code in existing_codes: continue
                         
                         data = fetch_stock_data_raw(c_code, c_name, c_extra, futures_copy, notes_copy, code_map_copy)
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
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                    st.toast(f"已更新顯示筆數，增加 {replenished_count} 檔。", icon="🔄")
                    st.rerun()

        st.markdown("---")
        
        # [修正] 調整按鈕顯示邏輯與排版，確保不消失
        col_btn, col_clear, _ = st.columns([2, 2, 4])
        with col_btn:
            btn_update = st.button("⚡ 執行更新&儲存手動備註", use_container_width=True, type="primary")
        with col_clear:
            btn_clear_notes = st.button("🧹 清除手動備註", use_container_width=True, help="清除所有記憶的戰略備註內容")
        
        if btn_clear_notes:
            st.session_state.saved_notes = {}
            st.toast("手動備註已清除", icon="🧹")
            if not st.session_state.stock_data.empty:
                 for idx, row in st.session_state.stock_data.iterrows():
                     points = row.get('_points', [])
                     clean_note, _ = generate_note_from_points(points, "", show_3d_hilo)
                     
                     st.session_state.stock_data.at[idx, '戰略備註'] = clean_note
                     if '_auto_note' in st.session_state.stock_data.columns:
                        st.session_state.stock_data.at[idx, '_auto_note'] = clean_note

            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.rerun()
        
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
                    new_val = update_map[code]['自訂價(可修)']
                    new_note = update_map[code]['戰略備註']
                    st.session_state.stock_data.at[i, '自訂價(可修)'] = new_val
                    
                    if str(row['戰略備註']) != str(new_note):
                        base_auto = auto_notes_dict.get(code, "")
                        pure_manual = ""
                        b_auto = str(base_auto).strip()
                        n_note = str(new_note).strip()
                        
                        if b_auto and n_note.startswith(b_auto):
                            pure_manual = n_note[len(b_auto):]
                        else:
                            pure_manual = f"[M]{n_note}"
                             
                        st.session_state.stock_data.at[i, '戰略備註'] = new_note
                        st.session_state.saved_notes[code] = pure_manual
                    else:
                        st.session_state.stock_data.at[i, '戰略備註'] = new_note
                
                new_status = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
                st.session_state.stock_data.at[i, '狀態'] = new_status
             
             save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
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
        if st.button("🔽 向下", use_container_width=True):
            if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
            st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, -tick_count)
            if st.session_state.calc_view_price < limit_down: st.session_state.calc_view_price = limit_down
            st.rerun()
    with b2:
        if st.button("🔼 向上", use_container_width=True):
            if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
            st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, tick_count)
            if st.session_state.calc_view_price > limit_up: st.session_state.calc_view_price = limit_up
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

# [修正] 台股行事曆 (修正版：含左右切換與週五選順延排除)
with tab3:
    # 透過回呼函式處理按鈕邏輯
    def change_month(delta):
        st.session_state.cal_month += delta
        if st.session_state.cal_month > 12:
            st.session_state.cal_month = 1
            st.session_state.cal_year += 1
        elif st.session_state.cal_month < 1:
            st.session_state.cal_month = 12
            st.session_state.cal_year -= 1
        
        # [關鍵修復] 強制清除下拉選單的暫存狀態，讓 selectbox 重新讀取 session_state
        if 'sel_year_box' in st.session_state:
            del st.session_state['sel_year_box']
        if 'sel_month_box' in st.session_state:
            del st.session_state['sel_month_box']

    # 頂部：下拉式選單 (恢復)
    col_sel_y, col_sel_m = st.columns(2)
    with col_sel_y:
        # 使用 key 綁定 session_state，但因為有按鈕互動，需額外處理同步
        # 這裡採用：如果 user 改變 selectbox -> 更新 state
        # 如果 user 按按鈕 -> 更新 state 並刪除 key 以重置 selectbox
        
        # 為了避免 key 衝突，這裡使用動態 index
        current_year_idx = range(2024, 2031).index(st.session_state.cal_year)
        
        new_year = st.selectbox(
            "年份", 
            range(2024, 2031), 
            index=current_year_idx,
            key='sel_year_box'
        )
        if new_year != st.session_state.cal_year:
            st.session_state.cal_year = new_year
            st.rerun()

    with col_sel_m:
        current_month_idx = st.session_state.cal_month - 1
        new_month = st.selectbox(
            "月份", 
            range(1, 13), 
            index=current_month_idx,
            key='sel_month_box'
        )
        if new_month != st.session_state.cal_month:
            st.session_state.cal_month = new_month
            st.rerun()

    sel_year = st.session_state.cal_year
    sel_month = st.session_state.cal_month

    # 中央：導覽列與大標題
    col_prev, col_header, col_next = st.columns([1, 8, 1])
    
    with col_prev:
        st.button("◀️", on_click=change_month, args=(-1,), use_container_width=True)

    with col_next:
        st.button("▶️", on_click=change_month, args=(1,), use_container_width=True)

    with col_header:
        st.markdown(f"<div class='calendar-header'>{sel_year}/{sel_month:02}</div>", unsafe_allow_html=True)

    # 取得該年度的國定假日資料
    def get_holidays(year):
        h = {}
        # 2025 年
        if year == 2025:
             h.update({
                 (1, 1): "元旦",
                 (1, 27): "春節", (1, 28): "春節", (1, 29): "春節", (1, 30): "春節", (1, 31): "春節",
                 (2, 3): "春節", (2, 28): "228紀念日",
                 (4, 3): "兒童節", (4, 4): "清明節",
                 (5, 1): "勞動節", (5, 30): "端午節",
                 (10, 6): "中秋節", (10, 10): "國慶日"
             })
             
        # 2026 年 (民國 115 年) 完整列表
        if year == 2026:
            h.update({
                (1, 1): "元旦",
                (2, 11): "封關日",
                (2, 12): "市場無交易", (2, 13): "市場無交易",
                (2, 14): "春節", (2, 15): "春節", (2, 16): "春節", (2, 17): "春節",
                (2, 18): "春節", (2, 19): "春節", (2, 20): "春節", (2, 21): "春節", (2, 22): "春節",
                (2, 27): "和平紀念日(補)", (2, 28): "和平紀念日",
                (4, 3): "兒童節(補)", (4, 4): "兒童節", (4, 5): "清明節", (4, 6): "清明節(補)",
                (5, 1): "勞動節",
                (6, 19): "端午節",
                (9, 25): "中秋節", (9, 28): "教師節",
                (10, 9): "國慶日(補)", (10, 10): "國慶日", (10, 25): "光復節", (10, 26): "光復節(補)",
                (12, 25): "行憲紀念日"
            })
        return h

    current_holidays = get_holidays(sel_year)

    def is_market_closed_func(d_date):
        if d_date.weekday() >= 5: return True
        name = current_holidays.get((d_date.month, d_date.day), "")
        if name and name != "封關日": # 移除行憲紀念日的排除邏輯，使其休市
             return True
        return False

    # 計算結算日 (嚴格順延邏輯 + 跨月檢查)
    real_settlements = {} 
    
    def calculate_month_settlements(y, m):
        cal_obj = calendar.Calendar(firstweekday=6)
        days_in_month = cal_obj.itermonthdays(y, m)
        d_list = [d for d in days_in_month if d != 0]
        
        w_count = 0
        f_count = 0
        month_raw_wed = []
        month_raw_fri = []
        
        for d in d_list:
            curr = date(y, m, d)
            if curr.weekday() == 2: # Wed
                w_count += 1
                month_raw_wed.append((curr, w_count))
            if curr.weekday() == 4: # Fri
                f_count += 1
                month_raw_fri.append((curr, f_count))
                
        monthly_raw = month_raw_wed[2][0] if len(month_raw_wed) >= 3 else None
        
        # 計算該月份「月結算」的實際日期 (用於排除同日週五選)
        real_monthly_date = None
        if monthly_raw:
            check = monthly_raw
            while is_market_closed_func(check):
                check += timedelta(days=1)
            real_monthly_date = check
        
        local_results = []
        if monthly_raw:
            local_results.append((monthly_raw, 'M', f"{m:02}月", real_monthly_date))

        for dt, idx in month_raw_wed:
            if dt != monthly_raw:
                yy = str(y)[2:]
                mm = f"{m:02}"
                code = f"{yy}{mm}W{idx}"
                local_results.append((dt, 'W', code, real_monthly_date))
                
        for dt, idx in month_raw_fri:
            yy = str(y)[2:]
            mm = f"{m:02}"
            code = f"{yy}{mm}F{idx}"
            local_results.append((dt, 'F', code, real_monthly_date))
            
        return local_results

    # 1. 取得當前月份資料
    current_month_data = calculate_month_settlements(sel_year, sel_month)
    
    # 2. 取得前一個月資料 (處理跨月順延，例如 2月週選延到3月)
    if sel_month == 1:
        prev_y, prev_m = sel_year - 1, 12
    else:
        prev_y, prev_m = sel_year, sel_month - 1
        
    prev_month_data = calculate_month_settlements(prev_y, prev_m)
    
    # 合併兩月資料進行檢查
    all_raw_data = prev_month_data + current_month_data
    
    for raw_date, s_type, s_code, m_date in all_raw_data:
        check_date = raw_date
        while is_market_closed_func(check_date):
            check_date += timedelta(days=1)
            # 防止無限迴圈
            if (check_date - raw_date).days > 30: break
        
        # [篩選1] 只顯示落在「當前選取月份」的結算日
        if check_date.year == sel_year and check_date.month == sel_month:
            
            # [篩選2] 若週五選順延後撞到「該契約所屬月份」的月結算日，則不顯示
            # 注意：m_date 是該 raw_date 原始月份的月結算日
            if s_type == 'F' and check_date == m_date:
                continue
            
            if check_date not in real_settlements:
                real_settlements[check_date] = []
            real_settlements[check_date].append((s_type, s_code))

    week_days = ["週", "日", "一", "二", "三", "四", "五", "六"]
    cols = st.columns([0.4, 1, 1, 1, 1, 1, 1, 1])
    for i, d in enumerate(week_days):
        cols[i].markdown(f"<div style='text-align: center; font-weight: bold;'>{d}</div>", unsafe_allow_html=True)

    cal_obj = calendar.Calendar(firstweekday=6)
    month_days = cal_obj.monthdayscalendar(sel_year, sel_month)

    for week in month_days:
        week_cols = st.columns([0.4, 1, 1, 1, 1, 1, 1, 1])
        
        first_valid_day = next((d for d in week if d != 0), None)
        if first_valid_day:
            iso_week = date(sel_year, sel_month, first_valid_day).isocalendar()[1]
            week_cols[0].markdown(f"<div class='cal-box cal-week'>{iso_week}</div>", unsafe_allow_html=True)
        else:
            week_cols[0].markdown("")

        for i, day in enumerate(week):
            if day == 0:
                week_cols[i+1].markdown("")
                continue
            
            curr_date = date(sel_year, sel_month, day)
            is_weekend = (i == 0 or i == 6)
            
            holiday_name = current_holidays.get((sel_month, day), "")
            is_closed = is_market_closed_func(curr_date)
            
            bg_class = "cal-closed" if is_closed else "cal-open"
            border_style = "today-border" if curr_date == now_tw.date() else ""
            
            content_html = []
            content_html.append(f"<b>{day}</b>")
            
            if holiday_name and holiday_name != "封關日":
                content_html.append(f"<div class='holiday-tag'>{holiday_name}</div>")
            if holiday_name == "封關日":
                 content_html.append(f"<div style='color:#ff9800; font-size:0.8em;'>{holiday_name}</div>")
            
            if curr_date in real_settlements:
                infos = real_settlements[curr_date]
                infos.sort(key=lambda x: 0 if x[0]=='M' else 1)
                
                for s_type, s_code in infos:
                    if s_type == 'M':
                        content_html.append(f"<div class='settle-m'>台指期{s_code}結算<br>月選結算</div>")
                    elif s_type == 'W':
                        content_html.append(f"<div class='settle-w'>週選(三) {s_code}</div>")
                    elif s_type == 'F':
                        content_html.append(f"<div class='settle-f'>週選(五) {s_code}</div>")

            final_html = "".join(content_html)
            week_cols[i+1].markdown(f"<div class='cal-box {bg_class} {border_style}'>{final_html}</div>", unsafe_allow_html=True)

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
from datetime import datetime, time as dt_time
import pytz
from decimal import Decimal, ROUND_HALF_UP
import io

# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="當沖戰略室", page_icon="⚡", layout="wide")

# 1. 標題
st.title("⚡ 當沖戰略室 ⚡")

CONFIG_FILE = "config.json"
DATA_CACHE_FILE = "data_cache.json"

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f: return json.load(f)
        except: return {}
    return {}

def save_config(font_size, limit_rows):
    try:
        config = {"font_size": font_size, "limit_rows": limit_rows}
        with open(CONFIG_FILE, "w") as f: json.dump(config, f)
        return True
    except: return False

def save_data_cache(df, ignored_set):
    try:
        df_save = df.fillna("") 
        data_to_save = {
            "stock_data": df_save.to_dict(orient='records'),
            "ignored_stocks": list(ignored_set)
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
            return df, ignored
        except: return pd.DataFrame(), set()
    return pd.DataFrame(), set()

# --- 初始化 Session State ---
if 'stock_data' not in st.session_state:
    cached_df, cached_ignored = load_data_cache()
    st.session_state.stock_data = cached_df
    st.session_state.ignored_stocks = cached_ignored

if 'ignored_stocks' not in st.session_state:
    st.session_state.ignored_stocks = set()

if 'calc_base_price' not in st.session_state:
    st.session_state.calc_base_price = 100.0

if 'calc_view_price' not in st.session_state:
    st.session_state.calc_view_price = 100.0

if 'cloud_url' not in st.session_state:
    st.session_state.cloud_url = ""

saved_config = load_config()

if 'font_size' not in st.session_state:
    st.session_state.font_size = saved_config.get('font_size', 15) # 預設 15

if 'limit_rows' not in st.session_state:
    st.session_state.limit_rows = saved_config.get('limit_rows', 5) # [修正] 預設 5

# --- 側邊欄設定 ---
with st.sidebar:
    st.header("⚙️ 設定")
    current_font_size = st.slider("字體大小 (表格)", 12, 72, value=st.session_state.font_size, key='font_size_slider')
    st.session_state.font_size = current_font_size
    
    hide_non_stock = st.checkbox("隱藏非個股 (ETF/權證/債券)", value=True)
    
    st.markdown("---")
    
    current_limit_rows = st.number_input("顯示筆數", min_value=1, value=st.session_state.limit_rows, key='limit_rows_input')
    st.session_state.limit_rows = current_limit_rows
    
    if st.button("💾 儲存設定"):
        if save_config(current_font_size, current_limit_rows):
            st.toast("設定已儲存！", icon="✅")
            
    st.markdown("### 資料管理")
    st.write(f"🚫 已忽略 **{len(st.session_state.ignored_stocks)}** 檔")
    
    col_restore, col_clear = st.columns([1, 1])
    with col_restore:
        if st.button("♻️ 復原", use_container_width=True):
            st.session_state.ignored_stocks.clear()
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks)
            st.toast("已重置忽略名單。", icon="🔄")
            st.rerun()
    with col_clear:
        if st.button("🗑️ 清空", type="primary", use_container_width=True):
            st.session_state.stock_data = pd.DataFrame()
            st.session_state.ignored_stocks = set()
            if os.path.exists(DATA_CACHE_FILE):
                os.remove(DATA_CACHE_FILE)
            st.toast("資料已全部清空", icon="🗑️")
            st.rerun()
    
    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n在表格左側勾選「移除」框，該股票將被隱藏。")

# --- 動態 CSS ---
font_px = f"{st.session_state.font_size}px"
zoom_level = current_font_size / 14.0

st.markdown(f"""
    <style>
    .block-container {{ padding-top: 4.5rem; padding-bottom: 1rem; }}
    div[data-testid="stDataFrame"] {{ width: 100%; zoom: {zoom_level}; }}
    div[data-testid="stDataFrame"] table, td, th, input, div, span, p {{
        font-family: 'Microsoft JhengHei', sans-serif !important;
    }}
    [data-testid="stMetricValue"] {{ font-size: 1.2em; }}
    thead tr th:first-child {{ display:none }}
    tbody th {{ display:none }}
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

# [修正] recalculate_row 增加 points 參數，不再從 row 讀取 _points
def recalculate_row(row, points_map):
    custom_price = row.get('自訂價(可修)')
    code = row.get('代號')
    status = ""
    
    if pd.isna(custom_price) or str(custom_price).strip() == "": return status
    
    try:
        price = float(custom_price)
        limit_up = row.get('當日漲停價')
        limit_down = row.get('當日跌停價')
        
        if pd.notna(limit_up) and abs(price - float(limit_up)) < 0.01: status = "🔴 漲停"
        elif pd.notna(limit_down) and abs(price - float(limit_down)) < 0.01: status = "🟢 跌停"
        else:
            # 從 map 取得 points
            points = points_map.get(code, [])
            if isinstance(points, list):
                for p in points:
                    if abs(p['val'] - price) < 0.01:
                        status = "🟡 命中"; break
        return status
    except: return status

def fetch_stock_data_raw(code, name_hint="", extra_data=None):
    code = str(code).strip()
    try:
        time.sleep(0.8) # 智慧延遲
        
        ticker = yf.Ticker(f"{code}.TW")
        hist = ticker.history(period="3mo") 
        if hist.empty:
            ticker = yf.Ticker(f"{code}.TWO")
            hist = ticker.history(period="3mo")
        
        if hist.empty: return None

        tz = pytz.timezone('Asia/Taipei')
        now = datetime.now(tz)
        last_date = hist.index[-1].date()
        is_today_data = (last_date == now.date())
        is_during_trading = (now.time() < dt_time(13, 45))
        
        if is_today_data and is_during_trading and len(hist) > 1:
            today = hist.iloc[-1]
            hist_prior = hist.iloc[:-1]
            prev_day = hist_prior.iloc[-1]
        else:
            today = hist.iloc[-1]
            if len(hist) >= 2:
                prev_day = hist.iloc[-2]
                hist_prior = hist.iloc[:-1]
            else:
                prev_day = today
                hist_prior = hist
        
        current_price = today['Close']
        pct_change = ((current_price - prev_day['Close']) / prev_day['Close']) * 100
        
        target_raw = current_price * 1.03
        stop_raw = current_price * 0.97
        target_price = apply_sr_rules(target_raw, current_price)
        stop_price = apply_sr_rules(stop_raw, current_price)
        
        limit_up_next, limit_down_next = calculate_limits(current_price) 
        limit_up_today, limit_down_today = calculate_limits(prev_day['Close'])

        points = []
        
        # 5MA
        ma5_raw = hist['Close'].tail(5).mean()
        ma5 = apply_sr_rules(ma5_raw, current_price)
        ma5_tag = "多" if ma5_raw < current_price else ("空" if ma5_raw > current_price else "平")
        points.append({"val": ma5, "tag": ma5_tag, "force": True})

        # 當日
        points.append({"val": apply_tick_rules(today['Open']), "tag": ""})
        points.append({"val": apply_tick_rules(today['High']), "tag": ""})
        points.append({"val": apply_tick_rules(today['Low']), "tag": ""})
        
        # 昨日 (範圍篩選)
        p_close = apply_tick_rules(prev_day['Close'])
        p_high = apply_tick_rules(prev_day['High'])
        p_low = apply_tick_rules(prev_day['Low'])
        
        points.append({"val": p_close, "tag": ""})
        if limit_down_next <= p_high <= limit_up_next: points.append({"val": p_high, "tag": ""})
        if limit_down_next <= p_low <= limit_up_next: points.append({"val": p_low, "tag": ""})
        
        # 近期高低 (90日)
        high_90_raw = max(hist['High'].max(), today['High'], current_price)
        low_90_raw = min(hist['Low'].min(), today['Low'], current_price)
        high_90 = apply_tick_rules(high_90_raw)
        low_90 = apply_tick_rules(low_90_raw)
        
        points.append({"val": high_90, "tag": "高"})
        points.append({"val": low_90, "tag": "低"})

        # 觸及
        touched_up = (today['High'] >= limit_up_today - 0.01) or (abs(current_price - limit_up_today) < 0.01)
        touched_down = (today['Low'] <= limit_down_today + 0.01) or (abs(current_price - limit_down_today) < 0.01)
        
        if target_price > high_90: points.append({"val": target_price, "tag": ""})
        if stop_price < low_90: points.append({"val": stop_price, "tag": ""})
        if touched_up: points.append({"val": limit_up_today, "tag": "漲停"})
        if touched_down: points.append({"val": limit_down_today, "tag": "跌停"})
            
        display_candidates = []
        for p in points:
            v = float(f"{p['val']:.2f}")
            is_force = p.get('force', False)
            if is_force or (limit_down_next <= v <= limit_up_next):
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
            else: item = v

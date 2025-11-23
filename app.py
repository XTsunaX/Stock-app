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

# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="當沖戰略室", page_icon="⚡", layout="wide")

# 1. 標題
st.title("⚡ 當沖戰略室 ⚡")

CONFIG_FILE = "config.json"

def load_config():
    """讀取設定檔"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_config(font_size, limit_rows):
    """儲存設定檔"""
    try:
        config = {"font_size": font_size, "limit_rows": limit_rows}
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f)
        return True
    except:
        return False

# --- 初始化 Session State ---
if 'stock_data' not in st.session_state:
    st.session_state.stock_data = pd.DataFrame()

# 計算機用的 Session State
if 'calc_base_price' not in st.session_state:
    st.session_state.calc_base_price = 100.0

if 'calc_view_price' not in st.session_state:
    st.session_state.calc_view_price = 100.0

# 優先從設定檔讀取
saved_config = load_config()

if 'font_size' not in st.session_state:
    st.session_state.font_size = saved_config.get('font_size', 18)

if 'limit_rows' not in st.session_state:
    st.session_state.limit_rows = saved_config.get('limit_rows', 5)

# --- 側邊欄設定 ---
with st.sidebar:
    st.header("⚙️ 設定")
    
    current_font_size = st.slider(
        "字體大小 (表格)", 
        min_value=12, 
        max_value=72, 
        key='font_size'
    )
    
    hide_etf = st.checkbox("隱藏 ETF (00開頭)", value=True)
    st.markdown("---")
    
    current_limit_rows = st.number_input(
        "顯示筆數", 
        min_value=1, 
        key='limit_rows'
    )
    
    if st.button("💾 儲存設定"):
        if save_config(current_font_size, current_limit_rows):
            st.toast("設定已儲存！下次開啟將自動套用。", icon="✅")
        else:
            st.error("設定儲存失敗。")
    
    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n勾選左側框框後按 `Delete` 鍵。")

# --- 動態 CSS ---
font_px = f"{st.session_state.font_size}px"

st.markdown(f"""
    <style>
    .block-container {{ padding-top: 4.5rem; padding-bottom: 1rem; }}
    
    /* 套用到所有 Streamlit 表格相關元素 */
    div[data-testid="stDataFrame"] table,
    div[data-testid="stDataFrame"] td,
    div[data-testid="stDataFrame"] th,
    div[data-testid="stDataFrame"] input,
    div[data-testid="stDataFrame"] div,
    div[data-testid="stDataFrame"] span {{
        font-size: {font_px} !important;
        font-family: 'Microsoft JhengHei', sans-serif !important;
        line-height: 1.5 !important;
    }}
    
    div[data-testid="stDataFrame"] {{
        width: 100%;
    }}
    
    /* 計算機頁面特定樣式 */
    [data-testid="stMetricValue"] {{
        font-size: 1.2em;
    }}
    
    /* 隱藏索引列的額外 CSS 確保 */
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
        except Exception as e:
            pass
    return code_map, name_map

@st.cache_data(ttl=86400)
def get_stock_name_online(code):
    code = str(code).strip()
    if not code.isdigit(): return code
    code_map, _ = load_local_stock_names()
    if code in code_map: return code_map[code]
    try:
        url = f"https://tw.stock.yahoo.com/quote/{code}.TW"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=2)
        soup = BeautifulSoup(r.text, "html.parser")
        if soup.title and "(" in soup.title.string:
            return soup.title.string.split('(')[0].strip()
        url_two = f"https://tw.stock.yahoo.com/quote/{code}.TWO"
        r_two = requests.get(url_two, headers=headers, timeout=2)
        soup_two = BeautifulSoup(r_two.text, "html.parser")
        if soup_two.title and "(" in soup_two.title.string:
            return soup_two.title.string.split('(')[0].strip()
        return code
    except:
        return code

@st.cache_data(ttl=86400)
def search_code_online(query):
    query = query.strip()
    if query.isdigit(): return query
    _, name_map = load_local_stock_names()
    if query in name_map: return name_map[query]
    try:
        url = f"https://tw.stock.yahoo.com/h/kimosearch/search_list.html?keyword={query}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=2)
        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.find_all('a', href=True)
        for link in links:
            if "/quote/" in link['href'] and ".TW" in link['href']:
                parts = link['href'].split("/quote/")[1].split(".")
                if parts[0].isdigit(): return parts[0]
    except:
        pass
    return None

# ==========================================
# 2. 核心計算邏輯 (含台股 Tick 規則)
# ==========================================

def get_tick_size(price):
    """取得台股價格對應的跳動檔位"""
    try:
        price = float(price)
    except:
        return 0.01
        
    if pd.isna(price) or price <= 0: return 0.01
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def calculate_limits(price):
    """計算漲跌停價 (10%)"""
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
    except:
        return 0, 0

def apply_tick_rules(price):
    """將任意價格修正為符合台股 Tick 規則的價格"""
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        rounded_price = round(p / tick) * tick
        return float(f"{rounded_price:.2f}")
    except:
        return price

def move_tick(price, steps):
    """計算價格往上或往下 N 檔後的價格"""
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
    except:
        return price

def fetch_stock_data_raw(code, name_hint="", extra_data=None):
    code = str(code).strip()
    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist = ticker.history(period="3mo") 
        if hist.empty:
            ticker = yf.Ticker(f"{code}.TWO")
            hist = ticker.history(period="3mo")
        if hist.empty: 
            st.error(f"⚠️ 代號 {code}: 抓取無資料 (Yahoo Finance 返回空值)。")
            return None

        today = hist.iloc[-1]
        current_price = today['Close']
        
        if len(hist) >= 2:
            prev_day = hist.iloc[-2]
        else:
            prev_day = today
        
        if pd.isna(current_price) or pd.isna(prev_day['Close']):
            return None

        pct_change = ((current_price - prev_day['Close']) / prev_day['Close']) * 100
        
        # 1. 欄位顯示用的數據 (以收盤價為基準)
        target_price = apply_tick_rules(current_price * 1.03)
        stop_price = apply_tick_rules(current_price * 0.97)
        limit_up_col, limit_down_col = calculate_limits(current_price) 

        # 2. 戰略備註用的漲跌停參考 (以昨日收盤為基準)
        limit_up_today, limit_down_today = calculate_limits(prev_day['Close'])

        # 點位收集
        points = []
        ma5 = apply_tick_rules(hist['Close'].tail(5).mean())
        points.append({"val": ma5, "tag": "多" if current_price > ma5 else "空"})
        points.append({"val": apply_tick_rules(today['Open']), "tag": ""})
        points.append({"val": apply_tick_rules(today['High']), "tag": ""})
        points.append({"val": apply_tick_rules(today['Low']), "tag": ""})
        
        if len(hist) >= 6:
            past_5 = hist.iloc[-6:-1]
        else:
            past_5 = hist.iloc[:-1]
            
        if not past_5.empty:
            points.append({"val": apply_tick_rules(past_5['High'].max()), "tag": ""})
            points.append({"val": apply_tick_rules(past_5['Low'].min()), "tag": ""})
            
        high_90 = apply_tick_rules(hist['High'].max())
        low_90 = apply_tick_rules(hist['Low'].min())
        points.append({"val": high_90, "tag": "高"})
        points.append({"val": low_90, "tag": "低"})

        # 戰略備註整理
        display_candidates = []
        for p in points:
            v = float(f"{p['val']:.2f}")
            # 備註過濾邏輯：確保顯示的點位不超過收盤價預測的漲跌停範圍
            is_in_range = limit_down_col <= v <= limit_up_col
            is_5ma = "多" in p['tag'] or "空" in p['tag']
            if is_in_range or is_5ma:
                display_candidates.append({"val": v, "tag": p['tag']})
        
        # 檢查是否觸及今日漲跌停 (基於昨日收盤價)
        touched_up = today['High'] >= limit_up_today - 0.01
        touched_down = today['Low'] <= limit_down_today + 0.01

        if touched_up:
            display_candidates.append({"val": limit_up_today, "tag": "漲停"})
        if touched_down:
            display_candidates.append({"val": limit_down_today, "tag": "跌停"})
            
        display_candidates.sort(key=lambda x: x['val'])
        
        final_display_points = []
        extra_points = [] 

        for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
            g_list = list(group)
            tags = [x['tag'] for x in g_list]
            
            final_tag = ""
            is_limit_up = "漲停" in tags
            is_limit_down = "跌停" in tags
            is_high = "高" in tags
            is_low = "低" in tags
            
            is_close_price = abs(val - current_price) < 0.01
            
            # --- 漲停高/跌停低 + 延伸計算 ---
            if is_limit_up:
                if is_high and is_close_price: 
                    final_tag = "漲停高"
                    ext_val = apply_tick_rules(val * 1.03)
                    extra_points.append({"val": ext_val, "tag": ""})
                else:
                    final_tag = "漲停"
            elif is_limit_down:
                if is_low and is_close_price:
                    final_tag = "跌停低"
                    ext_val = apply_tick_rules(val * 0.97)
                    extra_points.append({"val": ext_val, "tag": ""})
                else:
                    final_tag = "跌停"
            else:
                if is_high: final_tag = "高"
                elif is_low: final_tag = "低"
                elif "多" in tags: final_tag = "多"
                elif "空" in tags: final_tag = "空"
                else: final_tag = ""

            final_display_points.append({"val": val, "tag": final_tag})
        
        if extra_points:
            for ep in extra_points:
                final_display_points.append(ep)
            final_display_points.sort(key=lambda x: x['val'])
            
        note_parts = []
        seen_vals = set() 
        for p in final_display_points:
            if p['val'] in seen_vals and p['tag'] == "": continue
            seen_vals.add(p['val'])
            v_str = f"{p['val']:.0f}" if p['val'].is_integer() else f"{p['val']:.2f}"
            t = p['tag']
            if t in ["漲停", "漲停高", "跌停", "跌停低", "高", "低"]:
                item = f"{t}{v_str}"
            elif t: 
                item = f"{v_str}{t}"
            else: 
                item = v_str
            note_parts.append(item)
        
        strategy_note = "-".join(note_parts)
        
        calc_points = points.copy()
        calc_points.append({"val": limit_up_today, "tag": "漲停"})
        calc_points.append({"val": limit_down_today, "tag": "跌停"})
        for ep in extra_points:
            calc_points.append(ep)
        
        full_calc_points = []
        seen_calc = set()
        for p in calc_points:
             v = float(f"{p['val']:.2f}")

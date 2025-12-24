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

# ==========================================
# 1. 基礎設定與快取功能 (先定義)
# ==========================================

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
        # 從後台計算的點位
        points = points_map.get(code, [])
        if isinstance(points, list):
            for p in points: strat_values.append(p['val'])
            
        # 從戰略備註中抓取數字 (注意：這裡使用 regex 只會抓取數字，如果標籤是 "三高" 則不會被抓到，解決了 "3低" 被誤判的問題)
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

# ==========================================
# 3. 股票資料抓取
# ==========================================

def get_live_price(code):
    try:
        realtime_data = twstock.realtime.get(code)
        if realtime_data and realtime_data.get('success'):
            price_str = realtime_data['realtime'].get('latest_trade_price')
            if price_str and price_str != '-' and float(price_str) > 0: return float(price_str)
            bids = realtime_data['realtime'].get('best_bid_price', [])
            if bids and bids[0] and bids[0] != '-': return float(bids[0])
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
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(r.text, 'html.parser')
        price_tag = soup.find('span', class_='Fz(32px)')
        if not price_tag: return None
        price = float(price_tag.text.replace(',', ''))
        change_tag = soup.find('span', class_='Fz(20px)')
        change = 0.0
        if change_tag:
             change_txt = change_tag.text.strip().replace('▲', '').replace('▼', '').replace('+', '').replace(',', '')
             if 'C($c-trend-down)' in str(change_tag.parent): change = -float(change_txt)
             else: change = float(change_txt)
        data = {'Open': [price], 'High': [price], 'Low': [price], 'Close': [price], 'Volume': [0]}
        return pd.DataFrame(data, index=[pd.to_datetime(datetime.now().date())]), price - change
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
            df = df.set_index('Date').rename(columns={'open': 'Open', 'max': 'High', 'min': 'Low', 'close': 'Close', 'Trading_Volume': 'Volume'})
            return df[['Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric)
    except: pass
    return None

def fetch_stock_data_raw(code, name_hint="", extra_data=None, include_3d_hl=False):
    code = str(code).strip()
    hist = pd.DataFrame(); source_used = "none"
    def is_valid_data(df_check, code):
        if df_check is None or df_check.empty: return False
        try:
            last_price = df_check.iloc[-1]['Close']
            if last_price <= 0: return False
            last_dt = df_check.index[-1]
            if last_dt.tzinfo is not None: last_dt = last_dt.astimezone(pytz.timezone('Asia/Taipei')).replace(tzinfo=None)
            if (datetime.now().replace(tzinfo=None) - last_dt).days > 3: return False
            return True
        except: return False
    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist_yf = ticker.history(period="3mo")
        if hist_yf.empty or not is_valid_data(hist_yf, code):
            ticker = yf.Ticker(f"{code}.TWO")
            hist_yf = ticker.history(period="3mo")
        if not hist_yf.empty and is_valid_data(hist_yf, code): hist = hist_yf; source_used = "yfinance"
    except: pass
    if hist.empty:
        try:
            stock = twstock.Stock(code); tw_data = stock.fetch_31()
            if tw_data:
                df_tw = pd.DataFrame(tw_data); df_tw['Date'] = pd.to_datetime(df_tw['date'])
                df_tw = df_tw.set_index('Date').rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'capacity': 'Volume'})
                if not df_tw.empty and is_valid_data(df_tw, code): hist = df_tw; source_used = "twstock"
        except: pass
    if hist.empty:
        df_fm = fetch_finmind_backup(code)
        if df_fm is not None and is_valid_data(df_fm, code): hist = df_fm; source_used = "finmind"
    if hist.empty:
        df_web, _ = fetch_yahoo_web_backup(code)
        if df_web is not None: hist = df_web; source_used = "web_backup"
    if hist.empty: return None
    hist['High'] = hist[['High', 'Close']].max(axis=1)
    hist['Low'] = hist[['Low', 'Close']].min(axis=1)
    tz = pytz.timezone('Asia/Taipei'); now = datetime.now(tz)
    hist_strat = hist.copy()
    if now.time() < dt_time(13, 30) and (hist.index[-1].date() == now.date()): hist_strat = hist_strat.iloc[:-1]
    elif (hist.index[-1].date() != now.date()) and source_used != "web_backup":
        live = get_live_price(code)
        if live:
            new_row = pd.DataFrame({'Open': live, 'High': live, 'Low': live, 'Close': live, 'Volume': 0}, index=[pd.to_datetime(now.date())])
            hist_strat = pd.concat([hist_strat, new_row])
    if hist_strat.empty: return None
    strategy_base_price = hist_strat.iloc[-1]['Close']
    limit_up_show, limit_down_show = calculate_limits(strategy_base_price)
    limit_up_T = calculate_limits(hist_strat.iloc[-2]['Close'])[0] if len(hist_strat) >= 2 else None
    points = []
    
    # [修正] 使用中文 "三高", "三低" 避免 regex 抓到數字 3
    if include_3d_hl and len(hist_strat) >= 1:
        last_3 = hist_strat.tail(3)
        points.append({"val": apply_tick_rules(last_3['High'].max()), "tag": "三高"})
        points.append({"val": apply_tick_rules(last_3['Low'].min()), "tag": "三低"})
        
    if len(hist_strat) >= 5:
        ma5_raw = float((sum(Decimal(str(x)) for x in hist_strat['Close'].tail(5).values) / Decimal("5")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        points.append({"val": apply_sr_rules(ma5_raw, strategy_base_price), "tag": "多" if ma5_raw < strategy_base_price else ("空" if ma5_raw > strategy_base_price else "平"), "force": True})
    if len(hist_strat) >= 2:
        last = hist_strat.iloc[-1]
        for p in [last['Open'], last['High'], last['Low']]:
            v = apply_tick_rules(p)
            if limit_down_show <= v <= limit_up_show: points.append({"val": v, "tag": "跌停" if p == last['Low'] and limit_up_T and abs(v-limit_up_T*0.9)<0.01 else ""})
    high_90 = hist_strat['High'].max(); low_90 = hist_strat['Low'][hist_strat['Low']>0].min() if not hist_strat['Low'][hist_strat['Low']>0].empty else hist_strat['Low'].min()
    points.append({"val": apply_tick_rules(high_90), "tag": "高"}); points.append({"val": apply_tick_rules(low_90), "tag": "低"})
    if len(hist_strat) >= 2 and limit_up_T and abs(hist_strat.iloc[-1]['High'] - limit_up_T) < 0.01:
        points.append({"val": limit_up_T, "tag": "漲停高" if abs(limit_up_T - high_90) < 0.05 else "漲停"})
    final_points = []
    for val, group in itertools.groupby(sorted([p for p in points if p.get('force') or (limit_down_show <= p['val'] <= limit_up_show)], key=lambda x: x['val']), key=lambda x: round(x['val'], 2)):
        tags = [x['tag'] for x in list(group) if x['tag']]
        tag = ""
        # 優先順序判定
        if "漲停高" in tags: tag = "漲停高"
        elif "漲停" in tags: tag = "漲停"
        elif "跌停" in tags: tag = "跌停"
        elif "高" in tags: tag = "高"
        elif "低" in tags: tag = "低"
        elif "三高" in tags: tag = "三高"
        elif "三低" in tags: tag = "三低"
        elif "多" in tags: tag = "多"
        elif "空" in tags: tag = "空"
        elif "平" in tags: tag = "平"
        final_points.append({"val": val, "tag": tag})
    note = "-".join([f"{p['tag']}{fmt_price(p['val'])}" if p['tag'] in ["漲停", "漲停高", "跌停", "高", "低", "三高", "三低"] else (f"{fmt_price(p['val'])}{p['tag']}" if p['tag'] else fmt_price(p['val'])) for p in final_points])
    manual = st.session_state.saved_notes.get(code, "")
    strategy_note = f"{note} {manual}".strip() if manual else note
    final_name = name_hint if name_hint else get_stock_name_online(code)
    light = "🔴" if "多" in strategy_note else ("🟢" if "空" in strategy_note else "⚪")
    return {"代號": code, "名稱": f"{light} {final_name}", "收盤價": round(strategy_base_price, 2), "漲跌幅": ((strategy_base_price - hist_strat.iloc[-2]['Close'])/hist_strat.iloc[-2]['Close']*100 if len(hist_strat)>=2 else 0), "期貨": "✅" if code in st.session_state.futures_list else "", "當日漲停價": limit_up_show, "當日跌停價": limit_down_show, "自訂價(可修)": None, "獲利目標": apply_sr_rules(strategy_base_price*1.03, strategy_base_price), "防守停損": apply_sr_rules(strategy_base_price*0.97, strategy_base_price), "戰略備註": strategy_note, "_points": final_points, "狀態": "", "_auto_note": note}

# ==========================================
# 4. Session State 初始化
# ==========================================

if 'stock_data' not in st.session_state:
    cached_df, cached_ignored, cached_candidates, cached_notes = load_data_cache()
    st.session_state.stock_data = cached_df
    st.session_state.ignored_stocks = cached_ignored
    st.session_state.all_candidates = cached_candidates
    st.session_state.saved_notes = cached_notes

for key, default in [('ignored_stocks', set()), ('all_candidates', []), ('calc_base_price', 100.0), ('calc_view_price', 100.0), ('url_history', load_url_history()), ('search_multiselect', load_search_cache()), ('saved_notes', {}), ('futures_list', set())]:
    if key not in st.session_state: st.session_state[key] = default

if 'cloud_url_input' not in st.session_state:
    st.session_state.cloud_url_input = st.session_state.url_history[0] if st.session_state.url_history else ""

saved_config = load_config()
st.session_state.font_size = saved_config.get('font_size', 15) if 'font_size' not in st.session_state else st.session_state.font_size
st.session_state.limit_rows = saved_config.get('limit_rows', 5) if 'limit_rows' not in st.session_state else st.session_state.limit_rows
st.session_state.auto_update_last_row = saved_config.get('auto_update', True) if 'auto_update_last_row' not in st.session_state else st.session_state.auto_update_last_row
st.session_state.update_delay_sec = saved_config.get('delay_sec', 1.0) if 'update_delay_sec' not in st.session_state else st.session_state.update_delay_sec

# ==========================================
# 5. UI - 側邊欄
# ==========================================

with st.sidebar:
    st.header("⚙️ 設定")
    st.session_state.font_size = st.slider("字體大小 (表格)", 12, 72, st.session_state.font_size)
    hide_non_stock = st.checkbox("隱藏非個股 (ETF/權證/債券)", value=True)
    include_3d_hl = st.checkbox("戰略備註包含近三日高低點", value=False)
    st.markdown("---")
    st.session_state.limit_rows = st.number_input("顯示筆數 (檔案/雲端)", 1, value=st.session_state.limit_rows)
    if st.button("💾 儲存設定"):
        if save_config(st.session_state.font_size, st.session_state.limit_rows, st.session_state.auto_update_last_row, st.session_state.update_delay_sec): st.toast("設定已儲存！", icon="✅")
    st.markdown("### 資料管理")
    st.write(f"🚫 已忽略 **{len(st.session_state.ignored_stocks)}** 檔")
    if st.session_state.ignored_stocks:
        ignored_list = sorted(list(st.session_state.ignored_stocks))
        display_map = {f"{c} {get_stock_name_online(c)}": c for c in ignored_list}
        restore = st.multiselect("選取以復原股票:", options=list(display_map.keys()))
        if st.button("♻️ 復原選中股票", use_container_width=True):
            for d in restore: st.session_state.ignored_stocks.remove(display_map[d])
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes); st.rerun()
    c_res, c_clr = st.columns(2)
    if c_res.button("♻️ 全部復原", use_container_width=True): st.session_state.ignored_stocks.clear(); st.rerun()
    if c_clr.button("🗑️ 清空全部", type="primary", use_container_width=True): 
        st.session_state.stock_data = pd.DataFrame(); st.session_state.ignored_stocks = set(); st.session_state.saved_notes = {}; st.rerun()
    st.link_button("📥 Goodinfo 當日週轉率排行", "https://reurl.cc/Or9e37", use_container_width=True)

# ==========================================
# 6. UI - 主介面
# ==========================================

# [還原] 表格 CSS 樣式，恢復 zoom 與 width 100%
font_px = f"{st.session_state.font_size}px"
zoom_level = st.session_state.font_size / 14.0

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

tab1, tab2 = st.tabs(["⚡ 當沖戰略室 ⚡", "💰 當沖損益室 💰"])

with tab1:
    cs, cf = st.columns([2, 1])
    with cs:
        codes, names = load_local_stock_names()
        st_opts = [f"{c} {n}" for c, n in sorted(codes.items())]
        t1, t2 = st.tabs(["📂 本機", "☁️ 雲端"])
        with t1: uploaded_file = st.file_uploader("上傳檔案", type=['xlsx', 'csv', 'html', 'xls'], label_visibility="collapsed")
        with t2:
            st.selectbox("📜 歷史紀錄", options=st.session_state.url_history if st.session_state.url_history else ["(無紀錄)"], label_visibility="collapsed", on_change=lambda: setattr(st.session_state, 'cloud_url_input', st.session_state.history_selected), key="history_selected")
            st.text_input("輸入連結", key="cloud_url_input")
        search_sel = st.multiselect("🔍 快速查詢", options=st_opts, key="search_multiselect", on_change=lambda: save_search_cache(st.session_state.search_multiselect))

    if st.button("🚀 執行分析"):
        if not st.session_state.futures_list: st.session_state.futures_list = fetch_futures_list()
        targets = []; df_up = pd.DataFrame()
        if st.session_state.cloud_url_input.strip() and st.session_state.cloud_url_input not in st.session_state.url_history:
            st.session_state.url_history.insert(0, st.session_state.cloud_url_input); save_url_history(st.session_state.url_history)
        try:
            if uploaded_file:
                uploaded_file.seek(0); fname = uploaded_file.name.lower()
                if fname.endswith('.csv'): df_up = pd.read_csv(uploaded_file, dtype=str)
                elif fname.endswith(('.html', '.htm', '.xls', '.xlsx')): df_up = pd.read_html(uploaded_file)[0] if not fname.endswith('.xlsx') else pd.read_excel(uploaded_file, dtype=str)
            elif st.session_state.cloud_url_input:
                url = st.session_state.cloud_url_input
                if "docs.google.com" in url: url = url.split("/edit")[0] + "/export?format=csv"
                df_up = pd.read_csv(url, dtype=str)
        except: st.error("讀取失敗")
        if search_sel: targets.extend([(s.split(' ')[0], s.split(' ')[1], 'search', 999) for s in search_sel])
        if not df_up.empty:
            c_col = next((c for c in df_up.columns if "代號" in str(c)), None)
            if c_col:
                for i, row in df_up.iterrows():
                    c = str(row[c_col]).replace('=', '').replace('"', '').strip()
                    if c and c not in st.session_state.ignored_stocks: targets.append((c, "", 'upload', i))
        existing = {}; bar = st.progress(0); total = len(targets)
        for i, (c, n, s, o) in enumerate(targets):
            if s == 'upload' and i > st.session_state.limit_rows: continue
            data = fetch_stock_data_raw(c, n, include_3d_hl=include_3d_hl)
            if data: data.update({'_source': s, '_order': o, '_source_rank': 1 if s == 'upload' else 2}); existing[c] = data
            bar.progress((i+1)/total if total else 1)
        if existing: st.session_state.stock_data = pd.DataFrame(list(existing.values())); save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
        st.rerun()

    if not st.session_state.stock_data.empty:
        df_display = st.session_state.stock_data.sort_values(by=['_source_rank', '_order']).reset_index(drop=True)
        df_display["移除"] = False
        points_map = df_display.set_index('代號')['_points'].to_dict() if '_points' in df_display.columns else {}
        auto_notes = df_display.set_index('代號')['_auto_note'].to_dict() if '_auto_note' in df_display.columns else {}
        
        # 確保在顯示前計算狀態
        for i in range(len(df_display)):
            df_display.at[i, '狀態'] = recalculate_row(df_display.iloc[i], points_map)

        for c in ["當日漲停價", "當日跌停價", "獲利目標", "防守停損", "自訂價(可修)"]: df_display[c] = df_display[c].apply(fmt_price)
        for i in range(len(df_display)):
            icon = "🔴" if df_display.at[i, "漲跌幅"] > 0 else ("🟢" if df_display.at[i, "漲跌幅"] < 0 else "⚪")
            df_display.at[i, "收盤價"] = f"{icon} {fmt_price(df_display.at[i, '收盤價'])}"
            df_display.at[i, "漲跌幅"] = f"{icon} {df_display.at[i, '漲跌幅']:+.2f}%"

        note_width_px = calculate_note_width(df_display['戰略備註'], st.session_state.font_size)

        # [還原] use_container_width=False 配合 zoom CSS
        edited = st.data_editor(
            df_display[["移除", "代號", "名稱", "戰略備註", "自訂價(可修)", "狀態", "當日漲停價", "當日跌停價", "獲利目標", "防守停損", "收盤價", "漲跌幅", "期貨"]], 
            hide_index=True, 
            key="main_editor",
            use_container_width=False,
            column_config={
                 "戰略備註": st.column_config.TextColumn("戰略備註 ✏️", width=note_width_px),
                 "自訂價(可修)": st.column_config.TextColumn("自訂價 ✏️", width=60),
                 "移除": st.column_config.CheckboxColumn("刪除", width=40),
            }
        )
        c1, c2, c3, _ = st.columns([1.5, 1.2, 1.2, 6.1])
        if c1.button("⚡ 執行更新", type="primary", use_container_width=True) or c2.button("💾 儲存備註", use_container_width=True):
            up_map = edited.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
            for i, row in st.session_state.stock_data.iterrows():
                code = row['代號']
                if code in up_map:
                    nv, nn = up_map[code]['自訂價(可修)'], up_map[code]['戰略備註']
                    st.session_state.stock_data.at[i, '自訂價(可修)'] = nv
                    auto = auto_notes.get(code, "")
                    manual = nn[len(auto):].strip() if auto and nn.startswith(auto) else nn
                    st.session_state.saved_notes[code] = manual
                    st.session_state.stock_data.at[i, '戰略備註'] = f"{auto} {manual}".strip()
                    st.session_state.stock_data.at[i, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes); st.rerun()
        if c3.button("🗑️ 刪除備註", use_container_width=True):
            st.session_state.saved_notes = {}; st.rerun()
        if not edited.empty and edited["移除"].any():
            for c in edited[edited["移除"]]["代號"]: st.session_state.ignored_stocks.add(str(c))
            st.session_state.stock_data = st.session_state.stock_data[~st.session_state.stock_data["代號"].isin(edited[edited["移除"]]["代號"])]
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes); st.rerun()

with tab2:
    st.markdown("#### 💰 當沖損益室 💰")
    # [還原] 當沖損益室邏輯 (修正 number_input 參數順序)
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

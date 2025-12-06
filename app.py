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

if 'cloud_url_input' not in st.session_state:
    st.session_state.cloud_url_input = ""

if 'cloud_url' not in st.session_state:
    st.session_state.cloud_url = ""

saved_config = load_config()

if 'font_size' not in st.session_state:
    st.session_state.font_size = saved_config.get('font_size', 15)

if 'limit_rows' not in st.session_state:
    st.session_state.limit_rows = saved_config.get('limit_rows', 5)

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
        "顯示筆數 (分析上限)", 
        min_value=1, 
        value=st.session_state.limit_rows,
        key='limit_rows_input'
    )
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
    st.info("🗑️ **如何刪除股票？**\n\n在表格左側勾選「刪除」框，該股票將被隱藏。")

# --- 動態 CSS ---
font_px = f"{st.session_state.font_size}px"
zoom_level = current_font_size / 14.0

st.markdown(f"""
    <style>
    /* 表格容器縮放 */
    div[data-testid="stDataFrame"] {{
        width: 100%;
        zoom: {zoom_level};
    }}
    
    /* 只針對表格內的元素設定字體 */
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

    /* 輸入框字體修正 */
    div[data-testid="stDataFrame"] input {{
        font-family: 'Microsoft JhengHei', sans-serif !important;
        font-size: 0.9rem !important; 
    }}
    
    /* 隱藏索引 */
    thead tr th:first-child {{ display:none }}
    tbody th {{ display:none }}
    
    .block-container {{ padding-top: 4.5rem; padding-bottom: 1rem; }}
    [data-testid="stMetricValue"] {{ font-size: 1.2em; }}
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
        
        if l_up is not None and abs(price - l_up) < 0.01: status = "🔴 漲停"
        elif l_down is not None and abs(price - l_down) < 0.01: status = "🟢 跌停"
        else:
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
        time.sleep(0.1)
        
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
        
        # 昨日
        p_close = apply_tick_rules(prev_day['Close'])
        p_high = apply_tick_rules(prev_day['High'])
        p_low = apply_tick_rules(prev_day['Low'])
        
        points.append({"val": p_close, "tag": ""})
        if limit_down_next <= p_high <= limit_up_next: points.append({"val": p_high, "tag": ""})
        if limit_down_next <= p_low <= limit_up_next: points.append({"val": p_low, "tag": ""})
        
        # 近期高低
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
            "代號": code, "名稱": final_name_display, "收盤價": round(current_price, 2),
            "漲跌幅": pct_change, "當日漲停價": limit_up_next, "當日跌停價": limit_down_next,
            "自訂價(可修)": None, "獲利目標": target_price, "防守停損": stop_price,   
            "戰略備註": strategy_note, "_points": full_calc_points, "狀態": ""
        }
    except Exception as e: return None

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
            def update_url():
                 st.session_state.cloud_url = st.session_state.cloud_url_input
            
            st.text_input(
                "輸入連結 (CSV/Excel/Google Sheet)", 
                key="cloud_url_input",
                value=st.session_state.cloud_url,
                on_change=update_url,
                placeholder="https://..."
            )
            
        search_selection = st.multiselect("🔍 快速查詢 (中文/代號)", options=stock_options, placeholder="輸入 2330 或 台積電...")

    if st.button("🚀 執行分析"):
        targets = []
        df_up = pd.DataFrame()
        
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
                    
                    if count >= limit_rows: break 
                    
                    n = str(row[n_col]) if n_col else ""
                    if n.lower() == 'nan': n = ""
                    targets.append((c_raw, n, 'upload', count))
                    count += 1

        if search_selection:
            for item in search_selection:
                parts = item.split(' ', 1)
                targets.append((parts[0], parts[1] if len(parts) > 1 else "", 'search', 9999))

        results = []
        seen = set()
        status_text = st.empty()
        bar = st.progress(0)
        total = len(targets)
        
        existing_data = {}
        st.session_state.stock_data = pd.DataFrame()

        fetch_cache = {}
        for i, (code, name, source, extra) in enumerate(targets):
            status_text.text(f"正在分析 {i+1}/{total}: {code} {name} ...")
            
            if code in st.session_state.ignored_stocks: continue
            if (code, source) in seen: continue
            
            time.sleep(0.1)
            
            if code in fetch_cache: data = fetch_cache[code]
            else:
                data = fetch_stock_data_raw(code, name, extra)
                if data: fetch_cache[code] = data
            
            if data:
                data['_source'] = source
                data['_order'] = extra
                data['_source_rank'] = 1 if source == 'upload' else 2
                existing_data[code] = data
                seen.add((code, source))
                
            if total > 0: bar.progress((i+1)/total)
        
        bar.empty()
        status_text.empty()
        
        if existing_data:
            st.session_state.stock_data = pd.DataFrame(list(existing_data.values()))
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks)

    if not st.session_state.stock_data.empty:
        limit = st.session_state.limit_rows
        df_all = st.session_state.stock_data.copy()
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

        # ------------------------------------------------------------------
        # [Callback] 使用者編輯表格時觸發的邏輯
        # ------------------------------------------------------------------
        def on_editor_change():
            """
            當表格內容變動時觸發此函數。
            邏輯：
            1. 中間列的修改 -> 靜默更新 Session State，不重算，不中斷。
            2. 最後一列的修改 -> 觸發所有列的狀態計算，Streamlit 自動重整畫面。
            """
            state = st.session_state["main_editor"]
            edited_rows = state.get("edited_rows", {})
            
            # 建立顯示索引對應到原始數據的索引
            display_to_source_map = {} 
            for disp_idx, row in df_display.iterrows():
                code = row['代號']
                src_indices = st.session_state.stock_data.index[st.session_state.stock_data['代號'] == code].tolist()
                if src_indices:
                    display_to_source_map[disp_idx] = src_indices[0]

            need_recalc_all = False
            last_row_idx = len(df_display) - 1

            for idx, changes in edited_rows.items():
                idx = int(idx)
                if idx not in display_to_source_map: continue
                src_idx = display_to_source_map[idx]
                
                # A. 刪除
                if "移除" in changes and changes["移除"] is True:
                     code_to_remove = df_display.at[idx, '代號']
                     st.session_state.ignored_stocks.add(code_to_remove)
                     save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks)
                
                # B. 修改內容
                for col, val in changes.items():
                    if col in ['自訂價(可修)', '戰略備註']:
                        st.session_state.stock_data.at[src_idx, col] = val
                        df_display.at[idx, col] = val
                
                # C. [重點邏輯] 如果是「最後一列」修改了自訂價 -> 標記需要重算全表
                if idx == last_row_idx and '自訂價(可修)' in changes:
                    need_recalc_all = True
            
            # 防呆檢查：若最後一列已有值但狀態仍空，也觸發重算
            last_val = df_display.at[last_row_idx, '自訂價(可修)']
            last_status = df_display.at[last_row_idx, '狀態']
            if pd.notna(last_val) and str(last_val).strip() != "" and str(last_val).strip() != "None" and (pd.isna(last_status) or str(last_status) == ""):
                need_recalc_all = True

            # D. 若需要重算，更新所有列的狀態
            if need_recalc_all:
                for i, row in st.session_state.stock_data.iterrows():
                    new_status = recalculate_row(row, points_map)
                    st.session_state.stock_data.at[i, '狀態'] = new_status
            
            # Callback 結束後，Streamlit 會自動執行一次 Rerun，
            # 若 need_recalc_all 為 False，因為沒有修改其他 state 觸發組件重繪，
            # 通常不會造成全頁重整，僅會默默更新資料。

        st.data_editor(
            df_display[input_cols],
            column_config={
                "移除": st.column_config.CheckboxColumn("刪除", width=30),
                "代號": st.column_config.TextColumn(disabled=True, width=50),
                "名稱": st.column_config.TextColumn(disabled=True, width="small"),
                "收盤價": st.column_config.TextColumn(width="small", disabled=True),
                "漲跌幅": st.column_config.NumberColumn(format="%.2f%%", disabled=True, width="small"),
                "自訂價(可修)": st.column_config.TextColumn("自訂價 ✏️", width=60),
                "當日漲停價": st.column_config.TextColumn(width="small", disabled=True),
                "當日跌停價": st.column_config.TextColumn(width="small", disabled=True),
                "+3%": st.column_config.TextColumn(width="small", disabled=True),
                "-3%": st.column_config.TextColumn(width="small", disabled=True),
                "狀態": st.column_config.TextColumn(width=60, disabled=True),
                "戰略備註": st.column_config.TextColumn(width=note_width_px, disabled=False),
            },
            hide_index=True, 
            use_container_width=False, 
            num_rows="fixed", 
            key="main_editor",
            on_change=on_editor_change
        )
        
        # [修改] 按鈕縮小並調整位置
        col_btn, _ = st.columns([1, 10])
        with col_btn:
             if st.button("⚡ 更新狀態", help="手動重新計算狀態"):
                 for i, row in st.session_state.stock_data.iterrows():
                    new_status = recalculate_row(row, points_map)
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

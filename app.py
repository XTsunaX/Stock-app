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

# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="當沖戰略室", page_icon="⚡", layout="wide")

# 1. 標題
st.title("⚡ 當沖戰略室 ⚡")

CONFIG_FILE = "config.json"
DATA_CACHE_FILE = "data_cache.json"

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

def save_data_cache(df, ignored_set):
    """儲存資料到硬碟"""
    try:
        df_save = df.fillna("") 
        data_to_save = {
            "stock_data": df_save.to_dict(orient='records'),
            "ignored_stocks": list(ignored_set)
        }
        with open(DATA_CACHE_FILE, "w", encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=4)
    except Exception as e:
        pass

def load_data_cache():
    """從硬碟讀取資料"""
    if os.path.exists(DATA_CACHE_FILE):
        try:
            with open(DATA_CACHE_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
            
            df = pd.DataFrame(data.get('stock_data', []))
            ignored = set(data.get('ignored_stocks', []))
            return df, ignored
        except Exception as e:
            return pd.DataFrame(), set()
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
    
    hide_non_stock = st.checkbox("隱藏非個股 (ETF/權證/債券)", value=True, help="勾選後將隱藏 00開頭及代號大於4碼之標的。")
    
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
            
    st.markdown("### 資料管理")
    st.write(f"🚫 已忽略 **{len(st.session_state.ignored_stocks)}** 檔")
    
    if st.button("♻️ 復原忽略", use_container_width=True):
        st.session_state.ignored_stocks.clear()
        save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks)
        st.toast("已重置忽略名單。", icon="🔄")
        st.rerun()
            
    if st.button("🗑️ 清空資料", type="primary", use_container_width=True):
        st.session_state.stock_data = pd.DataFrame()
        st.session_state.ignored_stocks = set()
        if os.path.exists(DATA_CACHE_FILE):
            os.remove(DATA_CACHE_FILE)
        st.toast("資料已全部清空", icon="🗑️")
        st.rerun()
    
    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n在表格左側勾選並按 `Delete`，該股票將被隱藏。")

# --- 動態 CSS ---
font_px = f"{st.session_state.font_size}px"
zoom_level = current_font_size / 14.0

st.markdown(f"""
    <style>
    .block-container {{ padding-top: 4.5rem; padding-bottom: 1rem; }}
    
    div[data-testid="stDataFrame"] {{
        width: 100%;
        zoom: {zoom_level};
    }}
    
    div[data-testid="stDataFrame"] table,
    div[data-testid="stDataFrame"] td,
    div[data-testid="stDataFrame"] th,
    div[data-testid="stDataFrame"] input,
    div[data-testid="stDataFrame"] div,
    div[data-testid="stDataFrame"] span,
    div[data-testid="stDataFrame"] p {{
        font-family: 'Microsoft JhengHei', sans-serif !important;
    }}
    
    [data-testid="stMetricValue"] {{
        font-size: 1.2em;
    }}
    
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
# 2. 核心計算邏輯
# ==========================================

def get_tick_size(price):
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
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        rounded = (Decimal(str(p)) / Decimal(str(tick))).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * Decimal(str(tick))
        return float(rounded)
    except:
        return price

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
    except:
        return price

def calculate_note_width(series, font_size):
    def get_width(s):
        w = 0
        for c in str(s):
            w += 2.0 if ord(c) > 127 else 1.0
        return w
    
    if series.empty: return 100
    max_w = series.apply(get_width).max()
    if pd.isna(max_w): max_w = 10
    pixel_width = int(max_w * (font_size * 0.55)) + 20
    return max(100, min(pixel_width, 1000))

def recalculate_row(row):
    custom_price = row.get('自訂價(可修)')
    status = ""
    
    if pd.isna(custom_price) or custom_price == "":
        return status
        
    try:
        price = float(custom_price)
        points = row.get('_points', [])
        limit_up = row.get('當日漲停價')
        limit_down = row.get('當日跌停價')
        
        if pd.notna(limit_up) and abs(price - limit_up) < 0.01:
            status = "🔴 漲停"
        elif pd.notna(limit_down) and abs(price - limit_down) < 0.01:
            status = "🟢 跌停"
        else:
            if isinstance(points, list):
                for p in points:
                    if abs(p['val'] - price) < 0.01:
                        status = "🟡 命中"
                        break
        return status
    except:
        return status

# [核心修改] 更新後的資料抓取與戰略備註計算邏輯
def fetch_stock_data_raw(code, name_hint="", extra_data=None):
    code = str(code).strip()
    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist = ticker.history(period="3mo") 
        if hist.empty:
            ticker = yf.Ticker(f"{code}.TWO")
            hist = ticker.history(period="3mo")
        if hist.empty: 
            return None

        # 基礎資料：取最後一筆作為今日，倒數第二筆作為昨日 (若有)
        today = hist.iloc[-1]
        current_price = float(today['Close']) # 當日盤後收盤價
        
        if len(hist) >= 2:
            prev_day = hist.iloc[-2]
            prev_close = float(prev_day['Close'])
        else:
            prev_day = today
            prev_close = current_price
            
        if pd.isna(current_price): return None

        pct_change = ((current_price - prev_close) / prev_close) * 100
        
        # 1. 計算明日漲跌停 (基於當日收盤價)
        next_limit_up, next_limit_down = calculate_limits(current_price)
        
        # 當日實際漲跌停 (用於判斷是否顯示漲跌停標籤)
        limit_up_today, limit_down_today = calculate_limits(prev_close)
        touched_limit_up = today['High'] >= limit_up_today - 0.01
        touched_limit_down = today['Low'] <= limit_down_today + 0.01

        # 2. 5MA 邏輯
        ma5_val = apply_tick_rules(hist['Close'].tail(5).mean())
        if ma5_val > current_price: ma_tag = "空"
        elif ma5_val < current_price: ma_tag = "多"
        else: ma_tag = "平"

        # 3. 技術點位
        # 5日內高低點 (不含當日)
        past_5 = hist.iloc[-6:-1] if len(hist) > 1 else pd.DataFrame()
        recent_high = apply_tick_rules(past_5['High'].max()) if not past_5.empty else apply_tick_rules(prev_day['High'])
        recent_low = apply_tick_rules(past_5['Low'].min()) if not past_5.empty else apply_tick_rules(prev_day['Low'])
        
        yesterday_high = apply_tick_rules(prev_day['High'])
        yesterday_low = apply_tick_rules(prev_day['Low'])
        
        today_open = apply_tick_rules(today['Open'])
        today_high = apply_tick_rules(today['High'])
        today_low = apply_tick_rules(today['Low'])
        
        # 4. 目標價
        target_3pct = apply_tick_rules(current_price * 1.03)
        stop_3pct = apply_tick_rules(current_price * 0.97)

        # 收集所有點位
        candidates = []
        
        # 輔助函式：加入點位並檢查範圍
        def add_point(val, tag, priority=1):
            # 檢查是否在明日可觸及範圍 (漲跌停內)
            if val > next_limit_up + 0.01: return
            if val < next_limit_down - 0.01: return
            candidates.append({"val": val, "tag": tag, "p": priority})

        # (2) 5MA (皆需顯示)
        add_point(ma5_val, ma_tag, 10)
        
        # (5) 補充資料點位
        add_point(today_open, "")
        add_point(today_high, "")
        add_point(today_low, "")
        add_point(yesterday_high, "")
        add_point(yesterday_low, "")
        
        # 近期高低點 + (3) 特殊標籤邏輯
        r_high_tag = ""
        r_low_tag = ""
        
        # 漲停標示邏輯：若明日漲停 == 近期高點 -> 漲停高
        if abs(next_limit_up - recent_high) < 0.01:
            r_high_tag = "漲停高"
        # 跌停標示邏輯：若明日跌停 == 近期低點 -> 跌停低
        if abs(next_limit_down - recent_low) < 0.01:
            r_low_tag = "跌停低"
            
        add_point(recent_high, r_high_tag)
        add_point(recent_low, r_low_tag)
        
        # (3) 漲跌停價顯示規則：若當日無漲跌停則不用呈現 (除非它是近期高低點，已被上面涵蓋)
        # 若當日有觸碰到漲跌停，則顯示明日的漲跌停價作為參考
        if touched_limit_up:
            add_point(next_limit_up, "漲停", 5)
        if touched_limit_down:
            add_point(next_limit_down, "跌停", 5)
            
        # (5) +/- 3% 顯示規則
        # 若近期最高點小於 +3% 則顯示 +3%
        if target_3pct > recent_high:
            add_point(target_3pct, "")
        # 若近期最低點大於 -3% 則顯示 -3%
        if stop_3pct < recent_low:
            add_point(stop_3pct, "")

        # 整理與格式化
        candidates.sort(key=lambda x: x['val'])
        
        unique_points = []
        seen_vals = set()
        
        # 去重與合併標籤邏輯
        for c in candidates:
            v_rounded = round(c['val'], 2)
            if v_rounded in seen_vals:
                # 若數值重複，保留有標籤的那個，或優先權高的
                for exist in unique_points:
                    if round(exist['val'], 2) == v_rounded:
                        if c['tag'] and not exist['tag']:
                            exist['tag'] = c['tag']
                        elif c['tag'] and exist['tag'] and c['p'] > exist['p']:
                             exist['tag'] = c['tag'] # 5MA標籤優先
                        break
                continue
            seen_vals.add(v_rounded)
            unique_points.append(c)
            
        note_parts = []
        full_calc_points = []
        
        for p in unique_points:
            val = p['val']
            tag = p['tag']
            
            # 存入原始資料供計算用
            full_calc_points.append({"val": val, "tag": tag})
            
            # 產生顯示字串
            v_str = f"{val:.0f}" if val.is_integer() else f"{val:.2f}"
            
            # 格式：漲停/跌停/漲停高/跌停低 放前面，多/空/平 放後面
            if tag in ["漲停", "漲停高", "跌停", "跌停低"]:
                item = f"{tag}{v_str}"
            elif tag in ["多", "空", "平"]:
                item = f"{v_str}{tag}"
            else:
                item = v_str
            note_parts.append(item)
        
        strategy_note = "-".join(note_parts)
        final_name = name_hint if name_hint else get_stock_name_online(code)
        
        # 燈號邏輯
        light = "⚪"
        if "多" in strategy_note: light = "🔴"
        elif "空" in strategy_note: light = "🟢"
        final_name_display = f"{light} {final_name}"
        
        return {
            "代號": code,
            "名稱": final_name_display, 
            "收盤價": round(current_price, 2),
            "漲跌幅": pct_change, 
            "當日漲停價": next_limit_up,   # 顯示為明日的漲停價
            "當日跌停價": next_limit_down, # 顯示為明日的跌停價
            "自訂價(可修)": None, 
            "獲利目標": target_3pct, 
            "防守停損": stop_3pct,   
            "戰略備註": strategy_note,
            "_points": full_calc_points,
            "狀態": ""
        }
    except Exception as e:
        return None

# ==========================================
# 主介面 (Tabs)
# ==========================================

tab1, tab2 = st.tabs(["⚡ 當沖戰略室 ⚡", "💰 當沖損益試算 💰"])

# -------------------------------------------------------
# Tab 1: 當沖戰略室
# -------------------------------------------------------
with tab1:
    col_search, col_file = st.columns([2, 1])
    with col_search:
        search_query = st.text_input("🔍 快速查詢 (中文/代號)", placeholder="鴻海, 2603, 緯創")
    with col_file:
        uploaded_file = st.file_uploader("📂 上傳清單", type=['xlsx', 'csv'])
        selected_sheet = None
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'):
                    xl = None 
                    df_up = pd.read_csv(uploaded_file, dtype=str)
                else:
                    import importlib.util
                    if importlib.util.find_spec("openpyxl") is None:
                        st.error("❌ 缺少 openpyxl。")
                        xl = None
                    else: xl = pd.ExcelFile(uploaded_file) 
            except Exception as e: st.error(f"❌ 讀取失敗: {e}")

            if xl:
                default_idx = 0
                if "週轉率" in xl.sheet_names: default_idx = xl.sheet_names.index("週轉率")
                selected_sheet = st.selectbox("工作表", xl.sheet_names, index=default_idx)

    if st.button("🚀 執行分析", type="primary"):
        targets = []
        
        if uploaded_file:
            uploaded_file.seek(0) 
            try:
                if uploaded_file.name.endswith('.csv'): df_up = pd.read_csv(uploaded_file, dtype=str)
                else: 
                    if 'xl' in locals() and xl: df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet, dtype=str)
                    else: df_up = pd.DataFrame()
                
                if not df_up.empty:
                    c_col = next((c for c in df_up.columns if "代號" in c), None)
                    n_col = next((c for c in df_up.columns if "名稱" in c), None)
                    if c_col:
                        for _, row in df_up.iterrows():
                            c_raw = str(row[c_col]).split('.')[0].strip()
                            if not c_raw or c_raw.lower() == 'nan': continue
                            if len(c_raw) > 10 or any('\u4e00' <= char <= '\u9fff' for char in c_raw): continue
                            
                            # ETF 補零邏輯
                            if c_raw.isdigit():
                                if len(c_raw) <= 3: c_raw = "00" + c_raw
                            elif len(c_raw) == 4 and c_raw[0].isdigit() and c_raw[-1].isalpha():
                                c_raw = "00" + c_raw

                            n = str(row[n_col]) if n_col else ""
                            if n.lower() == 'nan': n = ""
                            targets.append((c_raw, n, 'upload', {}))
            except Exception as e: st.error(f"讀取失敗: {e}")

        if search_query:
            inputs = [x.strip() for x in search_query.replace('，',',').split(',') if x.strip()]
            for inp in inputs:
                if inp.isdigit(): targets.append((inp, "", 'search', {}))
                else:
                    with st.spinner(f"搜尋「{inp}」..."):
                        code = search_code_online(inp)
                    if code: targets.append((code, inp, 'search', {}))
                    else: st.toast(f"找不到「{inp}」", icon="⚠️")

        results = []
        seen = set()
        bar = st.progress(0)
        total = len(targets)
        
        existing_data = {}
        if not st.session_state.stock_data.empty:
            for idx, row in st.session_state.stock_data.iterrows():
                existing_data[row['代號']] = row.to_dict()

        fetch_cache = {}
        for i, (code, name, source, extra) in enumerate(targets):
            if code in st.session_state.ignored_stocks: continue
            if (code, source) in seen: continue
            
            if hide_non_stock:
                if code.startswith("00"): continue
                if len(code) > 4 and code.isdigit(): continue
            
            if code in fetch_cache: data = fetch_cache[code]
            else:
                data = fetch_stock_data_raw(code, name, extra)
                if data: fetch_cache[code] = data
            
            if data:
                data['_source'] = source
                existing_data[code] = data
                seen.add((code, source))
                
            if total > 0: bar.progress((i+1)/total)
        bar.empty()
        
        if existing_data:
            st.session_state.stock_data = pd.DataFrame(list(existing_data.values()))
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks)

    if not st.session_state.stock_data.empty:
        limit = st.session_state.limit_rows
        df_all = st.session_state.stock_data.copy()
        
        rename_map = {"漲停價": "當日漲停價", "跌停價": "當日跌停價"}
        df_all = df_all.rename(columns=rename_map)
        
        df_all['代號'] = df_all['代號'].astype(str)
        df_all = df_all[~df_all['代號'].isin(st.session_state.ignored_stocks)]
        
        if hide_non_stock:
             mask_etf = df_all['代號'].str.startswith('00')
             mask_warrant = (df_all['代號'].str.len() > 4) & df_all['代號'].str.isdigit()
             df_all = df_all[~(mask_etf | mask_warrant)]
        
        if '_source' in df_all.columns:
            df_up = df_all[df_all['_source'] == 'upload'].head(limit)
            df_se = df_all[df_all['_source'] == 'search']
            df_display = pd.concat([df_up, df_se]).reset_index(drop=True)
        else:
            df_display = df_all.head(limit).reset_index(drop=True)
        
        note_width_px = calculate_note_width(df_display['戰略備註'], current_font_size)

        input_cols = ["代號", "名稱", "戰略備註", "自訂價(可修)", "狀態", "當日漲停價", "當日跌停價", "+3%", "-3%", "收盤價", "漲跌幅", "_points"]
        df_display = df_display.rename(columns={"獲利目標": "+3%", "防守停損": "-3%"})

        for col in input_cols:
            if col not in df_display.columns and col != "_points": df_display[col] = None

        edited_df = st.data_editor(
            df_display[input_cols],
            column_config={
                "代號": st.column_config.TextColumn(disabled=True, width="small"),
                "名稱": st.column_config.TextColumn(disabled=True, width="small"),
                "收盤價": st.column_config.NumberColumn(format="%.2f", disabled=True, width="small"),
                "漲跌幅": st.column_config.NumberColumn(format="%.2f%%", disabled=True, width="small"),
                "自訂價(可修)": st.column_config.NumberColumn("自訂價 ✏️", format="%.2f", step=0.01, width=120),
                "當日漲停價": st.column_config.NumberColumn(format="%.2f", disabled=True, width="small"),
                "當日跌停價": st.column_config.NumberColumn(format="%.2f", disabled=True, width="small"),
                "+3%": st.column_config.NumberColumn(format="%.2f", disabled=True, width="small"),
                "-3%": st.column_config.NumberColumn(format="%.2f", disabled=True, width="small"),
                "狀態": st.column_config.TextColumn(width=80, disabled=True),
                "戰略備註": st.column_config.TextColumn(width=note_width_px, disabled=True),
                "_points": None 
            },
            hide_index=True, 
            use_container_width=False,
            num_rows="dynamic",
            key="main_editor"
        )
        
        col_btn, _ = st.columns([2, 8])
        manual_update = col_btn.button("⚡ 立即更新狀態 (或輸入完最後一列自動更新)", use_container_width=True)
        
        should_update = False
        
        if len(edited_df) < len(df_display):
            original = set(df_display['代號']); new = set(edited_df['代號'])
            removed = original - new
            if removed:
                st.session_state.ignored_stocks.update(removed)
                save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks)
                st.rerun()

        if len(edited_df) > 0:
            last_idx = len(edited_df) - 1
            last_price = edited_df.iloc[last_idx]['自訂價(可修)']
            orig_last_price = df_display.iloc[last_idx]['自訂價(可修)']
            
            def is_diff(v1, v2):
                s1 = str(v1).strip() if pd.notna(v1) else ""
                s2 = str(v2).strip() if pd.notna(v2) else ""
                return s1 != s2
                
            if is_diff(last_price, orig_last_price):
                should_update = True
        
        if manual_update:
            should_update = True
            
        if should_update:
            updated_rows = []
            for idx, row in edited_df.iterrows():
                new_status = recalculate_row(row)
                row['狀態'] = new_status
                updated_rows.append(row)
            
            if updated_rows:
                df_updated = pd.DataFrame(updated_rows)
                update_map = df_updated.set_index('代號')[['自訂價(可修)', '狀態']].to_dict('index')
                
                for i, r in st.session_state.stock_data.iterrows():
                    code = r['代號']
                    if code in update_map:
                        st.session_state.stock_data.at[i, '自訂價(可修)'] = update_map[code]['自訂價(可修)']
                        st.session_state.stock_data.at[i, '狀態'] = update_map[code]['狀態']
                
                st.rerun()

# -------------------------------------------------------
# Tab 2: 當沖損益試算
# -------------------------------------------------------
with tab2:
    st.markdown("#### 💰 當沖損益試算 💰")
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
        diff_str = f"{diff:+.2f}" if diff != 0 else "0.00"
        note_type = ""
        if abs(p - limit_up) < 0.001: note_type = "up"
        elif abs(p - limit_down) < 0.001: note_type = "down"
        calc_data.append({"成交價": f"{p:.2f}", "漲跌": diff_str, "預估損益": int(profit), "報酬率%": f"{roi:+.2f}%", "手續費": int(total_fee), "交易稅": int(tax), "_profit": profit, "_note_type": note_type})
        
    df_calc = pd.DataFrame(calc_data)
    def style_calc_row(row):
        nt = row['_note_type']
        if nt == 'up': return ['background-color: #ff4b4b; color: white; font-weight: bold'] * len(row)
        elif nt == 'down': return ['background-color: #00cc00; color: white; font-weight: bold'] * len(row)
        prof = row['_profit']
        if prof > 0: return ['color: #ff4b4b; font-weight: bold'] * len(row) 
        elif prof < 0: return ['color: #00cc00; font-weight: bold'] * len(row) 
        else: return ['color: gray'] * len(row)

    if not df_calc.empty:
        st.dataframe(df_calc.style.apply(style_calc_row, axis=1), use_container_width=False, hide_index=True, column_config={"_profit": None, "_note_type": None})

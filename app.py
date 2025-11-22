import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import math
import time
import os
import itertools

# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="當沖戰略室 V8 (網路版)", page_icon="⚡", layout="wide")

# --- 初始化 Session State ---
if 'stock_data' not in st.session_state:
    st.session_state.stock_data = pd.DataFrame()

# 記憶設定：確保在 rerun 時保留數值
if 'font_size' not in st.session_state:
    st.session_state.font_size = 18
if 'limit_rows' not in st.session_state:
    st.session_state.limit_rows = 5

# --- 側邊欄設定 ---
with st.sidebar:
    st.header("⚙️ 設定")
    
    # 使用 key 自動綁定 session_state
    st.slider(
        "字體大小 (表格)", 
        min_value=12, 
        max_value=72, 
        key='font_size'
    )
    
    hide_etf = st.checkbox("隱藏 ETF (00開頭)", value=True)
    st.markdown("---")
    
    st.number_input(
        "顯示筆數", 
        min_value=1, 
        key='limit_rows'
    )
    
    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n勾選左側框框後按 `Delete` 鍵。")

# --- 動態 CSS ---
font_px = f"{st.session_state.font_size}px"

st.markdown(f"""
    <style>
    .block-container {{ padding-top: 0.5rem; padding-bottom: 1rem; }}
    
    div[data-testid="stDataFrame"] table,
    div[data-testid="stDataFrame"] td,
    div[data-testid="stDataFrame"] th,
    div[data-testid="stDataFrame"] input,
    div[data-testid="stDataFrame"] div {{
        font-size: {font_px} !important;
        font-family: 'Microsoft JhengHei', sans-serif !important;
        line-height: 1.5 !important;
    }}
    
    div[data-testid="stDataFrame"] {{
        width: 100%;
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
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def calculate_limits(price):
    """
    計算漲跌停價 (10%)
    """
    try:
        p = float(price)
        
        # 1. 漲停價 (無條件捨去至最近 Tick)
        raw_up = p * 1.10
        tick_up = get_tick_size(raw_up) 
        limit_up = math.floor(raw_up / tick_up) * tick_up
        
        # 2. 跌停價 (無條件進位至最近 Tick)
        raw_down = p * 0.90
        tick_down = get_tick_size(raw_down) 
        limit_down = math.ceil(raw_down / tick_down) * tick_down
        
        return float(f"{limit_up:.2f}"), float(f"{limit_down:.2f}")
    except:
        return 0, 0

def apply_tick_rules(price):
    """
    將任意價格修正為符合台股 Tick 規則的價格
    """
    try:
        p = float(price)
        tick = get_tick_size(p)
        rounded_price = round(p / tick) * tick
        return float(f"{rounded_price:.2f}")
    except:
        return price

def fetch_stock_data_raw(code, name_hint=""):
    code = str(code).strip()
    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist = ticker.history(period="3mo") 
        if hist.empty:
            ticker = yf.Ticker(f"{code}.TWO")
            hist = ticker.history(period="3mo")
        if hist.empty: return None

        today = hist.iloc[-1]
        current_price = today['Close']
        prev_day = hist.iloc[-2] if len(hist) >= 2 else today
        
        # 1. 欄位顯示用的數據 (以收盤價為基準)
        target_price = apply_tick_rules(current_price * 1.03)
        stop_price = apply_tick_rules(current_price * 0.97)
        limit_up_col, limit_down_col = calculate_limits(current_price) 

        # 2. 戰略備註用的數據 (以昨日收盤為基準，計算今日的漲跌停)
        limit_up_today, limit_down_today = calculate_limits(prev_day['Close'])

        # 點位收集
        points = []
        
        # MA5
        ma5 = apply_tick_rules(hist['Close'].tail(5).mean())
        points.append({"val": ma5, "tag": "多" if current_price > ma5 else "空"})
        
        # 今日開高低
        points.append({"val": apply_tick_rules(today['Open']), "tag": ""})
        points.append({"val": apply_tick_rules(today['High']), "tag": ""})
        points.append({"val": apply_tick_rules(today['Low']), "tag": ""})
        
        # 近期 5日 高低
        past_5 = hist.iloc[-6:-1] if len(hist) >= 6 else hist.iloc[:-1]
        if not past_5.empty:
            points.append({"val": apply_tick_rules(past_5['High'].max()), "tag": ""})
            points.append({"val": apply_tick_rules(past_5['Low'].min()), "tag": ""})
            
        # 90日 高低
        high_90 = apply_tick_rules(hist['High'].max())
        low_90 = apply_tick_rules(hist['Low'].min())
        points.append({"val": high_90, "tag": "高"})
        points.append({"val": low_90, "tag": "低"})

        # 戰略備註整理
        display_candidates = []
        for p in points:
            v = float(f"{p['val']:.2f}")
            # 只顯示在今日漲跌停範圍內的點位 (排除異常值)
            if limit_down_today <= v <= limit_up_today:
                display_candidates.append({"val": v, "tag": p['tag']})
        
        # 加入今日的漲跌停價，以便排序比對
        display_candidates.append({"val": limit_up_today, "tag": "漲停"})
        display_candidates.append({"val": limit_down_today, "tag": "跌停"})
            
        display_candidates.sort(key=lambda x: x['val'])
        
        final_display_points = []
        extra_points = [] # 用於存儲延伸計算的點 (漲停高+3%)

        for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
            g_list = list(group)
            tags = [x['tag'] for x in g_list]
            
            final_tag = ""
            is_limit_up = "漲停" in tags
            is_limit_down = "跌停" in tags
            
            # 判斷是否為 90日 高低
            is_high = "高" in tags
            is_low = "低" in tags
            
            # 判斷是否為今日收盤價 (誤差 0.01)
            is_close_price = abs(val - current_price) < 0.01
            
            # --- 關鍵修正：漲停高/跌停低 + 延伸計算 ---
            # 規則：必須是漲停價 AND 近期高點 AND 是今日收盤價
            if is_limit_up:
                if is_high and is_close_price: 
                    final_tag = "漲停高"
                    # 延伸計算：漲停價 * 1.03
                    ext_val = apply_tick_rules(val * 1.03)
                    extra_points.append({"val": ext_val, "tag": ""})
                else:
                    final_tag = "漲停"
                    
            elif is_limit_down:
                if is_low and is_close_price:
                    final_tag = "跌停低"
                    # 延伸計算：跌停價 * 0.97
                    ext_val = apply_tick_rules(val * 0.97)
                    extra_points.append({"val": ext_val, "tag": ""})
                else:
                    final_tag = "跌停"
            else:
                # 一般點位標籤
                if is_high: final_tag = "高"
                elif is_low: final_tag = "低"
                elif "多" in tags: final_tag = "多"
                elif "空" in tags: final_tag = "空"
                else: final_tag = ""

            final_display_points.append({"val": val, "tag": final_tag})
        
        # 將延伸點位加入列表
        if extra_points:
            for ep in extra_points:
                final_display_points.append(ep)
            final_display_points.sort(key=lambda x: x['val'])
            
        note_parts = []
        seen_vals = set() 
        
        for p in final_display_points:
            # 去重
            if p['val'] in seen_vals and p['tag'] == "": 
                continue
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

        # 計算用的完整點位 (用於命中檢查)
        calc_points = points.copy()
        calc_points.append({"val": limit_up_today, "tag": "漲停"})
        calc_points.append({"val": limit_down_today, "tag": "跌停"})
        for ep in extra_points:
            calc_points.append(ep)
        
        full_calc_points = []
        seen_calc = set()
        for p in calc_points:
             v = float(f"{p['val']:.2f}")
             if v not in seen_calc:
                 full_calc_points.append({"val": v, "tag": p['tag']})
                 seen_calc.add(v)
        full_calc_points.sort(key=lambda x: x['val'])

        final_name = name_hint if name_hint else get_stock_name_online(code)
        pct_change = (current_price - prev_day['Close']) / prev_day['Close'] * 100

        return {
            "代號": code,
            "名稱": final_name,
            "收盤價": round(current_price, 2),
            "自訂價(可修)": None, 
            "漲跌幅": pct_change,
            "漲停價": limit_up_col,   # 表格顯示 (收盤價基準)
            "跌停價": limit_down_col, # 表格顯示 (收盤價基準)
            "獲利目標": target_price, 
            "防守停損": stop_price,   
            "戰略備註": strategy_note,
            "_points": full_calc_points
        }
    except Exception as e:
        return None

# ==========================================
# 3. 介面與互動
# ==========================================

st.title("⚡ 當沖戰略室 V8 (網路版)")

col_search, col_file = st.columns([2, 1])
with col_search:
    search_query = st.text_input("🔍 快速查詢 (中文/代號)", placeholder="鴻海, 2603, 緯創")
with col_file:
    uploaded_file = st.file_uploader("📂 上傳清單", type=['xlsx', 'csv'])
    selected_sheet = None
    if uploaded_file and not uploaded_file.name.endswith('.csv'):
        xl = pd.ExcelFile(uploaded_file)
        default_idx = 0
        if "週轉率" in xl.sheet_names: default_idx = xl.sheet_names.index("週轉率")
        selected_sheet = st.selectbox("工作表", xl.sheet_names, index=default_idx)

if st.button("🚀 執行分析", type="primary"):
    targets = []
    if search_query:
        inputs = [x.strip() for x in search_query.replace('，',',').split(',') if x.strip()]
        for inp in inputs:
            if inp.isdigit(): targets.append((inp, ""))
            else:
                with st.spinner(f"搜尋「{inp}」..."):
                    code = search_code_online(inp)
                if code: targets.append((code, inp))
                else: st.toast(f"找不到「{inp}」", icon="⚠️")

    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'): df_up = pd.read_csv(uploaded_file)
            else: df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
            c_col = next((c for c in df_up.columns if "代號" in c), None)
            n_col = next((c for c in df_up.columns if "名稱" in c), None)
            if c_col:
                for _, row in df_up.iterrows():
                    c = str(row[c_col]).split('.')[0]
                    n = str(row[n_col]) if n_col else ""
                    if c.isdigit(): targets.append((c, n))
        except Exception as e:
            st.error(f"讀取失敗: {e}")

    results = []
    seen = set()
    bar = st.progress(0)
    total = len(targets)
    for i, (code, name) in enumerate(targets):
        if code in seen: continue
        if hide_etf and code.startswith("00"): continue
        data = fetch_stock_data_raw(code, name)
        if data:
            results.append(data)
            seen.add(code)
        if total > 0: bar.progress((i+1)/total)
    bar.empty()
    if results:
        st.session_state.stock_data = pd.DataFrame(results)
    else:
        st.warning("無資料")

# ==========================================
# 4. 表格顯示與計算
# ==========================================

if not st.session_state.stock_data.empty:
    
    limit = st.session_state.limit_rows
    df_display = st.session_state.stock_data.head(limit).copy()
    
    # 1. 輸入區
    input_cols = ["代號", "名稱", "收盤價", "自訂價(可修)", "漲跌幅", "獲利目標", "防守停損", "漲停價", "跌停價", "戰略備註", "_points"]
    
    edited_df = st.data_editor(
        df_display[input_cols],
        column_config={
            "代號": st.column_config.TextColumn(disabled=True, width="small"),
            "名稱": st.column_config.TextColumn(disabled=True, width="medium"),
            "收盤價": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "自訂價(可修)": st.column_config.NumberColumn(
                "自訂價 ✏️",
                help="輸入後查看命中結果",
                format="%.2f",
                step=0.01,
                required=False,
                width="medium" 
            ),
            "漲跌幅": st.column_config.NumberColumn("漲跌%", format="%.2f%%", disabled=True),
            "獲利目標": st.column_config.NumberColumn("獲利(+3%)", format="%.2f", disabled=True),
            "防守停損": st.column_config.NumberColumn("停損(-3%)", format="%.2f", disabled=True),
            "漲停價": st.column_config.NumberColumn("🔥漲停", format="%.2f", disabled=True),
            "跌停價": st.column_config.NumberColumn("💚跌停", format="%.2f", disabled=True),
            "戰略備註": st.column_config.TextColumn(width="large", disabled=True),
            "_points": None 
        },
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic",
        key="main_editor"
    )
    
    # 2. 結果計算 (只做命中檢查)
    results = []
    for idx, row in edited_df.iterrows():
        custom_price = row['自訂價(可修)']
        is_hit = False 

        if not (pd.isna(custom_price) or custom_price == ""):
            price = float(custom_price)
            points = row['_points']
            
            # 命中判斷 (誤差0.01內)
            for p in points:
                if abs(p['val'] - price) < 0.01:
                    is_hit = True
                    break
        
        results.append({
            "_is_hit": is_hit
        })
    
    res_df_calced = pd.DataFrame(results, index=edited_df.index)
    final_df = pd.concat([edited_df, res_df_calced], axis=1)

    # --- 下方表格：結果區 ---
    st.markdown("### 🎯 計算結果 (命中亮色提示)")
    
    mask = final_df['自訂價(可修)'].notna() & (final_df['自訂價(可修)'] != "")
    
    if mask.any():
        display_cols = ["代號", "名稱", "自訂價(可修)", "漲跌幅", "獲利目標", "防守停損", "戰略備註", "_is_hit"]
        display_df = final_df[mask][display_cols]
        
        def highlight_hit_row(row):
            if row['_is_hit']:
                return ['background-color: #fff9c4; color: black; font-weight: bold;'] * len(row)
            return [''] * len(row)

        def color_change(val):
            if isinstance(val, (float, int)):
                if val > 0: return 'color: #ff4b4b'
                if val < 0: return 'color: #00cc00'
            return ''

        st.dataframe(
            display_df.style.apply(highlight_hit_row, axis=1).applymap(color_change, subset=['漲跌幅']),
            use_container_width=True,
            hide_index=True,
            column_config={
                "自訂價(可修)": st.column_config.NumberColumn("自訂價", format="%.2f"),
                "漲跌幅": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
                "獲利目標": st.column_config.NumberColumn("獲利(+3%)", format="%.2f"),
                "防守停損": st.column_config.NumberColumn("停損(-3%)", format="%.2f"),
                "_is_hit": None 
            }
        )
    else:
        st.info("請在上方表格輸入「自訂價」以進行戰略點位比對。")

elif not uploaded_file and not search_query:
    st.info("請輸入代號/中文名稱或上傳檔案。")

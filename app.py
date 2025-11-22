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

# 記憶設定：字體大小 (預設 18)
if 'font_size' not in st.session_state:
    st.session_state.font_size = 18

# 記憶設定：顯示筆數 (預設 5)
if 'limit_rows' not in st.session_state:
    st.session_state.limit_rows = 5

# --- 側邊欄設定 ---
with st.sidebar:
    st.header("⚙️ 設定")
    
    # 字體大小 (key='font_size')
    st.slider(
        "字體大小 (表格)", 
        min_value=12, 
        max_value=72, 
        key='font_size' 
    )
    
    hide_etf = st.checkbox("隱藏 ETF (00開頭)", value=True)
    st.markdown("---")
    
    # 顯示筆數 (key='limit_rows')
    st.number_input(
        "顯示筆數", 
        min_value=1, 
        key='limit_rows'
    )
    
    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n勾選左側框框後按 `Delete` 鍵。")

# --- 動態 CSS (字體與穩定性優化) ---
# 使用 zoom 與針對性的 font-size 來確保放大效果顯著
zoom_level = st.session_state.font_size / 14.0 if st.session_state.font_size > 14 else 1.0
css_font_size = f"{st.session_state.font_size}px"

st.markdown(f"""
    <style>
    .block-container {{ padding-top: 0.5rem; padding-bottom: 1rem; }}
    
    /* 強制調整表格內所有文字的大小 */
    div[data-testid="stDataFrame"] div, 
    div[data-testid="stDataFrame"] span, 
    div[data-testid="stDataFrame"] input,
    div[data-testid="stDataFrame"] p {{ 
        font-size: {css_font_size} !important; 
        font-family: 'Microsoft JhengHei', sans-serif !important;
        line-height: 1.5 !important;
    }}
    
    /* 命中標籤樣式 */
    .hit-tag {{ background-color: #ffff00; color: black; padding: 2px 6px; border-radius: 4px; font-weight: bold; }}
    
    /* 表格容器穩定性與視覺放大 */
    div[data-testid="stDataFrame"] {{
        min-height: 400px; 
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
# 2. 核心計算邏輯
# ==========================================

def get_tick_size(price):
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def calculate_limits(price):
    try:
        p = float(price)
        tick = get_tick_size(p)
        limit_up = math.floor((p * 1.10) / tick) * tick
        limit_down = math.ceil((p * 0.90) / tick) * tick
        return limit_up, limit_down
    except:
        return 0, 0

def fetch_stock_data_raw(code, name_hint=""):
    code = str(code).strip()
    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist = ticker.history(period="10d")
        if hist.empty:
            ticker = yf.Ticker(f"{code}.TWO")
            hist = ticker.history(period="10d")
        if hist.empty: return None

        # 1. 基礎數據
        today = hist.iloc[-1]
        current_price = today['Close']
        prev_day = hist.iloc[-2] if len(hist) >= 2 else today
        
        # 今日漲跌停
        limit_up, limit_down = calculate_limits(prev_day['Close'])

        # 2. 戰略點位收集
        points = []
        ma5 = hist['Close'].tail(5).mean()
        points.append({"val": ma5, "tag": "多" if current_price > ma5 else "空"})
        points.append({"val": today['Open'], "tag": ""})
        points.append({"val": today['High'], "tag": ""})
        points.append({"val": today['Low'], "tag": ""})
        
        past_5 = hist.iloc[-6:-1] if len(hist) >= 6 else hist.iloc[:-1]
        if not past_5.empty:
            points.append({"val": past_5['High'].max(), "tag": "高"})
            points.append({"val": past_5['Low'].min(), "tag": ""})

        # --- 戰略備註邏輯 (優化版: 去除多餘的高/低字樣) ---
        display_candidates = []
        
        for p in points:
            v = float(f"{p['val']:.2f}")
            if limit_down <= v <= limit_up:
                display_candidates.append({"val": v, "tag": p['tag']})
        
        # 檢查是否觸及漲跌停
        touched_up = today['High'] >= limit_up - 0.01
        touched_down = today['Low'] <= limit_down + 0.01
        
        if touched_up:
            display_candidates.append({"val": limit_up, "tag": "漲停"})
        if touched_down:
            display_candidates.append({"val": limit_down, "tag": "跌停"})
            
        display_candidates.sort(key=lambda x: x['val'])
        
        final_display_points = []
        for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
            g_list = list(group)
            tags = [x['tag'] for x in g_list]
            
            final_tag = ""
            is_limit_up = "漲停" in tags
            is_limit_down = "跌停" in tags
            is_high = "高" in tags
            is_low = "低" in tags
            
            if is_limit_up:
                final_tag = "漲停高" if is_high else "漲停"
            elif is_limit_down:
                final_tag = "跌停低" if is_low else "跌停"
            else:
                # 若不是漲跌停，但有高/低標籤，則去除標籤文字 (只顯示數值)
                # 保留 "多" / "空"
                if "多" in tags: final_tag = "多"
                elif "空" in tags: final_tag = "空"
                else: final_tag = "" # 移除 "高", "低" 的文字顯示

            final_display_points.append({"val": val, "tag": final_tag})
            
        note_parts = []
        for p in final_display_points:
            v_str = f"{p['val']:.0f}" if p['val'].is_integer() else f"{p['val']:.2f}"
            t = p['tag']
            
            if t in ["漲停", "漲停高", "跌停", "跌停低"]:
                item = f"{t}{v_str}"
            elif t: 
                item = f"{v_str}{t}"
            else: 
                item = v_str
            note_parts.append(item)
        
        strategy_note = "-".join(note_parts)

        # 計算用的完整點位
        calc_points = points.copy()
        calc_points.append({"val": limit_up, "tag": "漲停"})
        calc_points.append({"val": limit_down, "tag": "跌停"})
        
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
            "漲停價": limit_up,
            "跌停價": limit_down,
            "獲利目標": None,
            "防守停損": None,
            "戰略備註": strategy_note,
            "命中狀態": "",
            "_points": full_calc_points,
            "_limit_up": limit_up,
            "_limit_down": limit_down
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

# --- 執行按鈕 ---
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
# 4. 表格顯示與即時計算
# ==========================================

if not st.session_state.stock_data.empty:
    
    limit = st.session_state.limit_rows
    # 使用原始資料的副本進行顯示，避免編輯器直接修改原始資料導致迴圈
    df_display = st.session_state.stock_data.head(limit).copy()
    
    # 1. 上方表格：輸入區 (Input)
    # 注意：這裡我們不再於代碼中強制 update session_state，解決跳動問題
    edited_df = st.data_editor(
        df_display,
        column_config={
            "代號": st.column_config.TextColumn(disabled=True, width="small"),
            "名稱": st.column_config.TextColumn(disabled=True, width="medium"),
            "收盤價": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "自訂價(可修)": st.column_config.NumberColumn(
                "自訂價 ✏️",
                help="輸入價格計算 (Enter不跳動)",
                format="%.2f",
                step=0.1,
                required=False,
                width="medium" 
            ),
            "漲跌幅": st.column_config.NumberColumn("漲跌%", format="%.2f%%", disabled=True),
            "漲停價": st.column_config.NumberColumn("🔥漲停", format="%.2f", disabled=True),
            "跌停價": st.column_config.NumberColumn("💚跌停", format="%.2f", disabled=True),
            # 以下欄位在上表僅供顯示結構，計算結果會在下表呈現
            "獲利目標": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "防守停損": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "戰略備註": st.column_config.TextColumn(width="large", disabled=True),
            "命中狀態": st.column_config.TextColumn(width="small", disabled=True),
            "_points": None, "_limit_up": None, "_limit_down": None
        },
        column_order=["代號", "名稱", "收盤價", "自訂價(可修)", "漲跌幅", "漲停價", "跌停價", "戰略備註"], # 簡化上表顯示
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic",
        key="main_editor"
    )
    
    # --- 計算邏輯 (只針對 edited_df 計算，不寫回 source) ---
    updates = []
    
    for idx, row in edited_df.iterrows():
        custom_price = row['自訂價(可修)']
        
        # 預設值
        target = None
        stop = None
        hit_msg = ""

        if not (pd.isna(custom_price) or custom_price == ""):
            price = float(custom_price)
            points = row['_points'] # 從隱藏欄位取得點位資料
            
            # 獲利目標
            for p in points:
                if p['val'] > price:
                    target = p['val']
                    break
            if target is None:
                target = price * 1.03
                
            # 防守停損
            for p in reversed(points):
                if p['val'] < price:
                    stop = p['val']
                    break
            if stop is None:
                stop = price * 0.97
                
            # 命中檢查
            for p in points:
                if abs(p['val'] - price) < 0.05:
                    t = p['tag'] if p['tag'] else "點"
                    hit_msg = f"⚡{p['val']}({t})"
                    break
        
        updates.append({
            "獲利目標": target,
            "防守停損": stop,
            "命中狀態": hit_msg
        })
    
    # 將計算結果合併到 edited_df 用於下方顯示 (不寫回 session_state 以免跳動)
    df_calced = pd.DataFrame(updates, index=edited_df.index)
    result_df = pd.concat([edited_df, df_calced], axis=1)

    # --- 下方表格：結果區 (Result) ---
    def color_change(val):
        if isinstance(val, (float, int)):
            if val > 0: return 'color: #ff4b4b'
            if val < 0: return 'color: #00cc00'
        return ''

    def highlight_hit(s):
        return ['background-color: #ffffcc; color: black' if '⚡' in str(s['命中狀態']) else '' for _ in s]

    st.markdown("### 🎯 計算結果 (即時更新)")
    
    # 只顯示有輸入自訂價的列
    mask = result_df['自訂價(可修)'].notna() & (result_df['自訂價(可修)'] != "")
    
    if mask.any():
        res_display = result_df[mask][["代號", "名稱", "自訂價(可修)", "漲跌幅", "獲利目標", "防守停損", "命中狀態", "戰略備註"]]
        
        st.dataframe(
            res_display.style.applymap(color_change, subset=['漲跌幅']).apply(highlight_hit, axis=1),
            use_container_width=True,
            hide_index=True,
            column_config={
                "自訂價(可修)": st.column_config.NumberColumn("自訂價", format="%.2f"),
                "漲跌幅": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
                "獲利目標": st.column_config.NumberColumn(format="%.2f"),
                "防守停損": st.column_config.NumberColumn(format="%.2f"),
            }
        )
    else:
        st.info("請在上方表格輸入「自訂價」以查看計算結果。")

elif not uploaded_file and not search_query:
    st.info("請輸入代號/中文名稱或上傳檔案。")

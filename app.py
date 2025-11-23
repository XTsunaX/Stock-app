import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import math
import time

# --- 1. 頁面與 CSS ---
st.set_page_config(page_title="當沖戰略室 V8 (網路版)", page_icon="⚡", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 0.5rem; padding-bottom: 1rem; padding-left: 1rem; padding-right: 1rem; }
    div[data-testid="stDataFrame"] { font-size: 14px; }
    .hit-tag { background-color: #ffff00; color: black; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 核心功能 A: 網路爬蟲 (自動抓名稱)
# ==========================================

@st.cache_data(ttl=86400) # 快取一天，避免重複爬
def get_stock_name_online(code):
    """
    輸入代號 (2330)，去 Yahoo 抓取中文名稱 (台積電)
    """
    code = str(code).strip()
    if not code.isdigit(): return code # 防呆
    
    try:
        # 嘗試上市
        url = f"https://tw.stock.yahoo.com/quote/{code}.TW"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=2)
        
        # 解析 Title: <title>台積電(2330) - 個股走勢...</title>
        soup = BeautifulSoup(r.text, "html.parser")
        if soup.title:
            title_text = soup.title.string
            # 格式通常是 "台積電(2330)..."
            if "(" in title_text and ")" in title_text:
                name = title_text.split('(')[0].strip()
                return name
        
        # 若上市找不到，嘗試上櫃
        url_two = f"https://tw.stock.yahoo.com/quote/{code}.TWO"
        r_two = requests.get(url_two, headers=headers, timeout=2)
        soup_two = BeautifulSoup(r_two.text, "html.parser")
        if soup_two.title:
            title_text = soup_two.title.string
            if "(" in title_text:
                return title_text.split('(')[0].strip()
                
        return code # 真的抓不到就回傳代號
    except:
        return code

@st.cache_data(ttl=86400)
def search_code_online(query):
    """
    輸入中文 (鴻海)，去 Yahoo 搜尋代號 (2317)
    """
    query = query.strip()
    if query.isdigit(): return query
    
    try:
        url = f"https://tw.stock.yahoo.com/h/kimosearch/search_list.html?keyword={query}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=2)
        soup = BeautifulSoup(r.text, "html.parser")
        
        # 抓取連結中的代號
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            # 尋找類似 /quote/2317.TW 的連結
            if "/quote/" in href and ".TW" in href:
                parts = href.split("/quote/")[1].split(".")
                if parts[0].isdigit():
                    return parts[0]
    except:
        pass
    return None # 找不到

# ==========================================
# 核心邏輯 B: 計算與抓取
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

        # 1. 數據提取
        today = hist.iloc[-1]
        current_price = today['Close']
        
        # 2. 昨日狀態
        prev_day = hist.iloc[-2] if len(hist) >= 2 else today
        prev_prev_close = hist.iloc[-3]['Close'] if len(hist) >= 3 else prev_day['Open']
        p_limit_up, p_limit_down = calculate_limits(prev_prev_close)
        
        yesterday_status = ""
        if prev_day['Close'] >= p_limit_up: yesterday_status = "🔥昨漲停"
        elif prev_day['Close'] <= p_limit_down: yesterday_status = "💚昨跌停"

        # 3. 今日漲跌停 (獨立顯示)
        limit_up, limit_down = calculate_limits(prev_day['Close'])

        # 4. 戰略點位
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
            
        # 計算用的點位 (包含漲跌停，為了計算獲利目標)
        calc_points = points.copy()
        calc_points.append({"val": limit_up, "tag": "漲停"})
        calc_points.append({"val": limit_down, "tag": "跌停"})

        # 過濾與排序 (用於顯示備註 - 不含漲跌停)
        display_points = []
        seen = set()
        for p in points:
            v = float(f"{p['val']:.2f}")
            if limit_down <= v <= limit_up:
                if v not in seen:
                    display_points.append({"val": v, "tag": p['tag']})
                    seen.add(v)
        display_points.sort(key=lambda x: x['val'])
        
        # 生成戰略備註字串
        note_parts = []
        if yesterday_status: note_parts.append(yesterday_status)
        
        for p in display_points:
            v_str = f"{p['val']:.0f}" if p['val'].is_integer() else f"{p['val']:.2f}"
            tag = p['tag']
            if "高" in tag: item = f"高{v_str}"
            elif tag: item = f"{v_str}{tag}"
            else: item = v_str
            note_parts.append(item)
        
        strategy_note = "-".join(note_parts)
        
        # 準備計算用的完整點位 (排序)
        full_calc_points = []
        seen_calc = set()
        for p in calc_points:
             v = float(f"{p['val']:.2f}")
             if v not in seen_calc:
                 full_calc_points.append({"val": v, "tag": p['tag']})
                 seen_calc.add(v)
        full_calc_points.sort(key=lambda x: x['val'])

        # 自動抓取名稱 (如果沒有提供)
        final_name = name_hint
        if not final_name:
            final_name = get_stock_name_online(code)
        
        # 漲跌幅
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
# 介面邏輯
# ==========================================

if 'stock_data' not in st.session_state:
    st.session_state.stock_data = pd.DataFrame()

# --- 側邊欄 ---
with st.sidebar:
    st.header("⚙️ 設定")
    hide_etf = st.checkbox("隱藏 ETF (00開頭)", value=True)
    
    st.markdown("---")
    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n在下方表格左側勾選該列，按下鍵盤 `Delete` 鍵，或點擊表格右上角的垃圾桶圖示。")
    
    limit_rows = st.number_input("顯示筆數", min_value=1, value=50)

st.title("⚡ 當沖戰略室 V8 (網路版)")

# --- 上方輸入區 ---
col_search, col_file = st.columns([2, 1])

with col_search:
    # 修改 placeholder 提示支援中文
    search_query = st.text_input("🔍 快速查詢 (輸入中文名稱或代號，用逗號分隔)", placeholder="鴻海, 2603, 緯創")

with col_file:
    uploaded_file = st.file_uploader("📂 上傳選股清單 (Excel/CSV)", type=['xlsx', 'csv'])
    selected_sheet = None
    if uploaded_file and not uploaded_file.name.endswith('.csv'):
        xl = pd.ExcelFile(uploaded_file)
        default_idx = 0
        if "週轉率" in xl.sheet_names:
            default_idx = xl.sheet_names.index("週轉率")
        selected_sheet = st.selectbox("選擇工作表", xl.sheet_names, index=default_idx)

# --- 按鈕執行 ---
if st.button("🚀 執行分析", type="primary"):
    targets = []
    
    # 1. 處理搜尋 (支援中文)
    if search_query:
        inputs = [x.strip() for x in search_query.replace('，',',').split(',') if x.strip()]
        for inp in inputs:
            if inp.isdigit(): 
                targets.append((inp, ""))
            else:
                # 中文轉代號 (網路爬蟲)
                with st.spinner(f"正在搜尋「{inp}」..."):
                    code = search_code_online(inp)
                if code:
                    targets.append((code, inp))
                else:
                    st.toast(f"網路上找不到「{inp}」的代號。", icon="⚠️")

    # 2. 處理選股清單
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'):
                df_up = pd.read_csv(uploaded_file)
            else:
                df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
            
            c_col = next((c for c in df_up.columns if "代號" in c), None)
            n_col = next((c for c in df_up.columns if "名稱" in c), None)
            
            if c_col:
                for _, row in df_up.iterrows():
                    c = str(row[c_col]).split('.')[0]
                    n = str(row[n_col]) if n_col else ""
                    if c.isdigit():
                        targets.append((c, n))
        except Exception as e:
            st.error(f"檔案讀取失敗: {e}")

    # 3. 批次抓取
    results = []
    seen = set()
    bar = st.progress(0)
    
    total_items = len(targets)
    for i, (code, name) in enumerate(targets):
        if code in seen: continue
        if hide_etf and code.startswith("00"): continue
        
        # 若 name 為空，fetch 內部會自動去網路抓
        data = fetch_stock_data_raw(code, name)
        if data:
            results.append(data)
            seen.add(code)
        
        if total_items > 0:
            bar.progress((i+1)/total_items)
    
    bar.empty()
    
    if results:
        st.session_state.stock_data = pd.DataFrame(results)
    else:
        st.warning("無資料。")

# ==========================================
# 顯示與編輯層
# ==========================================

if not st.session_state.stock_data.empty:
    
    df_display = st.session_state.stock_data.reset_index(drop=True)
    
    # 這裡開啟 num_rows="dynamic"，允許使用者刪除行 (User Point 2)
    edited_df = st.data_editor(
        df_display,
        column_config={
            "代號": st.column_config.TextColumn(disabled=True, width="small"),
            "名稱": st.column_config.TextColumn(disabled=True, width="medium"),
            "收盤價": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "自訂價(可修)": st.column_config.NumberColumn(
                "自訂價 ✏️",
                help="輸入後按 Enter 計算",
                format="%.2f",
                step=0.1,
                required=False
            ),
            "漲跌幅": st.column_config.NumberColumn("漲跌%", format="%.2f%%", disabled=True),
            "漲停價": st.column_config.NumberColumn("🔥漲停", format="%.2f", disabled=True),
            "跌停價": st.column_config.NumberColumn("💚跌停", format="%.2f", disabled=True),
            "獲利目標": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "防守停損": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "戰略備註": st.column_config.TextColumn(width="large", disabled=True),
            "命中狀態": st.column_config.TextColumn(width="small", disabled=True),
            "_points": None, "_limit_up": None, "_limit_down": None
        },
        column_order=["代號", "名稱", "收盤價", "自訂價(可修)", "漲跌幅", "漲停價", "跌停價", "獲利目標", "防守停損", "命中狀態", "戰略備註"],
        hide_index=True,
        use_container_width=False,
        num_rows="dynamic", # 關鍵: 允許刪除行
        key="main_editor" 
    )
    
    # --- 即時運算 ---
    updates = []
    
    for idx, row in edited_df.iterrows():
        custom_price = row['自訂價(可修)']
        
        if pd.isna(custom_price) or custom_price == "":
            updates.append({"獲利目標": None, "防守停損": None, "命中狀態": ""})
            continue
            
        price = float(custom_price)
        points = row['_points']
        limit_up = row['_limit_up']
        limit_down = row['_limit_down']
        
        # 獲利邏輯
        target = None
        for p in points:
            if p['val'] > price:
                target = p['val']
                break
        if target is None:
            target = price * 1.03
            if target > limit_up: target = limit_up
        
        # 防守邏輯
        stop = None
        for p in reversed(points):
            if p['val'] < price:
                stop = p['val']
                break
        if stop is None:
            stop = price * 0.97
            if stop < limit_down: stop = limit_down
        
        # 命中檢查
        hit_msg = ""
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
    
    # 更新顯示
    df_updates = pd.DataFrame(updates, index=edited_df.index)
    edited_df.update(df_updates)
    st.session_state.stock_data = edited_df

    # --- 下方詳細結果 ---
    def color_change(val):
        if isinstance(val, (float, int)):
            if val > 0: return 'color: #ff4b4b'
            if val < 0: return 'color: #00cc00'
        return ''

    def highlight_hit(s):
        return ['background-color: #ffffcc; color: black' if '⚡' in str(s['命中狀態']) else '' for _ in s]

    st.markdown("### 🎯 計算結果")
    
    # 只顯示有輸入的行 (乾淨)
    mask = edited_df['自訂價(可修)'].notna()
    
    if mask.any():
        res_df = edited_df[mask][["代號", "名稱", "自訂價(可修)", "漲跌幅", "獲利目標", "防守停損", "命中狀態", "戰略備註"]]
        
        st.dataframe(
            res_df.style.applymap(color_change, subset=['漲跌幅']).apply(highlight_hit, axis=1),
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
        st.info("請在上方表格輸入「自訂價」以查看計算結果。若有無法當沖的股票，請選取該行並刪除 (Delete)。")

elif not uploaded_file and not search_query:
    st.info("請輸入代號/中文名稱或上傳檔案。")

import streamlit as st
import pandas as pd
import yfinance as yf
import math
import os

# --- 1. 頁面設定 ---
st.set_page_config(page_title="當沖戰略室 V7", page_icon="⚡", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 1rem; padding-bottom: 1rem; padding-left: 1rem; padding-right: 1rem; }
    div[data-testid="stDataFrame"] { font-size: 14px; }
    /* 讓編輯器表頭對齊 */
    div[data-testid="stDataEditor"] table { text-align: center; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 功能模組: 名稱對照 (後端自動讀取)
# ==========================================

@st.cache_data(ttl=3600)
def load_stock_mapping():
    """
    自動讀取同目錄下的 stock_names.csv
    格式預期: 第一欄代號, 第二欄名稱
    """
    mapping = {}
    try:
        # 優先讀取 CSV
        if os.path.exists("stock_names.csv"):
            df = pd.read_csv("stock_names.csv")
            # 清洗欄位 (去除空格)
            df.columns = [c.strip() for c in df.columns]
            # 假設欄位可能是 [代號, 名稱] 或 [股票代號, 股票名稱]
            code_col = df.columns[0]
            name_col = df.columns[1]
            
            for _, row in df.iterrows():
                code = str(row[code_col]).split('.')[0].strip()
                name = str(row[name_col]).strip()
                mapping[code] = name
    except:
        pass
    
    # 內建備援熱門股 (若讀不到檔案時使用)
    fallback = {
        "2330":"台積電", "2317":"鴻海", "2454":"聯發科", "2603":"長榮", 
        "2609":"陽明", "2615":"萬海", "3231":"緯創", "2382":"廣達",
        "2376":"技嘉", "2356":"英業達", "3008":"大立光", "3034":"聯詠",
        "2303":"聯電", "2881":"富邦金", "2882":"國泰金", "6173":"信昌電",
        "8043":"蜜望實", "8358":"金居"
    }
    # 合併 (檔案優先)
    fallback.update(mapping)
    return fallback

# 載入全域對照表
STOCK_MAP = load_stock_mapping()

def get_stock_name(code):
    return STOCK_MAP.get(str(code), code) # 找不到回傳代號

def get_code_by_name(name):
    # 反向搜尋 (名稱 -> 代號)
    for code, stock_name in STOCK_MAP.items():
        if name == stock_name:
            return code
    return None

# ==========================================
# 核心邏輯: 計算與抓取
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
        
        # 2. 昨日狀態 (判斷昨漲跌停)
        prev_day = hist.iloc[-2] if len(hist) >= 2 else today
        prev_prev_close = hist.iloc[-3]['Close'] if len(hist) >= 3 else prev_day['Open']
        p_limit_up, p_limit_down = calculate_limits(prev_prev_close)
        
        yesterday_status = ""
        if prev_day['Close'] >= p_limit_up: yesterday_status = "🔥昨漲停"
        elif prev_day['Close'] <= p_limit_down: yesterday_status = "💚昨跌停"

        # 3. 今日漲跌停
        limit_up, limit_down = calculate_limits(prev_day['Close'])

        # 4. 戰略點位 (近低-5MA-近高)
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
            
        # 計算用的點位 (包含漲跌停，為了計算支撐壓力)
        calc_points = points.copy()
        calc_points.append({"val": limit_up, "tag": "漲停"})
        calc_points.append({"val": limit_down, "tag": "跌停"})

        # 過濾與排序 (用於顯示備註)
        # User Request: 備註內不要合併漲跌停
        display_points = []
        seen = set()
        
        for p in points: # 使用不含漲跌停的 points 列表
            v = float(f"{p['val']:.2f}")
            if limit_down <= v <= limit_up: # 只取區間內的
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
        
        # 為了計算邏輯，我們需要一個完整的點位列表
        full_calc_points = []
        seen_calc = set()
        for p in calc_points:
             v = float(f"{p['val']:.2f}")
             if v not in seen_calc:
                 full_calc_points.append({"val": v, "tag": p['tag']})
                 seen_calc.add(v)
        full_calc_points.sort(key=lambda x: x['val'])

        # 名稱處理
        final_name = name_hint
        if not final_name or final_name == code:
            final_name = get_stock_name(code)
        
        # 漲跌幅
        pct_change = (current_price - prev_day['Close']) / prev_day['Close'] * 100

        return {
            "代號": code,
            "名稱": final_name,
            "收盤價": round(current_price, 2),
            "自訂價(可修)": None, 
            "漲跌幅": pct_change,
            "漲停價": limit_up,   # 獨立欄位
            "跌停價": limit_down, # 獨立欄位
            "獲利目標": None,
            "防守停損": None,
            "戰略備註": strategy_note,
            "命中狀態": "",
            # 隱藏欄位
            "_points": full_calc_points, # 包含漲跌停的完整點位 (計算用)
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
    st.info(f"已內建載入 {len(STOCK_MAP)} 檔股票名稱對照。")
    
    st.markdown("---")
    limit_rows = st.number_input("顯示筆數", min_value=1, value=50)

st.title("⚡ 當沖戰略室 V7")

# --- 上方輸入區 ---
col_search, col_file = st.columns([2, 1])

with col_search:
    search_query = st.text_input("🔍 快速查詢 (輸入代號或中文，用逗號分隔)", placeholder="台積電, 2317, 鴻海")

with col_file:
    uploaded_file = st.file_uploader("2. 上傳選股清單 (Excel/CSV)", type=['xlsx', 'csv'])
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
    
    # 1. 處理搜尋
    if search_query:
        inputs = [x.strip() for x in search_query.replace('，',',').split(',') if x.strip()]
        for inp in inputs:
            if inp.isdigit(): 
                targets.append((inp, ""))
            else:
                # 中文轉代號 (直接查表)
                code = get_code_by_name(inp)
                if code:
                    targets.append((code, inp))
                else:
                    st.toast(f"找不到「{inp}」，請確認 stock_names.csv。", icon="⚠️")

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
    
    for i, (code, name) in enumerate(targets):
        if code in seen: continue
        if hide_etf and code.startswith("00"): continue
        
        data = fetch_stock_data_raw(code, name)
        if data:
            results.append(data)
            seen.add(code)
        bar.progress((i+1)/len(targets))
    
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
    
    # 編輯器設定
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
        # 新增獨立的 漲停/跌停 欄位
        column_order=["代號", "名稱", "收盤價", "自訂價(可修)", "漲跌幅", "漲停價", "跌停價", "獲利目標", "防守停損", "命中狀態", "戰略備註"],
        hide_index=True,
        use_container_width=False,
        num_rows="dynamic",
        key="main_editor" 
    )
    
    # --- 即時運算 (純數學) ---
    updates = []
    
    for idx, row in edited_df.iterrows():
        custom_price = row['自訂價(可修)']
        
        if pd.isna(custom_price) or custom_price == "":
            updates.append({"獲利目標": None, "防守停損": None, "命中狀態": ""})
            continue
            
        price = float(custom_price)
        points = row['_points'] # 包含漲跌停的完整點位
        limit_up = row['_limit_up']
        limit_down = row['_limit_down']
        
        # 獲利邏輯 (優先找壓力)
        target = None
        for p in points:
            if p['val'] > price:
                target = p['val']
                break
        # 若無壓力，使用 +3% (但不大於漲停)
        if target is None:
            target = price * 1.03
            if target > limit_up: target = limit_up
        
        # 防守邏輯
        stop = None
        for p in reversed(points):
            if p['val'] < price:
                stop = p['val']
                break
        # 若無支撐，使用 -3% (但不小於跌停)
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
    
    # 更新並顯示結果
    df_updates = pd.DataFrame(updates, index=edited_df.index)
    edited_df.update(df_updates)
    st.session_state.stock_data = edited_df

    # --- 下方詳細結果 (含顏色) ---
    def color_change(val):
        if isinstance(val, (float, int)):
            if val > 0: return 'color: #ff4b4b' # Red
            if val < 0: return 'color: #00cc00' # Green
        return ''

    def highlight_hit(s):
        return ['background-color: #ffffcc; color: black' if '⚡' in str(s['命中狀態']) else '' for _ in s]

    st.markdown("### 🎯 計算結果")
    res_df = edited_df[["代號", "名稱", "自訂價(可修)", "漲跌幅", "獲利目標", "防守停損", "命中狀態", "戰略備註"]]
    
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

elif not uploaded_file and not search_query:
    st.info("請先上傳資料或輸入代號。")

import streamlit as st
import pandas as pd
import yfinance as yf
import math
import numpy as np

# --- 1. 頁面與 CSS (緊湊版面 + 綠跌紅漲) ---
st.set_page_config(page_title="當沖戰略室 V6", page_icon="⚡", layout="wide")

st.markdown("""
    <style>
    /* 縮減頁面留白 */
    .block-container { padding-top: 0.5rem; padding-bottom: 1rem; padding-left: 1rem; padding-right: 1rem; }
    
    /* 表格字體 */
    div[data-testid="stDataFrame"] { font-size: 14px; }
    
    /* 命中狀態醒目提示 */
    .hit-tag { background-color: #ffff00; color: black; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 功能模組
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

def get_stock_name(code, mapping_df=None):
    """從上傳的對照表找名稱"""
    code = str(code).strip()
    if mapping_df is not None and not mapping_df.empty:
        # 依照使用者提供的格式: 股票代號, 股票名稱
        # 先確保轉成字串比對
        try:
            row = mapping_df[mapping_df['股票代號'].astype(str) == code]
            if not row.empty:
                return row.iloc[0]['股票名稱']
        except:
            pass # 欄位名稱不符或其他錯誤
            
    return code # 找不到回傳代號

# ==========================================
# 核心邏輯: 資料抓取 (Fetch)
# ==========================================

def fetch_stock_data_raw(code, name_hint="", mapping_df=None):
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
        
        # 2. 昨日狀態 (用於備註)
        prev_day = hist.iloc[-2] if len(hist) >= 2 else today
        prev_prev_close = hist.iloc[-3]['Close'] if len(hist) >= 3 else prev_day['Open']
        p_limit_up, p_limit_down = calculate_limits(prev_prev_close)
        
        yesterday_status = ""
        if prev_day['Close'] >= p_limit_up: yesterday_status = "🔥昨漲停"
        elif prev_day['Close'] <= p_limit_down: yesterday_status = "💚昨跌停"

        # 3. 今日漲跌停 (User Point 3 & 4)
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
            
        # 強制加入今日漲跌停到點位列表，以便排序顯示 (User Point 4)
        points.append({"val": limit_up, "tag": "漲停"})
        points.append({"val": limit_down, "tag": "跌停"})

        # 過濾與排序
        valid_points = []
        seen = set()
        for p in points:
            v = float(f"{p['val']:.2f}")
            # 過濾掉超出漲跌停範圍太多的雜訊，但保留漲跌停本身
            if limit_down <= v <= limit_up:
                if v not in seen:
                    valid_points.append({"val": v, "tag": p['tag']})
                    seen.add(v)
        valid_points.sort(key=lambda x: x['val'])
        
        # 生成戰略備註字串
        note_parts = []
        if yesterday_status: note_parts.append(yesterday_status)
        
        for p in valid_points:
            v_str = f"{p['val']:.0f}" if p['val'].is_integer() else f"{p['val']:.2f}"
            tag = p['tag']
            
            # 格式美化
            if "漲停" in tag: item = f"🔥漲停{v_str}"
            elif "跌停" in tag: item = f"💚跌停{v_str}"
            elif "高" in tag: item = f"高{v_str}"
            elif tag: item = f"{v_str}{tag}"
            else: item = v_str
            
            note_parts.append(item)
        
        strategy_note = "-".join(note_parts)
        
        # 名稱處理
        final_name = name_hint
        if not final_name or final_name == code:
            final_name = get_stock_name(code, mapping_df)
        
        # 漲跌幅計算
        pct_change = (current_price - prev_day['Close']) / prev_day['Close'] * 100

        return {
            "代號": code,
            "名稱": final_name, # 不顯示代號了，因為代號在第一欄
            "收盤價": round(current_price, 2),
            "自訂價(可修)": None, # 預設空白
            "漲跌幅": pct_change, # 純數值，後續用 Column Config 變色
            "漲跌停": f"{limit_up} / {limit_down}", # User Point 3: 顯示數值
            "獲利目標": None,
            "防守停損": None,
            "戰略備註": strategy_note,
            "命中狀態": "",
            # 隱藏欄位 (用於計算)
            "_points": valid_points,
            "_limit_up": limit_up,
            "_limit_down": limit_down,
            "_ma5": ma5 # 用於判斷多空
        }
    except Exception as e:
        return None

# ==========================================
# 介面邏輯
# ==========================================

# 初始化 State
if 'stock_data' not in st.session_state:
    st.session_state.stock_data = pd.DataFrame()

# --- 側邊欄 ---
with st.sidebar:
    st.header("⚙️ 設定")
    hide_etf = st.checkbox("隱藏 ETF (00開頭)", value=True)
    
    st.markdown("---")
    st.markdown("📂 **資料對照**")
    
    # 1. 名稱對照表上傳
    mapping_file = st.file_uploader("1. 上傳股票代號名稱.xlsx (CSV)", type=['csv'])
    mapping_df = None
    if mapping_file:
        try:
            mapping_df = pd.read_csv(mapping_file)
            # 寬容度處理：去除欄位空白
            mapping_df.columns = [c.strip() for c in mapping_df.columns]
        except:
            st.error("對照表讀取失敗")

    st.markdown("---")
    limit_rows = st.number_input("顯示筆數", min_value=1, value=50)

st.title("⚡ 當沖戰略室 V6")

# --- 上方輸入區 ---
col_search, col_file = st.columns([2, 1])

with col_search:
    search_query = st.text_input("🔍 快速查詢 (代號，用逗號分隔)", placeholder="2330, 2317")

with col_file:
    uploaded_file = st.file_uploader("2. 上傳選股清單 (Excel/CSV)", type=['xlsx', 'csv'])
    selected_sheet = None
    if uploaded_file and not uploaded_file.name.endswith('.csv'):
        xl = pd.ExcelFile(uploaded_file)
        default_idx = 0
        if "週轉率" in xl.sheet_names:
            default_idx = xl.sheet_names.index("週轉率")
        selected_sheet = st.selectbox("選擇工作表", xl.sheet_names, index=default_idx)

# --- 按鈕執行 (抓取資料) ---
if st.button("🚀 執行分析", type="primary"):
    targets = []
    
    # 1. 處理搜尋
    if search_query:
        inputs = [x.strip() for x in search_query.replace('，',',').split(',') if x.strip()]
        for inp in inputs:
            if inp.isdigit(): targets.append((inp, ""))
            # 中文搜尋依賴 mapping_df (若有)
            elif mapping_df is not None:
                found = mapping_df[mapping_df['股票名稱'] == inp]
                if not found.empty:
                    targets.append((str(found.iloc[0]['股票代號']), inp))

    # 2. 處理選股清單
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'):
                df_up = pd.read_csv(uploaded_file)
            else:
                df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
            
            c_col = next((c for c in df_up.columns if "代號" in c), None)
            # User Requested: 盡量用網路或Mapping名稱
            # 但若檔案內有名稱，先讀取作為 hint
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
        
        data = fetch_stock_data_raw(code, name, mapping_df)
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
    
    # 使用者要求: 負數用綠色，正數用紅色 (User Point 2)
    # Streamlit 的 NumberColumn format 無法直接指定顏色
    # 我們必須在計算結果的 dataframe 使用 Styler，但 Editor 本身只能顯示數值
    # 這裡我們使用 TextColumn 搭配 Emoji 或 +- 符號來呈現漲跌幅，以利閱讀
    
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
            "漲跌幅": st.column_config.NumberColumn(
                "漲跌%",
                format="%.2f%%",
                disabled=True,
            ),
            "漲跌停": st.column_config.TextColumn("漲停 / 跌停", disabled=True),
            "獲利目標": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "防守停損": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "戰略備註": st.column_config.TextColumn(width="large", disabled=True),
            "命中狀態": st.column_config.TextColumn(width="small", disabled=True),
            # 隱藏
            "_points": None, "_limit_up": None, "_limit_down": None, "_ma5": None
        },
        column_order=["代號", "名稱", "收盤價", "自訂價(可修)", "漲跌幅", "漲跌停", "獲利目標", "防守停損", "命中狀態", "戰略備註"],
        hide_index=True,
        use_container_width=False, # User Point 5: 表格不要太寬
        num_rows="dynamic", 
        key="main_editor" 
    )
    
    # --- 即時運算 ---
    updates = []
    
    for idx, row in edited_df.iterrows():
        custom_price = row['自訂價(可修)']
        
        # 若未輸入 (NaN)，回傳空值
        if pd.isna(custom_price) or custom_price == "":
            updates.append({
                "獲利目標": None,
                "防守停損": None,
                "命中狀態": ""
            })
            continue
            
        price = float(custom_price)
        points = row['_points']
        limit_up = row['_limit_up']
        limit_down = row['_limit_down']
        
        # User Point 6: 獲利目標邏輯
        # 1. 優先找壓力 (大於 price 的點)
        target = None
        for p in points:
            if p['val'] > price:
                target = p['val']
                break
        # 2. 若找不到壓力 (創新高/漲停)，使用 +3% 規則
        if target is None:
            target = price * 1.03
            # 但不能超過漲停價 (除非漲停價本身就是目標)
            if target > limit_up: target = limit_up
        
        # 防守 (往下找支撐)
        stop = None
        for p in reversed(points):
            if p['val'] < price:
                stop = p['val']
                break
        # 若找不到支撐，使用 -3%
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
    st.session_state.stock_data = edited_df # 同步回 State

    # --- 下方結果表 (含顏色) ---
    # User Point 2: 漲跌負數綠色
    # 我們只針對有輸入資料的列顯示詳細結果，或顯示全部
    
    def color_change(val):
        if isinstance(val, (float, int)):
            if val > 0: return 'color: #ff4b4b' # Red
            if val < 0: return 'color: #00cc00' # Green
        return ''

    def highlight_hit(s):
        return ['background-color: #ffffcc; color: black' if '⚡' in str(s['命中狀態']) else '' for _ in s]

    st.markdown("### 🎯 計算結果")
    
    # 選取要顯示的欄位
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

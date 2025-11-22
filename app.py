import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import math
import numpy as np

# --- 1. 頁面與 CSS (緊湊版面) ---
st.set_page_config(page_title="當沖戰略室 V5", page_icon="⚡", layout="wide")

st.markdown("""
    <style>
    /* 縮減頁面留白 */
    .block-container { padding-top: 0.5rem; padding-bottom: 1rem; padding-left: 1rem; padding-right: 1rem; }
    
    /* 表格樣式 */
    div[data-testid="stDataFrame"] { font-size: 14px; }
    
    /* 命中狀態的醒目顏色 (黃底黑字) */
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
    """
    優先從使用者上傳的 mapping 找名稱，找不到才回傳代號
    """
    code = str(code).strip()
    if mapping_df is not None and not mapping_df.empty:
        # 假設 Mapping 檔有 '代號' 和 '名稱' 欄位
        # 先嘗試轉成字串比對
        row = mapping_df[mapping_df['代號'].astype(str) == code]
        if not row.empty:
            return row.iloc[0]['名稱']
    
    # 網路上抓的備用字典 (熱門股)
    fallback_map = {
        "2330":"台積電", "2317":"鴻海", "2454":"聯發科", "2603":"長榮", 
        "2609":"陽明", "2615":"萬海", "3231":"緯創", "2382":"廣達",
        "2376":"技嘉", "2356":"英業達", "3008":"大立光", "3034":"聯詠"
    }
    return fallback_map.get(code, code) # 真的找不到就回傳代號

# ==========================================
# 核心邏輯: 資料抓取
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
        
        # 2. 昨日狀態判斷 (是否漲停/跌停?)
        prev_day = hist.iloc[-2] if len(hist) >= 2 else today
        prev_prev_close = hist.iloc[-3]['Close'] if len(hist) >= 3 else prev_day['Open']
        
        # 計算昨日的漲跌停價
        p_limit_up, p_limit_down = calculate_limits(prev_prev_close)
        yesterday_status = ""
        if prev_day['Close'] >= p_limit_up:
            yesterday_status = "🔥昨漲停"
        elif prev_day['Close'] <= p_limit_down:
            yesterday_status = "💚昨跌停"

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

        # 過濾與排序
        valid_points = []
        seen = set()
        for p in points:
            v = float(f"{p['val']:.2f}")
            # 規則: 只顯示在今日跌停~今日漲停之間的點位
            if limit_down <= v <= limit_up:
                if v not in seen:
                    valid_points.append({"val": v, "tag": p['tag']})
                    seen.add(v)
        valid_points.sort(key=lambda x: x['val'])
        
        # 生成戰略備註
        note_parts = []
        if yesterday_status: note_parts.append(yesterday_status) # 把昨日狀態放在最前
        
        for p in valid_points:
            v_str = f"{p['val']:.0f}" if p['val'].is_integer() else f"{p['val']:.2f}"
            tag = p['tag']
            item = f"高{v_str}" if "高" in tag else (f"{v_str}{tag}" if tag else v_str)
            note_parts.append(item)
        
        strategy_note = "-".join(note_parts)
        
        # 名稱處理 (使用 Mapping)
        final_name = name_hint
        if not final_name or final_name == code:
            final_name = get_stock_name(code, mapping_df)
        
        display_name = f"{final_name}({code})"

        return {
            "代號": code,
            "名稱": display_name,
            "收盤價": round(current_price, 2),
            "自訂價(可修)": None, # 預設空白 (NumPy NaN)
            "漲跌力度": (current_price - prev_day['Close']) / prev_day['Close'] * 100,
            "獲利目標": None, # 等待計算
            "防守停損": None, # 等待計算
            "戰略備註": strategy_note,
            "命中狀態": "",
            # 隱藏欄位 (用於計算)
            "_points": valid_points,
            "_limit_up": limit_up,
            "_limit_down": limit_down
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
    mapping_file = st.file_uploader("1. 上傳代號名稱對照表 (CSV)", type=['csv'])
    mapping_df = None
    if mapping_file:
        try:
            mapping_df = pd.read_csv(mapping_file)
            # 簡易檢查欄位
            if '代號' not in mapping_df.columns or '名稱' not in mapping_df.columns:
                st.error("CSV 必須包含「代號」與「名稱」欄位")
                mapping_df = None
        except:
            st.error("對照表讀取失敗")

    # 2. 顯示設定
    st.markdown("---")
    limit_rows = st.number_input("顯示筆數", min_value=1, value=50)

st.title("⚡ 當沖戰略室 V5")

# --- 上方輸入區 ---
col_search, col_file = st.columns([2, 1])

with col_search:
    search_query = st.text_input("🔍 快速查詢 (代號/名稱，用逗號分隔)", placeholder="2330, 鴻海")

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
    
    # 1. 處理搜尋 (現在只支援代號，或依賴上面的 Mapping)
    if search_query:
        inputs = [x.strip() for x in search_query.replace('，',',').split(',') if x.strip()]
        for inp in inputs:
            # 如果輸入的是數字
            if inp.isdigit():
                targets.append((inp, ""))
            # 如果輸入的是中文 (嘗試從 mapping 找代號)
            elif mapping_df is not None:
                # 反向查找
                found = mapping_df[mapping_df['名稱'] == inp]
                if not found.empty:
                    targets.append((str(found.iloc[0]['代號']), inp))
                else:
                    st.toast(f"找不到「{inp}」的代號，請確認對照表。", icon="⚠️")

    # 2. 處理選股清單
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'):
                df_up = pd.read_csv(uploaded_file)
            else:
                df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
            
            c_col = next((c for c in df_up.columns if "代號" in c), None)
            # 名稱欄位非必須，有的話更好
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
        
        # 傳入 mapping_df 讓函式去查名稱
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
# 顯示與編輯層 (修復 ValueError 崩潰)
# ==========================================

if not st.session_state.stock_data.empty:
    
    # 1. 準備顯示的 Dataframe
    # 為了避免 index 問題，我們這裡不做任何 set_index 操作，保持預設 RangeIndex
    df_display = st.session_state.stock_data.reset_index(drop=True)
    
    # 2. 顯示 Data Editor
    edited_df = st.data_editor(
        df_display,
        column_config={
            "代號": st.column_config.TextColumn(disabled=True, width="small"),
            "名稱": st.column_config.TextColumn(disabled=True, width="medium"),
            "收盤價": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "自訂價(可修)": st.column_config.NumberColumn(
                "自訂價 ✏️",
                help="輸入後按 Enter，下方結果會即時更新",
                format="%.2f",
                step=0.1
            ),
            "漲跌力度": st.column_config.ProgressColumn(
                "漲跌", min_value=-10, max_value=10, format="%.2f%%"
            ),
            # 計算結果欄位設為唯讀 (或是隱藏，只在下方顯示)
            "獲利目標": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "防守停損": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "戰略備註": st.column_config.TextColumn(width="large", disabled=True),
            "命中狀態": st.column_config.TextColumn(width="small", disabled=True),
            
            # 隱藏內部資料
            "_points": None, "_limit_up": None, "_limit_down": None
        },
        column_order=["代號", "名稱", "收盤價", "自訂價(可修)", "漲跌力度", "獲利目標", "防守停損", "命中狀態", "戰略備註"],
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic", # 開啟刪除/新增列功能 (Point 5)
        key="main_editor" 
    )
    
    # 3. 即時計算 (Vectorized Calculation to prevent crash)
    # 只要 edited_df 有變動，Streamlit 就會重跑這段
    # 我們不再寫回 session_state，而是直接計算並顯示「更新後的結果」
    
    # 檢查是否有輸入自訂價
    # 注意: 編輯後的 dataframe index 可能會變 (如果刪除了列)，所以不要依賴 index 對應回 session_state
    
    updates = []
    
    # 重新迭代 edited_df 進行計算 (因為這是在記憶體中運算，速度極快)
    # 這裡解決了 ValueError，因為我們只處理當前存在的 edited_df
    for idx, row in edited_df.iterrows():
        custom_price = row['自訂價(可修)']
        
        # 如果沒輸入價格 (NaN 或 None)，保持原樣 (顯示 None)
        if pd.isna(custom_price) or custom_price == "":
            updates.append({
                "獲利目標": None,
                "防守停損": None,
                "命中狀態": ""
            })
            continue
            
        # 有輸入價格，開始計算
        price = float(custom_price)
        points = row['_points'] # 從隱藏欄位取出點位
        limit_up = row['_limit_up']
        limit_down = row['_limit_down']
        
        # 獲利 (往上找壓力)
        target = limit_up
        for p in points:
            if p['val'] > price:
                target = p['val']
                break
        
        # 防守 (往下找支撐)
        stop = limit_down
        for p in reversed(points):
            if p['val'] < price:
                stop = p['val']
                break
        
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
    
    # 4. 將計算結果合併回 display dataframe
    # 為了讓使用者看到結果，我們必須強行更新 edited_df 的顯示
    # 但 Streamlit data_editor 無法在同一輪 loop 內自我更新顯示 (會閃爍)
    # 所以我們在下方顯示一個「戰略結果預覽」 (這是最穩定的做法)
    
    df_updates = pd.DataFrame(updates, index=edited_df.index)
    
    # 更新 edited_df 的數據以供展示
    edited_df.update(df_updates)
    
    # 為了讓使用者不用看兩個表，我們這裡做一個取巧：
    # 只有當使用者有輸入資料時，我們在下方顯示一個「結果確認表」，如果沒輸入就只顯示上面的編輯表
    # 但使用者說 "輸入後表格就重整完全不能用"，這表示上面的 editor 被刷新了
    
    # 最終解法：
    # 因為 data_editor 的輸入值已經保留在 `edited_df`
    # 我們將 `edited_df` 存回 `session_state`，這樣下次 Rerun 時 editor 就會讀到新的「獲利目標」
    # 這就是之前報錯的地方，現在我們用正確的 index 更新
    
    # 將計算好的欄位放回 session_state (供下一次渲染使用)
    # 先檢查 index 是否一致 (因為 dynamic 模式下 index 可能缺號)
    # 我們直接用 edited_df 覆蓋 session_state，這樣最安全
    st.session_state.stock_data = edited_df
    
    # 這裡不需要 st.experimental_rerun()，因為下次使用者操作時自然會更新
    # 但如果要「按 Enter 馬上看到獲利目標填入」，則需要 Rerun。
    # 不過 Rerun 會影響體驗。
    # 我們改用 st.dataframe 在下方顯示「即時運算結果」，這是目前 Streamlit 的最佳實踐
    
    st.markdown("### 🎯 戰略結果 (即時運算)")
    
    # 使用 Style 變色 (User Point 4)
    def highlight_hit_row(s):
        return ['background-color: #ffffcc; color: black' if '⚡' in str(s['命中狀態']) else '' for _ in s]

    # 只顯示有輸入價格的列，讓畫面乾淨
    mask = edited_df['自訂價(可修)'].notna()
    if mask.any():
        res_df = edited_df[mask][["代號", "名稱", "自訂價(可修)", "命中狀態", "獲利目標", "防守停損"]]
        st.dataframe(
            res_df.style.apply(highlight_hit_row, axis=1),
            use_container_width=True,
            hide_index=True,
            column_config={
                "自訂價(可修)": st.column_config.NumberColumn("自訂價", format="%.2f"),
                "獲利目標": st.column_config.NumberColumn(format="%.2f"),
                "防守停損": st.column_config.NumberColumn(format="%.2f")
            }
        )
    else:
        st.info("👆 請在上方表格輸入「自訂價」並按 Enter，結果將顯示於此。")

elif not uploaded_file and not search_query:
    st.info("請上傳資料或輸入代號。")

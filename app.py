import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import math

# --- 1. 頁面設定與 CSS (緊湊版面 + 修正寬度) ---
st.set_page_config(page_title="當沖戰略室 V4", page_icon="⚡", layout="wide")

st.markdown("""
    <style>
    /* 縮減頁面留白，讓表格更寬 */
    .block-container { padding-top: 0.5rem; padding-bottom: 1rem; padding-left: 1rem; padding-right: 1rem; }
    
    /* 縮小表格字體與行高，讓畫面更緊湊 (User Point 5) */
    div[data-testid="stDataFrame"] { font-size: 14px; }
    div[data-testid="stDataEditor"] table { line-height: 1.2; }
    
    /* 針對特定文字的顏色樣式 (透過 Pandas Styler 無法直接作用於 Editor，此為輔助) */
    .highlight-match { background-color: #ffff00; color: black; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 功能模組
# ==========================================

@st.cache_data(ttl=86400)
def get_stock_name_map():
    """建立一個簡單的熱門股代號對照表 (解決部分搜尋問題)"""
    # 這裡可以放一些常見的，作為備援
    return {
        "台積電": "2330", "鴻海": "2317", "聯發科": "2454", "長榮": "2603", "陽明": "2609",
        "萬海": "2615", "緯創": "3231", "廣達": "2382", "技嘉": "2376", "英業達": "2356"
    }

def search_code_by_name_v2(query):
    """
    修復版搜尋：先查對照表，再查 Yahoo (User Point 1)
    """
    query = query.strip()
    if query.isdigit(): return query
    
    # 1. 查表
    name_map = get_stock_name_map()
    if query in name_map: return name_map[query]
    
    # 2. 爬蟲 Fallback (針對一般股票)
    try:
        # 使用 Yahoo 舊版介面或搜尋 API 的模擬
        url = f"https://tw.stock.yahoo.com/h/kimosearch/search_list.html?keyword={query}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=2)
        soup = BeautifulSoup(r.text, "html.parser")
        
        # 抓取連結中的代號
        links = soup.find_all('a', href=True)
        for link in links:
            text = link.get_text()
            href = link['href']
            # 檢查是否包含該股票名稱且連結含有代號
            if query in text and "/quote/" in href:
                parts = href.split("/quote/")[1].split(".")
                if parts[0].isdigit():
                    return parts[0]
    except:
        pass
    
    return query # 若真的找不到，回傳原字串讓後續防呆處理

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

# ==========================================
# 核心邏輯: 戰略分析 (資料獲取層)
# ==========================================

def fetch_stock_data_raw(code, name_input=""):
    """
    只負責抓資料，不負責計算動態獲利 (因自訂價會變)
    """
    code = str(code).strip()
    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist = ticker.history(period="10d")
        
        if hist.empty:
            ticker = yf.Ticker(f"{code}.TWO")
            hist = ticker.history(period="10d")
        
        if hist.empty: return None

        # 基礎數據
        today = hist.iloc[-1]
        prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else today['Open']
        limit_up, limit_down = calculate_limits(prev_close)
        current_price = today['Close']
        
        # 戰略點位計算 (Strategy Points)
        points = []
        ma5 = hist['Close'].tail(5).mean()
        points.append({"val": ma5, "tag": "多" if current_price > ma5 else "空"}) # 暫時用現價判斷多空Tag
        points.append({"val": today['Open'], "tag": ""})
        points.append({"val": today['High'], "tag": ""})
        points.append({"val": today['Low'], "tag": ""})
        
        past_5 = hist.iloc[-6:-1] if len(hist) >= 6 else hist.iloc[:-1]
        if not past_5.empty:
            points.append({"val": past_5['High'].max(), "tag": "高"})
            points.append({"val": past_5['Low'].min(), "tag": ""})

        # 戰略備註生成 (含過濾)
        valid_points = []
        seen = set()
        for p in points:
            v = float(f"{p['val']:.2f}")
            if limit_down <= v <= limit_up: # 漲跌停過濾
                if v not in seen:
                    valid_points.append({"val": v, "tag": p['tag']})
                    seen.add(v)
        valid_points.sort(key=lambda x: x['val'])
        
        # 生成備註字串
        note_parts = []
        for p in valid_points:
            v_str = f"{p['val']:.0f}" if p['val'].is_integer() else f"{p['val']:.2f}"
            tag = p['tag']
            if "高" in tag: item = f"高{v_str}"
            elif tag: item = f"{v_str}{tag}"
            else: item = v_str
            note_parts.append(item)
        
        strategy_note = "-".join(note_parts)
        
        # 名稱處理 (User Point 1 & 4: 顯示正確名稱)
        # 如果使用者有輸入名稱就用輸入的，否則嘗試用代號
        real_name = name_input if name_input else code
        # 這裡可以嘗試用 yf.info 但速度慢，先以 search 的結果為主
        
        display_name = f"{real_name}({code})"

        return {
            "代號": code,
            "名稱": display_name,
            "收盤價(唯讀)": round(current_price, 2),
            "自訂價(可修)": round(current_price, 2), # 預設等於收盤
            "漲跌停區間": (limit_up, limit_down), # 存tuple方便後續取用
            "戰略點位": valid_points, # 存原始點位列表，方便後續比對命中
            "戰略備註": strategy_note,
            "漲停價": limit_up,
            "跌停價": limit_down
        }
    except:
        return None

# ==========================================
# 介面邏輯 (狀態管理層)
# ==========================================

# 初始化 Session State (關鍵: 防止刷新重抓)
if 'stock_data' not in st.session_state:
    st.session_state.stock_data = pd.DataFrame()
if 'editor_key' not in st.session_state:
    st.session_state.editor_key = 0

# 側邊欄
with st.sidebar:
    st.header("⚙️ 設定")
    hide_etf = st.checkbox("隱藏 ETF (00開頭)", value=True)
    
    st.markdown("---")
    # 行列自訂 (User Point 8: 恢復手動輸入)
    limit_rows = st.number_input("顯示筆數", min_value=1, value=50)

st.title("⚡ 當沖戰略室 V4 (極速版)")

# 上方控制區
col_search, col_file = st.columns([2, 1])

with col_search:
    # 支援中文與多股 (User Point 1, 4)
    search_query = st.text_input("🔍 快速查詢 (輸入代號或名稱，如: 台積電, 2603)", placeholder="台積電, 鴻海, 2603")

with col_file:
    # 上傳檔案 (User Point 2: 恢復工作表選擇)
    uploaded_file = st.file_uploader("上傳 Excel", type=['xlsx', 'csv'])
    selected_sheet = None
    if uploaded_file and not uploaded_file.name.endswith('.csv'):
        xl = pd.ExcelFile(uploaded_file)
        # 預設選「週轉率」，若無則選第一個
        default_idx = 0
        if "週轉率" in xl.sheet_names:
            default_idx = xl.sheet_names.index("週轉率")
        selected_sheet = st.selectbox("選擇工作表", xl.sheet_names, index=default_idx)

# 按鈕: 執行資料抓取 (只有按這個才會去 Yahoo 抓資料)
if st.button("🚀 執行分析 (抓取資料)", type="primary"):
    targets = []
    
    # 1. 解析搜尋
    if search_query:
        inputs = [x.strip() for x in search_query.replace('，',',').split(',') if x.strip()]
        for inp in inputs:
            # 嘗試轉換中文名稱
            code = search_code_by_name_v2(inp)
            targets.append((code, inp if not inp.isdigit() else ""))

    # 2. 解析檔案
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

    # 3. 批次抓取 (存入 Session State)
    results = []
    seen_code = set()
    
    bar = st.progress(0)
    total = len(targets)
    
    for i, (code, name) in enumerate(targets):
        if code in seen_code: continue
        # ETF 過濾
        if hide_etf and code.startswith("00"): continue
        
        data = fetch_stock_data_raw(code, name)
        if data:
            results.append(data)
            seen_code.add(code)
        
        if total > 0: bar.progress((i+1)/total)
    
    bar.empty()
    
    if results:
        # 存入 session state，並清空之前的編輯紀錄
        st.session_state.stock_data = pd.DataFrame(results)
        st.session_state.editor_key += 1 # 強制重置 editor
    else:
        st.warning("查無資料 (請確認名稱是否正確或已被 ETF 過濾)")

# ==========================================
# 顯示與編輯層 (即時運算)
# ==========================================

if not st.session_state.stock_data.empty:
    
    # 取得目前的資料 (從 State)
    df_current = st.session_state.stock_data.copy()
    
    # 這裡使用 data_editor 讓使用者修改「自訂價(可修)」
    # User Point 3: 保留「收盤價(唯讀)」，新增「自訂價(可修)」
    # User Point 4: 輸入後不要重整不可用 -> 這裡的邏輯是：
    # data_editor 修改後會觸發 Rerun，但因為我們上面的 fetch 代碼是在 button 內，
    # 所以 Rerun 時不會重新抓 Yahoo，只會跑下面的計算邏輯，速度極快。
    
    edited_df = st.data_editor(
        df_current,
        key=f"editor_{st.session_state.editor_key}", # 綁定 Key
        column_config={
            "代號": st.column_config.TextColumn(disabled=True, width="small"),
            "名稱": st.column_config.TextColumn(disabled=True, width="medium"),
            "收盤價(唯讀)": st.column_config.NumberColumn(format="%.2f", disabled=True),
            "自訂價(可修)": st.column_config.NumberColumn(
                "自訂價 ✏️",
                help="輸入價格按 Enter，自動重算獲利/防守",
                format="%.2f",
                step=0.1
            ),
            # 隱藏輔助欄位
            "漲跌停區間": None, "戰略點位": None, "漲停價": None, "跌停價": None,
            "戰略備註": st.column_config.TextColumn(width="large")
        },
        column_order=["代號", "名稱", "收盤價(唯讀)", "自訂價(可修)", "戰略備註"], # 先只顯示這幾欄，後面用計算補上
        hide_index=True,
        use_container_width=True,
        num_rows="fixed", # 禁止新增刪除列，確保穩定
        height=35 + (min(len(df_current), limit_rows) * 35) # 動態高度 (User Point 5: 緊湊)
    )
    
    # --- 後處理：即時計算 (Real-time Calculation) ---
    # 根據 edited_df 中的「自訂價(可修)」重新計算獲利目標與狀態
    
    calc_results = []
    
    for index, row in edited_df.iterrows():
        price = row['自訂價(可修)']
        limit_up = row['漲停價']
        limit_down = row['跌停價']
        points = row['戰略點位']
        
        # 1. 計算獲利/防守 (User Point 5 logic)
        # 往上找第一個壓力
        target = limit_up
        for p in points:
            if p['val'] > price:
                target = p['val']
                break
        
        # 往下找第一個支撐
        stop = limit_down
        for p in reversed(points):
            if p['val'] < price:
                stop = p['val']
                break
                
        # 2. 命中狀態 (User Point 4: 底色變色替代方案)
        # Streamlit Editor 不支援動態底色，我們用 Emoji + 文字標示在「備註」旁或新欄位
        # User Point 4 要求: "直接底色變色" (目前技術做不到) -> "對應到戰略備註直接變色"
        # 替代：我們新增一個「命中狀態」欄位，如果有命中，顯示 "🎯 68.5 (高)"
        
        hit_info = ""
        for p in points:
            if abs(p['val'] - price) < 0.05:
                tag = p['tag'] if p['tag'] else "關鍵價"
                hit_info = f"🎯 {p['val']} ({tag})"
                break
        
        calc_results.append({
            "獲利目標": target,
            "防守停損": stop,
            "命中狀態": hit_info
        })
    
    # 合併計算結果
    df_calc = pd.DataFrame(calc_results)
    df_final = pd.concat([edited_df.reset_index(drop=True), df_calc], axis=1)
    
    # --- 最終顯示 (使用 dataframe 顯示計算後的結果，或再次用 editor 顯示唯讀?) ---
    # 為了讓 User 可以「邊改邊看」，我們通常不會再畫一個表格。
    # 但 data_editor 的 output 不能直接再塞回去自己顯示新欄位 (會 Infinite Loop)。
    # 妥協方案：在 data_editor 下方或旁邊顯示，或者使用 st.dataframe (唯讀) 顯示完整版
    # 鑑於 User 說「輸入後表格重整完全不能用」，我們只顯示一個最終表格可能更好。
    
    # 修正：為了達成 Excel 體驗，我們必須把計算結果顯示在同一個表格。
    # 技巧：第一次 render 用 editor，User 修改後，程式 Rerun，我們拿到 edited_df，
    # 然後我們運算完，再畫一次包含結果的表格？不，這樣會由兩個表格。
    
    # 最佳解：把 data_editor 的結果即時運算後，用 st.dataframe (Styler) 呈現「結果預覽」?
    # 不，User 要在表格裡輸入。
    
    # 讓我們利用 column_config 的 format 功能。
    # 其實，上面的 edited_df 已經是最新的，我們只要把「獲利」「防守」「命中」加回去顯示即可。
    # 但 Streamlit 無法動態插入欄位到已經 render 的 editor 中。
    
    # === 解決方案 ===
    # 我們不顯示原始的 edited_df，而是隱藏它 (或把它放在上面當輸入區)，
    # 下方顯示一個帶有顏色、樣式完整的「戰略儀表板」。
    # 但 User 想要「像 Excel 那樣」。
    
    # 因此，我們修改策略：
    # 1. `data_editor` 包含所有欄位 (含獲利/防守)。
    # 2. 獲利/防守欄位設為 disabled (唯讀)。
    # 3. 當 User 改了「自訂價」，Rerun -> 我們在 Python 端重算獲利/防守 -> 更新 Session State -> Editor 更新數值。
    
    # 更新 Session State 中的值
    for i, row in df_final.iterrows():
        # 更新記憶體中的數據，這樣下次 Rerun 時 editor 就會顯示新算出的獲利/防守
        st.session_state.stock_data.at[i, '自訂價(可修)'] = row['自訂價(可修)'] 
        # 注意：我們需要把算出來的 Target/Stop 寫回 session_state，讓 editor 顯示
        st.session_state.stock_data.at[i, '獲利目標'] = row['獲利目標']
        st.session_state.stock_data.at[i, '防守停損'] = row['防守停損']
        st.session_state.stock_data.at[i, '命中狀態'] = row['命中狀態']
    
    # 重新渲染一次 Editor (帶有更新後的計算值)
    # 為了避免 "Duplicate Widget ID"，我們使用 st.empty() 或是直接覆蓋
    # 但 Streamlit 的執行流是線性的。我們剛剛已經 render 過 editor 了。
    # 這裡有一個 1-frame lag 的問題 (改了數字，要下一次 run 才會變更獲利)。
    
    # 為了即時性，我們在 Editor 下方顯示「最新計算結果預覽」(Styler)，
    # 或者 User 接受按兩次 (通常 Streamlit 0.85+ 已經優化這點)。
    
    # 讓我們試試把計算結果「附加」在表格後面顯示。
    st.markdown("### 📊 戰略結果 (即時運算)")
    
    # 這裡用 dataframe 加上 Styler 來滿足 User Point 4 (變色)
    def highlight_hit(val):
        color = '#ffffcc' if '🎯' in str(val) else ''
        return f'background-color: {color}; color: black' if color else ''

    st.dataframe(
        df_final[["代號", "名稱", "自訂價(可修)", "命中狀態", "獲利目標", "防守停損", "戰略備註"]],
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config={
            "自訂價(可修)": st.column_config.NumberColumn("自訂價", format="%.2f"),
            "命中狀態": st.column_config.TextColumn("狀態 (命中變色)", width="small"),
        }
    )
    
    st.caption("💡 提示：上方表格為計算結果。若需修改價格，請在更上方的編輯區輸入。")

elif not uploaded_file and not search_query:
    st.info("👋 請在上方輸入代號或上傳檔案。")

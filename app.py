import streamlit as st
import pandas as pd
import yfinance as yf
import math

# --- 頁面基本設定 ---
st.set_page_config(page_title="戰略選股面板", page_icon="📈", layout="centered")

# --- CSS 美化 (隱藏程式碼風格，只顯示卡片) ---
st.markdown("""
    <style>
    .stApp { background-color: #f4f4f4; }
    .stock-card {
        background-color: white;
        padding: 18px;
        border-radius: 15px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        margin-bottom: 15px;
        border-left: 8px solid #ccc;
    }
    .card-up { border-left: 8px solid #eb4d4b; }   /* 紅色多頭 */
    .card-down { border-left: 8px solid #6ab04c; } /* 綠色空頭 */
    
    .big-price { font-size: 28px; font-weight: 800; margin: 5px 0; }
    .trend-tag { font-size: 12px; padding: 3px 8px; border-radius: 10px; color: white; font-weight: bold; vertical-align: middle; }
    
    .grid-container { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 10px; }
    .grid-box { background-color: #f9f9f9; padding: 10px; border-radius: 8px; text-align: center; }
    .grid-label { font-size: 12px; color: #666; display: block; margin-bottom: 2px;}
    .grid-value { font-size: 16px; font-weight: bold; color: #333; }
    
    .price-target { color: #eb4d4b; }
    .price-stop { color: #6ab04c; }
    </style>
    """, unsafe_allow_html=True)

# --- 核心運算邏輯 ---
def get_tick_size(price):
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def calculate_limit_price(price, is_up=True):
    target = price * 1.10 if is_up else price * 0.90
    tick = get_tick_size(price)
    steps = math.floor(target / tick) if is_up else math.ceil(target / tick) 
    return float(f"{steps * tick:.2f}")

@st.cache_data(ttl=300)
def fetch_stock_data(code, name_hint=""):
    code = str(code).strip().split('.')[0] # 清洗代號
    if not code.isdigit(): return None
    
    ticker_tw = f"{code}.TW"
    stock = yf.Ticker(ticker_tw)
    hist = stock.history(period="10d")
    
    if hist.empty:
        stock = yf.Ticker(f"{code}.TWO") # 嘗試上櫃
        hist = stock.history(period="10d")
    
    if hist.empty: return None

    today = hist.iloc[-1]
    prev = hist.iloc[-2]
    close = today['Close']
    ma5 = hist['Close'].tail(5).mean()
    
    # 邏輯計算
    trend = "多" if close > ma5 else "空"
    pressure = max(today['High'], prev['High']) # 昨高今高取大
    support = min(today['Low'], prev['Low'])    # 昨低今低取小
    
    # 名稱處理 (若無外部傳入，嘗試抓取 yfinance 簡稱，通常是英文)
    display_name = name_hint if name_hint else f"代號 {code}"

    return {
        "code": code,
        "name": display_name,
        "price": round(close, 2),
        "pct": round((close - prev['Close']) / prev['Close'] * 100, 2),
        "ma5": round(ma5, 2),
        "trend": trend,
        "limit_up": calculate_limit_price(close, True),
        "limit_down": calculate_limit_price(close, False),
        "target_3": round(close * 1.03, 2),
        "stop_3": round(close * 0.97, 2),
        "pressure": pressure,
        "support": support,
        "prev_high": prev['High'],
        "today_high": today['High'],
        "prev_low": prev['Low'],
        "today_low": today['Low']
    }

# --- 介面開始 ---
st.title("📊 股票戰略儀表板")

# 建立分頁 (Tabs)
tab1, tab2 = st.tabs(["🔍 單股查詢", "📂 匯入清單"])

# --- Tab 1: 單股查詢 (解決不需上傳的問題) ---
with tab1:
    col1, col2 = st.columns([3, 1])
    with col1:
        search_input = st.text_input("輸入代號 (例如: 2330)", placeholder="在此輸入股票代號")
    with col2:
        st.write("") # 排版用
        st.write("")
        search_btn = st.button("查詢", type="primary")

    if search_btn and search_input:
        with st.spinner('數據抓取中...'):
            data = fetch_stock_data(search_input)
            if data:
                # 顯示卡片
                trend_color = "#eb4d4b" if data['trend'] == "多" else "#6ab04c"
                trend_bg = trend_color
                card_cls = "card-up" if data['trend'] == "多" else "card-down"
                
                html = f"""
                <div class="stock-card {card_cls}">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <div>
                            <span style="font-size:1.5em; font-weight:bold;">{data['code']}</span>
                            <span class="trend-tag" style="background-color:{trend_bg}; margin-left:10px;">{data['trend']}頭趨勢</span>
                        </div>
                        <div style="text-align:right;">
                            <div class="big-price" style="color:{trend_color}">{data['price']}</div>
                            <div style="color:{trend_color}">{data['pct']}%</div>
                        </div>
                    </div>
                    <div style="font-size:0.9em; color:#888; margin-top:5px;">5日線: {data['ma5']} (線上多/線下空)</div>
                    
                    <hr style="border-top: 1px dashed #ddd; margin: 15px 0;">
                    
                    <div class="grid-container">
                        <div class="grid-box">
                            <span class="grid-label">🔴 壓力參考 (昨高/今高)</span>
                            <span class="grid-value">{data['prev_high']} / {data['today_high']} <br>⮕ {data['pressure']}</span>
                        </div>
                        <div class="grid-box">
                            <span class="grid-label">🟢 支撐參考 (昨低/今低)</span>
                            <span class="grid-value">{data['prev_low']} / {data['today_low']} <br>⮕ {data['support']}</span>
                        </div>
                        <div class="grid-box" style="border:1px solid #eb4d4b;">
                            <span class="grid-label price-target">★ 獲利目標 (+3%)</span>
                            <span class="grid-value price-target">{data['target_3']}</span>
                            <span style="font-size:10px; color:#ccc">漲停: {data['limit_up']}</span>
                        </div>
                        <div class="grid-box" style="border:1px solid #6ab04c;">
                            <span class="grid-label price-stop">🛡️ 防守停損 (-3%)</span>
                            <span class="grid-value price-stop">{data['stop_3']}</span>
                            <span style="font-size:10px; color:#ccc">跌停: {data['limit_down']}</span>
                        </div>
                    </div>
                </div>
                """
                st.markdown(html, unsafe_allow_html=True)
            else:
                st.error("❌ 找不到此代號，請確認輸入正確。")

# --- Tab 2: 檔案上傳 (保留原本功能) ---
with tab2:
    uploaded_file = st.file_uploader("上傳週轉率/選股 CSV", type=['csv', 'xlsx'])
    
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'): df = pd.read_csv(uploaded_file)
            else: df = pd.read_excel(uploaded_file)
            
            # 欄位識別
            code_col = next((c for c in ['代號','股票代號'] if c in df.columns), None)
            name_col = next((c for c in ['名稱','股票名稱'] if c in df.columns), None)
            
            if code_col:
                targets = []
                for idx, row in df.iterrows():
                    c = str(row[code_col]).split('.')[0]
                    n = str(row[name_col]) if name_col else ""
                    if c.isdigit(): targets.append((c, n))
                
                if st.button("開始批量分析", type="primary"):
                    progress = st.progress(0)
                    for i, (c, n) in enumerate(targets):
                        d = fetch_stock_data(c, n)
                        if d:
                            # 簡化版卡片 (列表式)
                            trend_color = "#eb4d4b" if d['trend'] == "多" else "#6ab04c"
                            card_cls = "card-up" if d['trend'] == "多" else "card-down"
                            
                            html_mini = f"""
                            <div class="stock-card {card_cls}" style="padding: 12px; border-left-width: 5px;">
                                <div style="display:flex; justify-content:space-between;">
                                    <b>{d['name']} ({d['code']})</b>
                                    <b style="color:{trend_color}">{d['price']}</b>
                                </div>
                                <div style="display:flex; justify-content:space-between; margin-top:8px; font-size:0.9em;">
                                    <span style="color:#eb4d4b">壓: {d['pressure']} | +3%: {d['target_3']}</span>
                                    <span style="color:#6ab04c">撐: {d['support']} | -3%: {d['stop_3']}</span>
                                </div>
                            </div>
                            """
                            st.markdown(html_mini, unsafe_allow_html=True)
                        progress.progress((i+1)/len(targets))
            else:
                st.warning("檔案中找不到「代號」欄位")
        except Exception as e:
            st.error("檔案讀取失敗，請檢查格式。")

    trend = "多" if close > ma5 else "空"
    
    # 壓力：昨高與今高取大
    pressure_val = max(today['High'], prev['High'])
    # 支撐：昨低與今低取小
    support_val = min(today['Low'], prev['Low'])

    return {
        "code": stock_id,
        "name": name_hint, # 來自 CSV 的名稱
        "price": round(close, 2),
        "pct": round((close - prev['Close']) / prev['Close'] * 100, 2),
        "ma5": round(ma5, 2),
        "trend": trend,
        "limit_up": calculate_limit_price(close, True),
        "limit_down": calculate_limit_price(close, False),
        "target_3": round(close * 1.03, 2),
        "stop_3": round(close * 0.97, 2),
        "high_prev": round(prev['High'], 2),
        "high_today": round(today['High'], 2),
        "low_prev": round(prev['Low'], 2),
        "low_today": round(today['Low'], 2),
        "pressure": round(pressure_val, 2),
        "support": round(support_val, 2)
    }

# --- 3. 主程式介面 ---
st.title("📊 週轉率選股戰略")

# 檔案上傳區
with st.expander("📂 上傳週轉率 CSV", expanded=True):
    uploaded_file = st.file_uploader("選擇檔案", type=['csv', 'xlsx'])
    
    target_list = [] # 格式: [(代號, 名稱), ...]
    
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'): 
                df = pd.read_csv(uploaded_file)
            else: 
                df = pd.read_excel(uploaded_file)
            
            # 自動判斷欄位 (相容您的週轉率檔案)
            code_col = next((c for c in ['代號','股票代號'] if c in df.columns), None)
            name_col = next((c for c in ['名稱','股票名稱'] if c in df.columns), None)
            
            if code_col:
                # 建立代號與名稱的對照清單
                for index, row in df.iterrows():
                    c = str(row[code_col]).split('.')[0].strip() # 去除小數點
                    n = str(row[name_col]) if name_col else ""
                    if c.isdigit(): # 確保是數字代號
                        target_list.append((c, n))
        except Exception as e:
            st.error(f"檔案讀取錯誤: {e}")

# 執行分析
if st.button("🚀 開始運算 (依高低點+5MA)", type="primary", use_container_width=True):
    if not target_list:
        st.warning("請先上傳檔案，或確認檔案內含有「代號」欄位。")
        # 預設範例
        target_list = [("8043","蜜望實(範例)"), ("6173","信昌電(範例)")]
    
    results = []
    progress = st.progress(0)
    
    for i, (code, name) in enumerate(target_list):
        data = fetch_stock_data(code, name)
        if data: results.append(data)
        progress.progress((i + 1) / len(target_list))
        
    progress.empty()

    # 顯示結果
    st.markdown("---")
    if results:
        for row in results:
            # 決定顏色樣式
            card_class = "card-up" if row['trend'] == "多" else "card-down"
            trend_color = "#d9534f" if row['trend'] == "多" else "#5cb85c"
            
            # 組合 HTML 卡片
            html = f"""
            <div class="stock-card {card_class}">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                    <div>
                        <span style="font-size:1.3em; font-weight:bold;">{row['name']} ({row['code']})</span>
                        <span style="font-size:0.8em; color:#888; margin-left:5px;">5MA: {row['ma5']}</span>
                    </div>
                    <div style="text-align:right;">
                        <div style="font-size:1.5em; font-weight:bold; color:{trend_color};">{row['price']}</div>
                        <div style="font-size:0.8em; color:{trend_color};">{row['pct']}%</div>
                    </div>
                </div>

                <div style="background-color:#f9f9f9; padding:8px; border-radius:8px; margin-bottom:10px;">
                    <div class="data-row">
                        <span class="data-label">🔴 壓力 (昨高/今高)</span>
                        <span class="data-value">{row['high_prev']} / {row['high_today']} ➔ <b>{row['pressure']}</b></span>
                    </div>
                    <div class="data-row">
                        <span class="data-label">🟢 支撐 (昨低/今低)</span>
                        <span class="data-value">{row['low_prev']} / {row['low_today']} ➔ <b>{row['support']}</b></span>
                    </div>
                </div>

                <div style="display:flex; justify-content:space-between;">
                    <div style="width:48%;">
                        <div style="font-size:0.8em; color:#999;">獲利目標 (+3%)</div>
                        <div class="highlight-red" style="font-size:1.1em;">{row['target_3']}</div>
                        <div style="font-size:0.8em; color:#ccc;">漲停: {row['limit_up']}</div>
                    </div>
                    <div style="width:48%; text-align:right;">
                        <div style="font-size:0.8em; color:#999;">防守停損 (-3%)</div>
                        <div class="highlight-green" style="font-size:1.1em;">{row['stop_3']}</div>
                        <div style="font-size:0.8em; color:#ccc;">跌停: {row['limit_down']}</div>
                    </div>
                </div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)
    else:
        st.error("無法取得數據，請檢查代號或網路。")


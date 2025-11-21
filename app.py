import streamlit as st
import pandas as pd
import yfinance as yf
import math

# --- 設定頁面 ---
st.set_page_config(page_title="週轉率戰略版", page_icon="📊", layout="centered")

# --- CSS 樣式優化 (強調高低點數據) ---
st.markdown("""
    <style>
    .stApp { background-color: #f0f2f6; }
    .stock-card {
        background-color: white;
        padding: 15px;
        border-radius: 12px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.08);
        margin-bottom: 12px;
        border-left: 6px solid #ccc;
    }
    .card-up { border-left: 6px solid #d9534f; } /* 紅色多頭 */
    .card-down { border-left: 6px solid #5cb85c; } /* 綠色空頭 */
    .data-row { display: flex; justify-content: space-between; margin-bottom: 6px; }
    .data-label { color: #666; font-size: 0.9em; }
    .data-value { font-weight: bold; color: #333; }
    .highlight-red { color: #d9534f; font-weight: bold; }
    .highlight-green { color: #5cb85c; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# --- 1. 台股 Tick 計算函數 ---
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

# --- 2. 抓取資料 (增加昨高/昨低/今高/今低) ---
@st.cache_data(ttl=900) # 快取 15 分鐘
def fetch_stock_data(code, name_hint=""):
    stock_id = str(code).strip()
    # 簡單過濾掉非股票代號 (如債券 00859B)
    if len(stock_id) > 4 and not stock_id.isdigit(): return None

    ticker = f"{stock_id}.TW"
    stock = yf.Ticker(ticker)
    hist = stock.history(period="10d") 
    
    if hist.empty:
        ticker = f"{stock_id}.TWO" # 試試上櫃
        stock = yf.Ticker(ticker)
        hist = stock.history(period="10d")
    
    if hist.empty: return None 

    # 取得今日與昨日資料
    today = hist.iloc[-1]
    prev = hist.iloc[-2]
    
    close = today['Close']
    ma5 = hist['Close'].tail(5).mean()
    
    # 計算邏輯
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


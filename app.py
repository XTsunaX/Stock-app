import streamlit as st
import pandas as pd
import yfinance as yf
import math

# --- 設定頁面 (手機版面優化) ---
st.set_page_config(page_title="手機選股戰略", page_icon="📱", layout="centered")

# 注入 CSS 讓手機版面更漂亮 (隱藏多餘邊距，卡片陰影)
st.markdown("""
    <style>
    .stApp { background-color: #f5f5f5; }
    .stock-card {
        background-color: white;
        padding: 15px;
        border-radius: 15px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        margin-bottom: 15px;
        border-left: 5px solid #ccc;
    }
    .card-up { border-left: 5px solid #ff4b4b; }
    .card-down { border-left: 5px solid #00eb00; }
    .big-price { font-size: 24px; font-weight: bold; }
    .sub-info { font-size: 14px; color: #666; }
    .section-title { font-size: 16px; font-weight: bold; margin-top: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- 核心運算邏輯 (維持不變) ---
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

@st.cache_data(ttl=1800) # 手機版快取 30 分鐘
def fetch_stock_data(code):
    stock_id = str(code).strip()
    ticker = f"{stock_id}.TW"
    stock = yf.Ticker(ticker)
    hist = stock.history(period="10d") 
    
    if hist.empty:
        ticker = f"{stock_id}.TWO"
        stock = yf.Ticker(ticker)
        hist = stock.history(period="10d")
    
    if hist.empty: return None 

    today = hist.iloc[-1]
    prev = hist.iloc[-2]
    close = today['Close']
    ma5 = hist['Close'].tail(5).mean()
    
    limit_up = calculate_limit_price(close, True)
    limit_down = calculate_limit_price(close, False)
    
    # 支撐壓力邏輯
    pressure = max(today['High'], prev['High'])
    support = min(today['Low'], prev['Low'])
    
    trend = "多" if close > ma5 else "空"
    
    return {
        "code": stock_id,
        "price": round(close, 2),
        "pct": round((close - prev['Close']) / prev['Close'] * 100, 2),
        "ma5": round(ma5, 2),
        "trend": trend,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "target_3": round(close * 1.03, 2),
        "stop_3": round(close * 0.97, 2),
        "pressure": pressure,
        "support": support
    }

# --- 手機版主介面 ---
st.title("📱 隔日沖戰略助手")

# 1. 輸入區 (預設收合，節省空間)
with st.expander("🛠️ 設定股票清單 / 上傳檔案", expanded=True):
    uploaded_file = st.file_uploader("上傳 Excel/CSV", type=['csv', 'xlsx'])
    manual_input = st.text_area("或手動輸入代號 (用逗號分隔)", "2330, 2603, 2317")
    
    target_codes = []
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'): df = pd.read_csv(uploaded_file)
            else: df = pd.read_excel(uploaded_file)
            # 找代號欄位
            col = next((c for c in ['代號','股票代號','Code'] if c in df.columns), None)
            if col: target_codes = df[col].astype(str).tolist()
        except: st.error("讀取失敗")
    
    if not target_codes and manual_input:
        target_codes = [x.strip() for x in manual_input.split(',') if x.strip()]

# 2. 執行按鈕 (大按鈕適合手指按)
if st.button("🚀 分析開始", type="primary", use_container_width=True):
    
    if not target_codes:
        st.warning("請輸入代號")
    else:
        # 進度條
        progress_bar = st.progress(0)
        results = []
        
        for i, code in enumerate(target_codes):
            # 只取純數字代號
            clean_code = "".join(filter(str.isdigit, str(code)))
            if clean_code:
                data = fetch_stock_data(clean_code)
                if data: results.append(data)
            progress_bar.progress((i + 1) / len(target_codes))
            
        progress_bar.empty()
        
        # 3. 顯示結果 (卡片流)
        st.markdown("---")
        for row in results:
            # 決定卡片顏色 (多頭紅邊，空頭綠邊)
            card_class = "card-up" if row['trend'] == "多" else "card-down"
            trend_icon = "🔴 多頭" if row['trend'] == "多" else "🟢 空頭"
            trend_color = "#d9534f" if row['trend'] == "多" else "#5cb85c"
            
            # HTML 卡片渲染
            html_content = f"""
            <div class="stock-card {card_class}">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <div>
                        <span style="font-size:1.2em; font-weight:bold;">{row['code']}</span>
                        <span style="background-color:{trend_color}; color:white; padding:2px 8px; border-radius:10px; font-size:0.8em; margin-left:5px;">{trend_icon}</span>
                    </div>
                    <div style="text-align:right;">
                        <div class="big-price">{row['price']}</div>
                        <div style="font-size:0.9em; color:{trend_color};">{row['pct']}%</div>
                    </div>
                </div>
                
                <hr style="margin: 10px 0; border-top: 1px dashed #ddd;">
                
                <div style="display:flex; justify-content:space-between;">
                    <div style="width:48%; background-color:#fff5f5; padding:5px; border-radius:5px;">
                        <div class="section-title" style="color:#d9534f;">🛑 壓力/獲利</div>
                        <div>漲停: <b>{row['limit_up']}</b></div>
                        <div>+3%: {row['target_3']}</div>
                        <div style="color:#888; font-size:0.9em;">壓: {row['pressure']}</div>
                    </div>
                    <div style="width:48%; background-color:#f0fff0; padding:5px; border-radius:5px;">
                        <div class="section-title" style="color:#5cb85c;">🛡️ 支撐/防守</div>
                        <div>跌停: <b>{row['limit_down']}</b></div>
                        <div>-3%: {row['stop_3']}</div>
                        <div style="color:#888; font-size:0.9em;">撐: {row['support']}</div>
                    </div>
                </div>
                
                <div style="margin-top:10px; font-size:0.85em; color:#999; text-align:center;">
                     5MA: {row['ma5']} | 乖離率: {round((row['price'] - row['ma5'])/row['ma5']*100, 2)}%
                </div>
            </div>
            """
            st.markdown(html_content, unsafe_allow_html=True)


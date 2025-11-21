import streamlit as st
import pandas as pd
import yfinance as yf
import math

# --- 1. 頁面設定 (強制寬版以容納表格) ---
st.set_page_config(page_title="選股戰略表格", page_icon="📊", layout="wide")

# --- 2. 運算核心 (維持不變) ---
def get_tick_size(price):
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def calculate_limit(price, is_up=True):
    target = price * 1.10 if is_up else price * 0.90
    tick = get_tick_size(price)
    steps = math.floor(target / tick) if is_up else math.ceil(target / tick)
    return float(f"{steps * tick:.2f}")

@st.cache_data(ttl=300)
def get_stock_data(code, name=""):
    code = str(code).strip().split('.')[0]
    if not code.isdigit(): return None
    
    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist = ticker.history(period="10d")
        if hist.empty:
            ticker = yf.Ticker(f"{code}.TWO")
            hist = ticker.history(period="10d")
        
        if hist.empty: return None

        # 取得數據
        today = hist.iloc[-1]
        prev = hist.iloc[-2]
        close = today['Close']
        ma5 = hist['Close'].tail(5).mean()
        
        # 運算邏輯
        pressure = max(today['High'], prev['High']) # 壓力：取高點最大值
        support = min(today['Low'], prev['Low'])    # 支撐：取低點最小值
        
        return {
            "代號": code,
            "名稱": name if name else code,
            "現價": round(close, 2),
            "漲跌幅%": round((close - prev['Close']) / prev['Close'] * 100, 2),
            "趨勢(5MA)": "多 (撐)" if close > ma5 else "空 (壓)",
            "5MA價": round(ma5, 2),
            "壓力(昨/今高)": round(pressure, 2),
            "支撐(昨/今低)": round(support, 2),
            "獲利(+3%)": round(close * 1.03, 2),
            "停損(-3%)": round(close * 0.97, 2),
            "漲停價": calculate_limit(close, True),
            "跌停價": calculate_limit(close, False)
        }
    except:
        return None

# --- 3. 網站主介面 ---
st.title("📊 戰略選股總表")

# 側邊欄：輸入與設定
with st.sidebar:
    st.header("1. 資料輸入")
    # 頁籤：單股 vs 檔案
    mode = st.radio("選擇模式", ["🔍 單股搜尋", "📂 上傳檔案 (週轉率)"])
    
    target_codes = []
    
    if mode == "🔍 單股搜尋":
        user_input = st.text_input("輸入代號 (如 2330, 2603)", "")
        if user_input:
            # 支援輸入多個代號用逗號分開
            target_codes = [(c.strip(), "") for c in user_input.replace('，',',').split(',') if c.strip()]
            
    elif mode == "📂 上傳檔案 (週轉率)":
        uploaded_file = st.file_uploader("上傳 CSV / Excel", type=['csv', 'xlsx'])
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'): df = pd.read_csv(uploaded_file)
                else: df = pd.read_excel(uploaded_file)
                
                # 自動找代號與名稱欄位
                c_col = next((c for c in ['代號','股票代號'] if c in df.columns), None)
                n_col = next((c for c in ['名稱','股票名稱'] if c in df.columns), None)
                
                if c_col:
                    for _, row in df.iterrows():
                        c = str(row[c_col]).split('.')[0]
                        n = str(row[n_col]) if n_col else ""
                        if c.isdigit(): target_codes.append((c, n))
                else:
                    st.error("❌ 找不到「代號」欄位")
            except:
                st.error("❌ 檔案讀取失敗")

    run_btn = st.button("🚀 開始分析", type="primary")

# --- 4. 顯示結果 (表格模式) ---
if run_btn and target_codes:
    results = []
    bar = st.progress(0)
    
    for i, (code, name) in enumerate(target_codes):
        data = get_stock_data(code, name)
        if data: results.append(data)
        bar.progress((i + 1) / len(target_codes))
        
    bar.empty() # 隱藏進度條

    if results:
        df_res = pd.DataFrame(results)
        
        # 設定表格顯示格式
        st.subheader(f"📋 分析結果 ({len(df_res)} 筆)")
        
        # 使用 Streamlit 互動式表格
        st.dataframe(
            df_res,
            column_config={
                "代號": st.column_config.TextColumn("代號", width="small"),
                "現價": st.column_config.NumberColumn("現價", format="$%.2f"),
                "漲跌幅%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
                "獲利(+3%)": st.column_config.NumberColumn("獲利(+3%)", format="$%.2f"),
                "停損(-3%)": st.column_config.NumberColumn("停損(-3%)", format="$%.2f"),
                "趨勢(5MA)": st.column_config.TextColumn("趨勢", width="small"),
            },
            use_container_width=True, # 手機上自動填滿寬度
            hide_index=True # 隱藏索引列
        )
        
        st.info("💡 提示：手機橫放可查看更多欄位，點擊欄位名稱可排序。")
        
    else:
        st.warning("⚠️ 查無資料，請檢查代號或網路。")
        
elif not target_codes and run_btn:
    st.warning("請先輸入代號或上傳檔案。")
else:
    st.markdown("""
    ### 👋 歡迎使用
    請由左側 (手機按左上角 `>`) 選擇：
    1. **輸入代號**：快速查詢。
    2. **上傳檔案**：整批計算。
    """)

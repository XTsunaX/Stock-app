import streamlit as st
import pandas as pd
import yfinance as yf
import math
from datetime import datetime, timedelta

# 設定頁面配置
st.set_page_config(layout="wide", page_title="台股戰略分析系統")

# --- 輔助函式：計算台股漲跌停價 (含升降單位) ---
def get_tick_size(price):
    if price < 10:
        return 0.01
    elif price < 50:
        return 0.05
    elif price < 100:
        return 0.1
    elif price < 500:
        return 0.5
    elif price < 1000:
        return 1.0
    else:
        return 5.0

def calculate_limit_prices(ref_price):
    # 台股漲跌幅限制為 10%
    limit_up = ref_price * 1.10
    limit_down = ref_price * 0.90
    
    # 根據價格區間調整 tick (簡易模擬，實際需針對每一檔位精確計算，此為通用邏輯)
    # 這裡為了簡化，直接無條件捨去/進位到小數點後兩位，實際交易需配合 Tick 規則
    # 為了精確判定 "觸及漲停"，我們使用寬鬆比對或簡單四捨五入
    return limit_down, limit_up

# --- 核心邏輯函式 ---
def analyze_stock_data(ticker_symbol):
    try:
        # 加上 .TW 後綴
        full_ticker = f"{ticker_symbol}.TW" if not ticker_symbol.endswith('.TW') else ticker_symbol
        stock = yf.Ticker(full_ticker)
        
        # 抓取足夠長的歷史數據以計算 MA 和 240日高點
        df = stock.history(period="2y")
        
        if df.empty:
            return None

        # --- 數據前處理 ---
        # 計算移動平均線
        df['MA5'] = df['Close'].rolling(window=5).mean()
        df['MA10'] = df['Close'].rolling(window=10).mean()
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA60'] = df['Close'].rolling(window=60).mean()
        
        # 計算區間高點 (不含今日，用於比較) - 模擬盤中邏輯，我們看當下
        # 這裡計算過去 240 天(包含今日)的最高價
        df['High_240'] = df['High'].rolling(window=240).max()
        
        # 取得最新一筆資料 (今日)
        today_data = df.iloc[-1]
        prev_close = df.iloc[-2]['Close'] # 昨日收盤價 (作為今日參考價)
        
        # 計算今日的漲停價 (用於判斷邏輯3)
        _, today_limit_up = calculate_limit_prices(prev_close)
        
        # 格式化數值
        current_price = today_data['Close']
        open_price = today_data['Open']
        high_price = today_data['High']
        volume = today_data['Volume']
        
        # --- 戰略備註邏輯生成 ---
        remarks = []
        next_day_refs = []
        
        # ---------------------------------------------------------
        # 邏輯 1: 隔日參考數值 (盤後將「今日開盤價」加入，需符合隔日範圍)
        # ---------------------------------------------------------
        # 計算"隔日"的漲跌停範圍 (基於今日收盤價)
        next_limit_down, next_limit_up = calculate_limit_prices(current_price)
        
        # 加入既有的參考 (範例: MA, +/- 3%)
        # 這裡保留您之前的 +3% / -3% 顯示邏輯
        three_pct_up = current_price * 1.03
        three_pct_down = current_price * 0.97
        next_day_refs.append(f"+3%: {three_pct_up:.2f}")
        next_day_refs.append(f"-3%: {three_pct_down:.2f}")
        
        # 檢查今日開盤價是否在隔日漲跌停範圍內
        # 注意：比較時需考慮浮點數誤差，或直接比較數值
        if next_limit_down <= open_price <= next_limit_up:
            next_day_refs.append(f"開盤: {open_price:.2f}")
            
        # ---------------------------------------------------------
        # 邏輯 2 & 3: 特殊型態與漲停高判斷
        # ---------------------------------------------------------
        
        # 判斷是否為 240 日新高
        # 若今日最高價 >= 過去240日最高價 (或是該欄位就是今日最高)
        is_new_high_240 = high_price >= today_data['High_240']
        
        # 判斷是否觸及今日漲停 (允許微小誤差，或直接比對價格)
        # 這裡假設如果 High >= 參考價 * 1.095 (接近10%) 視為觸及，或嚴格使用計算出的 limit_up
        # 為了精確，我們檢查 High 是否非常接近 calculate_limit_prices 算出的上限
        is_limit_up_touched = high_price >= (prev_close * 1.095) 
        
        # 判斷收盤是否未鎖漲停
        is_not_locked = current_price < high_price # 收盤價小於最高價 (代表打開了)
        
        high_label = ""
        
        if is_new_high_240:
            # 邏輯修正：如果觸及漲停 且 未鎖死 且 創新高 -> 顯示 "漲停高"
            if is_limit_up_touched and is_not_locked:
                high_label = f"漲停高{int(high_price)}" # 根據要求顯示整數或小數，這裡範例用整數
            else:
                high_label = f"高{int(high_price)}"
            
            remarks.append(high_label)

        # 簡單的大量判斷 (範例邏輯：大於5日均量2倍)
        vol_ma5 = df['Volume'].iloc[-6:-1].mean()
        if volume > vol_ma5 * 2:
            remarks.append("大量")

        # 組合結果
        strategy_note = " ".join(remarks)
        next_ref_note = " | ".join(next_day_refs)
        
        return {
            "代號": ticker_symbol,
            "收盤價": f"{current_price:.2f}",
            "漲跌幅": f"{(current_price - prev_close) / prev_close * 100:.2f}%",
            "戰略備註": strategy_note,
            "隔日參考": next_ref_note
        }

    except Exception as e:
        return {"代號": ticker_symbol, "錯誤": str(e)}

# --- Streamlit 介面 ---
st.title("台股戰略備註生成器")

st.sidebar.header("設定")
input_tickers = st.sidebar.text_area("輸入股票代號 (用逗號分隔)", "2330, 3167, 2317")

if st.button("開始分析"):
    tickers = [t.strip() for t in input_tickers.split(",")]
    results = []
    
    progress_bar = st.progress(0)
    
    for i, ticker in enumerate(tickers):
        if ticker:
            data = analyze_stock_data(ticker)
            if data:
                results.append(data)
        progress_bar.progress((i + 1) / len(tickers))
        
    if results:
        res_df = pd.DataFrame(results)
        # 調整欄位順序
        cols = ["代號", "收盤價", "漲跌幅", "戰略備註", "隔日參考"]
        # 確保錯誤欄位存在時也能顯示
        if "錯誤" in res_df.columns:
            cols.append("錯誤")
            
        final_df = res_df[cols]
        
        st.subheader("分析結果")
        st.dataframe(final_df, use_container_width=True)
        
        # 顯示邏輯說明 (供使用者驗證)
        st.info("""
        **邏輯更新說明：**
        1. **隔日參考**：已加入「今日開盤價」，僅當其數值位於隔日預估漲跌停範圍內時顯示。
        2. **漲停高**：若股價創新高(240日)，且盤中曾觸及漲停但收盤未鎖死，備註顯示為「漲停高」。
        """)
    else:
        st.warning("查無資料")

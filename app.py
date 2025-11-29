import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

# --- 頁面設定 ---
st.set_page_config(
    page_title="專業股票分析儀表板",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 輔助函式：載入股票清單 ---
@st.cache_data
def load_stock_names(csv_path='stock_names.csv'):
    """
    讀取股票代碼 CSV 檔案。
    假設格式包含 'Symbol' 和 'Name' 欄位，如果沒有表頭則嘗試自動偵測。
    """
    try:
        # 嘗試讀取，假設有 header
        df = pd.read_csv(csv_path)
        
        # 簡單的欄位標準化處理
        df.columns = [c.strip().lower() for c in df.columns]
        
        # 尋找代碼與名稱欄位
        symbol_col = next((c for c in df.columns if 'symbol' in c or 'ticker' in c or 'code' in c), None)
        name_col = next((c for c in df.columns if 'name' in c), None)
        
        if symbol_col:
            # 建立顯示用的標籤
            if name_col:
                df['display'] = df[symbol_col].astype(str) + " - " + df[name_col].astype(str)
            else:
                df['display'] = df[symbol_col].astype(str)
            return df, symbol_col
        else:
            # 找不到代碼欄位，回傳空
            st.error("CSV 檔案中找不到股票代碼欄位 (需包含 Symbol, Ticker 或 Code)。")
            return pd.DataFrame(), None
            
    except FileNotFoundError:
        st.warning("找不到 stock_names.csv，將使用預設熱門股票。")
        # 預設資料
        data = {
            'symbol': ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'NVDA', '2330.TW'],
            'name': ['Apple', 'Alphabet', 'Microsoft', 'Tesla', 'NVIDIA', '台積電']
        }
        df = pd.DataFrame(data)
        df['display'] = df['symbol'] + " - " + df['name']
        return df, 'symbol'

# --- 輔助函式：下載股票數據 ---
@st.cache_data(ttl=3600)  # 快取 1 小時
def get_stock_data(ticker, start_date, end_date):
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        if data.empty:
            return None
        return data
    except Exception as e:
        st.error(f"下載數據時發生錯誤: {e}")
        return None

# --- 輔助函式：取得公司資訊 ---
@st.cache_data(ttl=86400) # 快取 1 天
def get_company_info(ticker):
    try:
        stock = yf.Ticker(ticker)
        return stock.info
    except:
        return {}

# --- 主介面邏輯 ---
def main():
    # 側邊欄：控制面板
    with st.sidebar:
        st.title("⚙️ 設定面板")
        
        # 1. 股票選擇
        df_stocks, symbol_col = load_stock_names()
        
        selected_stock_label = None
        ticker = "AAPL" # Default
        
        if not df_stocks.empty and symbol_col:
            selected_stock_label = st.selectbox("選擇股票", df_stocks['display'])
            ticker = df_stocks[df_stocks['display'] == selected_stock_label][symbol_col].values[0]
        else:
            ticker = st.text_input("輸入股票代碼 (例如: AAPL, 2330.TW)", value="AAPL")

        # 2. 日期選擇
        st.divider()
        st.subheader("📅 時間範圍")
        col_date1, col_date2 = st.columns(2)
        start_date = col_date1.date_input("開始", datetime.now() - timedelta(days=365))
        end_date = col_date2.date_input("結束", datetime.now())
        
        # 3. 技術指標
        st.divider()
        st.subheader("📊 技術指標")
        show_ma_50 = st.checkbox("SMA 50 (50日均線)", value=True)
        show_ma_200 = st.checkbox("SMA 200 (200日均線)")
        show_bb = st.checkbox("Bollinger Bands (布林通道)")
        show_volume = st.checkbox("Volume (成交量)", value=True)

    # 主畫面內容
    st.title(f"📈 {ticker} 股價分析")

    # 獲取數據
    data = get_stock_data(ticker, start_date, end_date)
    info = get_company_info(ticker)

    if data is not None:
        # --- 頂部指標卡片 (Metrics) ---
        latest_price = data['Close'].iloc[-1]
        if isinstance(latest_price, pd.Series): # 處理 yfinance 可能返回 Series 的情況
            latest_price = latest_price.item()
            
        previous_price = data['Close'].iloc[-2]
        if isinstance(previous_price, pd.Series):
            previous_price = previous_price.item()

        delta = latest_price - previous_price
        delta_percent = (delta / previous_price) * 100

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("最新收盤價", f"{latest_price:,.2f}", f"{delta:+.2f} ({delta_percent:+.2f}%)")
        with col2:
            high_52 = info.get('fiftyTwoWeekHigh', 'N/A')
            st.metric("52週最高", f"{high_52}")
        with col3:
            low_52 = info.get('fiftyTwoWeekLow', 'N/A')
            st.metric("52週最低", f"{low_52}")
        with col4:
            volume = data['Volume'].iloc[-1]
            if isinstance(volume, pd.Series): volume = volume.item()
            st.metric("成交量", f"{volume:,.0f}")

        st.divider()

        # --- Plotly 互動式圖表 ---
        # 建立子圖 (若有成交量，則分為上下兩塊)
        if show_volume:
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                                vertical_spacing=0.05, row_heights=[0.7, 0.3],
                                subplot_titles=(f'{ticker} 股價走勢', '成交量'))
        else:
            fig = make_subplots(rows=1, cols=1)

        # 1. K線圖 (Candlestick)
        fig.add_trace(go.Candlestick(
            x=data.index,
            open=data['Open'], high=data['High'],
            low=data['Low'], close=data['Close'],
            name='K線'
        ), row=1, col=1)

        # 2. 技術指標疊加
        if show_ma_50:
            ma50 = data['Close'].rolling(window=50).mean()
            fig.add_trace(go.Scatter(x=data.index, y=ma50, line=dict(color='orange', width=1.5), name='SMA 50'), row=1, col=1)
        
        if show_ma_200:
            ma200 = data['Close'].rolling(window=200).mean()
            fig.add_trace(go.Scatter(x=data.index, y=ma200, line=dict(color='purple', width=1.5), name='SMA 200'), row=1, col=1)

        if show_bb:
            window = 20
            ma = data['Close'].rolling(window).mean()
            std = data['Close'].rolling(window).std()
            upper = ma + (2 * std)
            lower = ma - (2 * std)
            
            fig.add_trace(go.Scatter(x=data.index, y=upper, line=dict(color='rgba(173, 216, 230, 0.5)'), name='BB Upper', showlegend=False), row=1, col=1)
            fig.add_trace(go.Scatter(x=data.index, y=lower, line=dict(color='rgba(173, 216, 230, 0.5)'), fill='tonexty', fillcolor='rgba(173, 216, 230, 0.1)', name='Bollinger Bands'), row=1, col=1)

        # 3. 成交量 (Bar)
        if show_volume:
            colors = ['red' if row['Open'] - row['Close'] >= 0 else 'green' for index, row in data.iterrows()]
            fig.add_trace(go.Bar(x=data.index, y=data['Volume'], marker_color=colors, name='Volume'), row=2, col=1)

        # 圖表佈局設定
        fig.update_layout(
            height=600,
            xaxis_rangeslider_visible=False,
            template="plotly_dark", # 使用深色主題
            margin=dict(l=20, r=20, t=50, b=20),
            legend=dict(orientation="h", y=1.02, yanchor="bottom", x=0, xanchor="left")
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- 頁籤區域：公司資訊與數據 ---
        tab1, tab2 = st.tabs(["📋 公司簡介", "🔢 歷史數據"])
        
        with tab1:
            if info:
                st.subheader(info.get('longName', ticker))
                col_info1, col_info2 = st.columns([2, 1])
                with col_info1:
                    st.info(info.get('longBusinessSummary', '無公司簡介資訊。'))
                with col_info2:
                    st.write(f"**產業:** {info.get('industry', 'N/A')}")
                    st.write(f"**板塊:** {info.get('sector', 'N/A')}")
                    st.write(f"**市值:** {info.get('marketCap', 'N/A'):,}")
                    st.write(f"**本益比 (PE):** {info.get('trailingPE', 'N/A')}")
                    st.write(f"**股息率:** {info.get('dividendYield', 0)*100:.2f}%" if info.get('dividendYield') else "**股息率:** N/A")
            else:
                st.write("無法取得公司詳細資訊。")

        with tab2:
            st.dataframe(data.sort_index(ascending=False), use_container_width=True)
            # CSV 下載按鈕
            csv = data.to_csv().encode('utf-8')
            st.download_button(
                label="📥 下載 CSV 數據",
                data=csv,
                file_name=f'{ticker}_data.csv',
                mime='text/csv',
            )

    else:
        st.info("請從左側選擇股票或輸入代碼以開始分析。")

if __name__ == "__main__":
    main()

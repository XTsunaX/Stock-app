import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import math

# --- 1. 頁面設定 ---
st.set_page_config(page_title="戰略選股表格", page_icon="📊", layout="wide")

# --- 2. 核心功能：抓取中文股名 ---
@st.cache_data(ttl=86400) # 快取一天，不用每次都抓
def get_tw_stock_name(code):
    """從 Yahoo 股市頁面標題抓取中文名稱"""
    try:
        # 嘗試上市
        url = f"https://tw.stock.yahoo.com/quote/{code}.TW"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=3)
        if "404" in r.text or "查無" in r.text:
             # 嘗試上櫃
            url = f"https://tw.stock.yahoo.com/quote/{code}.TWO"
            r = requests.get(url, headers=headers, timeout=3)
        
        # 簡單解析 HTML <title>台積電(2330)...</title>
        if "<title>" in r.text:
            start = r.text.find("<title>") + 7
            end = r.text.find("</title>")
            title = r.text[start:end]
            # 取括號前的文字
            name = title.split('(')[0].strip()
            # 再次確認是否抓到怪怪的東西
            if len(name) > 10 or "Yahoo" in name: 
                return str(code)
            return name
        return str(code)
    except:
        return str(code)

# --- 3. 核心功能：數據運算 ---
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

@st.cache_data(ttl=60) # 股價快取 60 秒
def get_stock_data(code, custom_note=None, custom_name=None):
    code = str(code).strip().split('.')[0]
    if not code.isdigit(): return None
    
    try:
        ticker = yf.Ticker(f"{code}.TW")
        hist = ticker.history(period="5d") # 抓5天確保有資料
        if hist.empty:
            ticker = yf.Ticker(f"{code}.TWO")
            hist = ticker.history(period="5d")
        
        if hist.empty: return None

        # 取得最新一筆數據
        today = hist.iloc[-1]
        prev = hist.iloc[-2]
        close = today['Close']
        ma5 = hist['Close'].tail(5).mean()
        
        # 運算邏輯
        pressure = max(today['High'], prev['High']) # 壓力
        support = min(today['Low'], prev['Low'])    # 支撐
        
        limit_up = calculate_limit(close, True)
        
        # 名稱處理：如果有傳入(來自檔案)就用，沒有就上網抓
        stock_name = custom_name if custom_name else get_tw_stock_name(code)
        
        # 備註處理：如果有傳入(來自檔案)就用，沒有就自動生成
        if custom_note and str(custom_note) != "nan":
            note_str = str(custom_note)
        else:
            # 自動生成戰略路徑: 支撐 -> 5MA -> 現價 -> 壓力 -> 漲停
            trend_mark = "多" if close > ma5 else "空"
            note_str = f"📉{support:.1f} ⮕ 5MA:{ma5:.1f}({trend_mark}) ⮕ 🛑{pressure:.1f} ⮕ 漲停{limit_up}"

        return {
            "代號": code,
            "名稱": stock_name,
            "現價 (即時/收盤)": round(close, 2),
            "戰略備註 (撐-壓-漲停)": note_str,  # 新增的備註欄位
            "漲跌幅%": round((close - prev['Close']) / prev['Close'] * 100, 2),
            "獲利目標(+3%)": round(close * 1.03, 2),
            "防守停損(-3%)": round(close * 0.97, 2),
            "趨勢": "多" if close > ma5 else "空",
            "5MA": round(ma5, 2),
            "壓力": round(pressure, 2),
            "支撐": round(support, 2)
        }
    except Exception as e:
        return None

# --- 4. 網站主介面 ---
st.title("📊 戰略選股總表 (即時版)")

# 側邊欄
with st.sidebar:
    st.header("設定")
    mode = st.radio("模式選擇", ["🔍 單股搜尋", "📂 上傳檔案"])
    
    target_codes = []
    
    if mode == "🔍 單股搜尋":
        user_input = st.text_input("輸入代號 (如 2330, 2603)", "")
        if user_input:
            # 處理輸入格式
            codes = [c.strip() for c in user_input.replace('，',',').split(',') if c.strip()]
            for c in codes:
                target_codes.append((c, None, None)) # (代號, 備註, 名稱)
            
    elif mode == "📂 上傳檔案":
        uploaded_file = st.file_uploader("上傳 Excel/CSV", type=['csv', 'xlsx'])
        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'): df = pd.read_csv(uploaded_file)
                else: df = pd.read_excel(uploaded_file)
                
                # 智慧欄位對應
                c_col = next((c for c in ['代號','股票代號'] if c in df.columns), None)
                n_col = next((c for c in ['名稱','股票名稱'] if c in df.columns), None)
                
                # 尋找備註欄位 (通常是長字串那欄，或叫 '撐/壓', '備註')
                note_col = next((c for c in ['撐/壓', '備註', '說明', 'Notes'] if c in df.columns), None)
                
                if c_col:
                    for _, row in df.iterrows():
                        c = str(row[c_col]).split('.')[0]
                        n = str(row[n_col]) if n_col else ""
                        note = row[note_col] if note_col else None
                        if c.isdigit(): 
                            target_codes.append((c, note, n))
                else:
                    st.error("❌ 找不到「代號」欄位")
            except:
                st.error("❌ 檔案讀取失敗")

    run_btn = st.button("🚀 開始分析", type="primary")

# --- 5. 顯示結果 ---
if run_btn and target_codes:
    results = []
    bar = st.progress(0)
    
    for i, (code, note, name) in enumerate(target_codes):
        data = get_stock_data(code, note, name)
        if data: results.append(data)
        bar.progress((i + 1) / len(target_codes))
        
    bar.empty()

    if results:
        df_res = pd.DataFrame(results)
        
        st.subheader(f"📋 分析結果 ({len(df_res)} 筆)")
        
        # 設定表格樣式
        st.dataframe(
            df_res,
            column_config={
                "代號": st.column_config.TextColumn("代號", width="small"),
                "名稱": st.column_config.TextColumn("名稱", width="small"),
                "現價 (即時/收盤)": st.column_config.NumberColumn("現價", format="$%.2f"),
                "戰略備註 (撐-壓-漲停)": st.column_config.TextColumn("戰略備註", width="large"), # 設定寬度大一點
                "漲跌幅%": st.column_config.NumberColumn("漲跌%", format="%.2f%%"),
                "獲利目標(+3%)": st.column_config.NumberColumn("獲利(+3%)", format="$%.2f"),
                "防守停損(-3%)": st.column_config.NumberColumn("停損(-3%)", format="$%.2f"),
            },
            use_container_width=True,
            hide_index=True
        )
        
        # 手機版提示
        st.caption("💡 手機版：請左右滑動表格以查看「戰略備註」與更多欄位。")
        
    else:
        st.warning("⚠️ 查無資料，請確認代號或網路連線。")
        
elif not target_codes and run_btn:
    st.warning("請輸入代號或上傳檔案。")

import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import math
import time
import os
import itertools
import json

# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="當沖戰略室", page_icon="⚡", layout="wide")

# 1. 標題
st.title("⚡ 當沖戰略室 ⚡")

CONFIG_FILE = "config.json"

def load_config():
    """讀取設定檔"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_config(font_size, limit_rows):
    """儲存設定檔"""
    try:
        config = {"font_size": font_size, "limit_rows": limit_rows}
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f)
        return True
    except:
        return False

# --- 初始化 Session State ---
if 'stock_data' not in st.session_state:
    st.session_state.stock_data = pd.DataFrame()

# 計算機用的 Session State
if 'calc_base_price' not in st.session_state:
    st.session_state.calc_base_price = 100.0

# 優先從設定檔讀取
saved_config = load_config()

if 'font_size' not in st.session_state:
    st.session_state.font_size = saved_config.get('font_size', 18)

if 'limit_rows' not in st.session_state:
    st.session_state.limit_rows = saved_config.get('limit_rows', 5)

# --- 側邊欄設定 ---
with st.sidebar:
    st.header("⚙️ 設定")
    
    current_font_size = st.slider(
        "字體大小 (表格)", 
        min_value=12, 
        max_value=72, 
        key='font_size'
    )
    
    hide_etf = st.checkbox("隱藏 ETF (00開頭)", value=True)
    st.markdown("---")
    
    current_limit_rows = st.number_input(
        "顯示筆數", 
        min_value=1, 
        key='limit_rows'
    )
    
    if st.button("💾 儲存設定"):
        if save_config(current_font_size, current_limit_rows):
            st.toast("設定已儲存！下次開啟將自動套用。", icon="✅")
        else:
            st.error("設定儲存失敗。")
    
    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n勾選左側框框後按 `Delete` 鍵。")

# --- 動態 CSS ---
font_px = f"{st.session_state.font_size}px"

st.markdown(f"""
    <style>
    .block-container {{ padding-top: 4.5rem; padding-bottom: 1rem; }}
    
    /* 套用到所有 Streamlit 表格相關元素 */
    div[data-testid="stDataFrame"] table,
    div[data-testid="stDataFrame"] td,
    div[data-testid="stDataFrame"] th,
    div[data-testid="stDataFrame"] input,
    div[data-testid="stDataFrame"] div,
    div[data-testid="stDataFrame"] span {{
        font-size: {font_px} !important;
        font-family: 'Microsoft JhengHei', sans-serif !important;
        line-height: 1.5 !important;
    }}
    
    div[data-testid="stDataFrame"] {{
        width: 100%;
    }}
    
    /* 讓計算機的 Metric 顯示大一點 */
    [data-testid="stMetricValue"] {{
        font-size: 1.2em;
    }}
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 1. 資料庫與網路功能
# ==========================================

@st.cache_data
def load_local_stock_names():
    code_map = {}
    name_map = {}
    if os.path.exists("stock_names.csv"):
        try:
            df = pd.read_csv("stock_names.csv", header=None, names=["code", "name"], dtype=str)
            for _, row in df.iterrows():
                c = str(row['code']).strip()
                n = str(row['name']).strip()
                code_map[c] = n
                name_map[n] = c
        except Exception as e:
            pass
    return code_map, name_map

@st.cache_data(ttl=86400)
def get_stock_name_online(code):
    code = str(code).strip()
    if not code.isdigit(): return code
    code_map, _ = load_local_stock_names()
    if code in code_map: return code_map[code]
    try:
        url = f"https://tw.stock.yahoo.com/quote/{code}.TW"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=2)
        soup = BeautifulSoup(r.text, "html.parser")
        if soup.title and "(" in soup.title.string:
            return soup.title.string.split('(')[0].strip()
        url_two = f"https://tw.stock.yahoo.com/quote/{code}.TWO"
        r_two = requests.get(url_two, headers=headers, timeout=2)
        soup_two = BeautifulSoup(r_two.text, "html.parser")
        if soup_two.title and "(" in soup_two.title.string:
            return soup_two.title.string.split('(')[0].strip()
        return code
    except:
        return code

@st.cache_data(ttl=86400)
def search_code_online(query):
    query = query

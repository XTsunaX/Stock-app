import streamlit as st
import pandas as pd
import requests
import io

# 這是專門用來抓取 Goodinfo 資料的獨立模組
# 設定快取 ttl=3600 (1小時)，避免頻繁連線被封鎖 IP
@st.cache_data(ttl=3600)
def fetch_goodinfo_ranking():
    """
    抓取 Goodinfo 台灣股市資訊網的「成交量週轉率排行(當日)」
    """
    # 目標網址
    url = "https://goodinfo.tw/tw/StockList.asp?RPT_TIME=&MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E7%B4%AF%E8%A8%88%E6%88%90%E4%BA%A4%E9%87%8F%E9%80%B1%E8%BD%89%E7%8E%87%28%E7%95%B6%E6%97%A5%29%40%40%E7%B4%AF%E8%A8%88%E6%88%90%E4%BA%A4%E9%87%8F%E9%80%B1%E8%BD%89%E7%8E%87%40%40%E7%95%B6%E6%97%A5"
    
    # 偽裝 headers，非常重要
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Referer": "https://goodinfo.tw/tw/StockList.asp"
    }
    
    try:
        # 發送請求
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = "utf-8"
        
        # 使用 Pandas 解析 HTML 表格
        dfs = pd.read_html(io.StringIO(res.text))
        
        # 篩選正確的表格 (找欄位最多的或是包含'代號'的)
        target_df = None
        for df in dfs:
            if "代號" in str(df.columns) and len(df) > 10:
                target_df = df
                break
        
        if target_df is not None:
            # 資料清理：處理多層索引、移除重複標題列
            target_df.columns = [c[-1] if isinstance(c, tuple) else c for c in target_df.columns]
            target_df = target_df[target_df['代號'] != '代號']
            target_df.reset_index(drop=True, inplace=True)
            
            # 只回傳我們需要的欄位：代號與名稱
            return target_df[['代號', '名稱']]
            
        return None
    except Exception as e:
        # 這裡可以選擇 print 錯誤，或是直接回傳 None
        print(f"Goodinfo 抓取錯誤: {e}")
        return None

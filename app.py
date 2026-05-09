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
import re
from datetime import datetime, time as dt_time, timedelta, date
import pytz
from decimal import Decimal, ROUND_HALF_UP
import io
import twstock
from concurrent.futures import ThreadPoolExecutor, as_completed
import calendar
import random
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import streamlit.components.v1 as components
import urllib3
import base64
import pdfplumber

# 關閉 SSL 驗證警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 引入 yahoo_fin 與 shioaji
try:
    import yahoo_fin.stock_info as si
except ImportError:
    si = None

try:
    import shioaji as sj
except ImportError:
    sj = None

# ==========================================
# 永豐 API (Shioaji) 擷取核心
# ==========================================
def fetch_shioaji_data(api, code, interval='1d', lookback_days=10):
    try:
        # 解析合約
        if code in ["^TWII", "加權指數", "TSE", "加權指數(^TWII)"]:
            contract = api.Contracts.Indices.TSE.TSE01
        elif code in ["TWF=F", "台指期貨", "TXF", "台指期貨(TWF=F)"]:
            contract = api.Contracts.Futures.TXF.TXFR1  # 台指期近月
        elif code in ["TMF=F", "微型台指期貨", "TMF", "微型台指", "微型台指期貨(TMF=F)"]:
            contract = api.Contracts.Futures.TMF.TMFR1  # 微型台指期近月
        else:
            try:
                contract = api.Contracts.Stocks[code]
            except:
                contract = None
        
        if not contract:
            return pd.DataFrame()
            
        tz_tw = pytz.timezone('Asia/Taipei')
        end_date = datetime.now(tz_tw).strftime("%Y-%m-%d")
        start_date = (datetime.now(tz_tw) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        
        # 獲取 K 線資料
        kbars = api.kbars(contract, start=start_date, end=end_date)
        if not kbars or not kbars.get('ts'):
            return pd.DataFrame()
            
        df = pd.DataFrame({**kbars})
        df['ts'] = pd.to_datetime(df['ts']).dt.tz_localize('Asia/Taipei').dt.tz_convert('Asia/Taipei').dt.tz_localize(None)
        df.set_index('ts', inplace=True)
        
        # 處理欄位大小寫，相容不同版本的 shioaji
        rename_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
        
        agg_dict = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        agg_dict = {k: v for k, v in agg_dict.items() if k in df.columns}
        
        # 依照要求的頻率重新取樣 (Resample)
        if interval == '1m':
            pass # 預設即為 1 分鐘
        elif interval == '1d':
            df = df.resample('D').agg(agg_dict).dropna()
        elif interval == '1wk':
            df = df.resample('W').agg(agg_dict).dropna()
        elif interval == '1mo':
            df = df.resample('M').agg(agg_dict).dropna()
        else:
            resample_map = {'5m': '5T', '15m': '15T', '60m': '60T'}
            if interval in resample_map:
                df = df.resample(resample_map[interval]).agg(agg_dict).dropna()
                
        return df
    except Exception as e:
        print(f"Shioaji fetch error for {code}: {e}")
        return pd.DataFrame()

# ==========================================
# 費波計算核心函數
# ==========================================
def get_taiwan_tick_size(price):
    if price < 10: return 0.01
    elif price < 50: return 0.05
    elif price < 100: return 0.1
    elif price < 500: return 0.5
    elif price < 1000: return 1
    else: return 5

def round_to_tick(price):
    tick = get_taiwan_tick_size(price)
    p_dec = Decimal(str(price))
    t_dec = Decimal(str(tick))
    rounded = (p_dec / t_dec).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * t_dec
    return float(rounded)

def plot_fibonacci_chart(symbol, interval, lookback=60, font_size=15, ma_flags=None, ma_width=1.5, show_vol=True):
    if ma_flags is None:
        ma_flags = {'5': True, '10': True, '20': True, '60': True}

    code_map_fibo, name_map_fibo = load_local_stock_names()
    
    # 處理輸入(支援名稱或代號)
    raw_input = symbol.strip()
    ticker_code = raw_input
    display_name = raw_input

    # 針對大盤與期貨的特例處理
    if raw_input in ["^TWII", "加權指數", "加權指數(^TWII)"]:
        ticker_code = "^TWII"
        display_name = "加權指數(^TWII)"
    elif raw_input in ["TWF=F", "台指期貨", "台指", "小型台指", "台指期貨(TWF=F)"]:
        ticker_code = "TWF=F"
        display_name = "台指期貨(TWF=F)"
    elif raw_input in ["TMF=F", "微型台指期貨", "微台", "微型台指", "微型台指期貨(TMF=F)"]:
        ticker_code = "TMF=F"
        display_name = "微型台指期貨(TMF=F)"
    else:
        if "(" in raw_input and raw_input.endswith(")"):
            name_part, code_part = raw_input.rsplit("(", 1)
            ticker_code = code_part[:-1]
            display_name = raw_input
        elif " " in raw_input:
            parts = raw_input.split(" ", 1)
            if parts[0].isdigit() or parts[0].endswith(".TW") or parts[0].endswith(".TWO"):
                ticker_code = parts[0]
                display_name = f"{parts[1]}({parts[0]})"
            elif parts[1].isdigit() or parts[1].endswith(".TW") or parts[1].endswith(".TWO"):
                ticker_code = parts[1]
                display_name = f"{parts[0]}({parts[1]})"
        else:
            if raw_input.isdigit():
                ticker_code = raw_input
                name = code_map_fibo.get(ticker_code, "")
                display_name = f"{name}({ticker_code})" if name else ticker_code
            else:
                if raw_input in name_map_fibo:
                    ticker_code = name_map_fibo[raw_input]
                    display_name = f"{raw_input}({ticker_code})"
                else:
                    ticker_code = raw_input

    ticker = ticker_code if (ticker_code.endswith(".TW") or ticker_code.endswith(".TWO") or ticker_code.startswith("^") or "=" in ticker_code) else f"{ticker_code}.TW"
    period_map = {"1m": "7d", "5m": "30d", "15m": "60d", "60m": "730d", "1d": "max", "1wk": "max", "1mo": "max"}
    is_index = ticker.startswith('^') or 'TWF' in ticker or 'TMF' in ticker
    
    try:
        df = pd.DataFrame()
        raw_code = ticker_code.split('.')[0]
        sj_kbars_used = False
        sj_snap_used = False
        twstock_used = False
        
        # 優先使用永豐 API 獲取盤中即時 K 線
        if st.session_state.get('sj_logged_in', False):
            # 依據 interval 設定合理的 lookback_days 避免 API Timeout
            days_needed = {"1m": 3, "5m": 7, "15m": 15, "60m": 45}
            
            if interval in days_needed:
                req_days = days_needed[interval]
                sj_df = fetch_shioaji_data(st.session_state.sj_api, raw_code, interval=interval, lookback_days=req_days)
                if not sj_df.empty:
                    df = sj_df
                    sj_kbars_used = True

        # 若永豐未登入、沒抓到、或請求區間過大 (如日、週、月K)，退回使用 yfinance 並用永豐即時更新最新一根K棒
        if not sj_kbars_used:
            stock_data = yf.Ticker(ticker)
            df = stock_data.history(interval=interval, period=period_map.get(interval, "max"))

            # 自動處理上櫃代號
            if (df.empty or 'High' not in df.columns) and ticker.endswith(".TW"):
                ticker_two = ticker.replace(".TW", ".TWO")
                stock_data = yf.Ticker(ticker_two)
                df = stock_data.history(interval=interval, period=period_map.get(interval, "max"))
                if not df.empty:
                    ticker = ticker_two 

            # 期貨異常保護：若完全無資料，以加權指數代替顯示以防報錯
            if (df.empty or 'High' not in df.columns) and (ticker == "TWF=F" or ticker == "TMF=F"):
                st.warning("⚠️ Yahoo Finance 目前缺少台指期貨歷史資料，已自動替換為加權指數(^TWII)作參考。")
                ticker = "^TWII"
                display_name = "加權指數(^TWII) *替代台指"
                is_index = True
                stock_data = yf.Ticker(ticker)
                df = stock_data.history(interval=interval, period=period_map.get(interval, "max"))
                
            # 將 YF 的個股成交量 (股) 統一轉換為 (張)
            if not df.empty and not is_index and 'Volume' in df.columns:
                df['Volume'] = df['Volume'] / 1000
                
        # 只要有登入永豐，一律透過 snapshots 即時快照更新圖表最後一筆資料 (確保大盤成交量與期貨夜盤報價是最新的)
        if st.session_state.get('sj_logged_in', False) and not df.empty:
            try:
                contract_snap = None
                if ticker.startswith("^TWII"):
                    contract_snap = st.session_state.sj_api.Contracts.Indices.TSE.TSE01
                elif ticker == "TWF=F":
                    contract_snap = st.session_state.sj_api.Contracts.Futures.TXF.TXFR1
                elif ticker == "TMF=F":
                    contract_snap = st.session_state.sj_api.Contracts.Futures.TMF.TMFR1
                else:
                    try: contract_snap = st.session_state.sj_api.Contracts.Stocks[raw_code]
                    except: pass
                
                if contract_snap:
                    snap = st.session_state.sj_api.snapshots([contract_snap])
                    if snap and len(snap) > 0:
                        s = snap[0]
                        rt_price = s.close
                        rt_open = s.open
                        rt_high = s.high
                        rt_low = s.low
                        
                        # 永豐 API snapshot 的 volume 是「單筆量」，total_volume 才是「累積總量(張)」
                        rt_vol = s.total_volume 
                        
                        if df.index.tzinfo is not None: df.index = df.index.tz_localize(None)
                        
                        # 確保時間是最新的
                        tz_tw = pytz.timezone('Asia/Taipei')
                        now_dt = datetime.now(tz_tw).replace(tzinfo=None)
                        if interval in ["1d", "1wk", "1mo"]:
                            now_dt = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                            
                        if df.index[-1] < now_dt and s.volume > 0:
                            # 建立新的一根 K 棒
                            new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[now_dt])
                            df = pd.concat([df, new_row])
                        else:
                            df.at[df.index[-1], 'Close'] = rt_price
                            df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_high)
                            df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_low)
                            if interval in ["1d", "1wk", "1mo"]:
                                df.at[df.index[-1], 'Volume'] = rt_vol
                            
                        sj_snap_used = True
            except Exception as e:
                pass
                
        # 完全無永豐資源下的 twstock 盤後修補方案 (避免 YF 收盤後延遲)
        if not sj_kbars_used and not sj_snap_used and not df.empty and not is_index and interval in ["1d", "1wk", "1mo"]:
            try:
                tz_tw = pytz.timezone('Asia/Taipei')
                now_tw = datetime.now(tz_tw)
                today_date = pd.Timestamp(now_tw.date())
                is_post_market = now_tw.time() >= dt_time(15, 0)
                
                rt_price = None
                rt_open = None
                rt_high = None
                rt_low = None
                rt_vol = 0.0

                rt_data = twstock.realtime.get(raw_code)
                if rt_data and rt_data.get('success') and rt_data['realtime']['latest_trade_price'] not in ['-', None, '']:
                    rt_price = float(rt_data['realtime']['latest_trade_price'])
                    rt_open = float(rt_data['realtime']['open']) if rt_data['realtime']['open'] != '-' else rt_price
                    rt_high = float(rt_data['realtime']['high']) if rt_data['realtime']['high'] != '-' else rt_price
                    rt_low = float(rt_data['realtime']['low']) if rt_data['realtime']['low'] != '-' else rt_price
                    rt_vol = float(rt_data['realtime']['accumulate_trade_volume']) if rt_data['realtime']['accumulate_trade_volume'] != '-' else 0.0
                
                if is_post_market and (rt_price is None or rt_price == 0):
                    stock = twstock.Stock(raw_code)
                    if len(stock.date) > 0 and stock.date[-1].date() == today_date.date():
                        rt_price = float(stock.price[-1])
                        rt_open = float(stock.open[-1])
                        rt_high = float(stock.high[-1])
                        rt_low = float(stock.low[-1])
                        rt_vol = float(stock.capacity[-1]) / 1000 # stock.capacity 是股數，轉為張

                if rt_price is not None:
                    if df.index.tzinfo is not None: df.index = df.index.tz_localize(None)
                    last_hist_date = pd.Timestamp(df.index[-1].date())
                    
                    if last_hist_date < today_date and now_tw.weekday() < 5:
                        new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                        df = pd.concat([df, new_row])
                    elif last_hist_date == today_date:
                        df.at[df.index[-1], 'Close'] = rt_price
                        df.at[df.index[-1], 'High'] = max(float(df['High'].iloc[-1]), rt_high)
                        df.at[df.index[-1], 'Low'] = min(float(df['Low'].iloc[-1]), rt_low)
                        df.at[df.index[-1], 'Volume'] = max(float(df['Volume'].iloc[-1]), rt_vol)
                        if df['Open'].iloc[-1] == 0: df.at[df.index[-1], 'Open'] = rt_open
                    twstock_used = True
            except Exception: pass

    except Exception as e:
        st.error(f"⚠️ 獲取數據失敗: {e}")
        return

    if df.empty or 'High' not in df.columns or 'Low' not in df.columns:
        st.warning(f"無法獲取有效的交易數據 ({ticker}, {interval})，可能是該區間無資料或代號錯誤。")
        return

    # 計算均線 (在裁切前計算確保準確)
    if ma_flags['5']: df['MA5'] = df['Close'].rolling(window=5).mean()
    if ma_flags['10']: df['MA10'] = df['Close'].rolling(window=10).mean()
    if ma_flags['20']: df['MA20'] = df['Close'].rolling(window=20).mean()
    if ma_flags['60']: df['MA60'] = df['Close'].rolling(window=60).mean()

    # 裁切近期 K 棒
    df_subset = df.tail(lookback).copy()
    df_subset = df_subset.dropna(subset=['High', 'Low'])
    
    if df_subset.empty:
        st.error(f"該股票 ({ticker}, {interval}) 的近期 K 線資料不完整或為空。")
        return

    try:
        high_60 = float(df_subset['High'].max())
        low_60 = float(df_subset['Low'].min())
        if high_60 == low_60:
            st.warning(f"該股票 ({ticker}, {interval}) 近期高低點相同，無法畫出波段比例。")
            return
    except Exception as e:
        st.error(f"計算高低點時發生錯誤：{e}")
        return

    diff = high_60 - low_60
    ratios = [-2.618, -2.0, -1.618, -1.0, 0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.618, 2.0, 2.618]
    
    fmt = '%Y-%m-%d %H:%M:%S'
    x_strings = df_subset.index.strftime(fmt).tolist()
    if interval in ["1d", "1wk", "1mo"]: x_display = df_subset.index.strftime('%Y-%m-%d').tolist()
    else: x_display = df_subset.index.strftime('%m-%d %H:%M').tolist()

    if show_vol and 'Volume' in df_subset.columns:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.8, 0.2])
    else:
        fig = go.Figure()

    # K棒實體顏色套用台股專用色 (紅漲綠跌)
    kline_trace = go.Candlestick(
        x=x_strings, open=df_subset['Open'], high=df_subset['High'],
        low=df_subset['Low'], close=df_subset['Close'], name="K線",
        increasing=dict(line=dict(color='#ff4b4b'), fillcolor='#ff4b4b'),
        decreasing=dict(line=dict(color='#00e676'), fillcolor='#00e676')
    )
    
    if show_vol and 'Volume' in df_subset.columns: fig.add_trace(kline_trace, row=1, col=1)
    else: fig.add_trace(kline_trace)

    # 繪製均線
    ma_settings = {
        'MA5': ('orange', ma_flags['5']),
        'MA10': ('lightblue', ma_flags['10']),
        'MA20': ('green', ma_flags['20']),
        'MA60': ('yellow', ma_flags['60'])
    }
    for ma_name, (color, is_show) in ma_settings.items():
        if is_show and ma_name in df_subset.columns:
            ma_trace = go.Scatter(x=x_strings, y=df_subset[ma_name], mode='lines', name=ma_name, line=dict(color=color, width=ma_width))
            if show_vol and 'Volume' in df_subset.columns: fig.add_trace(ma_trace, row=1, col=1)
            else: fig.add_trace(ma_trace)

    # 繪製成交量
    if show_vol and 'Volume' in df_subset.columns:
        # 成交量柱體顏色同步台股標準
        colors = ['#ff4b4b' if close >= open else '#00e676' for close, open in zip(df_subset['Close'], df_subset['Open'])]
        vol_trace = go.Bar(x=x_strings, y=df_subset['Volume'], name="成交量", marker_color=colors)
        fig.add_trace(vol_trace, row=2, col=1)

    high_idx_str = df_subset['High'].idxmax().strftime(fmt)
    low_idx_str = df_subset['Low'].idxmin().strftime(fmt)
    disp_high = round_to_tick(high_60)
    disp_low = round_to_tick(low_60)
    
    target_row = 1 if (show_vol and 'Volume' in df_subset.columns) else None
    target_col = 1 if (show_vol and 'Volume' in df_subset.columns) else None

    fig.add_annotation(x=high_idx_str, y=high_60, text=f"最高:{disp_high:g}", showarrow=True, arrowhead=1, yshift=10, font=dict(color="red", size=font_size), row=target_row, col=target_col)
    fig.add_annotation(x=low_idx_str, y=low_60, text=f"最低:{disp_low:g}", showarrow=True, arrowhead=1, ay=40, font=dict(color="green", size=font_size), row=target_row, col=target_col)

    last_date_str = x_strings[-1]
    first_date_str = x_strings[0]
    
    for r in ratios:
        price = low_60 + r * diff
        rounded_price = round_to_tick(price)
        fig.add_shape(type="line", x0=first_date_str, y0=price, x1=last_date_str, y1=price,
            line=dict(color="rgba(150, 150, 150, 0.5)", width=1, dash="dash" if r not in [0, 1] else "solid"), row=target_row, col=target_col)
            
        r_label = "1" if r == 1.0 else ("0" if r == 0.0 else f"{r:g}")
        fig.add_annotation(x=last_date_str, y=price, text=f"{r_label} ({rounded_price:g})",
            showarrow=False, xanchor="left", xshift=10, font=dict(size=font_size, color="orange" if 0 <= r <= 1 else "gray"), row=target_row, col=target_col)

    y_min_view = low_60 - diff * 1.05
    y_max_view = high_60 + diff * 0.05
    if pd.isna(y_min_view) or pd.isna(y_max_view): y_min_view, y_max_view = None, None

    interval_display_map = {"1m": "1分K", "5m": "5分K", "15m": "15分K", "60m": "60分K", "1d": "日K", "1wk": "週K", "1mo": "月K"}
    interval_name = interval_display_map.get(interval, interval)
    ticker_suffix = ".TW" if ticker.endswith(".TW") else (".TWO" if ticker.endswith(".TWO") else "")
    
    try:
        last_date_obj = df_subset.index[-1]
        
        if interval in ["1d", "1wk", "1mo"]:
            date_str = last_date_obj.strftime('%Y/%m/%d')
        else:
            date_str = last_date_obj.strftime('%Y/%m/%d %H:%M')

        op = float(df_subset['Open'].iloc[-1])
        hi = float(df_subset['High'].iloc[-1])
        lo = float(df_subset['Low'].iloc[-1])
        cl = float(df_subset['Close'].iloc[-1])
        vol = float(df_subset['Volume'].iloc[-1]) if 'Volume' in df_subset.columns else 0.0

        if len(df_subset) > 1:
            prev_cl = float(df_subset['Close'].iloc[-2])
        else:
            prev_cl = cl

        chg = cl - prev_cl
        pct_chg = (chg / prev_cl * 100) if prev_cl > 0 else 0.0

        color = "#ff4b4b" if chg > 0 else ("#00e676" if chg < 0 else "white")
        sign = "+" if chg > 0 else ""

        if is_index:
            if ticker == '^TWII':
                if vol == 0 or pd.isna(vol):
                    vol_num = "無資料(缺漏)"
                 
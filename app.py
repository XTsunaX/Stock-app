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
import fitz  # PyMuPDF 用於將 PDF 轉為圖片
from PIL import Image

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
        
        # 預防 Volume 為 NaN 引發 ValueError 格式化錯誤
        if pd.isna(vol): vol = 0.0

        # 精準計算漲跌幅: 無論何種週期，嚴格與昨日收盤價(或對應的正確前K棒)進行比較
        ref_prev_close = cl
        if interval in ["1m", "5m", "15m", "60m"]:
            daily_closes = df['Close'].resample('D').last().dropna()
            if len(daily_closes) > 1:
                current_date = df_subset.index[-1].date()
                if current_date == daily_closes.index[-1].date():
                    ref_prev_close = float(daily_closes.iloc[-2])
                else:
                    ref_prev_close = float(daily_closes.iloc[-1])
            else:
                ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl
        elif interval == "1d":
            ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl
        else:
            # 週K、月K 另外快速抓取近期的日K線以獲取真實昨日收盤價，避免跨度過大
            try:
                temp_df = yf.Ticker(ticker).history(period="5d", interval="1d")
                if isinstance(temp_df.columns, pd.MultiIndex):
                    temp_df.columns = temp_df.columns.droplevel(1)
                if len(temp_df) >= 2:
                    current_date = df_subset.index[-1].date()
                    if temp_df.index[-1].date() == current_date:
                        ref_prev_close = float(temp_df['Close'].iloc[-2])
                    else:
                        ref_prev_close = float(temp_df['Close'].iloc[-1])
                else:
                    ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl
            except:
                ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl

        chg = cl - ref_prev_close
        pct_chg = (chg / ref_prev_close * 100) if ref_prev_close > 0 else 0.0

        # === 標題顯示資訊準備 (移出 try 區塊以防 UnboundLocalError) ===
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
        if pd.isna(vol): vol = 0.0

        # 精準計算昨日收盤基準 (ref_prev_close)
        ref_prev_close = cl
        if interval in ["1m", "5m", "15m", "60m"]:
            daily_closes = df['Close'].resample('D').last().dropna()
            if len(daily_closes) > 1:
                current_date = df_subset.index[-1].date()
                if current_date == daily_closes.index[-1].date():
                    ref_prev_close = float(daily_closes.iloc[-2])
                else:
                    ref_prev_close = float(daily_closes.iloc[-1])
            else:
                ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl
        elif interval == "1d":
            # 日K直接取前一根K棒
            ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl
        else:
            # 週K、月K 透過 yfinance 抓取最近 5 日的日K來找出真正的「昨日收盤價」
            try:
                temp_df = yf.Ticker(ticker).history(period="5d", interval="1d")
                if isinstance(temp_df.columns, pd.MultiIndex):
                    temp_df.columns = temp_df.columns.droplevel(1)
                if len(temp_df) >= 2:
                    current_date = df_subset.index[-1].date()
                    if temp_df.index[-1].date() == current_date:
                        ref_prev_close = float(temp_df['Close'].iloc[-2])
                    else:
                        ref_prev_close = float(temp_df['Close'].iloc[-1])
                else:
                    ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl
            except:
                ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl

        chg = cl - ref_prev_close
        pct_chg = (chg / ref_prev_close * 100) if ref_prev_close > 0 else 0.0
        chg, pct_chg = round(chg, 2), round(pct_chg, 2)
        
        # 顏色判定
        def get_color(val, ref):
            return "#ff4b4b" if val > ref else ("#00e676" if val < ref else "white")

        c_cl = get_color(cl, ref_prev_close)
        sign = "+" if chg > 0 else ""

        if is_index:
            if ticker == '^TWII':
                vol_num = f"{vol/100000000:.2f}" if vol > 0 else "0"
                vol_unit, price_unit = " 億", " 點"
            else:
                vol_num, vol_unit, price_unit = f"{vol:,.0f}", " 單位(口)", " 點"
        else:
            vol_num, vol_unit, price_unit = f"{vol:,.0f}", " 張", " 元"

        disp_title = display_name.replace('(^TWII)', '(TSE)') if ticker == '^TWII' else display_name
        
        title_html = (
            f"{disp_title}{ticker_suffix} - {interval_name} {date_str} "
            f"開 {op:.2f} 高 {hi:.2f} 低 {lo:.2f} "
            f"收 <span style='color:{c_cl};'>{cl:.2f}</span>{price_unit} "
            f"量 {vol_num}{vol_unit} "
            f"<span style='color:{c_cl};'>{sign}{chg:.2f}({sign}{pct_chg:.2f}%)</span>"
        )
    except Exception as e:
        # 若上方發生任何錯誤，至少保證 title_html 有基本內容，且不會因變數未定義崩潰
        title_html = f"{display_name}{ticker_suffix} - {interval_name}"
                
        # twstock 盤後修補方案 (修正: 避免 1wk, 1mo 被錯誤加上單日 K棒)
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
                        rt_vol = float(stock.capacity[-1]) / 1000 

                if rt_price is not None:
                    if df.index.tzinfo is not None: df.index = df.index.tz_localize(None)
                    last_hist_date = pd.Timestamp(df.index[-1].date())
                    
                    # 只有日K且為最新日，才往後新增；若是週/月K，只需更新最後一根棒子即可，避免圖表異常
                    if interval == "1d" and last_hist_date < today_date and now_tw.weekday() < 5:
                        new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                        df = pd.concat([df, new_row])
                    else:
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

    # 計算均線
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
    
    # 費波顏色映射表
    color_map = {
        1.0: "#ff4b4b",
        0.786: "#ff9d00",
        0.618: "#7fff00",
        0.5: "#00ffff",
        0.382: "#1e90ff",
        0.236: "#9370db",
        0.0: "#ffffff"
    }
    
    fmt = '%Y-%m-%d %H:%M:%S'
    x_strings = df_subset.index.strftime(fmt).tolist()
    if interval in ["1d", "1wk", "1mo"]: x_display = df_subset.index.strftime('%Y-%m-%d').tolist()
    else: x_display = df_subset.index.strftime('%m-%d %H:%M').tolist()

    if show_vol and 'Volume' in df_subset.columns:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.8, 0.2])
    else:
        fig = go.Figure()

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
        line_col = color_map.get(r, "rgba(150, 150, 150, 0.5)")
        
        fig.add_shape(type="line", x0=first_date_str, y0=price, x1=last_date_str, y1=price,
            line=dict(color=line_col, width=1, dash="dash" if r not in [0, 1] else "solid"), row=target_row, col=target_col)
            
        r_label = "1" if r == 1.0 else ("0" if r == 0.0 else f"{r:g}")
        fig.add_annotation(x=last_date_str, y=price, text=f"{r_label} ({rounded_price:g})",
            showarrow=False, xanchor="left", xshift=10, font=dict(size=font_size, color=line_col), row=target_row, col=target_col)

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

        # 精準計算漲跌幅: 無論何種週期，嚴格與昨日收盤價(或對應的正確前K棒)進行比較
        ref_prev_close = cl
        if interval in ["1m", "5m", "15m", "60m"]:
            daily_closes = df['Close'].resample('D').last().dropna()
            if len(daily_closes) > 1:
                current_date = df_subset.index[-1].date()
                if current_date == daily_closes.index[-1].date():
                    ref_prev_close = float(daily_closes.iloc[-2])
                else:
                    ref_prev_close = float(daily_closes.iloc[-1])
            else:
                ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl
        elif interval == "1d":
            ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl
        else:
            # 週K、月K 另外快速抓取近期的日K線以獲取真實昨日收盤價，避免跨度過大
            try:
                temp_df = yf.Ticker(ticker).history(period="5d", interval="1d")
                if len(temp_df) >= 2:
                    current_date = df_subset.index[-1].date()
                    if temp_df.index[-1].date() == current_date:
                        ref_prev_close = float(temp_df['Close'].iloc[-2])
                    else:
                        ref_prev_close = float(temp_df['Close'].iloc[-1])
                else:
                    ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl
            except:
                ref_prev_close = float(df_subset['Close'].iloc[-2]) if len(df_subset) > 1 else cl

        chg = cl - ref_prev_close
        pct_chg = (chg / ref_prev_close * 100) if ref_prev_close > 0 else 0.0

        chg = round(chg, 2)
        pct_chg = round(pct_chg, 2)
        color = "#ff4b4b" if chg > 0 else ("#00e676" if chg < 0 else "white")
        sign = "+" if chg > 0 else ""

        if is_index:
            if ticker == '^TWII':
                if vol == 0 or pd.isna(vol):
                    vol_num = "無資料(缺漏)"
                    vol_unit = ""
                else:
                    vol_num = f"{vol/100000000:.2f}" if vol > 100000000 else f"{vol:,.2f}"
                    vol_unit = " 億"
            else:
                vol_num = f"{vol:,.0f}"
                vol_unit = " 單位(口)"
            price_unit = " 點"
        else:
            vol_num = f"{vol:,.0f}"
            vol_unit = " 張"
            price_unit = " 元"

        disp_title = display_name.replace('(^TWII)', '(TSE)') if ticker == '^TWII' else display_name
        
        # 標題僅針對 開、高、低、收「後方數值」以及「漲跌、漲跌幅」套用顏色，其餘保持原色
        title_html = (
            f"{disp_title}{ticker_suffix} - {interval_name} {date_str} "
            f"開 <span style='color:{color};'>{op:.2f}</span> "
            f"高 <span style='color:{color};'>{hi:.2f}</span> "
            f"低 <span style='color:{color};'>{lo:.2f}</span> "
            f"收 <span style='color:{color};'>{cl:.2f}</span>{price_unit} "
            f"量 {vol_num}{vol_unit} "
            f"<span style='color:{color};'>{sign}{chg:.2f}({sign}{pct_chg:.2f}%)</span>"
        )
    except Exception:
        title_html = f"{display_name}{ticker_suffix} - {interval_name}"

    layout_update = dict(
        title=dict(text=title_html, font=dict(size=16)),
        template="plotly_dark",
        height=800 if show_vol else 700,
        showlegend=True,
    )

    if show_vol and 'Volume' in df_subset.columns:
        fig.update_yaxes(title_text="點數", range=[y_min_view, y_max_view] if y_min_view and y_max_view else None, autorange=False if y_min_view and y_max_view else True, fixedrange=False, row=1, col=1)
        fig.update_yaxes(title_text="成交量", fixedrange=False, row=2, col=1)
        fig.update_xaxes(type='category', tickmode='array', tickvals=x_strings[::max(1, len(x_strings)//10)], ticktext=x_display[::max(1, len(x_display)//10)], showgrid=False, rangeslider_visible=False, row=2, col=1)
        fig.update_xaxes(type='category', showgrid=False, rangeslider_visible=False, showticklabels=False, row=1, col=1) 
    else:
        layout_update.update(
            yaxis_title="點數",
            yaxis=dict(range=[y_min_view, y_max_view] if y_min_view and y_max_view else None, autorange=False if y_min_view and y_max_view else True, fixedrange=False),
            xaxis=dict(type='category', tickmode='array', tickvals=x_strings[::max(1, len(x_strings)//10)], ticktext=x_display[::max(1, len(x_display)//10)], showgrid=False),
            xaxis_rangeslider_visible=False
        )

    fig.update_layout(**layout_update)
    st.plotly_chart(fig, use_container_width=True)
    
    fetch_time_str = datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S')
    
    if sj_kbars_used:
        data_source_text = "永豐 Shioaji API (即時K線)"
    elif sj_snap_used:
        data_source_text = "YF歷史 + 永豐即時快照"
    elif twstock_used:
        data_source_text = "YF歷史 + Twstock即時"
    else:
        data_source_text = "YF 歷史數據"
        
    st.caption(f"📊 數據最後更新時間: {fetch_time_str} ({data_source_text})")


# ==========================================
# 網路爬蟲加入快取，避免切換分頁時卡死
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fubon_html(url):
    """解決富邦 DJ 拒絕 iframe 連線的問題、處理亂碼與排版"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=10, verify=False)
        r.encoding = 'cp950' 
        html = r.text
        
        # 強制轉換 meta charset 避免瀏覽器以預設編碼解析造成亂碼
        html = re.sub(r'charset=["\']?(big5|utf-8|cp950)["\']?', 'charset=utf-8', html, flags=re.IGNORECASE)
        
        # 注入 base 標籤與 CSS：消除右方空白但保留預設字體大小
        injection = '''
        <base href="https://fubon-ebrokerdj.fbs.com.tw/">
        <meta charset="utf-8">
        <style>
            body { margin: 0 !important; padding: 0 !important; text-align: left; background-color: white;}
            center { text-align: left !important; margin: 0 !important; }
            table { margin: 0 !important; width: 100% !important; max-width: 100% !important; }
            .dj-container, .wrapper { margin: 0 !important; padding: 0 !important; width: 100% !important; }
            a { text-decoration: none; color: #333; }
        </style>
        '''
        html = re.sub(r'<head>', f'<head>{injection}', html, flags=re.IGNORECASE)
        return html
    except Exception as e:
        return f"<html><body><h3>無法載入資料: {e}</h3></body></html>"

@st.cache_data(ttl=3600, show_spinner=False)
def get_report_list():
    """爬取永豐期貨盤後快訊列表，嚴格過濾台指期籌碼快訊"""
    url = "https://www.spf.com.tw/sinopacSPF/research/list.do?id=1709f20d3ff00000d8e2039e8984ed51"
    base_url = "https://www.spf.com.tw"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        reports = []
        items = soup.select('div.list_news ul li')
        
        if items:
            for item in items:
                link_tag = item.find('a')
                date_tag = item.find('span', class_='date')
                if link_tag:
                    title = link_tag.get_text(strip=True)
                    # 嚴格過濾: 只有標題含有 "台指期籌碼快訊" 才會抓取
                    if "台指期籌碼快訊" not in title:
                        continue
                        
                    href = link_tag['href']
                    pdf_url = f"{base_url}{href}" if href.startswith('/') else f"{base_url}/sinopacSPF/research/{href}"
                    
                    date_str = ""
                    date_match = re.search(r'(202\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])', title)
                    if date_match:
                        date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                    elif date_tag:
                        date_str = date_tag.get_text(strip=True).replace("/", "-")
                    else:
                        date_str = "近期發布"
                        
                    reports.append({"日期": date_str, "title": title, "url": pdf_url})
        else:
            # 舊的 DOM 結構 fallback
            for a_tag in soup.find_all('a', href=re.compile(r'\.pdf')):
                title = a_tag.get_text(strip=True) or a_tag.get('title', '')
                if "台指期籌碼快訊" not in title:
                    continue
                    
                href = a_tag['href']
                pdf_url = f"{base_url}{href}" if href.startswith('/') else href
                
                date_match = re.search(r'(202\d)(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])', title)
                if date_match:
                    date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                else:
                    parent = a_tag.find_parent(['tr', 'li'])
                    dt_span = parent.find(text=re.compile(r'202\d[/-]\d{2}[/-]\d{2}')) if parent else None
                    date_str = re.search(r'202\d[/-]\d{2}[/-]\d{2}', dt_span).group() if dt_span else "近期發布"

                reports.append({"日期": date_str, "title": title, "url": pdf_url})
                
        # 過濾重複連結並依照日期降冪排列 (確保第一筆一定是最新的)
        unique_reports = []
        seen = set()
        for r in reports:
            if r['url'] not in seen:
                unique_reports.append(r)
                seen.add(r['url'])
                
        unique_reports.sort(key=lambda x: x['日期'], reverse=True)
        return unique_reports
    except Exception as e:
        return []

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_and_parse_pdf(pdf_url):
    """下載、解析數值並將 PDF 轉為圖片供直接預覽"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        if not pdf_url.lower().endswith('.pdf'):
            r_inner = requests.get(pdf_url, headers=headers, timeout=10, verify=False)
            soup_inner = BeautifulSoup(r_inner.text, 'html.parser')
            for tag in soup_inner.find_all(['a', 'iframe']):
                link = tag.get('href') or tag.get('src')
                if link and link.lower().endswith('.pdf'):
                    pdf_url = link
                    if not pdf_url.startswith('http'):
                        pdf_url = "https://www.spf.com.tw" + pdf_url
                    break

        response = requests.get(pdf_url, headers=headers, timeout=15, verify=False)
        pdf_bytes = response.content
        
        text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text += page.extract_text() or ""
            
        ratio_match = re.search(r"散戶小台多空比[:：]\s*([-+]?[\d\.]+)%", text)
        ratio = ratio_match.group(1) if ratio_match else "N/A"
        
        images = []
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            for page in doc:
                pix = page.get_pixmap(dpi=150)
                img = Image.open(io.BytesIO(pix.tobytes("png")))
                images.append(img)
        except Exception as e:
            pass
            
        return {
            "ratio": ratio,
            "images": images
        }
    except Exception as e:
        return {"ratio": "解析錯誤", "images": []}

@st.cache_data(ttl=1800, show_spinner=False)
def get_major_institutional_data(date_str):
    """從證交所 API 抓取三大法人買賣金額統計 (套用正確 API 結構並增加 Headers 防阻擋)"""
    url = f"[https://www.twse.com.tw/rwd/zh/fund/BFI82U?date=](https://www.twse.com.tw/rwd/zh/fund/BFI82U?date=){date_str}&response=json"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': '[https://www.twse.com.tw/zh/trading/foreign/bfi82u.html](https://www.twse.com.tw/zh/trading/foreign/bfi82u.html)',
        'X-Requested-With': 'XMLHttpRequest'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        
        # 若 RWD 版 API 失敗，退回舊版 API
        if response.status_code != 200:
            url = f"[https://www.twse.com.tw/fund/BFI82U?response=json&date=](https://www.twse.com.tw/fund/BFI82U?response=json&date=){date_str}"
            response = requests.get(url, headers=headers, timeout=10, verify=False)
            
        data = response.json()
        
        if data.get("stat") != "OK":
            return None
        
        # 轉換為 DataFrame
        df = pd.DataFrame(data["data"], columns=data["fields"])
        
        # 清理數據：移除千分號並轉為數字
        cols_to_fix = ['買進金額', '賣出金額', '買賣差額']
        for col in cols_to_fix:
            df[col] = df[col].astype(str).str.replace(',', '').astype(float)
            
        return df
    except Exception as e:
        return None

def color_negative_positive(val):
    """定義表格文字顏色：正數紅、負數綠"""
    if isinstance(val, (int, float)):
        color = '#ff4b4b' if val > 0 else '#00e676' if val < 0 else 'white'
        return f'color: {color}'
    return ''

@st.cache_data(ttl=3600, show_spinner=False)
def get_tw_stocker_data(direction):
    url = f"https://voidful.github.io/tw-institutional-stocker/data/top_three_inst_change_20_{direction}.json"
    try:
        r = requests.get(url, timeout=3, verify=False)
        if r.status_code == 200:
            data = r.json()
            if data:
                df = pd.DataFrame(data).head(20)
                if 'code' in df.columns:
                    df = df.rename(columns={
                        'code': '代號',
                        'name': '名稱',
                        'change': '持股變化(%)',
                        'three_inst_ratio': '三大法人持股(%)'
                    })
                    return df[['代號', '名稱', '持股變化(%)', '三大法人持股(%)']]
    except:
        pass
    return pd.DataFrame()


# ==========================================
# 0. 頁面設定與初始化
# ==========================================
st.set_page_config(page_title="當沖戰略室", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
    [data-testid="stSidebar"] button { white-space: nowrap !important; text-overflow: clip !important; padding-left: 5px !important; padding-right: 5px !important; }
    div.stButton > button { min-height: 45px; font-size: 20px; }
    .stButton { margin-top: 5px; }
    .calendar-header { font-size: 2.5em; font-weight: 900; text-align: center; color: #ff9800; margin-bottom: 10px; line-height: 1.5; font-family: 'Arial', sans-serif; }
    .cal-box { text-align: center; padding: 5px; border-radius: 4px; margin: 2px; min-height: 90px; border: 1px solid #555; font-size: 0.9em; display: flex; flex-direction: column; justify-content: space-between; }
    .cal-open { background-color: #000000 !important; color: #ffffff !important; }
    .cal-closed { background-color: #d32f2f !important; color: #ffffff !important; font-weight: bold; }
    .cal-week { background-color: #f0f0f0; color: #333; font-weight: bold; display: flex; align-items: center; justify-content: center; font-size: 0.8em; }
    .settle-m { color: #ffff00; font-weight: bold; font-size: 0.85em; margin-top: 2px; line-height: 1.2; } 
    .settle-w { color: #00e676; font-size: 0.8em; margin-top: 2px; } 
    .settle-f { color: #29b6f6; font-size: 0.8em; margin-top: 2px; } 
    .holiday-tag { font-size: 0.85em; margin-bottom: 2px; color: #ffeb3b; background-color: rgba(0,0,0,0.5); border-radius: 3px; padding: 1px;}
    .today-border { border: 3px solid #ffff00 !important; }
    div[data-testid="column"] { text-align: center; }
</style>
""", unsafe_allow_html=True)

st.title("⚡ 當沖戰略室 ⚡")

CONFIG_FILE = "config.json"
DATA_CACHE_FILE = "data_cache.json"
URL_CACHE_FILE = "url_cache.json"
SEARCH_CACHE_FILE = "search_cache.json"

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f: return json.load(f)
        except: return {}
    return {}

def save_config(font_size, limit_rows, auto_update, delay_sec, sj_key="", sj_secret="", remember_sj=False):
    try:
        config = load_config()
        config.update({
            "font_size": font_size, 
            "limit_rows": limit_rows, 
            "auto_update": auto_update, 
            "delay_sec": delay_sec,
            "sj_key": sj_key if remember_sj else "",
            "sj_secret": sj_secret if remember_sj else "",
            "remember_sj": remember_sj
        })
        with open(CONFIG_FILE, "w") as f: json.dump(config, f)
        return True
    except: return False

def save_fibo_config():
    config = load_config()
    config['fibo_tags'] = [
        st.session_state.get('custom_tag_1', "台積電(2330)"), 
        st.session_state.get('custom_tag_2', "鴻海(2317)"), 
        st.session_state.get('custom_tag_3', "聯發科(2454)"), 
        st.session_state.get('custom_tag_4', "長榮(2603)"), 
        st.session_state.get('custom_tag_5', "聯鈞(3450)")
    ]
    if 'ma_w' in st.session_state:
        config['ma_width'] = st.session_state.ma_w
    try:
        with open(CONFIG_FILE, "w") as f: json.dump(config, f)
    except: pass

def save_data_cache(df, ignored_set, candidates=[], saved_notes={}):
    try:
        df_save = df.fillna("") 
        data_to_save = {"stock_data": df_save.to_dict(orient='records'), "ignored_stocks": list(ignored_set), "all_candidates": candidates, "saved_notes": saved_notes}
        with open(DATA_CACHE_FILE, "w", encoding='utf-8') as f: json.dump(data_to_save, f, ensure_ascii=False, indent=4)
    except: pass

def load_data_cache():
    if os.path.exists(DATA_CACHE_FILE):
        try:
            with open(DATA_CACHE_FILE, "r", encoding='utf-8') as f: data = json.load(f)
            df = pd.DataFrame(data.get('stock_data', []))
            ignored = set(data.get('ignored_stocks', []))
            candidates = data.get('all_candidates', [])
            saved_notes = data.get('saved_notes', {}) 
            return df, ignored, candidates, saved_notes
        except: return pd.DataFrame(), set(), [], {}
    return pd.DataFrame(), set(), [], {}

def load_url_history():
    if os.path.exists(URL_CACHE_FILE):
        try:
            with open(URL_CACHE_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
                if "url" in data and isinstance(data["url"], str) and data["url"]: return [data["url"]]
                return data.get("urls", [])
        except: return []
    return []

def save_url_history(urls):
    try:
        unique_urls = []
        seen = set()
        for u in urls:
            u_clean = u.strip()
            if u_clean and u_clean not in seen:
                unique_urls.append(u_clean)
                seen.add(u_clean)
        with open(URL_CACHE_FILE, "w", encoding='utf-8') as f: json.dump({"urls": unique_urls}, f)
        return True
    except: return False

def load_search_cache():
    if os.path.exists(SEARCH_CACHE_FILE):
        try:
            with open(SEARCH_CACHE_FILE, "r", encoding='utf-8') as f: data = json.load(f)
            return data.get("selected", [])
        except: return []
    return []

def save_search_cache(selected_items):
    try:
        with open(SEARCH_CACHE_FILE, "w", encoding='utf-8') as f: json.dump({"selected": selected_items}, f, ensure_ascii=False)
    except: pass

if 'stock_data' not in st.session_state:
    cached_df, cached_ignored, cached_candidates, cached_notes = load_data_cache()
    st.session_state.stock_data = cached_df
    st.session_state.ignored_stocks = cached_ignored
    st.session_state.all_candidates = cached_candidates
    st.session_state.saved_notes = cached_notes

if 'ignored_stocks' not in st.session_state: st.session_state.ignored_stocks = set()
if 'all_candidates' not in st.session_state: st.session_state.all_candidates = []
if 'calc_base_price' not in st.session_state: st.session_state.calc_base_price = 100.0
if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = 100.0
if 'url_history' not in st.session_state: st.session_state.url_history = load_url_history()
if 'cloud_url_input' not in st.session_state: st.session_state.cloud_url_input = st.session_state.url_history[0] if st.session_state.url_history else ""
if 'search_multiselect' not in st.session_state: st.session_state.search_multiselect = load_search_cache()
if 'saved_notes' not in st.session_state: st.session_state.saved_notes = {}
if 'futures_list' not in st.session_state: st.session_state.futures_list = set()

# Fibo 標籤與狀態初始化
saved_config = load_config()
fibo_tags = saved_config.get('fibo_tags', ["台積電(2330)", "鴻海(2317)", "聯發科(2454)", "長榮(2603)", "聯鈞(3450)"])

if 'fibo_search_input' not in st.session_state: st.session_state.fibo_search_input = "加權指數(^TWII)"
if 'fibo_trigger_search' not in st.session_state: st.session_state.fibo_trigger_search = False

if 'custom_tag_1' not in st.session_state: st.session_state.custom_tag_1 = fibo_tags[0] if len(fibo_tags)>0 else "台積電(2330)"
if 'custom_tag_2' not in st.session_state: st.session_state.custom_tag_2 = fibo_tags[1] if len(fibo_tags)>1 else "鴻海(2317)"
if 'custom_tag_3' not in st.session_state: st.session_state.custom_tag_3 = fibo_tags[2] if len(fibo_tags)>2 else "聯發科(2454)"
if 'custom_tag_4' not in st.session_state: st.session_state.custom_tag_4 = fibo_tags[3] if len(fibo_tags)>3 else "長榮(2603)"
if 'custom_tag_5' not in st.session_state: st.session_state.custom_tag_5 = fibo_tags[4] if len(fibo_tags)>4 else "聯鈞(3450)"

if 'ma_w' not in st.session_state: st.session_state.ma_w = saved_config.get('ma_width', 1.5)

# 控制圖表預設時間週期 ("1d" 或 "5m")
if 'fibo_interval' not in st.session_state: st.session_state.fibo_interval = "5m" 
if 'fibo_font_size' not in st.session_state: st.session_state.fibo_font_size = 15

tz_tw = pytz.timezone('Asia/Taipei')
now_tw = datetime.now(tz_tw)
if 'cal_year' not in st.session_state: st.session_state.cal_year = now_tw.year
if 'cal_month' not in st.session_state: st.session_state.cal_month = now_tw.month

if 'font_size' not in st.session_state: st.session_state.font_size = saved_config.get('font_size', 15)
if 'limit_rows' not in st.session_state: st.session_state.limit_rows = saved_config.get('limit_rows', 5)
if 'auto_update_last_row' not in st.session_state: st.session_state.auto_update_last_row = saved_config.get('auto_update', True)
if 'update_delay_sec' not in st.session_state: st.session_state.update_delay_sec = saved_config.get('delay_sec', 1.0) 

# API 記憶功能初始化
if 'sj_key' not in st.session_state: st.session_state.sj_key = saved_config.get('sj_key', '')
if 'sj_secret' not in st.session_state: st.session_state.sj_secret = saved_config.get('sj_secret', '')
if 'remember_sj' not in st.session_state: st.session_state.remember_sj = saved_config.get('remember_sj', False)

if sj and st.session_state.remember_sj and st.session_state.sj_key and not st.session_state.get('sj_logged_in', False):
    try:
        if 'sj_api' not in st.session_state:
            st.session_state.sj_api = sj.Shioaji(simulation=False)
        st.session_state.sj_api.login(st.session_state.sj_key, st.session_state.sj_secret)
        st.session_state.sj_logged_in = True
    except:
        pass

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
        except: pass
    return code_map, name_map

@st.cache_data(ttl=86400)
def get_stock_name_online(code):
    code = str(code).strip()
    code_map, _ = load_local_stock_names()
    if code in code_map: return code_map[code]
    return code

@st.cache_data(ttl=86400)
def search_code_online(query):
    query = query.strip()
    if query.isdigit(): return query
    _, name_map = load_local_stock_names()
    if query in name_map: return name_map[query]
    return None

with st.sidebar:
    st.header("🔑 永豐證券 API 登入")
    if sj is None:
        st.error("⚠️ 未偵測到 shioaji 套件\n\n請先在終端機執行：\n`pip install shioaji`")
    else:
        if st.session_state.get('sj_logged_in', False):
            st.success("✅ 永豐 API 已登入")
            
            try:
                usage = st.session_state.sj_api.usage()
                rem_mb = usage.remaining_bytes / (1024 * 1024)
                st.caption(f"📊 API 今日剩餘流量: {rem_mb:.2f} MB")
            except:
                pass

            if st.button("登出", key="btn_logout_sj"):
                st.session_state.sj_logged_in = False
                try: st.session_state.sj_api.logout()
                except: pass
                st.rerun()
        else:
            sj_api_key = st.text_input("API Key", type="password", value=st.session_state.sj_key, key="input_sj_key")
            sj_secret = st.text_input("Secret Key", type="password", value=st.session_state.sj_secret, key="input_sj_secret")
            remember_sj = st.checkbox("記住 API 資訊", value=st.session_state.remember_sj, key="input_remember_sj")
            
            if st.button("登入 Shioaji"):
                try:
                    if 'sj_api' not in st.session_state:
                        st.session_state.sj_api = sj.Shioaji(simulation=False)
                    st.session_state.sj_api.login(sj_api_key, sj_secret)
                    st.session_state.sj_logged_in = True
                    
                    st.session_state.sj_key = sj_api_key
                    st.session_state.sj_secret = sj_secret
                    st.session_state.remember_sj = remember_sj
                    save_config(
                        st.session_state.font_size, 
                        st.session_state.limit_rows, 
                        st.session_state.auto_update_last_row, 
                        st.session_state.update_delay_sec, 
                        sj_api_key, 
                        sj_secret, 
                        remember_sj
                    )
                    st.success("✅ 永豐 API 登入成功！")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 登入失敗: {e}")
    st.markdown("---")

    st.header("⚙️ 設定")
    current_font_size = st.slider("字體大小 (表格)", min_value=12, max_value=72, value=st.session_state.font_size, key='font_size_slider')
    st.session_state.font_size = current_font_size
    hide_non_stock = st.checkbox("隱藏非個股 (ETF/權證/債券)", value=True)
    show_3d_hilo = st.checkbox("近3日高低點 (戰略備註)", value=False, help="勾選後，將於戰略備註中加入前天、昨天、今天的最高與最低價 (僅顯示數值)")
    st.markdown("---")
    current_limit_rows = st.number_input("顯示筆數 (檔案/雲端)", min_value=1, value=st.session_state.limit_rows, key='limit_rows_input')
    st.session_state.limit_rows = current_limit_rows
    
    if st.button("💾 儲存設定"):
        if save_config(current_font_size, current_limit_rows, st.session_state.auto_update_last_row, st.session_state.update_delay_sec, st.session_state.sj_key, st.session_state.sj_secret, st.session_state.remember_sj):
            st.toast("設定已儲存！", icon="✅")
            
    st.markdown("### 資料管理")
    if st.session_state.ignored_stocks:
        st.write(f"🚫 忽略名單 (取消勾選以復原):")
        ignored_list = sorted(list(st.session_state.ignored_stocks))
        options_map = {f"{c} {get_stock_name_online(c)}": c for c in ignored_list}
        options_display = list(options_map.keys())
        selected_ignored_display = st.multiselect("管理忽略股票", options=options_display, default=options_display, label_visibility="collapsed")
        current_selected_codes = set(options_map[opt] for opt in selected_ignored_display)
        if len(current_selected_codes) != len(st.session_state.ignored_stocks):
            st.session_state.ignored_stocks = current_selected_codes
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("已更新忽略名單。", icon="🔄")
            st.rerun()
    else:
        st.write("🚫 目前無忽略股票")
    
    col_restore, col_clear = st.columns([1, 1], gap="small")
    with col_restore:
        if st.button("♻️ 全部復原", use_container_width=True):
            st.session_state.ignored_stocks.clear()
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.toast("已重置忽略名單。", icon="🔄")
            st.rerun()
    with col_clear:
        if st.button("🗑️ 全部清空", type="primary", use_container_width=True):
            st.session_state.stock_data = pd.DataFrame()
            st.session_state.ignored_stocks = set()
            st.session_state.all_candidates = []
            st.session_state.search_multiselect = []
            st.session_state.saved_notes = {} 
            save_search_cache([])
            if os.path.exists(DATA_CACHE_FILE): os.remove(DATA_CACHE_FILE)
            st.toast("資料已全部清空", icon="🗑️")
            st.rerun()
    
    st.caption("功能說明")
    st.info("🗑️ **如何刪除股票？**\n\n在表格左側勾選「刪除」框，資料將會立即移除並**自動遞補下一檔**。")
    st.markdown("---")
    st.markdown("### 🔗 外部資源")
    st.link_button("📥 Goodinfo 當日週轉率排行", "https://reurl.cc/Or9e37", use_container_width=True)
    st.link_button("🚨 上市處置有價證券公告", "https://www.twse.com.tw/zh/announcement/punish.html", use_container_width=True)
    st.link_button("🚨 上櫃處置有價證券公告", "https://www.tpex.org.tw/zh-tw/announce/market/disposal.html", use_container_width=True)

@st.cache_data(ttl=86400)
def fetch_futures_list():
    try:
        url = "https://www.taifex.com.tw/cht/2/stockLists"
        dfs = pd.read_html(url)
        if dfs:
            for df in dfs:
                if '證券代號' in df.columns: return set(df['證券代號'].astype(str).str.strip().tolist())
                if 'Stock Code' in df.columns: return set(df['Stock Code'].astype(str).str.strip().tolist())
    except: pass
    return set()

def get_tick_size(price):
    try: price = float(price)
    except: return 0.01
    if pd.isna(price) or price <= 0: return 0.01
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.1
    if price < 500: return 0.5
    if price < 1000: return 1.0
    return 5.0

def apply_tick_rules(price):
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        rounded = (Decimal(str(p)) / Decimal(str(tick))).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * Decimal(str(tick))
        return float(rounded)
    except: return price

def calculate_limits(price):
    try:
        p = float(price)
        if math.isnan(p) or p <= 0: return 0, 0
        raw_up = p * 1.10
        tick_up = get_tick_size(raw_up) 
        limit_up = math.floor(raw_up / tick_up) * tick_up
        raw_down = p * 0.90
        tick_down = get_tick_size(raw_down) 
        limit_down = math.ceil(raw_down / tick_down) * tick_down
        return float(f"{limit_up:.2f}"), float(f"{limit_down:.2f}")
    except: return 0, 0

def move_tick(price, steps):
    try:
        curr = float(price)
        if steps > 0:
            for _ in range(steps):
                tick = get_tick_size(curr)
                curr = round(curr + tick, 2)
        elif steps < 0:
            for _ in range(abs(steps)):
                tick = get_tick_size(curr - 0.0001)
                curr = round(curr - tick, 2)
        return curr
    except: return price

def apply_sr_rules(price, base_price):
    try:
        p = float(price)
        if math.isnan(p): return 0.0
        tick = get_tick_size(p)
        d_val = Decimal(str(p))
        d_tick = Decimal(str(tick))
        if p < base_price: return float(math.ceil(d_val / d_tick) * d_tick)
        elif p > base_price: return float(math.floor(d_val / d_tick) * d_tick)
        else: return apply_tick_rules(p)
    except: return price

def fmt_price(v):
    try:
        if pd.isna(v) or v == "": return ""
        return f"{float(v):.2f}".rstrip('0').rstrip('.')
    except: return str(v)

def calculate_note_width(series, font_size):
    def get_width(s):
        w = 0
        for c in str(s): w += 2.0 if ord(c) > 127 else 1.0
        return w
    if series.empty: return 50
    max_w = series.apply(get_width).max()
    if pd.isna(max_w): max_w = 0
    pixel_width = int(max_w * (font_size * 0.44))
    return max(50, pixel_width)

def recalculate_row(row, points_map):
    custom_price = row.get('自訂價(可修)')
    code = row.get('代號')
    status = ""
    if pd.isna(custom_price) or str(custom_price).strip() == "": return status
    try:
        price = float(custom_price)
        limit_up = row.get('當日漲停價')
        limit_down = row.get('當日跌停價')
        l_up = float(limit_up) if limit_up and str(limit_up).replace('.','').isdigit() else None
        l_down = float(limit_down) if limit_down and str(limit_down).replace('.','').isdigit() else None
        
        strat_values = []
        points = points_map.get(code, [])
        if isinstance(points, list):
            for p in points: strat_values.append(p['val'])
            
        note_text = str(row.get('戰略備註', ''))
        found_prices = re.findall(r'\d+\.?\d*', note_text)
        for fp in found_prices:
            try: strat_values.append(float(fp))
            except: pass
            
        if l_up is not None and abs(price - l_up) < 0.01: status = "🔴 漲停"
        elif l_down is not None and abs(price - l_down) < 0.01: status = "🟢 跌停"
        elif strat_values:
            max_val = max(strat_values)
            min_val = min(strat_values)
            if price > max_val: status = "🔴 強"
            elif price < min_val: status = "🟢 弱"
            else:
                hit = False
                for v in strat_values:
                    if abs(v - price) < 0.01: hit = True; break
                if hit: status = "🟡 命中"
        return status
    except: return status

def generate_note_from_points(points, manual_note, show_3d):
    display_candidates = []
    target_tags = ['前高', '前低', '昨高', '昨低', '今高', '今低']
    for p in points:
        t = p.get('tag', '')
        if t in target_tags and not show_3d: continue
        if p['val'] <= 0: continue
        display_candidates.append(p)
        
    display_candidates.sort(key=lambda x: x['val'])
    note_parts = []
    seen_vals = set() 
    
    for val, group in itertools.groupby(display_candidates, key=lambda x: round(x['val'], 2)):
        if val in seen_vals: continue
        seen_vals.add(val)
        g_list = list(group)
        tags = [x['tag'] for x in g_list if x['tag']]
        
        final_tag = ""
        if "漲停高" in tags: final_tag = "漲停高"
        elif "跌停低" in tags: final_tag = "跌停低" 
        elif "漲停" in tags: final_tag = "漲停"
        elif "跌停" in tags: final_tag = "跌停"
        elif "多" in tags: final_tag = "多"
        elif "空" in tags: final_tag = "空"
        elif "平" in tags: final_tag = "平"
        elif "高" in tags: final_tag = "高"
        elif "低" in tags: final_tag = "低"
        elif "今高" in tags: final_tag = "今高"
        elif "今低" in tags: final_tag = "今低"
        elif "昨高" in tags: final_tag = "昨高"
        elif "昨低" in tags: final_tag = "昨低"
        elif "前高" in tags: final_tag = "前高"
        elif "前低" in tags: final_tag = "前低"
        
        v_str = fmt_price(val)
        suffix_tags = ["多", "空", "平"]
        prefix_tags = ["漲停", "漲停高", "跌停", "跌停低", "高", "低"]
        numeric_only_tags = ["前高", "前低", "昨高", "昨低", "今高", "今低"]
        
        if final_tag in suffix_tags: item = f"{v_str}{final_tag}" 
        elif final_tag in prefix_tags: item = f"{final_tag}{v_str}"
        elif final_tag in numeric_only_tags: item = v_str 
        elif final_tag: item = f"{v_str}{final_tag}" 
        else: item = v_str
        note_parts.append(item)
        
    auto_note = "-".join(note_parts)
    if manual_note:
        if manual_note.startswith("[M]"): return manual_note[3:], auto_note
        if auto_note and manual_note.strip().startswith(auto_note.strip()): return manual_note, auto_note
        return f"{auto_note}{manual_note}", auto_note
    return auto_note, auto_note

def fetch_stock_data_raw(code, name_hint="", extra_data=None, futures_set=None, saved_notes_dict=None, name_map_dict=None):
    code = str(code).strip()
    hist = pd.DataFrame()
    source_used = "none"
    
    # 優先使用永豐 API 擷取昨日/歷史日 K 線資料
    if st.session_state.get('sj_logged_in', False):
        sj_df = fetch_shioaji_data(st.session_state.sj_api, code, interval='1d', lookback_days=40)
        if not sj_df.empty:
            hist = sj_df
            source_used = "shioaji"

    # 若永豐未登入或沒抓到，退回使用 twstock 擷取
    if hist.empty:
        try:
            stock = twstock.Stock(code)
            tw_data = stock.fetch_31()
            if tw_data and len(tw_data) > 0:
                df_tw = pd.DataFrame(tw_data)
                df_tw['Date'] = pd.to_datetime(df_tw['date'])
                df_tw = df_tw.set_index('Date')
                rename_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'capacity': 'Volume'}
                df_tw = df_tw.rename(columns=rename_map)
                cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                for c in cols: df_tw[c] = pd.to_numeric(df_tw[c], errors='coerce')
                if not df_tw.empty:
                    hist = df_tw[cols]
                    source_used = "twstock"
        except: pass

    if hist.empty and si is not None:
        try:
            try: df_yf = si.get_data(f"{code}.TW", start_date=(datetime.now() - timedelta(days=40)))
            except:
                try: df_yf = si.get_data(f"{code}.TWO", start_date=(datetime.now() - timedelta(days=40)))
                except: df_yf = pd.DataFrame()
            
            if not df_yf.empty:
                rename_map = {'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}
                df_yf = df_yf.rename(columns=rename_map)
                cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                if all(c in df_yf.columns for c in cols):
                    hist = df_yf[cols]
                    source_used = "yahoo_fin"
        except: pass

    if hist.empty:
        try:
            ticker_obj = yf.Ticker(f"{code}.TW")
            hist_yf = ticker_obj.history(period="3mo")
            if hist_yf.empty:
                ticker_obj = yf.Ticker(f"{code}.TWO")
                hist_yf = ticker_obj.history(period="3mo")
            if not hist_yf.empty:
                hist = hist_yf
                source_used = "yfinance"
        except Exception: 
            pass

    # 僅當未使用永豐 API，且需獲取即時資訊時，才透過 twstock.realtime 補足今日最新
    if source_used != "shioaji":
        try:
            rt_data = twstock.realtime.get(code)
            if rt_data['success'] and rt_data['realtime']['latest_trade_price'] not in ['-', None, '']:
                rt_price = float(rt_data['realtime']['latest_trade_price'])
                rt_open = float(rt_data['realtime']['open']) if rt_data['realtime']['open'] != '-' else rt_price
                rt_high = float(rt_data['realtime']['high']) if rt_data['realtime']['high'] != '-' else rt_price
                rt_low = float(rt_data['realtime']['low']) if rt_data['realtime']['low'] != '-' else rt_price
                rt_vol = float(rt_data['realtime']['accumulate_trade_volume']) if rt_data['realtime']['accumulate_trade_volume'] != '-' else 0.0
                
                rt_time_str = rt_data['info']['time']
                rt_dt = datetime.strptime(rt_time_str, "%Y-%m-%d %H:%M:%S")
                today_date = pd.Timestamp(datetime.now(tz_tw).date())

                if hist.empty:
                    hist = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                else:
                    if hist.index.tzinfo is not None: hist.index = hist.index.tz_localize(None)
                    last_hist_date = hist.index[-1]
                    if last_hist_date < today_date:
                        if datetime.now(tz_tw).weekday() < 5:
                            new_row = pd.DataFrame([{'Open': rt_open, 'High': rt_high, 'Low': rt_low, 'Close': rt_price, 'Volume': rt_vol}], index=[today_date])
                            hist = pd.concat([hist, new_row])
                            hist.sort_index(inplace=True)
                    elif last_hist_date == today_date:
                        hist.at[last_hist_date, 'Close'] = rt_price
                        hist.at[last_hist_date, 'High'] = max(hist.at[last_hist_date, 'High'], rt_high)
                        hist.at[last_hist_date, 'Low'] = min(hist.at[last_hist_date, 'Low'], rt_low)
                        hist.at[last_hist_date, 'Volume'] = rt_vol
                        if hist.at[last_hist_date, 'Open'] == 0: hist.at[last_hist_date, 'Open'] = rt_open
        except: pass 

    if hist.empty: return None
    if hist.index.tzinfo is not None: hist.index = hist.index.tz_localize(None)
    hist['High'] = hist[['High', 'Close']].max(axis=1)
    hist['Low'] = hist[['Low', 'Close']].min(axis=1)

    # 包含收盤價、漲跌幅，15:00 前排除今日資料，套用昨日指標 (配合主要擷取昨日盤後資料需求)
    tz_tw_calc = pytz.timezone('Asia/Taipei')
    now_tw_calc = datetime.now(tz_tw_calc)
    switch_time = dt_time(15, 0)
    
    if now_tw_calc.time() < switch_time:
        if not hist.empty and hist.index[-1].date() == now_tw_calc.date():
            if len(hist) > 1:
                hist = hist.iloc[:-1]

    if hist.empty: return None

    live_base_price = hist.iloc[-1]['Close']
    if len(hist) >= 2: live_prev_price = hist.iloc[-2]['Close']
    else: live_prev_price = live_base_price
    if live_prev_price > 0: live_pct_change = ((live_base_price - live_prev_price) / live_prev_price) * 100
    else: live_pct_change = 0.0

    hist_strat = hist.copy()

    strategy_base_price = hist_strat.iloc[-1]['Close']
    if len(hist_strat) >= 2: prev_of_base = hist_strat.iloc[-2]['Close']
    else: prev_of_base = strategy_base_price 

    base_price_for_limit = strategy_base_price
    limit_up_show, limit_down_show = calculate_limits(base_price_for_limit)

    limit_up_T = None
    limit_down_T = None
    if len(hist_strat) >= 2:
        prev_close_T = hist_strat.iloc[-2]['Close']
        limit_up_T, limit_down_T = calculate_limits(prev_close_T)

    target_price = apply_sr_rules(strategy_base_price * 1.03, strategy_base_price)
    stop_price = apply_sr_rules(strategy_base_price * 0.97, strategy_base_price)
    
    points = []
    recent_records = hist_strat.tail(3).to_dict('records')
    recent_records.reverse()
    days_map = {0: "今", 1: "昨", 2: "前"}
    
    for idx, row in enumerate(recent_records):
        if idx in days_map:
            prefix = days_map[idx]
            h_val = apply_tick_rules(row['High'])
            l_val = apply_tick_rules(row['Low'])
            if h_val > 0 and limit_down_show <= h_val <= limit_up_show: points.append({"val": h_val, "tag": f"{prefix}高"})
            if l_val > 0 and limit_down_show <= l_val <= limit_up_show: points.append({"val": l_val, "tag": f"{prefix}低"})

    if len(hist_strat) >= 5:
        last_5_closes = hist_strat['Close'].tail(5).values
        avg_val = sum(Decimal(str(x)) for x in last_5_closes) / Decimal("5")
        ma5_raw = float(avg_val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        ma5 = apply_sr_rules(ma5_raw, strategy_base_price)
        ma5_tag = "多" if ma5_raw < strategy_base_price else ("空" if ma5_raw > strategy_base_price else "平")
        points.append({"val": ma5, "tag": ma5_tag, "force": True})

    if len(hist_strat) >= 2:
        last_candle = hist_strat.iloc[-1]
        p_open = apply_tick_rules(last_candle['Open'])
        if limit_down_show <= p_open <= limit_up_show: points.append({"val": p_open, "tag": ""})

        p_high = apply_tick_rules(last_candle['High'])
        p_low = apply_tick_rules(last_candle['Low'])
        if limit_down_show <= p_high <= limit_up_show: points.append({"val": p_high, "tag": ""})
        if limit_down_show <= p_low <= limit_up_show: 
             tag_low = "跌停" if limit_down_T and abs(p_low - limit_down_T) < 0.01 else ""
             points.append({"val": p_low, "tag": tag_low})

    if len(hist_strat) >= 3:
        pre_prev_candle = hist_strat.iloc[-2]
        pp_high = apply_tick_rules(pre_prev_candle['High'])
        pp_low = apply_tick_rules(pre_prev_candle['Low'])
        if limit_down_show <= pp_high <= limit_up_show: points.append({"val": pp_high, "tag": ""})
        if limit_down_show <= pp_low <= limit_up_show: points.append({"val": pp_low, "tag": ""})

    show_plus_3 = False
    show_minus_3 = False
    
    if not hist_strat.empty:
        high_90_raw = hist_strat['High'].max()
        low_vals = hist_strat['Low'][hist_strat['Low'] > 0]
        low_90_raw = low_vals.min() if not low_vals.empty else hist_strat['Low'].min()
            
        high_90 = apply_tick_rules(high_90_raw)
        low_90 = apply_tick_rules(low_90_raw)
        points.append({"val": high_90, "tag": "高"})
        points.append({"val": low_90, "tag": "低"})
        
        if len(hist_strat) >= 2:
             today_high = hist_strat.iloc[-1]['High']
             if limit_up_T and abs(today_high - limit_up_T) < 0.01:
                 tag_label = "漲停高" if (abs(limit_up_T - high_90_raw) < 0.05) else "漲停"
                 if limit_down_show <= limit_up_T <= limit_up_show: points.append({"val": limit_up_T, "tag": tag_label})

        if len(hist_strat) >= 2:
            high_T = hist_strat.iloc[-1]['High']
            low_T = hist_strat.iloc[-1]['Low']
            close_T = hist_strat.iloc[-1]['Close']
            if (limit_up_T and high_T >= limit_up_T - 0.01) and (limit_up_T and close_T >= limit_up_T * 0.97): show_plus_3 = True
            if (limit_down_T and low_T <= limit_down_T + 0.01) and (limit_down_T and close_T <= limit_down_T * 1.03): show_minus_3 = True

    if show_plus_3: points.append({"val": target_price, "tag": ""})
    if show_minus_3: points.append({"val": stop_price, "tag": ""})
        
    full_calc_points = []
    threed_tags = ['前高', '前低', '昨高', '昨低', '今高', '今低']
    for p in points:
        v = float(f"{p['val']:.2f}")
        if p.get('force', False) or p.get('tag') in threed_tags or (limit_down_show <= v <= limit_up_show): full_calc_points.append(p) 
    
    manual_note = saved_notes_dict.get(code, "") if saved_notes_dict else ""
    strategy_note, auto_note = generate_note_from_points(full_calc_points, manual_note, show_3d=False)
    
    if name_hint: final_name = name_hint
    elif name_map_dict and code in name_map_dict: final_name = name_map_dict[code]
    else: final_name = code

    light = "🔴" if "多" in strategy_note else ("🟢" if "空" in strategy_note else "⚪")
    final_name_display = f"{light} {final_name}"
    has_futures = "✅" if futures_set and code in futures_set else ""
    
    return {
        "代號": code, "名稱": final_name_display, "收盤價": round(live_base_price, 2), "漲跌幅": live_pct_change, "期貨": has_futures, 
        "當日漲停價": limit_up_show, "當日跌停價": limit_down_show, "自訂價(可修)": None, "獲利目標": target_price, "防守停損": stop_price,   
        "戰略備註": strategy_note, "_points": full_calc_points, "狀態": "", "_auto_note": auto_note 
    }

# ==========================================
# 主介面 (Tabs)
# ==========================================
tab1, tab2, tab_fibo, tab_db, tab3 = st.tabs(["⚡ 當沖戰略室 ⚡", "💰 當沖損益室 💰", "📈 費波計算", "📚 戰略資料庫", "📅 台股行事曆"])

with tab1:
    col_search, col_file = st.columns([2, 1])
    with col_search:
        code_map, name_map = load_local_stock_names()
        stock_options = [f"{code} {name}" for code, name in sorted(code_map.items())]
        
        src_tab1, src_tab2 = st.tabs(["📂 本機", "☁️ 雲端"])
        with src_tab1:
            uploaded_file = st.file_uploader("上傳檔案 (CSV/XLS/HTML)", type=['xlsx', 'csv', 'html', 'xls'], label_visibility="collapsed")
            selected_sheet = 0
            if uploaded_file:
                try:
                    if not uploaded_file.name.endswith('.csv'):
                        xl_file = pd.ExcelFile(uploaded_file)
                        sheet_options = xl_file.sheet_names
                        default_idx = sheet_options.index("週轉率") if "週轉率" in sheet_options else 0
                        selected_sheet = st.selectbox("選擇工作表", sheet_options, index=default_idx)
                except: pass

        with src_tab2:
            def on_history_change(): st.session_state.cloud_url_input = st.session_state.history_selected
            history_opts = st.session_state.url_history if st.session_state.url_history else ["(無紀錄)"]
            c_sel, c_del = st.columns([8, 1], gap="small")
            with c_sel:
                selected = st.selectbox("📜 歷史紀錄 (選取自動填入)", options=history_opts, key="history_selected", index=None, placeholder="請選擇...", on_change=on_history_change, label_visibility="collapsed")
            with c_del:
                if st.button("🗑️", help="刪除選取的歷史紀錄"):
                    if st.session_state.history_selected and st.session_state.history_selected in st.session_state.url_history:
                        st.session_state.url_history.remove(st.session_state.history_selected)
                        save_url_history(st.session_state.url_history)
                        st.toast("已刪除。", icon="🗑️")
                        st.rerun()
            st.text_input("輸入連結 (CSV/Excel/Google Sheet)", key="cloud_url_input", placeholder="https://...")
        
        def update_search_cache(): save_search_cache(st.session_state.search_multiselect)
        search_selection = st.multiselect("🔍 快速查詢 (中文/代號)", options=stock_options, key="search_multiselect", on_change=update_search_cache, placeholder="輸入 2330 或 台積電...")

    c_run, c_space = st.columns([1.5, 5])
    with c_run: btn_run = st.button("🚀 執行分析", use_container_width=True)

    if btn_run:
        save_search_cache(st.session_state.search_multiselect)
        if not st.session_state.futures_list: st.session_state.futures_list = fetch_futures_list()
        targets = []
        df_up = pd.DataFrame()
        current_url = st.session_state.cloud_url_input.strip()
        if current_url:
            if current_url not in st.session_state.url_history:
                st.session_state.url_history.insert(0, current_url) 
                save_url_history(st.session_state.url_history)
        
        try:
            if uploaded_file:
                uploaded_file.seek(0)
                fname = uploaded_file.name.lower()
                if fname.endswith('.csv'):
                    try: df_up = pd.read_csv(uploaded_file, dtype=str, encoding='cp950')
                    except: 
                        uploaded_file.seek(0)
                        df_up = pd.read_csv(uploaded_file, dtype=str)
                elif fname.endswith('.html') or fname.endswith('.htm') or fname.endswith('.xls'):
                    try: dfs = pd.read_html(uploaded_file, encoding='cp950')
                    except:
                        uploaded_file.seek(0)
                        dfs = pd.read_html(uploaded_file, encoding='utf-8')
                    for df in dfs:
                        if df.apply(lambda r: r.astype(str).str.contains('代號').any(), axis=1).any():
                             df_up = df
                             for i, row in df.iterrows():
                                 if "代號" in row.values:
                                     df_up.columns = row
                                     df_up = df_up.iloc[i+1:]
                                     break
                             break
                    if df_up.empty and dfs: df_up = dfs[0]
                elif fname.endswith('.xlsx'):
                    df_up = pd.read_excel(uploaded_file, sheet_name=selected_sheet, dtype=str)

            elif st.session_state.cloud_url_input:
                url = st.session_state.cloud_url_input
                if "docs.google.com" in url and "/spreadsheets/" in url and "/edit" in url:
                    url = url.split("/edit")[0] + "/export?format=csv"
                try: df_up = pd.read_csv(url, dtype=str)
                except:
                    try: df_up = pd.read_excel(url, dtype=str)
                    except: st.error("❌ 無法讀取雲端檔案。")
        except Exception as e: st.error(f"讀取失敗: {e}")

        if search_selection:
            for item in search_selection:
                parts = item.split(' ', 1)
                targets.append((parts[0], parts[1] if len(parts) > 1 else "", 'search', 9999))

        if not df_up.empty:
            df_up.columns = df_up.columns.astype(str).str.strip()
            c_col = next((c for c in df_up.columns if "代號" in str(c)), None)
            n_col = next((c for c in df_up.columns if "名稱" in str(c)), None)
            if c_col:
                limit_rows = st.session_state.limit_rows
                count = 0
                for _, row in df_up.iterrows():
                    c_raw = str(row[c_col]).replace('=', '').replace('"', '').strip()
                    if not c_raw or c_raw.lower() == 'nan': continue
                    is_valid = False
                    if c_raw.isdigit() and len(c_raw) <= 4: is_valid = True
                    elif len(c_raw) > 0 and (c_raw[0].isdigit() or c_raw[0] in ['0','00']): is_valid = True
                    if not is_valid: continue
                    if c_raw in st.session_state.ignored_stocks: continue
                    if hide_non_stock:
                        is_etf = c_raw.startswith('00')
                        is_warrant = (len(c_raw) > 4) and c_raw.isdigit()
                        if is_etf or is_warrant: continue
                    n = str(row[n_col]) if n_col else ""
                    if n.lower() == 'nan': n = ""
                    targets.append((c_raw, n, 'upload', count))
                    count += 1

        st.session_state.all_candidates = targets
        seen = set()
        status_text = st.empty()
        bar = st.progress(0)
        
        upload_limit = st.session_state.limit_rows
        upload_current = 0
        existing_data = {}
        st.session_state.stock_data = pd.DataFrame() 
        
        futures_copy = set(st.session_state.futures_list)
        notes_copy = dict(st.session_state.saved_notes)
        code_map_copy, _ = load_local_stock_names()

        def process_stock_task(t_code, t_name, t_source, t_extra, f_set, n_dict, c_map):
            time.sleep(random.uniform(0.5, 1.5))
            try: return (t_code, t_source, t_extra, fetch_stock_data_raw(t_code, t_name, t_extra, f_set, n_dict, c_map))
            except Exception: return (t_code, t_source, t_extra, None)

        tasks_to_run = []
        for i, (code, name, source, extra) in enumerate(targets):
            if source == 'upload' and upload_current >= upload_limit: continue
            if code in st.session_state.ignored_stocks: continue
            if (code, source) in seen: continue
            tasks_to_run.append((code, name, source, extra))
            if source == 'upload': upload_current += 1
            seen.add((code, source))

        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_task = {executor.submit(process_stock_task, t[0], t[1], t[2], t[3], futures_copy, notes_copy, code_map_copy): t for t in tasks_to_run}
            completed_count = 0
            total_tasks = len(tasks_to_run) if len(tasks_to_run) > 0 else 1
            
            for future in as_completed(future_to_task):
                t_code, t_source, t_extra, data = future.result()
                completed_count += 1
                bar.progress(min(completed_count / total_tasks, 1.0))
                status_text.text(f"正在分析 ({completed_count}/{total_tasks}): {t_code} ...")
                if data:
                    data['_source'] = t_source
                    data['_order'] = t_extra
                    data['_source_rank'] = 1 if t_source == 'upload' else 2
                    existing_data[t_code] = data
        
        bar.empty()
        status_text.empty()
        
        if existing_data:
            st.session_state.stock_data = pd.DataFrame(list(existing_data.values()))
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)

    if not st.session_state.stock_data.empty:
        df_all = st.session_state.stock_data.copy()
        if '_source' not in df_all.columns: df_all['_source'] = 'upload'
        df_all = df_all.rename(columns={"漲停價": "當日漲停價", "跌停價": "當日跌停價", "獲利目標": "+3%", "防守停損": "-3%"})
        df_all['代號'] = df_all['代號'].astype(str)
        df_all = df_all[~df_all['代號'].isin(st.session_state.ignored_stocks)]
        
        if hide_non_stock:
             mask_etf = df_all['代號'].str.startswith('00')
             mask_warrant = (df_all['代號'].str.len() > 4) & df_all['代號'].str.isdigit()
             df_all = df_all[~(mask_etf | mask_warrant)]
        
        if '_source_rank' in df_all.columns: df_all = df_all.sort_values(by=['_source_rank', '_order'])
        df_display = df_all.reset_index(drop=True)
        
        for i, row in df_display.iterrows():
            points = row.get('_points', [])
            manual = st.session_state.saved_notes.get(row['代號'], "")
            new_full_note, new_auto_note = generate_note_from_points(points, manual, show_3d_hilo)
            df_display.at[i, "戰略備註"] = new_full_note
            df_display.at[i, "_auto_note"] = new_auto_note
            light = "🔴" if "多" in new_full_note else ("🟢" if "空" in new_full_note else "⚪")
            df_display.at[i, "名稱"] = f"{light} {row['名稱'].split(' ', 1)[-1]}"

        note_width_px = calculate_note_width(df_display['戰略備註'], current_font_size)
        df_display["移除"] = False
        points_map = df_display.set_index('代號')['_points'].to_dict() if '_points' in df_display.columns else {}
        auto_notes_dict = df_display.set_index('代號')['_auto_note'].to_dict() if '_auto_note' in df_display.columns else {}

        input_cols = ["移除", "代號", "名稱", "戰略備註", "自訂價(可修)", "狀態", "當日漲停價", "當日跌停價", "+3%", "-3%", "收盤價", "漲跌幅", "期貨"]
        for col in input_cols:
            if col not in df_display.columns: df_display[col] = None

        cols_to_fmt = ["當日漲停價", "當日跌停價", "+3%", "-3%", "自訂價(可修)"]
        for c in cols_to_fmt:
            if c in df_display.columns: df_display[c] = df_display[c].apply(fmt_price)

        if "收盤價" in df_display.columns: df_display["收盤價"] = df_display["收盤價"].astype(object)
        if "漲跌幅" in df_display.columns: df_display["漲跌幅"] = df_display["漲跌幅"].astype(object)

        if "收盤價" in df_display.columns and "漲跌幅" in df_display.columns:
            for i in range(len(df_display)):
                try:
                    p = float(df_display.at[i, "收盤價"])
                    chg = float(df_display.at[i, "漲跌幅"])
                    color_icon = "🔴" if chg > 0 else ("🟢" if chg < 0 else "⚪")
                    df_display.at[i, "收盤價"] = f"{color_icon} {fmt_price(p)}"
                    df_display.at[i, "漲跌幅"] = f"{color_icon} {chg:+.2f}%"
                except:
                    df_display.at[i, "收盤價"] = fmt_price(df_display.at[i, "收盤價"])
                    df_display.at[i, "漲跌幅"] = f"{float(df_display.at[i, '漲跌幅']):.2f}%"

        df_display = df_display.reset_index(drop=True)
        for col in input_cols:
             if col != "移除": df_display[col] = df_display[col].astype(str)

        edited_df = st.data_editor(
            df_display[input_cols],
            column_config={
                "移除": st.column_config.CheckboxColumn("刪除", width=40, help="勾選後刪除並自動遞補"),
                "代號": st.column_config.TextColumn(disabled=True, width=50), 
                "名稱": st.column_config.TextColumn(disabled=True, width="small"),
                "收盤價": st.column_config.TextColumn(width="small", disabled=True),
                "漲跌幅": st.column_config.TextColumn(disabled=True, width="small"),
                "期貨": st.column_config.TextColumn(disabled=True, width=40), 
                "自訂價(可修)": st.column_config.TextColumn("自訂價 ✏️", width=60), 
                "當日漲停價": st.column_config.TextColumn(width="small", disabled=True),
                "當日跌停價": st.column_config.TextColumn(width="small", disabled=True),
                "+3%": st.column_config.TextColumn(width="small", disabled=True),
                "-3%": st.column_config.TextColumn(width="small", disabled=True),
                "狀態": st.column_config.TextColumn(width=60, disabled=True),
                "戰略備註": st.column_config.TextColumn("戰略備註 ✏️", width=note_width_px, disabled=False),
            },
            hide_index=True, use_container_width=False, num_rows="fixed", key="main_editor"
        )
        
        if not edited_df.empty:
            trigger_rerun = False
            if "移除" in edited_df.columns:
                to_remove = edited_df[edited_df["移除"] == True]
                if not to_remove.empty:
                    remove_codes = to_remove["代號"].unique()
                    for c in remove_codes: st.session_state.ignored_stocks.add(str(c))
                    st.session_state.stock_data = st.session_state.stock_data[~st.session_state.stock_data["代號"].isin(remove_codes)]
                    
                    upload_count = len(st.session_state.stock_data[st.session_state.stock_data['_source'] == 'upload'])
                    needed = st.session_state.limit_rows - upload_count
                    
                    if needed > 0 and st.session_state.all_candidates:
                        replenished_count = 0
                        existing_codes = set(st.session_state.stock_data['代號'].astype(str))
                        futures_copy = set(st.session_state.futures_list)
                        notes_copy = dict(st.session_state.saved_notes)
                        code_map_copy, _ = load_local_stock_names()
                        
                        for cand in st.session_state.all_candidates:
                             c_code, c_name, c_source, c_extra = str(cand[0]), cand[1], cand[2], cand[3]
                             if c_source != 'upload' or c_code in st.session_state.ignored_stocks or c_code in existing_codes: continue
                             
                             data = fetch_stock_data_raw(c_code, c_name, c_extra, futures_copy, notes_copy, code_map_copy)
                             if data:
                                 data.update({'_source': c_source, '_order': c_extra, '_source_rank': 1})
                                 st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame([data])], ignore_index=True)
                                 existing_codes.add(c_code)
                                 replenished_count += 1
                             if replenished_count >= needed: break
                    
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                    trigger_rerun = True

            if not trigger_rerun and st.session_state.auto_update_last_row:
                last_visible_idx = len(edited_df) - 1
                if last_visible_idx >= 0:
                    last_visible_code = edited_df.iloc[last_visible_idx]['代號']
                    update_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
                    
                    for i, row in st.session_state.stock_data.iterrows():
                        if row['代號'] == last_visible_code:
                            if last_visible_code in update_map:
                                new_price = update_map[last_visible_code]['自訂價(可修)']
                                old_price = str(row['自訂價(可修)'])
                                if old_price != str(new_price) and str(new_price).strip().lower() != 'nan':
                                    if st.session_state.update_delay_sec > 0: time.sleep(st.session_state.update_delay_sec)
                                    
                                    for j, r in st.session_state.stock_data.iterrows():
                                        c_code = r['代號']
                                        if c_code in update_map:
                                            np, nn = update_map[c_code]['自訂價(可修)'], update_map[c_code]['戰略備註']
                                            st.session_state.stock_data.at[j, '自訂價(可修)'] = np
                                            if str(r['戰略備註']) != str(nn):
                                                b_auto = str(auto_notes_dict.get(c_code, "")).strip()
                                                n_note = str(nn).strip()
                                                st.session_state.stock_data.at[j, '戰略備註'] = nn
                                                st.session_state.saved_notes[c_code] = n_note[len(b_auto):] if b_auto and n_note.startswith(b_auto) else f"[M]{n_note}"
                                        
                                        st.session_state.stock_data.at[j, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[j], points_map)
                                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                                    trigger_rerun = True
                            break

            if trigger_rerun: st.rerun()

        df_curr = st.session_state.stock_data
        if not df_curr.empty:
            upload_count = len(df_curr) if '_source' not in df_curr.columns else len(df_curr[df_curr['_source'] == 'upload'])
            limit = st.session_state.limit_rows
            
            if upload_count < limit and st.session_state.all_candidates:
                needed = limit - upload_count
                replenished_count = 0
                existing_codes = set(st.session_state.stock_data['代號'].astype(str))
                futures_copy = set(st.session_state.futures_list)
                notes_copy = dict(st.session_state.saved_notes)
                code_map_copy, _ = load_local_stock_names()

                with st.spinner("正在載入更多資料..."):
                    for cand in st.session_state.all_candidates:
                         c_code, c_name, c_source, c_extra = str(cand[0]), cand[1], cand[2], cand[3]
                         if c_source != 'upload' or c_code in st.session_state.ignored_stocks or c_code in existing_codes: continue
                         data = fetch_stock_data_raw(c_code, c_name, c_extra, futures_copy, notes_copy, code_map_copy)
                         if data:
                             data.update({'_source': c_source, '_order': c_extra, '_source_rank': 1})
                             st.session_state.stock_data = pd.concat([st.session_state.stock_data, pd.DataFrame([data])], ignore_index=True)
                             existing_codes.add(c_code)
                             replenished_count += 1
                         if replenished_count >= needed: break
                
                if replenished_count > 0:
                    save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
                    st.toast(f"已更新顯示筆數，增加 {replenished_count} 檔。", icon="🔄")
                    st.rerun()

        st.markdown("---")
        col_btn, col_clear, _ = st.columns([2, 2, 4])
        with col_btn: btn_update = st.button("⚡ 執行更新&儲存手動備註", use_container_width=True, type="primary")
        with col_clear: btn_clear_notes = st.button("🧹 清除手動備註", use_container_width=True, help="清除所有記憶的戰略備註內容")
        
        if btn_clear_notes:
            st.session_state.saved_notes = {}
            st.toast("手動備註已清除", icon="🧹")
            if not st.session_state.stock_data.empty:
                 for idx, row in st.session_state.stock_data.iterrows():
                     clean_note, _ = generate_note_from_points(row.get('_points', []), "", show_3d_hilo)
                     st.session_state.stock_data.at[idx, '戰略備註'] = clean_note
                     if '_auto_note' in st.session_state.stock_data.columns: st.session_state.stock_data.at[idx, '_auto_note'] = clean_note
            save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
            st.rerun()
        
        auto_update = st.checkbox("☑️ 啟用最後一列自動更新", value=st.session_state.auto_update_last_row, key="toggle_auto_update")
        st.session_state.auto_update_last_row = auto_update
        if auto_update:
            col_delay, _ = st.columns([2, 8])
            with col_delay: st.session_state.update_delay_sec = st.number_input("⏳ 緩衝秒數", min_value=0.0, max_value=5.0, step=0.1, value=st.session_state.update_delay_sec)

        if btn_update:
             update_map = edited_df.set_index('代號')[['自訂價(可修)', '戰略備註']].to_dict('index')
             for i, row in st.session_state.stock_data.iterrows():
                code = row['代號']
                if code in update_map:
                    new_note = update_map[code]['戰略備註']
                    st.session_state.stock_data.at[i, '自訂價(可修)'] = update_map[code]['自訂價(可修)']
                    if str(row['戰略備註']) != str(new_note):
                        b_auto = str(auto_notes_dict.get(code, "")).strip()
                        n_note = str(new_note).strip()
                        st.session_state.saved_notes[code] = n_note[len(b_auto):] if b_auto and n_note.startswith(b_auto) else f"[M]{n_note}"
                    st.session_state.stock_data.at[i, '戰略備註'] = new_note
                st.session_state.stock_data.at[i, '狀態'] = recalculate_row(st.session_state.stock_data.iloc[i], points_map)
             save_data_cache(st.session_state.stock_data, st.session_state.ignored_stocks, st.session_state.all_candidates, st.session_state.saved_notes)
             st.rerun()

with tab2:
    st.markdown("#### 💰 當沖損益室 💰")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        calc_price = st.number_input("基準價格", value=float(st.session_state.calc_base_price), step=0.01, format="%.2f", key="input_base_price")
        if calc_price != st.session_state.calc_base_price:
            st.session_state.calc_base_price = calc_price
            st.session_state.calc_view_price = apply_tick_rules(calc_price)
    with c2: shares = st.number_input("股數", value=1000, step=1000)
    with c3: discount = st.number_input("手續費折扣 (折)", value=2.8, step=0.1, min_value=0.1, max_value=10.0)
    with c4: min_fee = st.number_input("最低手續費 (元)", value=20, step=1)
    with c5: tick_count = st.number_input("顯示檔數 (檔)", value=10, min_value=1, max_value=50, step=1)
    direction = st.radio("交易方向", ["當沖多 (先買後賣)", "當沖空 (先賣後買)"], horizontal=True)
    limit_up, limit_down = calculate_limits(st.session_state.calc_base_price)
    b1, b2, _ = st.columns([1, 1, 6])
    with b1:
        if st.button("🔽 向下", use_container_width=True):
            if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
            st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, -tick_count)
            if st.session_state.calc_view_price < limit_down: st.session_state.calc_view_price = limit_down
            st.rerun()
    with b2:
        if st.button("🔼 向上", use_container_width=True):
            if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = st.session_state.calc_base_price
            st.session_state.calc_view_price = move_tick(st.session_state.calc_view_price, tick_count)
            if st.session_state.calc_view_price > limit_up: st.session_state.calc_view_price = limit_up
            st.rerun()
    
    ticks_range = range(tick_count, -(tick_count + 1), -1)
    calc_data = []
    base_p = st.session_state.calc_base_price
    if 'calc_view_price' not in st.session_state: st.session_state.calc_view_price = base_p
    view_p = st.session_state.calc_view_price
    is_long = "多" in direction
    fee_rate = 0.001425; tax_rate = 0.0015 
    
    for i in ticks_range:
        p = move_tick(view_p, i)
        
        if p > limit_up + 0.001 or p < limit_down - 0.001: continue
        
        if is_long:
            buy_price = base_p; sell_price = p
            buy_fee = max(min_fee, math.floor(buy_price * shares * fee_rate * (discount/10)))
            sell_fee = max(min_fee, math.floor(sell_price * shares * fee_rate * (discount/10)))
            tax = math.floor(sell_price * shares * tax_rate)
            cost = (buy_price * shares) + buy_fee
            income = (sell_price * shares) - sell_fee - tax
            profit = income - cost
            total_fee = buy_fee + sell_fee
        else: 
            sell_price = base_p; buy_price = p
            sell_fee = max(min_fee, math.floor(sell_price * shares * fee_rate * (discount/10)))
            buy_fee = max(min_fee, math.floor(buy_price * shares * fee_rate * (discount/10)))
            tax = math.floor(sell_price * shares * tax_rate)
            income = (sell_price * shares) - sell_fee - tax
            cost = (buy_price * shares) + buy_fee
            profit = income - cost
            total_fee = buy_fee + sell_fee
        roi = 0
        if (base_p * shares) != 0: roi = (profit / (base_p * shares)) * 100
        diff = p - base_p
        diff_str = f"{diff:+.2f}".rstrip('0').rstrip('.') if diff != 0 else "0"
        if diff > 0 and not diff_str.startswith('+'): diff_str = "+" + diff_str
        
        note_type = ""
        if abs(p - limit_up) < 0.001: note_type = "up"
        elif abs(p - limit_down) < 0.001: note_type = "down"
        is_base = (abs(p - base_p) < 0.001)
        
        calc_data.append({
            "成交價": fmt_price(p), "漲跌": diff_str, "預估損益": int(profit), "報酬率%": f"{roi:+.2f}%",
            "手續費": int(total_fee), "交易稅": int(tax), "_profit": profit, "_note_type": note_type, "_is_base": is_base
        })
        
    df_calc = pd.DataFrame(calc_data)
    
    def style_calc_row(row):
        is_base = row['_is_base']
        nt = row['_note_type']
        prof = row['_profit']
        
        if is_base: return ['background-color: #ffffcc; color: black; font-weight: bold; border: 2px solid #ffd700;'] * len(row)
        if nt == 'up': return ['background-color: #ff4b4b; color: white; font-weight: bold'] * len(row)
        if nt == 'down': return ['background-color: #00cc00; color: white; font-weight: bold'] * len(row)
        if prof > 0: return ['color: #ff4b4b; font-weight: bold'] * len(row) 
        if prof < 0: return ['color: #00cc00; font-weight: bold'] * len(row) 
        return ['color: gray'] * len(row)

    if not df_calc.empty:
        table_height = (len(df_calc) + 1) * 35 
        st.dataframe(
            df_calc.style.apply(style_calc_row, axis=1), 
            use_container_width=False, 
            hide_index=True, 
            height=table_height,
            column_config={"_profit": None, "_note_type": None, "_is_base": None}
        )

with tab_fibo:
    st.markdown("#### 📈 費波計算")
    
    def format_fibo_tag(key):
        val = st.session_state[key].strip()
        if not val: 
            save_fibo_config()
            return
        if "(" in val and val.endswith(")"): 
            save_fibo_config()
            return
        
        code_map, name_map = load_local_stock_names()
        if val.isdigit():
            name = code_map.get(val, "")
            if name: st.session_state[key] = f"{name}({val})"
        else:
            matched_stocks = []
            for name, code in name_map.items():
                if val in name:
                    matched_stocks.append((name, code))
            if matched_stocks:
                def sort_key(item):
                    c = item[1]
                    if c.isdigit() and len(c) <= 4: return 0
                    elif c.isdigit(): return 1
                    return 2
                matched_stocks.sort(key=sort_key)
                best_match = matched_stocks[0]
                st.session_state[key] = f"{best_match[0]}({best_match[1]})"
        save_fibo_config()
    
    tab_fibo_chart, tab_fibo_manual = st.tabs(["📊 圖表分析", "🧮 手動計算"])

    with tab_fibo_chart:
        code_map_fibo, name_map_fibo = load_local_stock_names()

        def set_fibo_search(val):
            st.session_state.fibo_search_input = val
            st.session_state.fibo_trigger_search = True
            if "^TWII" in val or "TWF=F" in val or "TMF=F" in val or "加權" in val or "台指" in val:
                st.session_state.fibo_interval = "5m"
            else:
                st.session_state.fibo_interval = "1d"

        with st.expander("⚙️ 設定快速標籤"):
            st.info("💡 將圖表字體大小設定獨立：")
            st.session_state.fibo_font_size = st.slider("圖表標籤字體大小", min_value=8, max_value=24, value=st.session_state.fibo_font_size)
            st.write("---")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.text_input("快速標籤 1", key="custom_tag_1", on_change=format_fibo_tag, args=("custom_tag_1",))
            c2.text_input("快速標籤 2", key="custom_tag_2", on_change=format_fibo_tag, args=("custom_tag_2",))
            c3.text_input("快速標籤 3", key="custom_tag_3", on_change=format_fibo_tag, args=("custom_tag_3",))
            c4.text_input("快速標籤 4", key="custom_tag_4", on_change=format_fibo_tag, args=("custom_tag_4",))
            c5.text_input("快速標籤 5", key="custom_tag_5", on_change=format_fibo_tag, args=("custom_tag_5",))

        st.write("📌 **快速查詢標籤** (點擊按鈕直接帶入)")
        btn_labels = [
            ("加權指數(^TWII)", "加權指數(^TWII)"),
            ("台指期貨(TWF=F)", "台指期貨(TWF=F)"),
            ("微型台指期貨(TMF=F)", "微型台指期貨(TMF=F)")
        ]
        for tag in [st.session_state.custom_tag_1, st.session_state.custom_tag_2, st.session_state.custom_tag_3, st.session_state.custom_tag_4, st.session_state.custom_tag_5]:
            if tag.strip(): btn_labels.append((tag.strip(), tag.strip()))
        
        if btn_labels:
            tag_cols = st.columns(len(btn_labels))
            for i, (label, val) in enumerate(btn_labels):
                if i < len(tag_cols):
                    tag_cols[i].button(label, on_click=set_fibo_search, args=(val,), use_container_width=True, key=f"btn_fibo_{i}")

        fibo_stock_options = [f"{n}({c})" for c, n in sorted(code_map_fibo.items())]
        current_val = st.session_state.fibo_search_input
        default_index = 0
        search_list = ["加權指數(^TWII)", "台指期貨(TWF=F)", "微型台指期貨(TMF=F)"] + fibo_stock_options
        
        for i, opt in enumerate(search_list):
            if current_val in opt:
                default_index = i
                break

        def selectbox_changed():
            val = st.session_state.fibo_selectbox
            st.session_state.fibo_search_input = val
            if "^TWII" in val or "TWF=F" in val or "TMF=F" in val or "加權" in val or "台指" in val:
                st.session_state.fibo_interval = "5m"
            else:
                st.session_state.fibo_interval = "1d"

        selected_raw = st.selectbox(
            "🔍 搜尋股票 (可直接輸入股號或股名查找，或點擊上方快捷按鈕)",
            options=search_list,
            index=default_index,
            key="fibo_selectbox",
            on_change=selectbox_changed
        )
        
        final_target = st.session_state.fibo_search_input
        
        st.write("---")
        st.write("⚙️ **圖表顯示設定**")
        col_m1, col_m2, col_m3, col_m4, col_v, col_w = st.columns([1, 1, 1, 1, 1.5, 2.5])
        s_ma5 = col_m1.checkbox("5MA (橘)", value=True)
        s_ma10 = col_m2.checkbox("10MA (淺藍)", value=True)
        s_ma20 = col_m3.checkbox("20MA (綠)", value=True)
        s_ma60 = col_m4.checkbox("60MA (黃)", value=True)
        s_vol = col_v.checkbox("📊 顯示成交量", value=True)
        ma_w = col_w.slider("均線粗細", min_value=1.0, max_value=5.0, value=st.session_state.ma_w, step=0.5, key="ma_w", on_change=save_fibo_config, label_visibility="collapsed")
        
        ma_flags = {'5': s_ma5, '10': s_ma10, '20': s_ma20, '60': s_ma60}

        st.write("---")
        interval_options = {"1m": "1分", "5m": "5分", "15m": "15分", "60m": "60分", "1d": "日", "1wk": "週", "1mo": "月"}
        try: default_radio_idx = list(interval_options.keys()).index(st.session_state.fibo_interval)
        except: default_radio_idx = 4 

        selected_interval_label = st.radio(
            "⏱️ 選擇時間標籤",
            options=list(interval_options.values()),
            index=default_radio_idx,
            horizontal=True
        )
        
        selected_interval = list(interval_options.keys())[list(interval_options.values()).index(selected_interval_label)]
        st.session_state.fibo_interval = selected_interval 
        
        plot_fibonacci_chart(final_target, selected_interval, font_size=st.session_state.fibo_font_size, ma_flags=ma_flags, ma_width=st.session_state.ma_w, show_vol=s_vol)

    with tab_fibo_manual:
        st.write("📌 **手動輸入高低點，計算費波納契回撤與延伸點位**")
        col_table, col_empty = st.columns([1, 2])
        
        with col_table:
            col_h, col_l = st.columns(2)
            with col_h:
                fibo_high = st.number_input("輸入波段高點：", value=None, step=1.0, format="%.2f")
            with col_l:
                fibo_low = st.number_input("輸入波段低點：", value=None, step=1.0, format="%.2f")
                
            if fibo_high is not None and fibo_low is not None:
                if fibo_high > 0 and fibo_low > 0 and fibo_high >= fibo_low:
                    diff = fibo_high - fibo_low
                    ratios_manual = [-2.618, -2.0, -1.618, -1.0, 0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.618, 2.0, 2.618]
                    
                    fibo_data = []
                    for r in ratios_manual:
                        price = fibo_low + (r * diff)
                        calc_price = round(price, 2)
                        r_label = "1" if r == 1.0 else ("0" if r == 0.0 else f"{r:g}")
                        fibo_data.append({
                            "比例": r_label,
                            "計算點位": f"{calc_price:.2f}",
                            "_raw_r": r 
                        })
                    
                    df_fibo = pd.DataFrame(fibo_data)
                    
                    def style_fibo_manual(row):
                        important_ratios = [0.0, 0.382, 0.5, 0.618, 1.0]
                        if row["_raw_r"] in important_ratios:
                            return ['background-color: #ffffcc; color: black; font-weight: bold;'] * len(row)
                        return [''] * len(row)
                        
                    table_height = (len(df_fibo) + 1) * 36
                    styled_fibo = df_fibo.style.apply(style_fibo_manual, axis=1)
                    
                    st.dataframe(
                        styled_fibo, 
                        use_container_width=True, 
                        height=table_height, 
                        hide_index=True,
                        column_config={"_raw_r": None}
                    )
                else:
                    st.warning("波段高點必須大於波段低點且大於0")
            else:
                st.info("請在上方輸入高低點數值開始計算。")


with tab_db:
    sub_tab1, sub_tab2, sub_tab3 = st.tabs(["三大法人買賣超", "台指期籌碼快訊", "處置股"])
    
    with sub_tab1:
        st.markdown("#### 📊 台股三大法人每日買賣超統計")
        selected_date = st.date_input("選擇日期", datetime.today())
        date_str = selected_date.strftime("%Y%m%d")
        
        df_inst = get_major_institutional_data(date_str)
        if df_inst is not None:
            st.subheader(f"📅 {selected_date.strftime('%Y-%m-%d')} 統計結果")
            
            try:
                styled_df = df_inst.style.map(color_negative_positive, subset=['買賣差額']).format({'買進金額': '{:,.0f}', '賣出金額': '{:,.0f}', '買賣差額': '{:,.0f}'})
            except AttributeError:
                styled_df = df_inst.style.applymap(color_negative_positive, subset=['買賣差額']).format({'買進金額': '{:,.0f}', '賣出金額': '{:,.0f}', '買賣差額': '{:,.0f}'})
            
            # 使用 columns 進行縮排，不讓表格佔滿全螢幕
            col_tbl, _ = st.columns([1.5, 1])
            with col_tbl:
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
            st.caption("數據來源：[台灣證券交易所 (TWSE)](https://www.twse.com.tw/zh/trading/foreign/bfi82u.html)")
        else:
            st.warning("該日期目前無資料（可能尚未開市或為休假日或證交所 API 觸發防護防阻）。")
                
        st.markdown("---")
        st.markdown("#### 📈 法人當日買賣超個股")
        inst_tabs = st.tabs(["外資當日買賣超", "投信當日買賣超", "自營商當日買賣超"])
        with inst_tabs[0]:
            components.html(fetch_fubon_html("https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGK_D.djhtm"), height=600, scrolling=True)
        with inst_tabs[1]:
            components.html(fetch_fubon_html("https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGK_DD.djhtm"), height=600, scrolling=True)
        with inst_tabs[2]:
            components.html(fetch_fubon_html("https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGK_DB.djhtm"), height=600, scrolling=True)

    with sub_tab2:
        st.markdown("#### 📑 永豐期貨盤後籌碼自動化工具")
        
        if st.button("🔄 刷新最新報告清單"):
            st.cache_data.clear()
            st.rerun()

        reports = get_report_list()

        if not reports:
            st.warning("目前找不到相關報告，請檢查官網是否變動或稍後再試。")
        else:
            latest_report = reports[0]
            st.markdown(f"### 🔥 最新快訊: {latest_report['日期']} | {latest_report['title']}")
            
            with st.spinner("正在下載並自動預覽最新報告..."):
                data = fetch_and_parse_pdf(latest_report['url'])
                
                if data['ratio'] != "N/A" and data['ratio'] != "解析錯誤":
                    val = float(data['ratio'])
                    st.metric("散戶小台多空比", f"{val}%", delta=f"{val}%", delta_color="inverse")
                
                if data.get('images'):
                    col_img, _ = st.columns([1, 2])
                    with col_img:
                        for img in data['images']:
                            st.image(img, use_container_width=True)
                else:
                    st.warning("⚠️ 無法自動轉譯為圖片，請使用下方連結開啟。")
                    
            st.link_button("📥 點此進入原始報告下載頁面", latest_report['url'])
            
            st.divider()
            st.markdown("#### 📅 歷史報告清單")
            for idx, report in enumerate(reports[1:], 1):
                with st.expander(f"📅 {report['日期']} | {report['title']}"):
                    st.write(f"連結: [點此查看原始 PDF]({report['url']})")

    with sub_tab3:
        st.markdown("#### 🚨 處置股預測與公告")
        components.iframe("https://cmfaren.github.io/dispositionforecast/", height=800, scrolling=True)

with tab3:
    def change_month(delta):
        st.session_state.cal_month += delta
        if st.session_state.cal_month > 12:
            st.session_state.cal_month = 1
            st.session_state.cal_year += 1
        elif st.session_state.cal_month < 1:
            st.session_state.cal_month = 12
            st.session_state.cal_year -= 1
        
        if 'sel_year_box' in st.session_state: del st.session_state['sel_year_box']
        if 'sel_month_box' in st.session_state: del st.session_state['sel_month_box']

    col_sel_y, col_sel_m = st.columns(2)
    with col_sel_y:
        current_year_idx = range(2024, 2031).index(st.session_state.cal_year)
        new_year = st.selectbox("年份", range(2024, 2031), index=current_year_idx, key='sel_year_box')
        if new_year != st.session_state.cal_year:
            st.session_state.cal_year = new_year
            st.rerun()

    with col_sel_m:
        current_month_idx = st.session_state.cal_month - 1
        new_month = st.selectbox("月份", range(1, 13), index=current_month_idx, key='sel_month_box')
        if new_month != st.session_state.cal_month:
            st.session_state.cal_month = new_month
            st.rerun()

    sel_year = st.session_state.cal_year
    sel_month = st.session_state.cal_month

    col_prev, col_header, col_next = st.columns([1, 8, 1])
    with col_prev: st.button("◀️", on_click=change_month, args=(-1,), use_container_width=True)
    with col_next: st.button("▶️", on_click=change_month, args=(1,), use_container_width=True)
    with col_header: st.markdown(f"<div class='calendar-header'>{sel_year}/{sel_month:02}</div>", unsafe_allow_html=True)

    def get_holidays(year):
        h = {}
        if year == 2025:
             h.update({
                 (1, 1): "元旦", (1, 27): "春節", (1, 28): "春節", (1, 29): "春節", (1, 30): "春節", (1, 31): "春節",
                 (2, 3): "春節", (2, 28): "228紀念日", (4, 3): "兒童節", (4, 4): "清明節",
                 (5, 1): "勞動節", (5, 30): "端午節", (10, 6): "中秋節", (10, 10): "國慶日"
             })
        if year == 2026:
            h.update({
                (1, 1): "元旦", (2, 11): "封關日", (2, 12): "市場無交易", (2, 13): "市場無交易",
                (2, 14): "春節", (2, 15): "春節", (2, 16): "春節", (2, 17): "春節",
                (2, 18): "春節", (2, 19): "春節", (2, 20): "春節", (2, 21): "春節", (2, 22): "春節",
                (2, 27): "和平紀念日(補)", (2, 28): "和平紀念日",
                (4, 3): "兒童節(補)", (4, 4): "兒童節", (4, 5): "清明節", (4, 6): "清明節(補)",
                (5, 1): "勞動節", (6, 19): "端午節", (9, 25): "中秋節", (9, 28): "教師節",
                (10, 9): "國慶日(補)", (10, 10): "國慶日", (10, 25): "光復節", (10, 26): "光復節(補)",
                (12, 25): "行憲紀念日"
            })
        return h

    current_holidays = get_holidays(sel_year)

    def is_market_closed_func(d_date):
        if d_date.weekday() >= 5: return True
        name = current_holidays.get((d_date.month, d_date.day), "")
        if name and name != "封關日": return True
        return False

    real_settlements = {} 
    def calculate_month_settlements(y, m):
        cal_obj = calendar.Calendar(firstweekday=6)
        days_in_month = cal_obj.itermonthdays(y, m)
        d_list = [d for d in days_in_month if d != 0]
        
        w_count, f_count = 0, 0
        month_raw_wed, month_raw_fri = [], []
        
        for d in d_list:
            curr = date(y, m, d)
            if curr.weekday() == 2: w_count += 1; month_raw_wed.append((curr, w_count))
            if curr.weekday() == 4: f_count += 1; month_raw_fri.append((curr, f_count))
                
        monthly_raw = month_raw_wed[2][0] if len(month_raw_wed) >= 3 else None
        real_monthly_date = None
        if monthly_raw:
            check = monthly_raw
            while is_market_closed_func(check): check += timedelta(days=1)
            real_monthly_date = check
        
        local_results = []
        if monthly_raw: local_results.append((monthly_raw, 'M', f"{m:02}月", real_monthly_date))

        for dt, idx in month_raw_wed:
            if dt != monthly_raw: local_results.append((dt, 'W', f"{str(y)[2:]}{m:02}W{idx}", real_monthly_date))
        for dt, idx in month_raw_fri: local_results.append((dt, 'F', f"{str(y)[2:]}{m:02}F{idx}", real_monthly_date))
        return local_results

    current_month_data = calculate_month_settlements(sel_year, sel_month)
    if sel_month == 1: prev_y, prev_m = sel_year - 1, 12
    else: prev_y, prev_m = sel_year, sel_month - 1
    prev_month_data = calculate_month_settlements(prev_y, prev_m)
    
    all_raw_data = prev_month_data + current_month_data
    
    for raw_date, s_type, s_code, m_date in all_raw_data:
        check_date = raw_date
        while is_market_closed_func(check_date):
            check_date += timedelta(days=1)
            if (check_date - raw_date).days > 30: break
        
        if check_date.year == sel_year and check_date.month == sel_month:
            if s_type == 'F' and check_date == m_date: continue
            if check_date not in real_settlements: real_settlements[check_date] = []
            real_settlements[check_date].append((s_type, s_code))

    week_days = ["週", "日", "一", "二", "三", "四", "五", "六"]
    cols = st.columns([0.4, 1, 1, 1, 1, 1, 1, 1])
    for i, d in enumerate(week_days): cols[i].markdown(f"<div style='text-align: center; font-weight: bold;'>{d}</div>", unsafe_allow_html=True)

    cal_obj = calendar.Calendar(firstweekday=6)
    month_days = cal_obj.monthdayscalendar(sel_year, sel_month)

    for week in month_days:
        week_cols = st.columns([0.4, 1, 1, 1, 1, 1, 1, 1])
        first_valid_day = next((d for d in week if d != 0), None)
        if first_valid_day:
            iso_week = date(sel_year, sel_month, first_valid_day).isocalendar()[1]
            week_cols[0].markdown(f"<div class='cal-box cal-week'>{iso_week}</div>", unsafe_allow_html=True)
        else: week_cols[0].markdown("")

        for i, day in enumerate(week):
            if day == 0:
                week_cols[i+1].markdown("")
                continue
            
            curr_date = date(sel_year, sel_month, day)
            holiday_name = current_holidays.get((sel_month, day), "")
            is_closed = is_market_closed_func(curr_date)
            bg_class = "cal-closed" if is_closed else "cal-open"
            border_style = "today-border" if curr_date == now_tw.date() else ""
            
            content_html = [f"<b>{day}</b>"]
            if holiday_name and holiday_name != "封關日": content_html.append(f"<div class='holiday-tag'>{holiday_name}</div>")
            if holiday_name == "封關日": content_html.append(f"<div style='color:#ff9800; font-size:0.8em;'>{holiday_name}</div>")
            
            if curr_date in real_settlements:
                infos = real_settlements[curr_date]
                infos.sort(key=lambda x: 0 if x[0]=='M' else 1)
                for s_type, s_code in infos:
                    if s_type == 'M': content_html.append(f"<div class='settle-m'>台指期{s_code}結算<br>月選結算</div>")
                    elif s_type == 'W': content_html.append(f"<div class='settle-w'>週選(三) {s_code}</div>")
                    elif s_type == 'F': content_html.append(f"<div class='settle-f'>週選(五) {s_code}</div>")

            week_cols[i+1].markdown(f"<div class='cal-box {bg_class} {border_style}'>{''.join(content_html)}</div>", unsafe_allow_html=True)

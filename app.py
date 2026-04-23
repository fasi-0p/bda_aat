import streamlit as st
import pandas as pd
import time
import os
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Pro Crypto Dashboard", layout="wide")
st.title("📈 Institutional Quant Dashboard")
st.caption("Live Kafka Ingestion | 50/200 SMA | MACD | BBands | Volume Profile")

# ==========================================
# THE FIX: CREATE STATIC BOXES OUTSIDE THE LOOP
# ==========================================
# 1. Metrics Boxes
col1, col2, col3, col4 = st.columns(4)
box_m1 = col1.empty()
box_m2 = col2.empty()
box_m3 = col3.empty()
box_m4 = col4.empty()

st.markdown("---")

# 2. Main Chart Box
box_main = st.empty()

# 3. Second Row Boxes
col_vol, col_macd = st.columns(2)
box_vol = col_vol.empty()
box_macd = col_macd.empty()

# 4. Third Row Boxes
col_rsi, col_atr = st.columns(2)
box_rsi = col_rsi.empty()
box_atr = col_atr.empty()

# ==========================================

csv_file = './output_data/stream.csv'

# Start the continuous loop without page reruns!
while True:
    if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
        
        df = pd.read_csv(csv_file, names=["symbol", "price", "volume", "timestamp"])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.tail(300).copy() 
        
        if len(df) > 5: 
            # --- CALCULATE INDICATORS ---
            df['SMA_50'] = df['price'].rolling(window=50).mean()
            df['SMA_200'] = df['price'].rolling(window=200).mean()
            
            df['SMA_20'] = df['price'].rolling(window=20).mean()
            df['STD_20'] = df['price'].rolling(window=20).std()
            df['Upper_Band'] = df['SMA_20'] + (df['STD_20'] * 2)
            df['Lower_Band'] = df['SMA_20'] - (df['STD_20'] * 2)
            
            df['EMA_12'] = df['price'].ewm(span=12, adjust=False).mean()
            df['EMA_26'] = df['price'].ewm(span=26, adjust=False).mean()
            df['MACD'] = df['EMA_12'] - df['EMA_26']
            df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()
            df['MACD_Hist'] = df['MACD'] - df['Signal_Line']
            
            delta = df['price'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI_14'] = 100 - (100 / (1 + rs))
            df['ATR_14'] = df['price'].diff().abs().rolling(window=14).mean()
            
            df['Vol_Color'] = df['price'].diff().apply(lambda x: '#00ff00' if x >= 0 else '#ff3366')

            # ==========================================
            # INJECT DATA DIRECTLY INTO THE STATIC BOXES
            # ==========================================
            
            # --- METRICS ---
            latest_price = df['price'].iloc[-1]
            box_m1.metric("Live BTC Price", f"${latest_price:.2f}")
            
            sma50_val = f"${df['SMA_50'].iloc[-1]:.2f}" if pd.notna(df['SMA_50'].iloc[-1]) else "Calculating..."
            sma200_val = f"${df['SMA_200'].iloc[-1]:.2f}" if pd.notna(df['SMA_200'].iloc[-1]) else "Calculating..."
            box_m2.metric("50 SMA", sma50_val)
            box_m3.metric("200 SMA", sma200_val)
            box_m4.metric("Live Volume", f"{df['volume'].iloc[-1]} units")
            
            # --- MAIN CHART ---
            fig_main = go.Figure()
            fig_main.add_trace(go.Scatter(x=df['timestamp'], y=df['Upper_Band'], mode='lines', line=dict(width=0), showlegend=False))
            fig_main.add_trace(go.Scatter(x=df['timestamp'], y=df['Lower_Band'], fill='tonexty', mode='lines', line=dict(width=0), fillcolor='rgba(255, 255, 255, 0.1)', name='Bollinger Bands'))
            fig_main.add_trace(go.Scatter(x=df['timestamp'], y=df['SMA_50'], mode='lines', name='50 SMA', line=dict(color='#00bfff', width=2)))
            fig_main.add_trace(go.Scatter(x=df['timestamp'], y=df['SMA_200'], mode='lines', name='200 SMA', line=dict(color='#ff00ff', width=2)))
            fig_main.add_trace(go.Scatter(x=df['timestamp'], y=df['price'], mode='lines', name='BTC Price', line=dict(color='#00ff00', width=3)))
            fig_main.update_layout(template="plotly_dark", margin=dict(l=0, r=0, t=10, b=0), height=400, legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01))
            box_main.plotly_chart(fig_main, width="stretch") # Injected!
            
            # --- VOLUME CHART ---
            fig_vol = go.Figure(data=[go.Bar(x=df['timestamp'], y=df['volume'], marker_color=df['Vol_Color'])])
            fig_vol.update_layout(template="plotly_dark", margin=dict(l=0, r=0, t=0, b=0), height=200)
            box_vol.plotly_chart(fig_vol, width="stretch") # Injected!

            # --- MACD CHART ---
            fig_macd = go.Figure()
            fig_macd.add_trace(go.Bar(x=df['timestamp'], y=df['MACD_Hist'], name='Histogram', marker_color='#a9a9a9'))
            fig_macd.add_trace(go.Scatter(x=df['timestamp'], y=df['MACD'], name='MACD', line=dict(color='#00bfff')))
            fig_macd.add_trace(go.Scatter(x=df['timestamp'], y=df['Signal_Line'], name='Signal', line=dict(color='#ff9900')))
            fig_macd.update_layout(template="plotly_dark", margin=dict(l=0, r=0, t=0, b=0), height=200, showlegend=False)
            box_macd.plotly_chart(fig_macd, width="stretch") # Injected!

            # --- RSI CHART ---
            fig_rsi = px.line(df, x='timestamp', y='RSI_14', template="plotly_dark")
            fig_rsi.update_traces(line_color='#ff3366')
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="red")
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="green")
            fig_rsi.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=200, yaxis_range=[0, 100])
            box_rsi.plotly_chart(fig_rsi, width="stretch") # Injected!
                
            # --- ATR CHART ---
            fig_atr = px.area(df, x='timestamp', y='ATR_14', template="plotly_dark")
            fig_atr.update_traces(line_color='#ffff00', fillcolor='rgba(255, 255, 0, 0.1)')
            fig_atr.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=200)
            box_atr.plotly_chart(fig_atr, width="stretch") # Injected!
                
    time.sleep(1)
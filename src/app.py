import streamlit as st
import pandas as pd
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from database import QuantDB
from ingestion import BinanceIngestion
from analytics import (
    calculate_ohlcv, calculate_hedge_ratio, calculate_spread,
    calculate_zscore, perform_adf_test, calculate_rolling_correlation
)

# --- Streamlit Config ---
st.set_page_config(page_title="Quant Dashboard", layout="wide")

# --- Ingestion Singleton ---
@st.cache_resource
def get_ingestion_thread(symbols):
    ingestion = BinanceIngestion(symbols)
    #ingestion.start() # Start manually
    return ingestion

# --- Sidebar Controls ---
st.sidebar.title("Configuration")
selected_symbols = st.sidebar.text_input("Symbols (comma sep)", "btcusdt,ethusdt")
symbols_list = [s.strip().upper() for s in selected_symbols.split(',')]

# Start Ingestion
if st.sidebar.button("Start Ingestion"):
    ingestion = get_ingestion_thread(symbols_list)
    if not ingestion.running:
        import threading
        t = threading.Thread(target=lambda: import_async_run(ingestion), daemon=True)
        t.start()
        st.sidebar.success("Ingestion started!")
    else:
        st.sidebar.info("Ingestion already running.")

def import_async_run(ingestion):
    import asyncio
    asyncio.run(ingestion.connect())

timeframe = st.sidebar.selectbox("Timeframe", ["1min", "5min", "1h", "1s"])
window = st.sidebar.number_input("Rolling Window", min_value=5, value=20)
z_threshold = st.sidebar.number_input("Z-Score Threshold", min_value=1.0, value=2.0, step=0.1)
regression_type = st.sidebar.selectbox("Regression Model", ["OLS (Ordinary Least Squares)"])
live_mode = st.sidebar.checkbox("Live Update", value=False)

# --- Data Fetching ---
db = QuantDB()

def load_data(symbols):
    data = {}
    for sym in symbols:
        df = db.get_trades(sym, lookback_minutes=60)
        if not df.empty:
            ohlcv = calculate_ohlcv(df, interval=timeframe)
            data[sym] = ohlcv
    return data

# --- Layout ---
st.title("Real-Time Quant Analytics Dashboard")

tab_live, tab_hist = st.tabs(["Live Analysis", "Historical Analysis"])

with tab_live:
    placeholder = st.empty()

with tab_hist:
    st.header("Historical Analysis")
    uploaded_file = st.file_uploader("Upload Historical OHLC Data (CSV)", type="csv")
    if uploaded_file:
        try:
            hist_df = pd.read_csv(uploaded_file)
            st.write("Preview:", hist_df.head())
            if set(['timestamp', 'close']).issubset(hist_df.columns):
                 st.line_chart(hist_df.set_index('timestamp')['close'])
            else:
                 st.info("CSV must have 'timestamp' and 'close' columns.")
        except Exception as e:
            st.error(f"Error reading file: {e}")

# --- Main Loop ---
while True:
    with placeholder.container():
        # 1. Fetch Data
        data_map = load_data(symbols_list)
        
        if len(data_map) < 2:
            st.warning("Waiting for data... Ensure ingestion is running and at least 2 symbols are active.")
            time.sleep(2)
            if not live_mode: break
            continue

        # Prepare Series
        sym_y = symbols_list[0]
        sym_x = symbols_list[1]
        
        if sym_y not in data_map or sym_x not in data_map:
            st.warning(f"Waiting for data for {sym_y} or {sym_x}...")
            time.sleep(2)
            if not live_mode: break
            continue

        df_y = data_map[sym_y]['close']
        df_x = data_map[sym_x]['close']
        common_idx = df_y.index.intersection(df_x.index)
        df_y = df_y.loc[common_idx]
        df_x = df_x.loc[common_idx]

        if len(common_idx) < window:
             st.info(f"Not enough data points yet ({len(common_idx)}/{window}). Collecting...")
             time.sleep(2)
             if not live_mode: break
             continue

        # 2. Analytics
        hedge_ratio = calculate_hedge_ratio(df_y, df_x)
        spread = calculate_spread(df_y, df_x, hedge_ratio)
        z_score = calculate_zscore(spread, window=window)
        last_z = z_score.iloc[-1]
        
        # Metrics
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric(f"{sym_y} Price", f"{df_y.iloc[-1]:.2f}")
        c2.metric(f"{sym_x} Price", f"{df_x.iloc[-1]:.2f}")
        c3.metric("Hedge Ratio", f"{hedge_ratio:.4f}")
        c4.metric("Spread Mean", f"{spread.mean():.4f}")
        c5.metric("Spread Std", f"{spread.std():.4f}")
        c6.metric("Z-Score", f"{last_z:.2f}", delta_color="inverse" if abs(last_z) > z_threshold else "normal")

        # Alerts
        if last_z > z_threshold:
            st.error(f"SELL SIGNAL: Z-Score ({last_z:.2f}) > {z_threshold}")
        elif last_z < -z_threshold:
            st.success(f"BUY SIGNAL: Z-Score ({last_z:.2f}) < -{z_threshold}")

        # 3. Visualizations
        # Chart 1: Price & Volume
        fig_price = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05, row_heights=[0.7, 0.3])
        fig_price.add_trace(go.Scatter(x=df_y.index, y=df_y, name=sym_y), row=1, col=1)
        fig_price.add_trace(go.Scatter(x=df_x.index, y=df_x, name=sym_x, yaxis="y2"), row=1, col=1)
        
        if 'volume' in data_map[sym_y]:
             fig_price.add_trace(go.Bar(x=df_y.index, y=data_map[sym_y]['volume'], name=f"{sym_y} Vol", opacity=0.5), row=2, col=1)
        if 'volume' in data_map[sym_x]:
             fig_price.add_trace(go.Bar(x=df_x.index, y=data_map[sym_x]['volume'], name=f"{sym_x} Vol", opacity=0.5), row=2, col=1)

        fig_price.update_layout(title="Price & Volume", height=500, yaxis3=dict(title="Volume"))
        st.plotly_chart(fig_price, use_container_width=True)

        # Chart 2: Spread & Z-Score
        fig_z = make_subplots(rows=2, cols=1, shared_xaxes=True, subplot_titles=("Spread", "Z-Score"))
        fig_z.add_trace(go.Scatter(x=spread.index, y=spread, name="Spread"), row=1, col=1)
        fig_z.add_trace(go.Scatter(x=z_score.index, y=z_score, name="Z-Score"), row=2, col=1)
        fig_z.add_hline(y=z_threshold, line_dash="dash", line_color="red", row=2, col=1)
        fig_z.add_hline(y=-z_threshold, line_dash="dash", line_color="green", row=2, col=1)
        fig_z.update_layout(height=400)
        st.plotly_chart(fig_z, use_container_width=True)

        # Chart 3: Correlation
        correlation = calculate_rolling_correlation(df_y, df_x, window=window)
        fig_corr = go.Figure()
        fig_corr.add_trace(go.Scatter(x=correlation.index, y=correlation, name=f"Corr({window})"))
        fig_corr.add_hline(y=0, line_dash="dash", line_color="gray")
        fig_corr.update_layout(title="Rolling Correlation", height=300)
        st.plotly_chart(fig_corr, use_container_width=True)

        # Advanced Analytics
        with st.expander("Advanced Analytics (ADF Test)"):
            if st.button("Run ADF Test"):
                adf_res = perform_adf_test(spread)
                if adf_res:
                    st.write("ADF Statistic:", adf_res['adf_statistic'])
                    st.write("P-Value:", adf_res['p_value'])

        # Data Table
        st.divider()
        st.subheader("Detailed Data View")
        display_df = pd.DataFrame({
            'Price Y': df_y,
            'Price X': df_x,
            'Spread': spread,
            'Z-Score': z_score,
            'Correlation': correlation
        }).dropna().sort_index(ascending=False)
        st.dataframe(display_df, use_container_width=True, height=250)

        # Export
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button("Download Prices", pd.DataFrame({sym_y: df_y, sym_x: df_x}).to_csv(), "prices.csv")
        with c2:
            st.download_button("Download Spread", spread.to_csv(), "spread.csv")
        with c3:
            st.download_button("Download Z-Score", z_score.to_csv(), "zscore.csv")

    if not live_mode:
        break
    time.sleep(1)

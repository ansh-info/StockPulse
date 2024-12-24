from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery, bigquery_storage

from config import GCP_CONFIG, STOCK_CONFIGS


class StockDashboard:
    def __init__(self):
        self.client = bigquery.Client()
        self.bqstorage_client = bigquery_storage.BigQueryReadClient()
        self.available_symbols = list(STOCK_CONFIGS.keys())

    def load_data(self, symbol: str, days: int = 7) -> pd.DataFrame:
        """Load data from BigQuery for a specific symbol"""
        query = f"""
        SELECT 
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp) as timestamp_str,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            FORMAT_DATE('%Y-%m-%d', CAST(date AS DATE)) as date_str,
            FORMAT_TIME('%H:%M:%S', CAST(time AS TIME)) as time_str,
            moving_average,
            cumulative_average
        FROM `{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        ORDER BY timestamp
        """
        df = self.client.query(query).to_dataframe(
            bqstorage_client=self.bqstorage_client
        )

        # Convert timestamps with explicit formats
        df["timestamp"] = pd.to_datetime(
            df["timestamp_str"], format="%Y-%m-%d %H:%M:%S"
        )
        df["date"] = pd.to_datetime(df["date_str"], format="%Y-%m-%d")
        df["time"] = pd.to_datetime(df["time_str"], format="%H:%M:%S").dt.time

        # Drop string columns
        df = df.drop(["timestamp_str", "date_str", "time_str"], axis=1)

        return df

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators for analysis"""
        # Bollinger Bands (20-day, 2 standard deviations)
        df["SMA20"] = df["close"].rolling(window=20).mean()
        df["stddev"] = df["close"].rolling(window=20).std()
        df["BB_upper"] = df["SMA20"] + (df["stddev"] * 2)
        df["BB_lower"] = df["SMA20"] - (df["stddev"] * 2)

        # RSI (14-period)
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df["RSI"] = 100 - (100 / (1 + rs))

        # MACD
        exp1 = df["close"].ewm(span=12, adjust=False).mean()
        exp2 = df["close"].ewm(span=26, adjust=False).mean()
        df["MACD"] = exp1 - exp2
        df["Signal_Line"] = df["MACD"].ewm(span=9, adjust=False).mean()

        # Additional Moving Averages
        df["SMA50"] = df["close"].rolling(window=50).mean()
        df["SMA200"] = df["close"].rolling(window=200).mean()

        return df

    def create_enhanced_candlestick(self, df: pd.DataFrame) -> go.Figure:
        """Create an enhanced candlestick chart with Bollinger Bands"""
        # First, handle any NaN values to prevent JSON serialization errors
        df = df.fillna(method="ffill").fillna(method="bfill")

        fig = go.Figure()

        # Candlestick chart with improved colors
        fig.add_trace(
            go.Candlestick(
                x=df["timestamp"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name="OHLC",
                increasing_line_color="#26A69A",  # Green for increasing
                decreasing_line_color="#EF5350",  # Red for decreasing
                increasing_fillcolor="#26A69A",
                decreasing_fillcolor="#EF5350",
            )
        )

        # Add Bollinger Bands with improved styling
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["BB_upper"],
                name="Upper BB",
                line=dict(dash="dash", color="rgba(173, 216, 230, 0.7)", width=1),
                showlegend=True,
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["BB_lower"],
                name="Lower BB",
                line=dict(dash="dash", color="rgba(173, 216, 230, 0.7)", width=1),
                fill="tonexty",
                fillcolor="rgba(173, 216, 230, 0.1)",
                showlegend=True,
            )
        )

        # Add Moving Averages with improved visibility
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["SMA50"],
                name="50-day MA",
                line=dict(color="rgba(255, 165, 0, 0.8)", width=1.5),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["SMA200"],
                name="200-day MA",
                line=dict(color="rgba(255, 69, 0, 0.8)", width=1.5),
            )
        )

        # Improve layout
        fig.update_layout(
            title={
                "text": "Enhanced Price Analysis with Technical Indicators",
                "y": 0.95,
                "x": 0.5,
                "xanchor": "center",
                "yanchor": "top",
                "font": dict(size=20),
            },
            yaxis_title="Price",
            xaxis_title="Date",
            height=800,
            template="plotly_dark",
            legend=dict(
                yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor="rgba(0,0,0,0.5)"
            ),
            margin=dict(l=50, r=50, t=50, b=50),
            xaxis=dict(
                rangeslider=dict(visible=False),  # Disable rangeslider
                type="date",
                gridcolor="rgba(128,128,128,0.1)",
                showgrid=True,
            ),
            yaxis=dict(
                gridcolor="rgba(128,128,128,0.1)",
                showgrid=True,
                side="right",  # Move price axis to right side
            ),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        # Add shapes for better visualization
        fig.update_xaxes(showline=True, linewidth=1, linecolor="rgba(128,128,128,0.2)")
        fig.update_yaxes(showline=True, linewidth=1, linecolor="rgba(128,128,128,0.2)")

        return fig

    def create_rsi_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create RSI chart with overbought/oversold levels"""
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"], y=df["RSI"], name="RSI", line=dict(color="yellow")
            )
        )

        # Add overbought/oversold levels
        fig.add_hline(
            y=70, line_dash="dash", line_color="red", annotation_text="Overbought"
        )
        fig.add_hline(
            y=30, line_dash="dash", line_color="green", annotation_text="Oversold"
        )

        fig.update_layout(
            title="Relative Strength Index (RSI)",
            yaxis_title="RSI",
            yaxis=dict(range=[0, 100]),
            height=400,
            template="plotly_dark",
        )

        return fig

    def create_macd_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create MACD chart"""
        fig = go.Figure()

        # Add MACD line
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"], y=df["MACD"], name="MACD", line=dict(color="blue")
            )
        )

        # Add Signal line
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["Signal_Line"],
                name="Signal Line",
                line=dict(color="orange"),
            )
        )

        # Add MACD histogram
        fig.add_trace(
            go.Bar(
                x=df["timestamp"],
                y=df["MACD"] - df["Signal_Line"],
                name="MACD Histogram",
                marker_color="gray",
            )
        )

        fig.update_layout(
            title="Moving Average Convergence Divergence (MACD)",
            yaxis_title="MACD",
            height=400,
            template="plotly_dark",
        )

        return fig

    def create_volume_analysis_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create advanced volume analysis chart"""
        fig = go.Figure()

        # Calculate volume-weighted price levels
        df["vwap"] = (df["volume"] * df["close"]).cumsum() / df["volume"].cumsum()

        # Create color array for volume bars based on price movement
        colors = [
            "red" if close < open else "green"
            for close, open in zip(df["close"], df["open"])
        ]

        # Add volume bars
        fig.add_trace(
            go.Bar(
                x=df["timestamp"],
                y=df["volume"],
                name="Volume",
                marker_color=colors,
                opacity=0.7,
            )
        )

        # Add VWAP line
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"], y=df["vwap"], name="VWAP", line=dict(color="white")
            )
        )

        fig.update_layout(
            title="Volume Analysis with VWAP",
            yaxis_title="Volume",
            height=400,
            template="plotly_dark",
        )

        return fig

    def create_daily_range_box(self, df: pd.DataFrame) -> go.Figure:
        """Create box plot of daily price ranges"""
        df["range"] = df["high"] - df["low"]
        df["week"] = df["date"].dt.strftime("%Y-%U")

        fig = px.box(df, x="week", y="range", title="Weekly Price Range Distribution")
        fig.update_layout(
            xaxis_title="Week",
            yaxis_title="Price Range",
            height=400,
            template="plotly_dark",
        )
        return fig

    def create_volume_heatmap(self, df: pd.DataFrame) -> go.Figure:
        """Create volume heatmap by hour and day"""
        df["hour"] = df["time"].apply(lambda x: x.hour)
        df["day"] = df["date"].dt.strftime("%A")

        volume_pivot = df.pivot_table(
            values="volume", index="day", columns="hour", aggfunc="mean"
        )

        fig = px.imshow(
            volume_pivot,
            title="Volume Heatmap by Hour and Day",
            labels=dict(x="Hour of Day", y="Day of Week", color="Volume"),
            template="plotly_dark",
        )
        fig.update_layout(height=400)
        return fig

    def create_price_momentum_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create price momentum chart"""
        # Calculate momentum indicators
        df["ROC"] = df["close"].pct_change(periods=10) * 100  # 10-period Rate of Change
        df["Momentum"] = df["close"] - df["close"].shift(10)  # 10-period Momentum

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["ROC"],
                name="Rate of Change",
                line=dict(color="cyan"),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["Momentum"],
                name="Momentum",
                line=dict(color="magenta"),
            )
        )

        fig.update_layout(
            title="Price Momentum Analysis",
            yaxis_title="Value",
            height=400,
            template="plotly_dark",
        )

        return fig


def main():
    st.set_page_config(
        page_title="Enhanced Stock Market Analysis Dashboard",
        page_icon="📈",
        layout="wide",
    )

    st.title("Enhanced Stock Market Analysis Dashboard")

    # Initialize dashboard
    dashboard = StockDashboard()

    # Sidebar controls
    st.sidebar.header("Controls")
    selected_symbol = st.sidebar.selectbox(
        "Select Stock Symbol", dashboard.available_symbols
    )

    days_to_load = st.sidebar.slider("Days of Data", min_value=1, max_value=30, value=7)

    # Technical Analysis Controls
    st.sidebar.header("Technical Analysis")
    show_technicals = st.sidebar.checkbox("Show Technical Indicators", True)
    show_volume_analysis = st.sidebar.checkbox("Show Volume Analysis", True)
    show_momentum = st.sidebar.checkbox("Show Momentum Analysis", True)

    try:
        # Load and process data
        df = dashboard.load_data(selected_symbol, days_to_load)
        df = dashboard.calculate_technical_indicators(df)

        if df.empty:
            st.error("No data available for the selected period.")
            return

        # Main dashboard layout
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Current Price",
                f"${df['close'].iloc[-1]:.2f}",
                f"{((df['close'].iloc[-1] - df['open'].iloc[-1]) / df['open'].iloc[-1] * 100):.2f}%",
            )

        with col2:
            st.metric(
                "RSI",
                f"{df['RSI'].iloc[-1]:.2f}",
                (
                    "Overbought"
                    if df["RSI"].iloc[-1] > 70
                    else "Oversold" if df["RSI"].iloc[-1] < 30 else "Neutral"
                ),
            )

        with col3:
            st.metric(
                "Volume",
                f"{df['volume'].iloc[-1]:,.0f}",
                f"{((df['volume'].iloc[-1] - df['volume'].mean()) / df['volume'].mean() * 100):.2f}%",
            )

        # Enhanced Candlestick Chart
        st.plotly_chart(
            dashboard.create_enhanced_candlestick(df), use_container_width=True
        )

        if show_technicals:
            col4, col5 = st.columns(2)
            with col4:
                st.plotly_chart(
                    dashboard.create_rsi_chart(df), use_container_width=True
                )
            with col5:
                st.plotly_chart(
                    dashboard.create_macd_chart(df), use_container_width=True
                )

        if show_volume_analysis:
            st.plotly_chart(
                dashboard.create_volume_analysis_chart(df), use_container_width=True
            )
            st.plotly_chart(
                dashboard.create_volume_heatmap(df), use_container_width=True
            )

        if show_momentum:
            st.plotly_chart(
                dashboard.create_price_momentum_chart(df), use_container_width=True
            )

        # Additional Analysis
        col6, col7 = st.columns(2)
        with col6:
            st.plotly_chart(
                dashboard.create_daily_range_box(df), use_container_width=True
            )

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.exception(e)


if __name__ == "__main__":
    main()

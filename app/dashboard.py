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

        df["timestamp"] = pd.to_datetime(
            df["timestamp_str"], format="%Y-%m-%d %H:%M:%S"
        )
        df["date"] = pd.to_datetime(df["date_str"], format="%Y-%m-%d")
        df["time"] = pd.to_datetime(df["time_str"], format="%H:%M:%S").dt.time
        df = df.drop(["timestamp_str", "date_str", "time_str"], axis=1)

        # Handle NaN values
        df = df.fillna(method="ffill").fillna(method="bfill")
        return df

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators for analysis"""
        # Handle NaN values first
        df = df.fillna(method="ffill").fillna(method="bfill")

        # Bollinger Bands (20-day, 2 standard deviations)
        df["SMA20"] = df["close"].rolling(window=20).mean()
        df["stddev"] = df["close"].rolling(window=20).std()
        df["BB_upper"] = df["SMA20"] + (df["stddev"] * 2)
        df["BB_lower"] = df["SMA20"] - (df["stddev"] * 2)

        # Moving Averages
        df["SMA50"] = df["close"].rolling(window=50).mean()
        df["SMA200"] = df["close"].rolling(window=200).mean()

        # RSI
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

        return df.fillna(method="ffill").fillna(method="bfill")

    def create_enhanced_candlestick(self, df: pd.DataFrame) -> go.Figure:
        """Create an enhanced candlestick chart with technical indicators"""
        # Handle NaN values
        df = df.fillna(method="ffill").fillna(method="bfill")

        fig = go.Figure()

        # Add grid first for proper layering
        fig.add_trace(
            go.Scatter(
                x=[df["timestamp"].min(), df["timestamp"].max()],
                y=[df["close"].mean(), df["close"].mean()],
                mode="lines",
                line=dict(color="rgba(128, 128, 128, 0.1)", width=1),
                showlegend=False,
                hoverinfo="none",
            )
        )

        # Add candlesticks
        fig.add_trace(
            go.Candlestick(
                x=df["timestamp"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name="OHLC",
                increasing_line_color="#26A69A",  # Green
                decreasing_line_color="#EF5350",  # Red
                increasing_fillcolor="#26A69A",
                decreasing_fillcolor="#EF5350",
                line=dict(width=1),
                whiskerwidth=0.8,
            )
        )

        # Add Bollinger Bands
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["BB_upper"],
                name="Upper BB",
                line=dict(color="rgba(200, 200, 200, 0.5)", width=1, dash="dash"),
                showlegend=True,
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["BB_lower"],
                name="Lower BB",
                line=dict(color="rgba(200, 200, 200, 0.5)", width=1, dash="dash"),
                fill="tonexty",
                fillcolor="rgba(200, 200, 200, 0.05)",
                showlegend=True,
            )
        )

        # Add Moving Averages
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["SMA50"],
                name="50 MA",
                line=dict(color="rgba(255, 165, 0, 0.7)", width=1.5),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["SMA200"],
                name="200 MA",
                line=dict(color="rgba(255, 69, 0, 0.7)", width=1.5),
            )
        )

        # Update layout
        fig.update_layout(
            title={
                "text": "Enhanced Price Analysis with Technical Indicators",
                "y": 0.95,
                "x": 0.5,
                "xanchor": "center",
                "yanchor": "top",
                "font": dict(size=20, color="white"),
            },
            yaxis_title="Price",
            xaxis_title="Date",
            height=800,
            template="plotly_dark",
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(0,0,0,0.5)",
                font=dict(color="white"),
            ),
            margin=dict(l=50, r=50, t=50, b=50),
            xaxis=dict(
                rangeslider=dict(visible=False),
                type="date",
                showgrid=True,
                gridcolor="rgba(128,128,128,0.15)",
                gridwidth=1,
                linecolor="rgba(128,128,128,0.15)",
                linewidth=1,
                tickfont=dict(size=12),
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="rgba(128,128,128,0.15)",
                gridwidth=1,
                linecolor="rgba(128,128,128,0.15)",
                linewidth=1,
                side="right",
                tickfont=dict(size=12),
            ),
            plot_bgcolor="rgb(25,25,25)",
            paper_bgcolor="rgb(25,25,25)",
            hovermode="x unified",
        )

        # Add custom spacing between candles
        fig.update_xaxes(
            rangebreaks=[
                dict(bounds=["sat", "mon"]),  # hide weekends
                dict(bounds=[16, 9.5], pattern="hour"),  # hide non-trading hours
            ]
        )

        # Update hover template
        fig.update_traces(xhoverformat="%Y-%m-%d %H:%M:%S", yhoverformat=".2f")

        return fig

    # [Rest of your methods remain the same...]


def main():
    st.set_page_config(
        page_title="Enhanced Stock Market Analysis Dashboard",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("Enhanced Stock Market Analysis Dashboard")

    # Initialize dashboard
    dashboard = StockDashboard()

    # Sidebar controls with improved styling
    st.sidebar.header("Controls")
    selected_symbol = st.sidebar.selectbox(
        "Select Stock Symbol",
        dashboard.available_symbols,
        help="Choose the stock symbol to analyze",
    )

    days_to_load = st.sidebar.slider(
        "Days of Data",
        min_value=1,
        max_value=30,
        value=7,
        help="Select the number of days of historical data to analyze",
    )

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

        # Display metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                "Current Price",
                f"${df['close'].iloc[-1]:.2f}",
                f"{((df['close'].iloc[-1] - df['open'].iloc[-1]) / df['open'].iloc[-1] * 100):.2f}%",
            )
        with col2:
            rsi_value = df["RSI"].iloc[-1]
            st.metric(
                "RSI",
                f"{rsi_value:.2f}",
                (
                    "Overbought"
                    if rsi_value > 70
                    else "Oversold" if rsi_value < 30 else "Neutral"
                ),
            )
        with col3:
            st.metric(
                "Volume",
                f"{df['volume'].iloc[-1]:,.0f}",
                f"{((df['volume'].iloc[-1] - df['volume'].mean()) / df['volume'].mean() * 100):.2f}%",
            )

        # Display charts
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

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.exception(e)


if __name__ == "__main__":
    main()

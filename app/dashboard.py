from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

from config import GCP_CONFIG, STOCK_CONFIGS


class StockDashboard:
    def __init__(self):
        self.client = bigquery.Client()
        self.available_symbols = list(STOCK_CONFIGS.keys())

    def load_data(self, symbol: str, days: int = 7) -> pd.DataFrame:
        """Load data from BigQuery for a specific symbol"""
        query = f"""
        SELECT *
        FROM `{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}.{STOCK_CONFIGS[symbol]['table_name']}`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        ORDER BY timestamp
        """
        df = self.client.query(query).to_dataframe()
        return df

    def calculate_vwap(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate VWAP for the dataset"""
        df["vwap"] = (df["close"] * df["volume"]).cumsum() / df["volume"].cumsum()
        return df

    def create_candlestick_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create a candlestick chart with volume bars"""
        fig = go.Figure()

        # Add candlestick
        fig.add_trace(
            go.Candlestick(
                x=df["timestamp"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name="OHLC",
            )
        )

        # Add volume bars on secondary y-axis
        fig.add_trace(
            go.Bar(
                x=df["timestamp"],
                y=df["volume"],
                name="Volume",
                yaxis="y2",
                opacity=0.3,
            )
        )

        # Update layout
        fig.update_layout(
            title="Stock Price & Volume",
            yaxis_title="Price",
            yaxis2=dict(title="Volume", overlaying="y", side="right"),
            xaxis_title="Date",
            height=600,
        )

        return fig

    def create_ma_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create moving averages chart"""
        fig = go.Figure()

        # Add close price
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["close"],
                name="Close Price",
                line=dict(color="blue"),
            )
        )

        # Add moving averages
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["moving_average"],
                name="5-period MA",
                line=dict(color="orange"),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["cumulative_average"],
                name="Cumulative MA",
                line=dict(color="green"),
            )
        )

        fig.update_layout(
            title="Moving Averages Analysis",
            yaxis_title="Price",
            xaxis_title="Date",
            height=500,
        )

        return fig

    def create_daily_range_box(self, df: pd.DataFrame) -> go.Figure:
        """Create box plot of daily price ranges"""
        df["date"] = pd.to_datetime(df["date"])
        df["range"] = df["high"] - df["low"]
        df["week"] = df["date"].dt.strftime("%Y-%U")

        fig = px.box(df, x="week", y="range", title="Weekly Price Range Distribution")

        fig.update_layout(xaxis_title="Week", yaxis_title="Price Range", height=400)

        return fig

    def create_price_change_hist(self, df: pd.DataFrame) -> go.Figure:
        """Create histogram of price changes"""
        df["change_pct"] = ((df["close"] - df["open"]) / df["open"]) * 100

        fig = px.histogram(
            df, x="change_pct", nbins=50, title="Distribution of Daily Price Changes"
        )

        fig.update_layout(
            xaxis_title="Price Change (%)", yaxis_title="Frequency", height=400
        )

        return fig

    def create_volume_heatmap(self, df: pd.DataFrame) -> go.Figure:
        """Create volume heatmap by hour and day"""
        df["hour"] = pd.to_datetime(df["time"]).dt.hour
        df["day"] = pd.to_datetime(df["date"]).dt.strftime("%A")

        volume_pivot = df.pivot_table(
            values="volume", index="day", columns="hour", aggfunc="mean"
        )

        fig = px.imshow(
            volume_pivot,
            title="Volume Heatmap by Hour and Day",
            labels=dict(x="Hour of Day", y="Day of Week", color="Volume"),
        )

        fig.update_layout(height=400)
        return fig

    def create_vwap_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create VWAP chart"""
        df = self.calculate_vwap(df)

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["close"],
                name="Close Price",
                line=dict(color="blue"),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp"], y=df["vwap"], name="VWAP", line=dict(color="red")
            )
        )

        fig.update_layout(
            title="Price vs VWAP", yaxis_title="Price", xaxis_title="Date", height=400
        )

        return fig


def main():
    st.set_page_config(
        page_title="Stock Market Dashboard", page_icon="📈", layout="wide"
    )

    st.title("Stock Market Analysis Dashboard")

    # Initialize dashboard
    dashboard = StockDashboard()

    # Sidebar controls
    st.sidebar.header("Controls")
    selected_symbol = st.sidebar.selectbox(
        "Select Stock Symbol", dashboard.available_symbols
    )

    days_to_load = st.sidebar.slider("Days of Data", min_value=1, max_value=30, value=7)

    # Load data
    try:
        df = dashboard.load_data(selected_symbol, days_to_load)

        if df.empty:
            st.error("No data available for the selected period.")
            return

        # Main dashboard layout
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Price Overview")
            latest = df.iloc[-1]
            st.metric(
                "Current Price",
                f"${latest['close']:.2f}",
                f"{((latest['close'] - latest['open']) / latest['open'] * 100):.2f}%",
            )

        with col2:
            st.subheader("Volume Overview")
            avg_volume = df["volume"].mean()
            st.metric(
                "Average Volume",
                f"{int(avg_volume):,}",
                f"{((df['volume'].iloc[-1] - avg_volume) / avg_volume * 100):.2f}%",
            )

        # Charts
        st.plotly_chart(
            dashboard.create_candlestick_chart(df), use_container_width=True
        )
        st.plotly_chart(dashboard.create_ma_chart(df), use_container_width=True)

        col3, col4 = st.columns(2)
        with col3:
            st.plotly_chart(
                dashboard.create_daily_range_box(df), use_container_width=True
            )
        with col4:
            st.plotly_chart(
                dashboard.create_price_change_hist(df), use_container_width=True
            )

        st.plotly_chart(dashboard.create_volume_heatmap(df), use_container_width=True)
        st.plotly_chart(dashboard.create_vwap_chart(df), use_container_width=True)

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")


if __name__ == "__main__":
    main()

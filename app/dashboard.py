import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery, bigquery_storage

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)


from src.config.config import GCP_CONFIG, STOCK_CONFIGS


class StockDashboard:
    def __init__(self):
        """Initialize the dashboard with BigQuery clients"""
        self.client = bigquery.Client()
        self.bqstorage_client = bigquery_storage.BigQueryReadClient()
        self.available_symbols = list(STOCK_CONFIGS.keys())

    def load_data(self, symbol: str, days: int = 7) -> pd.DataFrame:
        """Load data from BigQuery for a specific symbol"""
        try:
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

            # Convert string columns to proper datetime types with explicit formats
            df["timestamp"] = pd.to_datetime(
                df["timestamp_str"], format="%Y-%m-%d %H:%M:%S"
            )
            df["date"] = pd.to_datetime(df["date_str"], format="%Y-%m-%d")
            df["time"] = pd.to_datetime(df["time_str"], format="%H:%M:%S").dt.time
            df = df.drop(["timestamp_str", "date_str", "time_str"], axis=1)

            # Convert numeric columns to proper types
            numeric_columns = [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "moving_average",
                "cumulative_average",
            ]
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # Handle NaN values
            df = df.ffill().bfill()

            return df

        except bigquery.exceptions.NotFound:
            raise Exception(f"Table for symbol {symbol} not found in BigQuery")
        except Exception as e:
            raise Exception(f"Error loading data: {str(e)}")

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators for analysis"""
        # Make a copy to avoid modifying the input DataFrame
        df = df.copy()

        # Handle NaN values first
        df = df.ffill().bfill()

        # Ensure numeric types
        numeric_columns = ["close", "high", "low", "volume", "open"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

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

        # ATR
        high_low = df["high"] - df["low"]
        high_close = np.abs(df["high"] - df["close"].shift())
        low_close = np.abs(df["low"] - df["close"].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        df["ATR"] = true_range.rolling(14).mean()

        # Stochastic Oscillator
        low_min = df["low"].rolling(14).min()
        high_max = df["high"].rolling(14).max()
        df["%K"] = ((df["close"] - low_min) / (high_max - low_min)) * 100
        df["%D"] = df["%K"].rolling(3).mean()

        # Final cleanup of any remaining NA values
        df = df.ffill().bfill()

        # Replace any infinite values with NaN and then fill them
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.ffill().bfill()

        # Ensure all numeric columns are float64
        for col in df.select_dtypes(include=[np.number]).columns:
            df[col] = df[col].astype("float64")

        return df

    def create_enhanced_candlestick(self, df: pd.DataFrame) -> go.Figure:
        """Create an enhanced candlestick chart with technical indicators"""
        # Make a copy and clean data
        df = df.copy()
        df = df.ffill().bfill()
        df = df.replace([np.inf, -np.inf], np.nan).ffill().bfill()

        # Convert timestamp for JSON serialization
        df["timestamp_str"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        fig = go.Figure()

        # Add grid first for proper layering
        fig.add_trace(
            go.Scatter(
                x=[df["timestamp_str"].iloc[0], df["timestamp_str"].iloc[-1]],
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
                x=df["timestamp_str"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name="OHLC",
                increasing_line_color="#26A69A",
                decreasing_line_color="#EF5350",
                increasing_fillcolor="#26A69A",
                decreasing_fillcolor="#EF5350",
                line=dict(width=1),
                whiskerwidth=0.8,
            )
        )

        # Add Bollinger Bands
        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
                y=df["BB_upper"],
                name="Upper BB",
                line=dict(color="rgba(200, 200, 200, 0.5)", width=1, dash="dash"),
                showlegend=True,
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
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
                x=df["timestamp_str"],
                y=df["SMA50"],
                name="50 MA",
                line=dict(color="rgba(255, 165, 0, 0.7)", width=1.5),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
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
                "font": dict(size=20, color="black"),
            },
            yaxis_title="Price",
            xaxis_title="Date",
            height=800,
            template="none",
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255,255,255,0.8)",
                font=dict(color="black"),
            ),
            margin=dict(l=50, r=50, t=50, b=50),
            xaxis=dict(
                rangeslider=dict(visible=False),
                type="date",
                showgrid=True,
                gridcolor="rgba(200,200,200,0.5)",
                gridwidth=1,
                linecolor="rgba(200,200,200,0.5)",
                linewidth=1,
                tickfont=dict(size=12, color="black"),
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="rgba(200,200,200,0.5)",
                gridwidth=1,
                linecolor="rgba(200,200,200,0.5)",
                linewidth=1,
                side="right",
                tickfont=dict(size=12, color="black"),
            ),
            plot_bgcolor="white",
            paper_bgcolor="white",
            hovermode="x unified",
        )

        # Add custom spacing between candles
        fig.update_xaxes(
            rangebreaks=[
                dict(bounds=["sat", "mon"]),  # hide weekends
                dict(bounds=[16, 9.5], pattern="hour"),  # hide non-trading hours
            ]
        )

        fig.update_traces(xhoverformat="%Y-%m-%d %H:%M:%S", yhoverformat=".2f")

        return fig

    def create_rsi_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create RSI chart with overbought/oversold levels"""
        df = df.copy()
        df = df.ffill().bfill()
        df["timestamp_str"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
                y=df["RSI"],
                name="RSI",
                line=dict(color="yellow"),
            )
        )

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
            hovermode="x unified",
        )

        return fig

    def create_macd_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create MACD chart"""
        df = df.copy()
        df = df.ffill().bfill()
        df["timestamp_str"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
                y=df["MACD"],
                name="MACD",
                line=dict(color="blue"),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
                y=df["Signal_Line"],
                name="Signal Line",
                line=dict(color="orange"),
            )
        )

        fig.add_trace(
            go.Bar(
                x=df["timestamp_str"],
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
            hovermode="x unified",
        )

        return fig

    def create_volume_analysis_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create advanced volume analysis chart"""
        df = df.copy()
        df = df.ffill().bfill()
        df["timestamp_str"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Calculate volume-weighted price levels
        df["vwap"] = (df["volume"] * df["close"]).cumsum() / df["volume"].cumsum()

        # Create color array for volume bars based on price movement
        colors = [
            "red" if close < open else "green"
            for close, open in zip(df["close"], df["open"])
        ]

        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=df["timestamp_str"],
                y=df["volume"],
                name="Volume",
                marker_color=colors,
                opacity=0.7,
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
                y=df["vwap"],
                name="VWAP",
                line=dict(color="white"),
            )
        )

        fig.update_layout(
            title="Volume Analysis with VWAP",
            yaxis_title="Volume",
            height=400,
            template="plotly_dark",
            hovermode="x unified",
        )

        return fig

    def create_daily_range_box(self, df: pd.DataFrame) -> go.Figure:
        """Create box plot of daily price ranges"""
        df = df.copy()
        df["range"] = df["high"] - df["low"]
        df["week"] = df["date"].dt.strftime("%Y-%U")

        # Handle any NaN values
        df = df.ffill().bfill()

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
        df = df.copy()
        df["hour"] = df["time"].apply(lambda x: x.hour)
        df["day"] = df["date"].dt.strftime("%A")

        # Handle any NaN values before pivoting
        df = df.ffill().bfill()

        volume_pivot = df.pivot_table(
            values="volume", index="day", columns="hour", aggfunc="mean"
        ).fillna(
            0
        )  # Fill NaN values after pivot with explicit value

        fig = px.imshow(
            volume_pivot,
            title="Volume Heatmap by Hour and Day",
            labels=dict(x="Hour of Day", y="Day of Week", color="Volume"),
            template="plotly_dark",
        )
        fig.update_layout(height=400)
        return fig

    def create_stochastic_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create Stochastic Oscillator chart"""
        df = df.copy()
        df = df.ffill().bfill()
        df["timestamp_str"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"], y=df["%K"], name="%K", line=dict(color="blue")
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"], y=df["%D"], name="%D", line=dict(color="orange")
            )
        )

        fig.add_hline(
            y=80, line_dash="dash", line_color="red", annotation_text="Overbought"
        )
        fig.add_hline(
            y=20, line_dash="dash", line_color="green", annotation_text="Oversold"
        )

        fig.update_layout(
            title="Stochastic Oscillator",
            yaxis_title="Value",
            yaxis=dict(range=[0, 100]),
            height=400,
            template="plotly_dark",
            hovermode="x unified",
        )

        return fig

    def create_price_momentum_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create price momentum chart"""
        df = df.copy()
        df = df.ffill().bfill()
        df["timestamp_str"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Calculate momentum indicators
        df["ROC"] = df["close"].pct_change(periods=10) * 100
        df["Momentum"] = df["close"] - df["close"].shift(10)
        df = df.ffill().bfill()  # Handle NaN values from calculations

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
                y=df["ROC"],
                name="Rate of Change",
                line=dict(color="cyan"),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
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
            hovermode="x unified",
        )

        return fig

    def create_atr_chart(self, df: pd.DataFrame) -> go.Figure:
        """Create Average True Range chart"""
        df = df.copy()
        df = df.ffill().bfill()
        df["timestamp_str"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df["timestamp_str"],
                y=df["ATR"],
                name="ATR",
                line=dict(color="orange"),
            )
        )

        fig.update_layout(
            title="Average True Range (ATR)",
            yaxis_title="ATR Value",
            height=400,
            template="plotly_dark",
            hovermode="x unified",
        )

        return fig


def main():
    if "previous_symbol" not in st.session_state:
        st.session_state.previous_symbol = None

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
    show_stochastic = st.sidebar.checkbox("Show Stochastic Oscillator", True)
    show_atr = st.sidebar.checkbox("Show ATR", True)

    try:
        # Check if symbol changed
        if selected_symbol != st.session_state.previous_symbol:
            st.session_state.previous_symbol = selected_symbol

        # Load and process data
        df = dashboard.load_data(selected_symbol, days_to_load)

        # Validate and clean data
        if df.empty:
            st.error("No data available for the selected period.")
            return

        # Calculate technical indicators with clean data
        df = dashboard.calculate_technical_indicators(df)

        # Add Summary Statistics
        with st.expander("Summary Statistics"):
            summary_stats = pd.DataFrame(
                {
                    "Open": [df["open"].min(), df["open"].mean(), df["open"].max()],
                    "High": [df["high"].min(), df["high"].mean(), df["high"].max()],
                    "Low": [df["low"].min(), df["low"].mean(), df["low"].max()],
                    "Close": [df["close"].min(), df["close"].mean(), df["close"].max()],
                    "Volume": [
                        df["volume"].min(),
                        df["volume"].mean(),
                        df["volume"].max(),
                    ],
                },
                index=["Min", "Average", "Max"],
            )
            # Round values to prevent serialization issues
            summary_stats = summary_stats.round(2)
            st.dataframe(summary_stats.style.format("{:.2f}"))

        # Display metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            current_price = float(df["close"].iloc[-1])
            price_change = float(
                (df["close"].iloc[-1] - df["open"].iloc[-1]) / df["open"].iloc[-1] * 100
            )
            st.metric("Current Price", f"${current_price:.2f}", f"{price_change:.2f}%")
        with col2:
            rsi_value = float(df["RSI"].iloc[-1])
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
            current_volume = float(df["volume"].iloc[-1])
            volume_change = float(
                (df["volume"].iloc[-1] - df["volume"].mean())
                / df["volume"].mean()
                * 100
            )
            st.metric("Volume", f"{current_volume:,.0f}", f"{volume_change:.2f}%")

        # Display main chart
        st.plotly_chart(
            dashboard.create_enhanced_candlestick(df), use_container_width=True
        )

        # Technical Indicators Section
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

        # Volume Analysis Section
        if show_volume_analysis:
            st.plotly_chart(
                dashboard.create_volume_analysis_chart(df), use_container_width=True
            )
            col6, col7 = st.columns(2)
            with col6:
                st.plotly_chart(
                    dashboard.create_daily_range_box(df), use_container_width=True
                )
            with col7:
                st.plotly_chart(
                    dashboard.create_volume_heatmap(df), use_container_width=True
                )

        # Additional Technical Analysis Section
        if show_momentum:
            st.plotly_chart(
                dashboard.create_price_momentum_chart(df), use_container_width=True
            )

        if show_stochastic:
            st.plotly_chart(
                dashboard.create_stochastic_chart(df), use_container_width=True
            )

        if show_atr:
            st.plotly_chart(dashboard.create_atr_chart(df), use_container_width=True)

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.exception(e)


if __name__ == "__main__":
    main()

# dashboard.py
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

from config import GCP_CONFIG, STOCK_CONFIGS


class StockDashboard:
    def __init__(self):
        self.client = bigquery.Client()
        self.dataset = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"

    def get_stock_data(self, symbol, days=30, table_type="processed"):
        """Get stock data from BigQuery with option for raw or processed data"""
        table_suffix = "_processed" if table_type == "processed" else "_raw"
        query = f"""
        SELECT *
        FROM {self.dataset}.{STOCK_CONFIGS[symbol]['table_name']}{table_suffix}
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        ORDER BY timestamp
        """
        return pandas_gbq.read_gbq(
            query, project_id=GCP_CONFIG["PROJECT_ID"], progress_bar_type=None
        )

    def calculate_metrics(self, symbol, data):
        """Calculate key metrics for the stock"""
        latest = data.iloc[-1]
        prev_close = data.iloc[-2]["close"]

        return {
            "current_price": latest["close"],
            "day_change": (latest["close"] - prev_close) / prev_close * 100,
            "day_volume": latest["volume"],
            "ma7": latest["ma7"],
            "ma20": latest["ma20"],
            "volatility": latest["volatility"],
            "momentum": latest["momentum"],
        }


def main():
    st.set_page_config(
        page_title="Advanced Stock Market Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Custom CSS
    st.markdown(
        """
        <style>
        .stock-metric {
            font-size: 24px;
            font-weight: bold;
            margin: 10px 0;
        }
        .metric-label {
            font-size: 14px;
            color: #888;
        }
        .stButton>button {
            width: 100%;
        }
        </style>
    """,
        unsafe_allow_html=True,
    )

    # Initialize dashboard
    dashboard = StockDashboard()

    # Sidebar Controls
    st.sidebar.title("📊 Dashboard Controls")

    # Time range selector
    time_ranges = {"1 Day": 1, "1 Week": 7, "1 Month": 30, "3 Months": 90}
    selected_range = st.sidebar.selectbox("Select Time Range", list(time_ranges.keys()))
    selected_days = time_ranges[selected_range]

    # Stock selector
    selected_stock = st.sidebar.selectbox("Select Stock", list(STOCK_CONFIGS.keys()))

    # View type selector
    view_type = st.sidebar.radio(
        "Select View Type",
        ["Technical Analysis", "Volume Analysis", "Comparative Analysis"],
    )

    try:
        # Get data
        processed_data = dashboard.get_stock_data(
            selected_stock, selected_days, "processed"
        )

        # Main content
        st.title(f"📈 Advanced Stock Market Analysis - {selected_stock}")

        # Key metrics row
        metrics = dashboard.calculate_metrics(selected_stock, processed_data)
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Current Price",
                f"${metrics['current_price']:.2f}",
                f"{metrics['day_change']:.2f}%",
            )
        with col2:
            st.metric("Volume", f"{metrics['day_volume']:,}")
        with col3:
            st.metric("MA7", f"${metrics['ma7']:.2f}")
        with col4:
            st.metric("Volatility", f"{metrics['volatility']:.2f}%")

        if view_type == "Technical Analysis":
            # Technical Analysis View
            st.subheader("Technical Analysis")

            # Candlestick with MA
            fig = go.Figure()

            # Candlestick
            fig.add_trace(
                go.Candlestick(
                    x=processed_data["timestamp"],
                    open=processed_data["open"],
                    high=processed_data["high"],
                    low=processed_data["low"],
                    close=processed_data["close"],
                    name="OHLC",
                )
            )

            # Add MA lines
            fig.add_trace(
                go.Scatter(
                    x=processed_data["timestamp"],
                    y=processed_data["ma7"],
                    name="MA7",
                    line=dict(color="blue", width=1),
                )
            )

            fig.add_trace(
                go.Scatter(
                    x=processed_data["timestamp"],
                    y=processed_data["ma20"],
                    name="MA20",
                    line=dict(color="orange", width=1),
                )
            )

            fig.update_layout(
                title="Price Movement with Moving Averages",
                yaxis_title="Price (USD)",
                xaxis_title="Date",
                height=600,
                template="plotly_white",
            )

            st.plotly_chart(fig, use_container_width=True)

            # Technical Indicators
            col1, col2 = st.columns(2)

            with col1:
                # Daily Returns
                fig_returns = px.line(
                    processed_data,
                    x="timestamp",
                    y="daily_return",
                    title="Daily Returns (%)",
                )
                st.plotly_chart(fig_returns, use_container_width=True)

            with col2:
                # Momentum
                fig_momentum = px.line(
                    processed_data, x="timestamp", y="momentum", title="Price Momentum"
                )
                st.plotly_chart(fig_momentum, use_container_width=True)

        elif view_type == "Volume Analysis":
            # Volume Analysis View
            st.subheader("Volume Analysis")

            # Volume chart with MA
            fig_volume = go.Figure()

            fig_volume.add_trace(
                go.Bar(
                    x=processed_data["timestamp"],
                    y=processed_data["volume"],
                    name="Volume",
                    marker_color="lightblue",
                )
            )

            fig_volume.add_trace(
                go.Scatter(
                    x=processed_data["timestamp"],
                    y=processed_data["volume_ma5"],
                    name="Volume MA5",
                    line=dict(color="red", width=2),
                )
            )

            fig_volume.update_layout(
                title="Trading Volume Analysis",
                yaxis_title="Volume",
                xaxis_title="Date",
                height=500,
                template="plotly_white",
            )

            st.plotly_chart(fig_volume, use_container_width=True)

            # Volume distribution
            fig_vol_dist = px.histogram(
                processed_data, x="volume", nbins=50, title="Volume Distribution"
            )
            st.plotly_chart(fig_vol_dist, use_container_width=True)

        else:
            # Comparative Analysis View
            st.subheader("Comparative Analysis")

            # Get data for all stocks
            comparison_df = pd.DataFrame()

            for symbol in STOCK_CONFIGS.keys():
                try:
                    stock_data = dashboard.get_stock_data(
                        symbol, selected_days, "processed"
                    )
                    if not stock_data.empty:
                        first_price = stock_data["close"].iloc[0]
                        comparison_df[symbol] = (
                            stock_data["close"] / first_price - 1
                        ) * 100
                        comparison_df["timestamp"] = stock_data["timestamp"]
                except Exception as e:
                    st.warning(f"Could not fetch data for {symbol}: {str(e)}")

            if not comparison_df.empty:
                fig_comparison = px.line(
                    comparison_df,
                    x="timestamp",
                    y=[col for col in comparison_df.columns if col != "timestamp"],
                    title="Relative Performance Comparison (%)",
                    height=600,
                )
                st.plotly_chart(fig_comparison, use_container_width=True)

                # Correlation heatmap
                correlation = comparison_df.drop("timestamp", axis=1).corr()
                fig_corr = px.imshow(
                    correlation,
                    title="Stock Price Correlation Matrix",
                    color_continuous_scale="RdBu",
                )
                st.plotly_chart(fig_corr, use_container_width=True)

        # Additional Analysis Section
        st.subheader("Statistical Summary")
        summary_data = processed_data.describe()
        st.dataframe(summary_data)

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.write("Please try adjusting the date range or selecting a different stock.")

    # Footer
    st.markdown("---")
    st.markdown(
        "Data refreshes every 5 minutes. Last update: "
        + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )


if __name__ == "__main__":
    main()

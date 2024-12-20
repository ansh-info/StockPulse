# dashboard.py
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

from config import GCP_CONFIG, STOCK_CONFIGS


class StockDashboard:
    def __init__(self):
        self.client = bigquery.Client()
        self.dataset = f"{GCP_CONFIG['PROJECT_ID']}.{GCP_CONFIG['DATASET_NAME']}"

    def get_stock_data(self, symbol, days=30):
        query = f"""
        SELECT
            timestamp,
            symbol,
            open,
            high,
            low,
            close,
            volume
        FROM {self.dataset}.{STOCK_CONFIGS[symbol]['table_name']}
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        ORDER BY timestamp
        """
        return pd.read_gbq(query, project_id=GCP_CONFIG["PROJECT_ID"])

    def get_daily_summary(self, days=30):
        query = f"""
        SELECT *
        FROM {self.dataset}.daily_summary
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY date, symbol
        """
        return pd.read_gbq(query, project_id=GCP_CONFIG["PROJECT_ID"])

    def get_correlations(self):
        query = f"""
        SELECT *
        FROM {self.dataset}.stock_correlations
        ORDER BY ABS(return_correlation) DESC
        """
        return pd.read_gbq(query, project_id=GCP_CONFIG["PROJECT_ID"])


def main():
    st.set_page_config(page_title="Stock Market Dashboard", layout="wide")

    # Initialize dashboard
    dashboard = StockDashboard()

    # Sidebar
    st.sidebar.title("Dashboard Controls")
    selected_days = st.sidebar.slider("Select Days Range", 1, 90, 30)

    # Title
    st.title("Stock Market Analysis Dashboard")

    # Top level metrics
    st.header("Market Overview")
    daily_summary = dashboard.get_daily_summary(selected_days)

    # Create three columns for metrics
    col1, col2, col3 = st.columns(3)

    latest_date = daily_summary["date"].max()
    latest_data = daily_summary[daily_summary["date"] == latest_date]

    with col1:
        st.metric(
            "Best Performer Today",
            f"{latest_data.iloc[latest_data['daily_return'].argmax()]['symbol']}",
            f"{latest_data['daily_return'].max():.2f}%",
        )

    with col2:
        st.metric(
            "Worst Performer Today",
            f"{latest_data.iloc[latest_data['daily_return'].argmin()]['symbol']}",
            f"{latest_data['daily_return'].min():.2f}%",
        )

    with col3:
        st.metric(
            "Highest Volume Today",
            f"{latest_data.iloc[latest_data['total_volume'].argmax()]['symbol']}",
            f"{latest_data['total_volume'].max():,.0f}",
        )

    # Stock Price Charts
    st.header("Stock Price Analysis")
    selected_stock = st.selectbox("Select Stock", list(STOCK_CONFIGS.keys()))

    stock_data = dashboard.get_stock_data(selected_stock, selected_days)

    # Candlestick chart
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=stock_data["timestamp"],
                open=stock_data["open"],
                high=stock_data["high"],
                low=stock_data["low"],
                close=stock_data["close"],
            )
        ]
    )

    fig.update_layout(
        title=f"{selected_stock} Price Movement",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        height=600,
    )

    st.plotly_chart(fig, use_container_width=True)

    # Volume Analysis
    st.header("Volume Analysis")
    fig_volume = px.bar(
        stock_data, x="timestamp", y="volume", title=f"{selected_stock} Trading Volume"
    )
    st.plotly_chart(fig_volume, use_container_width=True)

    # Correlation Matrix
    st.header("Stock Correlations")
    correlations = dashboard.get_correlations()

    # Create correlation matrix visualization
    corr_fig = px.scatter(
        correlations,
        x="stock1",
        y="stock2",
        size="return_correlation",
        color="return_correlation",
        title="Stock Return Correlations",
        color_continuous_scale="RdBu",
    )

    st.plotly_chart(corr_fig, use_container_width=True)

    # Performance Comparison
    st.header("Performance Comparison")
    # Normalize prices to compare relative performance
    comparison_df = pd.DataFrame()

    for symbol in STOCK_CONFIGS.keys():
        stock_data = dashboard.get_stock_data(symbol, selected_days)
        first_price = stock_data["close"].iloc[0]
        comparison_df[symbol] = (stock_data["close"] / first_price - 1) * 100
        comparison_df["timestamp"] = stock_data["timestamp"]

    fig_comparison = px.line(
        comparison_df,
        x="timestamp",
        y=list(STOCK_CONFIGS.keys()),
        title="Relative Performance Comparison (%)",
    )

    st.plotly_chart(fig_comparison, use_container_width=True)


if __name__ == "__main__":
    main()

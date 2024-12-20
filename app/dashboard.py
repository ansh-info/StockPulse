from datetime import datetime, timedelta

import numpy as np
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

    def get_stock_data(self, symbol, days=30):
        """Get raw stock data from BigQuery"""
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
        return pandas_gbq.read_gbq(
            query,
            project_id=GCP_CONFIG["PROJECT_ID"],
            progress_bar_type=None,  # Disable progress bar to avoid cluttering streamlit
        )

    def calculate_daily_summary(self, days=30):
        """Calculate daily summary using pandas"""
        all_data = []

        # Fetch data for each stock
        for symbol in STOCK_CONFIGS.keys():
            try:
                df = self.get_stock_data(symbol, days)
                if not df.empty:
                    # Add date column
                    df["date"] = pd.to_datetime(df["timestamp"]).dt.date
                    all_data.append(df)
            except Exception as e:
                st.warning(f"Could not fetch data for {symbol}: {str(e)}")
                continue

        if not all_data:
            return pd.DataFrame()

        # Combine all stock data
        combined_data = pd.concat(all_data, ignore_index=True)

        # Calculate daily summary
        daily_summary = []

        for symbol in STOCK_CONFIGS.keys():
            stock_data = combined_data[combined_data["symbol"] == symbol]
            for date in stock_data["date"].unique():
                day_data = stock_data[stock_data["date"] == date]
                if not day_data.empty:
                    daily_return = (
                        (day_data["close"].iloc[-1] - day_data["open"].iloc[0])
                        / day_data["open"].iloc[0]
                        * 100
                    )

                    summary = {
                        "date": date,
                        "symbol": symbol,
                        "day_open": day_data["open"].iloc[0],
                        "day_high": day_data["high"].max(),
                        "day_low": day_data["low"].min(),
                        "day_close": day_data["close"].iloc[-1],
                        "total_volume": day_data["volume"].sum(),
                        "daily_return": daily_return,
                    }
                    daily_summary.append(summary)

        return pd.DataFrame(daily_summary)


def main():
    st.set_page_config(page_title="Stock Market Dashboard", layout="wide")

    # Initialize dashboard
    dashboard = StockDashboard()

    # Sidebar
    st.sidebar.title("Dashboard Controls")
    selected_days = st.sidebar.slider("Select Days Range", 1, 90, 30)

    # Title
    st.title("Stock Market Analysis Dashboard")

    try:
        # Top level metrics
        st.header("Market Overview")
        daily_summary = dashboard.calculate_daily_summary(selected_days)

        if not daily_summary.empty:
            # Create three columns for metrics
            col1, col2, col3 = st.columns(3)

            latest_date = daily_summary["date"].max()
            latest_data = daily_summary[daily_summary["date"] == latest_date]

            with col1:
                best_performer = latest_data.loc[latest_data["daily_return"].idxmax()]
                st.metric(
                    "Best Performer Today",
                    best_performer["symbol"],
                    f"{best_performer['daily_return']:.2f}%",
                )

            with col2:
                worst_performer = latest_data.loc[latest_data["daily_return"].idxmin()]
                st.metric(
                    "Worst Performer Today",
                    worst_performer["symbol"],
                    f"{worst_performer['daily_return']:.2f}%",
                )

            with col3:
                highest_volume = latest_data.loc[latest_data["total_volume"].idxmax()]
                st.metric(
                    "Highest Volume Today",
                    highest_volume["symbol"],
                    f"{highest_volume['total_volume']:,.0f}",
                )

            # Stock Price Charts
            st.header("Stock Price Analysis")
            selected_stock = st.selectbox("Select Stock", list(STOCK_CONFIGS.keys()))

            stock_data = dashboard.get_stock_data(selected_stock, selected_days)

            if not stock_data.empty:
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
                    stock_data,
                    x="timestamp",
                    y="volume",
                    title=f"{selected_stock} Trading Volume",
                )
                st.plotly_chart(fig_volume, use_container_width=True)

                # Performance Comparison
                st.header("Performance Comparison")
                comparison_df = pd.DataFrame()

                for symbol in STOCK_CONFIGS.keys():
                    symbol_data = dashboard.get_stock_data(symbol, selected_days)
                    if not symbol_data.empty:
                        first_price = symbol_data["close"].iloc[0]
                        comparison_df[symbol] = (
                            symbol_data["close"] / first_price - 1
                        ) * 100
                        comparison_df["timestamp"] = symbol_data["timestamp"]

                if not comparison_df.empty:
                    fig_comparison = px.line(
                        comparison_df,
                        x="timestamp",
                        y=[col for col in comparison_df.columns if col != "timestamp"],
                        title="Relative Performance Comparison (%)",
                    )
                    st.plotly_chart(fig_comparison, use_container_width=True)
        else:
            st.warning("No data available for the selected time range.")

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.write("Please try adjusting the date range or selecting a different stock.")


if __name__ == "__main__":
    main()

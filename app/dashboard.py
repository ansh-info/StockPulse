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
        """Get daily summary for all stocks directly from raw tables"""
        stock_queries = []
        for symbol, config in STOCK_CONFIGS.items():
            stock_queries.append(
                f"""
            WITH daily_stats AS (
                SELECT
                    DATE(timestamp) as date,
                    '{symbol}' as symbol,
                    FIRST_VALUE(open) OVER (PARTITION BY DATE(timestamp) ORDER BY timestamp ASC) as day_open,
                    MAX(high) as day_high,
                    MIN(low) as day_low,
                    LAST_VALUE(close) OVER (PARTITION BY DATE(timestamp) ORDER BY timestamp ASC) as day_close,
                    SUM(volume) as total_volume
                FROM {self.dataset}.{config['table_name']}
                WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
                GROUP BY DATE(timestamp), symbol, timestamp, open, close
            )
            SELECT 
                date,
                symbol,
                ANY_VALUE(day_open) as day_open,
                ANY_VALUE(day_high) as day_high,
                ANY_VALUE(day_low) as day_low,
                ANY_VALUE(day_close) as day_close,
                ANY_VALUE(total_volume) as total_volume,
                ((ANY_VALUE(day_close) - ANY_VALUE(day_open)) / ANY_VALUE(day_open) * 100) as daily_return
            FROM daily_stats
            GROUP BY date, symbol
            """
            )

        query = f"""
        {" UNION ALL ".join(stock_queries)}
        ORDER BY date DESC, symbol
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

    try:
        # Top level metrics
        st.header("Market Overview")
        daily_summary = dashboard.get_daily_summary(selected_days)

        # Create three columns for metrics
        col1, col2, col3 = st.columns(3)

        if not daily_summary.empty:
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

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.write("Please try adjusting the date range or selecting a different stock.")


if __name__ == "__main__":
    main()

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from google.cloud import bigquery

# Configure your project
PROJECT_ID = "stock-data-pipeline-444011"
DATASET = "stock_market"
TABLE = "amazon_stock"


def get_stock_data():
    """Query latest stock data from BigQuery"""
    client = bigquery.Client()
    query = f"""
    SELECT 
        timestamp,
        symbol,
        open,
        high,
        low,
        close,
        volume
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    ORDER BY timestamp DESC
    """
    return client.query(query).to_dataframe()


# Create Streamlit app
st.title("Amazon Stock Dashboard")
st.markdown("Real-time stock data analysis")

# Load data
data = get_stock_data()

# Display current stock price
if not data.empty:
    current_price = data.iloc[0]["close"]
    st.metric("Current Stock Price", f"${current_price:.2f}")

# Price chart
st.subheader("Stock Price (Last 24 Hours)")
st.line_chart(data.set_index("timestamp")["close"])

# Volume chart
st.subheader("Trading Volume")
st.bar_chart(data.set_index("timestamp")["volume"])

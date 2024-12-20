from datetime import datetime, time
from typing import Dict, Optional

import pandas as pd


class StockDataPreprocessor:
    def __init__(self):
        self.market_open = time(9, 30)  # 9:30 AM
        self.market_close = time(16, 0)  # 4:00 PM

    def is_market_hours(self, timestamp: datetime) -> bool:
        """Check if timestamp is within market hours"""
        current_time = timestamp.time()
        return (
            self.market_open <= current_time <= self.market_close
            and timestamp.weekday() < 5  # Monday = 0, Friday = 4
        )

    def process_stock_data(
        self,
        df: pd.DataFrame,
        resample_freq: Optional[str] = None,
        fill_gaps: bool = True,
        calculate_indicators: bool = True,
    ) -> pd.DataFrame:
        """
        Process stock data with various transformations and calculations

        Args:
            df: DataFrame with stock data
            resample_freq: Frequency for resampling ('1H' for hourly, '1D' for daily, etc.)
            fill_gaps: Whether to fill gaps in data
            calculate_indicators: Whether to calculate technical indicators
        """
        # Convert timestamp to datetime if needed
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")

        # Filter for market hours
        df = df[df.index.map(self.is_market_hours)]

        # Resample data if frequency specified
        if resample_freq:
            df = df.resample(resample_freq).agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )

        # Fill gaps if requested - using recommended methods
        if fill_gaps:
            # Forward fill for short gaps (within same trading day)
            df = df.ffill(limit=12)  # limit prevents filling across days
            # For any remaining gaps, use backward fill with limit
            df = df.bfill(limit=12)

        # Calculate technical indicators if requested
        if calculate_indicators:
            # Price changes
            df["daily_return"] = df["close"].pct_change() * 100

            # Moving averages
            df["ma7"] = df["close"].rolling(window=7, min_periods=1).mean()
            df["ma20"] = df["close"].rolling(window=20, min_periods=1).mean()

            # Volatility (20-period standard deviation)
            df["volatility"] = (
                df["daily_return"].rolling(window=20, min_periods=1).std()
            )

            # Trading volume indicators
            df["volume_ma5"] = df["volume"].rolling(window=5, min_periods=1).mean()

            # Price momentum (14-period)
            df["momentum"] = df["close"] - df["close"].shift(14)

        return df

    def get_summary_stats(self, df: pd.DataFrame) -> Dict:
        """Calculate summary statistics for the stock data"""
        return {
            "avg_daily_return": df["daily_return"].mean(),
            "volatility": df["daily_return"].std(),
            "avg_volume": df["volume"].mean(),
            "max_price": df["high"].max(),
            "min_price": df["low"].min(),
            "price_range": df["high"].max() - df["low"].min(),
        }

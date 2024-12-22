import logging
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd
import pytz


class StockDataPreprocessor:
    def __init__(self):
        # Initialize logging
        self.setup_logging()

        # Market hours in ET
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET

        # Initialize timezone
        self.et_timezone = pytz.timezone("US/Eastern")

        # Data quality thresholds
        self.max_price_change = 10.0  # Maximum allowed price change (%)
        self.min_volume = 0  # Minimum allowed volume

        # Define expected columns and their order
        self.raw_columns = [
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        self.processed_columns = self.raw_columns + [
            "daily_return",
            "ma7",
            "ma20",
            "volatility",
            "volume_ma5",
            "momentum",
        ]

        self.logger.info("Initialized StockDataPreprocessor")

    def setup_logging(self):
        """Configure logging for preprocessor"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"preprocessor_{timestamp}.log"

        self.logger = logging.getLogger("StockPreprocessor")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(file_formatter)

            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(console_formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def is_market_hours(self, timestamp: pd.Timestamp) -> bool:
        """Check if timestamp is within market hours"""
        try:
            # Convert to ET if not already
            if timestamp.tz is None:
                timestamp = timestamp.tz_localize("UTC").tz_convert(self.et_timezone)
            elif timestamp.tz != self.et_timezone:
                timestamp = timestamp.tz_convert(self.et_timezone)

            current_time = timestamp.time()
            return (
                self.market_open <= current_time <= self.market_close
                and timestamp.weekday() < 5  # Monday = 0, Friday = 4
            )
        except Exception as e:
            self.logger.error(f"Error checking market hours for {timestamp}: {e}")
            return False

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean data"""
        self.logger.info(f"Starting data validation on {len(df)} records")

        try:
            # Make copy to avoid modifying original
            df = df.copy()

            # Ensure timestamp column exists and is datetime
            if "timestamp" not in df.columns:
                raise ValueError("DataFrame must contain 'timestamp' column")

            # Convert timestamp to datetime if it's not already
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Remove duplicates
            initial_len = len(df)
            df = df.drop_duplicates(subset=["timestamp", "symbol"], keep="first")
            dupes_removed = initial_len - len(df)
            if dupes_removed > 0:
                self.logger.warning(f"Removed {dupes_removed} duplicate records")

            # Remove records with invalid prices
            df = df[df["open"] > 0]
            df = df[df["high"] > 0]
            df = df[df["low"] > 0]
            df = df[df["close"] > 0]

            # Ensure price consistency
            df = df[df["high"] >= df["low"]]
            df = df[df["open"] >= df["low"]]
            df = df[df["open"] <= df["high"]]
            df = df[df["close"] >= df["low"]]
            df = df[df["close"] <= df["high"]]

            # Remove records with invalid volume
            df = df[df["volume"] >= self.min_volume]

            # Check for extreme price changes
            df["price_change"] = abs(df["close"].pct_change() * 100)
            suspicious_changes = df[df["price_change"] > self.max_price_change]
            if not suspicious_changes.empty:
                self.logger.warning(
                    f"Found {len(suspicious_changes)} records with suspicious price changes"
                )

            # Remove temporary columns
            df = df.drop("price_change", axis=1, errors="ignore")

            # Ensure raw columns are in correct order
            df = df.reindex(columns=self.raw_columns)

            self.logger.info(
                f"Data validation complete. {len(df)} valid records remaining"
            )
            return df

        except Exception as e:
            self.logger.error(f"Error during data validation: {e}")
            raise

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators"""
        try:
            df = df.copy()

            # Calculate indicators
            df["daily_return"] = df["close"].pct_change() * 100
            df["ma7"] = df["close"].rolling(window=7, min_periods=1).mean()
            df["ma20"] = df["close"].rolling(window=20, min_periods=1).mean()
            df["volatility"] = (
                df["daily_return"].rolling(window=20, min_periods=1).std()
            )
            df["volume_ma5"] = df["volume"].rolling(window=5, min_periods=1).mean()
            df["momentum"] = df["close"] - df["close"].shift(14)

            # Ensure all columns are present and in correct order
            df = df.reindex(columns=self.processed_columns)

            return df

        except Exception as e:
            self.logger.error(f"Error calculating technical indicators: {e}")
            raise

    def fill_gaps(self, df: pd.DataFrame, max_gap: int = 5) -> pd.DataFrame:
        """Fill gaps in data with intelligent interpolation"""
        try:
            df = df.copy()

            # Sort by timestamp
            df = df.sort_values("timestamp")

            # Forward fill for short gaps (within same trading day)
            df = df.ffill(limit=max_gap)

            # Backward fill remaining gaps
            df = df.bfill(limit=max_gap)

            # Log any remaining gaps
            missing_data = df.isnull().sum()
            if missing_data.any():
                self.logger.warning(
                    f"Remaining missing values after gap filling:\n{missing_data}"
                )

            return df

        except Exception as e:
            self.logger.error(f"Error filling gaps: {e}")
            raise

    def process_stock_data(
        self,
        df: pd.DataFrame,
        resample_freq: Optional[str] = None,
        fill_gaps: bool = True,
        calculate_indicators: bool = True,
    ) -> pd.DataFrame:
        """
        Process stock data with various transformations and calculations
        """
        try:
            self.logger.info(f"Starting data processing for {len(df)} records")

            # Validate data first
            df = self.validate_data(df)

            # Filter for market hours
            df["market_hours"] = df["timestamp"].map(self.is_market_hours)
            initial_len = len(df)
            df = df[df["market_hours"]]
            df = df.drop("market_hours", axis=1)
            filtered_count = initial_len - len(df)
            self.logger.info(
                f"Filtered out {filtered_count} records outside market hours"
            )

            # Resample data if frequency specified
            if resample_freq:
                df = df.set_index("timestamp")
                df = df.resample(resample_freq).agg(
                    {
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "volume": "sum",
                        "symbol": "first",
                    }
                )
                df = df.reset_index()

            # Fill gaps if requested
            if fill_gaps:
                df = self.fill_gaps(df)

            # Calculate technical indicators if requested
            if calculate_indicators:
                df = self.calculate_technical_indicators(df)

            # Ensure all required columns are present and in correct order
            df = df.reindex(columns=self.processed_columns)

            self.logger.info(f"Data processing complete. Final record count: {len(df)}")
            return df

        except Exception as e:
            self.logger.error(f"Error during data processing: {e}")
            raise

    def get_summary_stats(self, df: pd.DataFrame) -> Dict:
        """Calculate summary statistics for the stock data"""
        try:
            df = df.copy()

            stats = {
                "avg_daily_return": (
                    df["daily_return"].mean() if "daily_return" in df.columns else None
                ),
                "volatility": (
                    df["daily_return"].std() if "daily_return" in df.columns else None
                ),
                "avg_volume": df["volume"].mean(),
                "max_price": df["high"].max(),
                "min_price": df["low"].min(),
                "price_range": df["high"].max() - df["low"].min(),
                "record_count": len(df),
                "first_timestamp": df["timestamp"].min(),
                "last_timestamp": df["timestamp"].max(),
            }

            return stats

        except Exception as e:
            self.logger.error(f"Error calculating summary stats: {e}")
            raise

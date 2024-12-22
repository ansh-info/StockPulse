import logging
from datetime import datetime, time
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd
import pytz
from pandas.tseries.holiday import USFederalHolidayCalendar


class StockDataPreprocessor:
    def __init__(self):
        # Initialize logging
        self.setup_logging()

        # Market hours in ET
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET

        # Initialize timezone
        self.et_timezone = pytz.timezone("US/Eastern")

        # Initialize holiday calendar
        self.holiday_calendar = USFederalHolidayCalendar()

        # Define expected columns
        self.required_columns = [
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]

        # Define processed columns
        self.processed_columns = self.required_columns + [
            "date",
            "time",
            "ma5",  # 5-period moving average
            "cma",  # Cumulative moving average
            "eod_ma5",  # End-of-day 5-period moving average
        ]

        self.logger.info("Initialized Improved Stock Data Preprocessor")

    def setup_logging(self):
        """Configure logging with both file and console handlers"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"preprocessor_{timestamp}.log"

        self.logger = logging.getLogger("StockPreprocessor")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            # File handler
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(file_formatter)

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(console_formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def is_market_hours(self, timestamp: pd.Timestamp) -> bool:
        """
        Check if timestamp is within market hours, considering holidays
        """
        try:
            # Convert to ET if not already
            if timestamp.tz is None:
                timestamp = timestamp.tz_localize("UTC").tz_convert(self.et_timezone)
            elif timestamp.tz != self.et_timezone:
                timestamp = timestamp.tz_convert(self.et_timezone)

            # Check if it's a holiday
            if timestamp.date() in self.holiday_calendar.holidays():
                return False

            # Check market hours
            current_time = timestamp.time()
            return (
                self.market_open <= current_time <= self.market_close
                and timestamp.weekday() < 5  # Monday = 0, Friday = 4
            )
        except Exception as e:
            self.logger.error(f"Error checking market hours for {timestamp}: {e}")
            return False

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean data with improved checks
        """
        try:
            df = df.copy()

            # Ensure required columns exist
            missing_cols = set(self.required_columns) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")

            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Add date and time columns
            df["date"] = df["timestamp"].dt.date
            df["time"] = df["timestamp"].dt.time

            # Remove exact duplicates
            df = df.drop_duplicates()

            # Remove records with null values in critical columns
            critical_cols = ["open", "high", "low", "close", "volume"]
            df = df.dropna(subset=critical_cols)

            # Basic price and volume validation
            df = df[df[critical_cols].gt(0).all(axis=1)]

            # Ensure high/low price consistency
            df = df[df["high"] >= df["low"]]

            return df

        except Exception as e:
            self.logger.error(f"Error during data validation: {e}")
            raise

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate technical indicators with improved methodology
        """
        try:
            df = df.copy()

            # Sort by timestamp
            df = df.sort_values("timestamp")

            # Calculate 5-period moving average
            df["ma5"] = df.groupby("symbol")["close"].transform(
                lambda x: x.rolling(window=5, min_periods=1).mean()
            )

            # Calculate cumulative moving average
            df["cma"] = df.groupby("symbol")["close"].transform(
                lambda x: x.expanding().mean()
            )

            # Calculate end-of-day 5-period moving average
            eod_data = df.groupby(["symbol", "date"])["close"].last().reset_index()
            eod_data["eod_ma5"] = eod_data.groupby("symbol")["close"].transform(
                lambda x: x.rolling(window=5, min_periods=1).mean()
            )

            # Merge end-of-day MA back to main DataFrame
            df = df.merge(
                eod_data[["symbol", "date", "eod_ma5"]],
                on=["symbol", "date"],
                how="left",
            )

            return df

        except Exception as e:
            self.logger.error(f"Error calculating indicators: {e}")
            raise

    def process_stock_data(
        self, df: pd.DataFrame, check_market_hours: bool = True
    ) -> pd.DataFrame:
        """
        Main processing function with improved methodology
        """
        try:
            self.logger.info(f"Starting data processing for {len(df)} records")

            # Validate data
            df = self.validate_data(df)

            # Filter for market hours if requested
            if check_market_hours:
                df["is_market_hours"] = df["timestamp"].map(self.is_market_hours)
                df = df[df["is_market_hours"]]
                df = df.drop("is_market_hours", axis=1)

            # Calculate indicators
            df = self.calculate_indicators(df)

            # Ensure all required columns are present
            df = df.reindex(columns=self.processed_columns)

            self.logger.info(f"Processing complete. Final record count: {len(df)}")
            return df

        except Exception as e:
            self.logger.error(f"Error during data processing: {e}")
            raise

    def get_missing_data_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate report of missing data points during market hours
        """
        try:
            # Generate expected timestamps
            start_date = df["date"].min()
            end_date = df["date"].max()

            business_days = pd.date_range(
                start=start_date, end=end_date, freq="B", tz=self.et_timezone
            )

            # Remove holidays
            business_days = business_days[
                ~business_days.date.isin(self.holiday_calendar.holidays())
            ]

            # Generate all 5-minute intervals
            market_times = pd.date_range(
                start=f"{business_days[0].date()} 09:30:00",
                end=f"{business_days[0].date()} 16:00:00",
                freq="5min",
                tz=self.et_timezone,
            ).time

            # Check for missing data points
            missing_data = []

            for date in business_days:
                date_data = df[df["date"] == date.date()]
                available_times = set(date_data["time"])

                missing_times = [
                    time for time in market_times if time not in available_times
                ]

                if missing_times:
                    missing_data.append(
                        {"date": date.date(), "missing_times": missing_times}
                    )

            return pd.DataFrame(missing_data)

        except Exception as e:
            self.logger.error(f"Error generating missing data report: {e}")
            raise

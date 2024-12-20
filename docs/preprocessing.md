Let me break down what the preprocessing pipeline is doing:

1. **Time-Based Filtering and Validation**:

```python
def is_market_hours(self, timestamp: datetime) -> bool:
    current_time = timestamp.time()
    return (
        self.market_open <= current_time <= self.market_close  # Between 9:30 AM and 4:00 PM
        and timestamp.weekday() < 5  # Only Monday through Friday
    )
```

This ensures we only keep data from actual market hours (9:30 AM - 4:00 PM) and trading days (Monday-Friday).

2. **Data Resampling** (Optional):

```python
if resample_freq:
    df = df.resample(resample_freq).agg({
        'open': 'first',    # First price of the period
        'high': 'max',      # Highest price
        'low': 'min',       # Lowest price
        'close': 'last',    # Last price
        'volume': 'sum'     # Total volume
    })
```

This lets you convert 5-minute data to different timeframes:

- Hourly data (`resample_freq='1H'`)
- Daily data (`resample_freq='1D'`)
- Weekly data (`resample_freq='1W'`)

3. **Gap Handling**:

```python
if fill_gaps:
    # Forward fill for short gaps (within same trading day)
    df = df.fillna(method='ffill', limit=12)
```

Fills missing data points within the same trading day (limited to prevent filling across days/weekends).

4. **Technical Indicators** (Optional):

```python
if calculate_indicators:
    # Returns
    df['daily_return'] = df['close'].pct_change() * 100

    # Moving averages
    df['ma7'] = df['close'].rolling(window=7).mean()
    df['ma20'] = df['close'].rolling(window=20).mean()

    # Volatility
    df['volatility'] = df['daily_return'].rolling(window=20).std()

    # Volume trends
    df['volume_ma5'] = df['volume'].rolling(window=5).mean()

    # Momentum
    df['momentum'] = df['close'] - df['close'].shift(14)
```

Calculates common technical indicators used in stock analysis:

- Daily returns (percentage price changes)
- Moving averages (7 and 20 periods)
- Volatility (standard deviation of returns)
- Volume moving average
- Price momentum

5. **Summary Statistics**:

```python
def get_summary_stats(self, df: pd.DataFrame) -> Dict:
    return {
        'avg_daily_return': df['daily_return'].mean(),
        'volatility': df['daily_return'].std(),
        'avg_volume': df['volume'].mean(),
        'max_price': df['high'].max(),
        'min_price': df['low'].min(),
        'price_range': df['high'].max() - df['low'].min()
    }
```

Provides summary metrics for overall stock performance.

You can use this pipeline in two ways:

1. **Real-time Processing**:

```python
# Process incoming 5-minute data
preprocessor = StockDataPreprocessor()
processed_df = preprocessor.process_stock_data(
    raw_df,
    resample_freq=None,  # Keep 5-minute intervals
    fill_gaps=True,
    calculate_indicators=True
)
```

2. **Different Timeframe Analysis**:

```python
# Convert to hourly data
hourly_df = preprocessor.process_stock_data(
    raw_df,
    resample_freq='1H',
    fill_gaps=True,
    calculate_indicators=True
)

# Convert to daily data
daily_df = preprocessor.process_stock_data(
    raw_df,
    resample_freq='1D',
    fill_gaps=True,
    calculate_indicators=True
)
```

This preprocessing ensures your data is:

1. Clean (no invalid times/dates)
2. Complete (handled gaps appropriately)
3. Enhanced (with technical indicators)
4. Ready for different types of analysis (multiple timeframes)

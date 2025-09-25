#!/usr/bin/env python3
"""Transform Economic Indicators data into ML-ready features.

Creates table ``transformed.economic_indicators_features`` containing normalized
economic indicators with forward-filled missing data, lookback-only normalization,
and daily frequency alignment for ML models.

Key Features:
- Forward-fills monthly data to create daily records
- Uses lookback-only normalization to prevent data leakage
- Creates momentum, volatility, and trend features
- Handles both daily and monthly economic indicators
- All features are ML-ready with proper scaling
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class EconomicIndicatorsTransformer:
    """Transform economic indicators into ML-ready features."""
    
    def __init__(self) -> None:
        self.db = PostgresDatabaseManager()
        
        # Normalization parameters (lookback-only to prevent leakage)
        self.lookback_window = 252  # ~1 year of trading days for normalization
        self.momentum_windows = [5, 10, 21, 63]  # 1w, 2w, 1m, 3m
        self.volatility_windows = [21, 63]  # 1m, 3m
        self.epsilon = 1e-8  # For safe division
        
        # Core economic indicators we'll focus on
        self.core_indicators = [
            'REAL_GDP', 'REAL_GDP_PER_CAPITA',
            'TREASURY_YIELD_10YEAR', 'TREASURY_YIELD_3MONTH', 'TREASURY_YIELD_2YEAR',
            'TREASURY_YIELD_5YEAR', 'TREASURY_YIELD_7YEAR', 'TREASURY_YIELD_30YEAR',
            'FEDERAL_FUNDS_RATE', 'CPI', 'INFLATION',
            'RETAIL_SALES', 'DURABLES', 'UNEMPLOYMENT', 'NONFARM_PAYROLL'
        ]

    def _fetch_economic_indicators(self) -> pd.DataFrame:
        """Fetch all economic indicators data from the source table."""
        query = """
            SELECT
                indicator_name,
                function_name,
                date,
                interval,
                value,
                unit,
                name
            FROM source.economic_indicators
            WHERE api_response_status = 'success'
                AND value IS NOT NULL
                AND date IS NOT NULL
            ORDER BY function_name, date
        """
        return self.db.fetch_dataframe(query)

    def _create_daily_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Create a daily calendar for the analysis period."""
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        return pd.DataFrame({'date': date_range})

    def _pivot_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pivot indicators to have each indicator as a column."""
        # Use function_name as the primary identifier since it's more standardized
        pivot_df = df.pivot_table(
            index='date',
            columns='function_name',
            values='value',
            aggfunc='first'  # Take first value if duplicates
        ).reset_index()
        
        # Flatten column names
        pivot_df.columns.name = None
        
        return pivot_df

    def _forward_fill_monthly_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Forward fill monthly data to create daily records."""
        # Get date range from data
        min_date = df['date'].min()
        max_date = df['date'].max()
        
        # Create daily calendar
        daily_calendar = self._create_daily_calendar(min_date, max_date)
        
        # Merge with daily calendar
        df_daily = daily_calendar.merge(df, on='date', how='left')
        
        # Forward fill all indicator columns
        indicator_columns = [col for col in df_daily.columns if col != 'date']
        for col in indicator_columns:
            df_daily[col] = df_daily[col].ffill()
        
        # Remove rows where all indicators are still null (before first data point)
        df_daily = df_daily.dropna(how='all', subset=indicator_columns)
        
        return df_daily

    def _safe_divide(self, numerator: pd.Series, denominator: pd.Series) -> pd.Series:
        """Safely divide two series, handling division by zero."""
        return numerator / (denominator + self.epsilon)

    def _calculate_rolling_zscore(self, series: pd.Series, window: int) -> pd.Series:
        """Calculate rolling z-score using only historical data."""
        rolling_mean = series.rolling(window=window, min_periods=window//2).mean()
        rolling_std = series.rolling(window=window, min_periods=window//2).std()
        
        # Prevent division by zero
        rolling_std = rolling_std.fillna(1.0)
        rolling_std = np.where(rolling_std < self.epsilon, 1.0, rolling_std)
        
        return (series - rolling_mean) / rolling_std

    def _calculate_momentum_features(self, df: pd.DataFrame, indicator_cols: list[str]) -> pd.DataFrame:
        """Calculate momentum features (returns) for different time windows."""
        df_features = df.copy()
        
        for col in indicator_cols:
            if col in df_features.columns:
                for window in self.momentum_windows:
                    # Calculate returns (percent change)
                    momentum_col = f"{col}_momentum_{window}d"
                    df_features[momentum_col] = df_features[col].pct_change(periods=window)
                    
                    # Calculate rolling average momentum
                    avg_momentum_col = f"{col}_avg_momentum_{window}d"
                    df_features[avg_momentum_col] = df_features[momentum_col].rolling(
                        window=window, min_periods=window//2
                    ).mean()
                    
        return df_features

    def _calculate_volatility_features(self, df: pd.DataFrame, indicator_cols: list[str]) -> pd.DataFrame:
        """Calculate volatility features for different time windows."""
        df_features = df.copy()
        
        for col in indicator_cols:
            if col in df_features.columns:
                # First calculate daily returns if not already present
                daily_returns_col = f"{col}_daily_returns"
                if daily_returns_col not in df_features.columns:
                    df_features[daily_returns_col] = df_features[col].pct_change()
                
                for window in self.volatility_windows:
                    # Rolling volatility (standard deviation of returns)
                    vol_col = f"{col}_volatility_{window}d"
                    df_features[vol_col] = df_features[daily_returns_col].rolling(
                        window=window, min_periods=window//2
                    ).std()
                    
        return df_features

    def _calculate_trend_features(self, df: pd.DataFrame, indicator_cols: list[str]) -> pd.DataFrame:
        """Calculate trend features using moving averages and slopes."""
        df_features = df.copy()
        
        for col in indicator_cols:
            if col in df_features.columns:
                # Short-term vs long-term moving average ratio
                ma_5 = df_features[col].rolling(window=5, min_periods=3).mean()
                ma_21 = df_features[col].rolling(window=21, min_periods=10).mean()
                ma_63 = df_features[col].rolling(window=63, min_periods=30).mean()
                
                df_features[f"{col}_ma5_ma21_ratio"] = self._safe_divide(ma_5, ma_21)
                df_features[f"{col}_ma21_ma63_ratio"] = self._safe_divide(ma_21, ma_63)
                
                # Trend strength (linear regression slope over different windows)
                for window in [21, 63]:
                    slope_col = f"{col}_trend_slope_{window}d"
                    # Calculate rolling linear regression slope
                    df_features[slope_col] = df_features[col].rolling(
                        window=window, min_periods=window//2
                    ).apply(self._calculate_slope, raw=False)
                    
        return df_features

    def _calculate_slope(self, series: pd.Series) -> float:
        """Calculate linear regression slope for a series."""
        if len(series) < 2 or series.isna().all():
            return np.nan
        
        # Remove NaN values
        clean_series = series.dropna()
        if len(clean_series) < 2:
            return np.nan
            
        x = np.arange(len(clean_series))
        y = clean_series.values
        
        # Calculate slope using least squares
        try:
            slope = np.polyfit(x, y, 1)[0]
            return slope
        except (np.linalg.LinAlgError, ValueError):
            return np.nan

    def _calculate_cross_indicator_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate features that combine multiple indicators."""
        df_features = df.copy()
        
        # Yield curve features (if treasury data exists)
        if 'TREASURY_YIELD_10YEAR' in df_features.columns and 'TREASURY_YIELD_3MONTH' in df_features.columns:
            # 10Y-3M yield spread (classic recession indicator)
            df_features['yield_spread_10y_3m'] = (
                df_features['TREASURY_YIELD_10YEAR'] - df_features['TREASURY_YIELD_3MONTH']
            )
            
        if 'TREASURY_YIELD_10YEAR' in df_features.columns and 'TREASURY_YIELD_2YEAR' in df_features.columns:
            # 10Y-2Y yield spread
            df_features['yield_spread_10y_2y'] = (
                df_features['TREASURY_YIELD_10YEAR'] - df_features['TREASURY_YIELD_2YEAR']
            )
            
        # Real interest rate (Federal Funds Rate - Inflation)
        if 'FEDERAL_FUNDS_RATE' in df_features.columns and 'INFLATION' in df_features.columns:
            df_features['real_interest_rate'] = (
                df_features['FEDERAL_FUNDS_RATE'] - df_features['INFLATION']
            )
            
        # Economic growth momentum (GDP vs Employment)
        if 'REAL_GDP' in df_features.columns and 'UNEMPLOYMENT' in df_features.columns:
            # Inverse unemployment as employment proxy
            df_features['employment_proxy'] = 100 - df_features['UNEMPLOYMENT']
            
        return df_features

    def _normalize_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply lookback-only normalization to all features."""
        df_normalized = df.copy()
        
        # Identify feature columns (exclude date and raw indicators)
        feature_columns = [
            col for col in df_normalized.columns 
            if col not in ['date'] + self.core_indicators
            and not col.endswith('_daily_returns')  # Skip daily returns as they're already normalized
        ]
        
        print(f"Normalizing {len(feature_columns)} feature columns...")  # noqa: T201
        
        for col in feature_columns:
            if df_normalized[col].dtype in ['float64', 'int64']:
                # Apply rolling z-score normalization with fred_econ_ prefix
                df_normalized[f"fred_econ_{col}_normalized"] = self._calculate_rolling_zscore(
                    df_normalized[col], self.lookback_window
                )
                
        return df_normalized

    def _create_output_table(self, df: pd.DataFrame) -> None:
        """Create the output table in the transformed schema with only ML-ready features."""
        # Create schema if it doesn't exist
        self.db.execute_query("CREATE SCHEMA IF NOT EXISTS transformed")
        
        # Identify only the normalized ML-ready feature columns (with fred_econ_ prefix)
        feature_columns = [col for col in df.columns if col.startswith('fred_econ_')]
        
        print(f"Creating table with {len(feature_columns)} ML-ready feature columns...")  # noqa: T201
        
        # Create column definitions
        column_definitions = [
            "date DATE PRIMARY KEY",
            "created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
            "updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()"
        ]
        
        # Add feature columns
        for col in feature_columns:
            column_definitions.append(f"{col} NUMERIC(15,6)")
        
        # Create table
        create_sql = f"""
            DROP TABLE IF EXISTS transformed.economic_indicators_features;
            CREATE TABLE transformed.economic_indicators_features (
                {',\n                '.join(column_definitions)}
            )
        """
        self.db.execute_query(create_sql)
        print(f"Created table with {len(feature_columns)} ML-ready feature columns")  # noqa: T201

    def _write_output(self, df: pd.DataFrame) -> None:
        """Write the transformed features to the database."""
        # Select only date and ML-ready features (with fred_econ_ prefix)
        feature_columns = [col for col in df.columns if col.startswith('fred_econ_')]
        output_columns = ['date'] + feature_columns
        
        df_output = df[output_columns].copy()
        
        # Replace infinite values with None
        numeric_columns = df_output.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            df_output[col] = df_output[col].replace([float('inf'), float('-inf')], None)
        
        # Create records for batch insert
        column_list = ', '.join(output_columns)
        placeholders = ', '.join(['%s'] * len(output_columns))
        
        records = []
        for _, row in df_output.iterrows():
            record = []
            for col in output_columns:
                value = row[col]
                if pd.isna(value):
                    record.append(None)
                else:
                    record.append(value)
            records.append(tuple(record))
        
        # Insert data with upsert logic
        insert_sql = f"""
            INSERT INTO transformed.economic_indicators_features (
                {column_list}
            ) VALUES ({placeholders})
            ON CONFLICT (date)
            DO UPDATE SET
                {', '.join([f'{col} = EXCLUDED.{col}' for col in feature_columns])},
                updated_at = NOW()
        """
        
        self.db.execute_many(insert_sql, records)
        print(f"Inserted {len(records)} records with {len(feature_columns)} ML-ready features")  # noqa: T201

    def run(self) -> pd.DataFrame:
        """Execute the complete economic indicators transformation pipeline."""
        print("Starting economic indicators transformation...")  # noqa: T201
        
        try:
            # Connect to database
            self.db.connect()
            
            # Fetch raw data
            print("Fetching economic indicators data...")  # noqa: T201
            raw_df = self._fetch_economic_indicators()
            print(f"Fetched {len(raw_df)} raw records")  # noqa: T201
            
            if raw_df.empty:
                print("No economic indicators data found. Skipping transformation.")  # noqa: T201
                return pd.DataFrame()
            
            # Convert date column to datetime
            raw_df['date'] = pd.to_datetime(raw_df['date'])
            
            # Pivot indicators to columns
            print("Pivoting indicators to columns...")  # noqa: T201
            pivot_df = self._pivot_indicators(raw_df)
            print(f"Pivoted to {len(pivot_df.columns)-1} indicator columns")  # noqa: T201
            
            # Forward fill to create daily data
            print("Forward filling monthly data to daily frequency...")  # noqa: T201
            daily_df = self._forward_fill_monthly_data(pivot_df)
            print(f"Created {len(daily_df)} daily records")  # noqa: T201
            
            # Calculate features
            indicator_cols = [col for col in daily_df.columns if col != 'date']
            
            print("Calculating momentum features...")  # noqa: T201
            df_with_momentum = self._calculate_momentum_features(daily_df, indicator_cols)
            
            print("Calculating volatility features...")  # noqa: T201
            df_with_volatility = self._calculate_volatility_features(df_with_momentum, indicator_cols)
            
            print("Calculating trend features...")  # noqa: T201
            df_with_trends = self._calculate_trend_features(df_with_volatility, indicator_cols)
            
            print("Calculating cross-indicator features...")  # noqa: T201
            df_with_cross = self._calculate_cross_indicator_features(df_with_trends)
            
            print("Applying lookback-only normalization...")  # noqa: T201
            df_normalized = self._normalize_features(df_with_cross)
            
            # Create output table and write data
            print("Creating output table...")  # noqa: T201
            self._create_output_table(df_normalized)
            
            print("Writing transformed data to database...")  # noqa: T201
            self._write_output(df_normalized)
            
            print("Economic indicators transformation completed successfully!")  # noqa: T201
            return df_normalized
            
        finally:
            self.db.close()


if __name__ == "__main__":  # pragma: no cover
    transformer = EconomicIndicatorsTransformer()
    df = transformer.run()
    print(f"Final dataset shape: {df.shape}")  # noqa: T201

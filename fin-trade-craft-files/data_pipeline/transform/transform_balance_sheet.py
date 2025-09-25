#!/usr/bin/env python3
"""Transform Balance Sheet data into analytical features.

Creates table ``transformed.balance_sheet_features`` containing liquidity,
leverage, asset efficiency, equity strength and market linked features along
with growth and risk indicators.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class BalanceSheetTransformer:
    """Create analytical features from balance sheet information."""

    def __init__(self, universe_id: str | None = None) -> None:
        self.db = PostgresDatabaseManager()
        self.universe_id = universe_id
        self.rolling_window = 4  # Quarterly data
        self.epsilon = 1e-6  # For safe division

    # ------------------------------------------------------------------
    # Data fetching helpers
    # ------------------------------------------------------------------
    def _fetch_balance_sheet(self) -> pd.DataFrame:
        if self.universe_id:
            query = """
                SELECT
                    b.symbol_id,
                    b.symbol,
                    b.fiscal_date_ending,
                    b.total_assets,
                    b.total_current_assets,
                    b.cash_and_short_term_investments,
                    b.cash_and_cash_equivalents_at_carrying_value,
                    b.current_net_receivables,
                    b.total_current_liabilities,
                    b.total_liabilities,
                    b.current_debt,
                    b.long_term_debt,
                    b.total_shareholder_equity,
                    b.retained_earnings,
                    b.treasury_stock,
                    b.goodwill,
                    b.intangible_assets,
                    b.property_plant_equipment,
                    b.common_stock_shares_outstanding
                FROM balance_sheet b
                INNER JOIN transformed.symbol_universes su ON b.symbol_id = su.symbol_id
                WHERE su.universe_id = %s
                    AND b.report_type = 'quarterly'
            """
            return self.db.fetch_dataframe(query, (self.universe_id,))
        else:
            query = """
                SELECT
                    b.symbol_id,
                    b.symbol,
                    b.fiscal_date_ending,
                    b.total_assets,
                    b.total_current_assets,
                    b.cash_and_short_term_investments,
                    b.cash_and_cash_equivalents_at_carrying_value,
                    b.current_net_receivables,
                    b.total_current_liabilities,
                    b.total_liabilities,
                    b.current_debt,
                    b.long_term_debt,
                    b.total_shareholder_equity,
                    b.retained_earnings,
                    b.treasury_stock,
                    b.goodwill,
                    b.intangible_assets,
                    b.property_plant_equipment,
                    b.common_stock_shares_outstanding
                FROM balance_sheet b
                WHERE b.report_type = 'quarterly'
            """
            return self.db.fetch_dataframe(query)

    def _fetch_income_statement(self) -> pd.DataFrame:
        if self.universe_id:
            query = """
                SELECT i.symbol, i.fiscal_date_ending, i.ebit, i.total_revenue
                FROM income_statement i
                INNER JOIN transformed.symbol_universes su ON i.symbol_id = su.symbol_id
                WHERE su.universe_id = %s
                    AND i.report_type = 'quarterly'
            """
            return self.db.fetch_dataframe(query, (self.universe_id,))
        else:
            query = """
                SELECT symbol, fiscal_date_ending, ebit, total_revenue
                FROM income_statement
                WHERE report_type = 'quarterly'
            """
            return self.db.fetch_dataframe(query)

    def _fetch_overview(self) -> pd.DataFrame:
        if self.universe_id:
            query = """
                SELECT o.symbol, o.sector, o.industry
                FROM extracted.overview o
                INNER JOIN transformed.symbol_universes su ON o.symbol_id = su.symbol_id
                WHERE su.universe_id = %s
            """
            return self.db.fetch_dataframe(query, (self.universe_id,))
        else:
            query = """
                SELECT symbol, sector, industry
                FROM extracted.overview
            """
            return self.db.fetch_dataframe(query)

    # def _fetch_price_data(self) -> pd.DataFrame:
    #     query = """
    #         SELECT symbol, date, adjusted_close
    #         FROM time_series_daily_adjusted
    #     """
    #     return self.db.fetch_dataframe(query)

    # ------------------------------------------------------------------
    # Normalization and preprocessing helpers
    # ------------------------------------------------------------------
    def _safe_div(self, a: pd.Series, b: pd.Series) -> pd.Series:
        """Safe division with robust handling of edge cases."""
        a_clean = a.fillna(0)
        b_clean = b.fillna(0)

        # Create result with proper null handling
        result = a_clean / (np.abs(b_clean) + self.epsilon)

        # Return null when both inputs are null (meaningful missingness)
        both_null = a.isna() & b.isna()
        result.loc[both_null] = None

        return result

    def _rolling_mean_past(self, df: pd.DataFrame, col: str, window: int) -> pd.Series:
        """Rolling mean using past data only (excluding current row)."""
        return df.groupby('symbol_id')[col].transform(
            lambda x: x.shift(1).rolling(window=window, min_periods=1).mean()
        )

    def _rolling_std_past(self, df: pd.DataFrame, col: str, window: int) -> pd.Series:
        """Rolling std using past data only (excluding current row)."""
        return df.groupby('symbol_id')[col].transform(
            lambda x: x.shift(1).rolling(window=window, min_periods=1).std()
        )

    def _rolling_quantile_past(self, df: pd.DataFrame, col: str, window: int, q: float) -> pd.Series:
        """Rolling quantile using past data only."""
        return df.groupby('symbol_id')[col].transform(
            lambda x: x.shift(1).rolling(window=window, min_periods=1).quantile(q)
        )

    def _winsorize(self, df: pd.DataFrame, col: str, window: int) -> pd.Series:
        """Apply rolling winsorization to reduce outlier impact."""
        q5 = self._rolling_quantile_past(df, col, window, 0.05)
        q95 = self._rolling_quantile_past(df, col, window, 0.95)

        # Fallback bounds for early periods
        q5 = q5.fillna(df[col].quantile(0.05))
        q95 = q95.fillna(df[col].quantile(0.95))

        return np.clip(df[col], q5, q95)

    def _zscore_rolling(self, df: pd.DataFrame, col: str, window: int, min_std_threshold: float = 1e-3) -> pd.Series:
        """Apply rolling z-score normalization with robust standard deviation handling."""
        rolling_mean = self._rolling_mean_past(df, col, window)
        rolling_std = self._rolling_std_past(df, col, window)

        # Handle cases with very low standard deviation
        rolling_std = np.where(
            (rolling_std.isna()) | (rolling_std < min_std_threshold),
            min_std_threshold,
            rolling_std
        )

        # Calculate z-score
        zscore = (df[col] - rolling_mean) / rolling_std

        # Apply final bounds to prevent extreme values
        return np.clip(zscore, -50, 50)

    def _asinh_scaled(self, df: pd.DataFrame, col: str, window: int) -> pd.Series:
        """Apply asinh scaling for monetary amounts."""
        rolling_median_abs = df.groupby('symbol_id')[col].transform(
            lambda x: np.abs(x).shift(1).rolling(window=window, min_periods=1).median()
        )

        # Use rolling median as scale, with fallback
        scale = rolling_median_abs.fillna(1e6)  # Default scale of 1 million
        scale = np.where(scale < 1e3, 1e6, scale)  # Minimum scale of 1 million

        return np.asinh(df[col] / scale)

    def _robust_ratio_preprocessing(self, df: pd.DataFrame, ratio_cols: list) -> pd.DataFrame:
        """Apply robust preprocessing to ratio columns before normalization."""
        df = df.copy()
        for col in ratio_cols:
            if col in df.columns:
                # Replace infinite values
                df[col] = df[col].replace([float('inf'), float('-inf')], np.nan)

                # Apply 99.9th percentile clipping to handle extreme outliers
                if df[col].notna().sum() > 10:  # Only if we have enough data
                    lower_bound = df[col].quantile(0.001)
                    upper_bound = df[col].quantile(0.999)
                    df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)

        return df

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------
    def _compute_base_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Liquidity - using safe division
        df['current_ratio_raw'] = self._safe_div(df['total_current_assets'], df['total_current_liabilities'])
        df['quick_ratio_raw'] = self._safe_div(
            df['cash_and_short_term_investments'] + df['current_net_receivables'],
            df['total_current_liabilities']
        )
        df['cash_ratio_raw'] = self._safe_div(
            df['cash_and_cash_equivalents_at_carrying_value'],
            df['total_current_liabilities']
        )
        df['working_capital'] = df['total_current_assets'] - df['total_current_liabilities']

        # Leverage - using safe division
        df['debt_to_equity_raw'] = self._safe_div(df['total_liabilities'], df['total_shareholder_equity'])
        df['current_debt_ratio_raw'] = self._safe_div(df['current_debt'], df['total_assets'])
        df['long_term_debt_ratio_raw'] = self._safe_div(df['long_term_debt'], df['total_assets'])
        df['debt_to_assets_raw'] = self._safe_div(df['total_liabilities'], df['total_assets'])

        # Asset efficiency - using safe division
        df['tangible_asset_ratio_raw'] = self._safe_div(
            (df['total_assets'] - df['goodwill'].fillna(0) - df['intangible_assets'].fillna(0)),
            df['total_assets']
        )
        df['intangibles_share_raw'] = self._safe_div(
            (df['goodwill'].fillna(0) + df['intangible_assets'].fillna(0)),
            df['total_assets']
        )
        df['ppe_intensity_raw'] = self._safe_div(df['property_plant_equipment'], df['total_assets'])
        df['cash_to_assets_raw'] = self._safe_div(df['cash_and_short_term_investments'], df['total_assets'])

        # Equity strength - using safe division
        df['book_value_per_share_raw'] = self._safe_div(
            df['total_shareholder_equity'],
            df['common_stock_shares_outstanding']
        )
        df['retained_earnings_ratio_raw'] = self._safe_div(
            df['retained_earnings'],
            df['total_shareholder_equity']
        )
        df['treasury_stock_effect_raw'] = self._safe_div(
            df['treasury_stock'].fillna(0),
            df['total_shareholder_equity']
        )
        return df

    def _compute_growth_and_momentum(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute growth metrics using raw ratios."""
        df = df.sort_values(['symbol_id', 'fiscal_date_ending']).reset_index(drop=True)

        # Define base metrics to compute growth for (using raw versions)
        base_metrics = [
            'current_ratio_raw', 'quick_ratio_raw', 'cash_ratio_raw',
            'debt_to_equity_raw', 'current_debt_ratio_raw', 'long_term_debt_ratio_raw',
            'debt_to_assets_raw', 'tangible_asset_ratio_raw', 'intangibles_share_raw',
            'ppe_intensity_raw', 'cash_to_assets_raw', 'book_value_per_share_raw',
            'retained_earnings_ratio_raw', 'treasury_stock_effect_raw'
        ]

        # Also compute growth for working capital (monetary amount)
        df['working_capital_asinh'] = self._asinh_scaled(df, 'working_capital', self.rolling_window)

        for col in base_metrics:
            if col in df.columns:
                # Quarter-over-quarter and year-over-year percentage changes
                df[f'{col}_qoq_pct'] = df.groupby('symbol_id')[col].pct_change(periods=1, fill_method=None)
                df[f'{col}_yoy_pct'] = df.groupby('symbol_id')[col].pct_change(periods=4, fill_method=None)

                # Rolling volatility (standard deviation of past values)
                df[f'{col}_volatility'] = df.groupby('symbol_id')[col].transform(
                    lambda x: x.rolling(window=self.rolling_window, min_periods=2).std()
                )

        # Working capital growth
        df['working_capital_qoq_pct'] = df.groupby('symbol_id')['working_capital'].pct_change(periods=1, fill_method=None)
        df['working_capital_yoy_pct'] = df.groupby('symbol_id')['working_capital'].pct_change(periods=4, fill_method=None)
        df['working_capital_volatility'] = df.groupby('symbol_id')['working_capital'].transform(
            lambda x: x.rolling(window=self.rolling_window, min_periods=2).std()
        )
        return df

    def _compute_rankings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute sector and industry rankings - these are already normalized to [0,1]."""
        base_metrics = [
            'current_ratio_raw', 'quick_ratio_raw', 'cash_ratio_raw', 'working_capital',
            'debt_to_equity_raw', 'current_debt_ratio_raw', 'long_term_debt_ratio_raw',
            'debt_to_assets_raw', 'tangible_asset_ratio_raw', 'intangibles_share_raw',
            'ppe_intensity_raw', 'cash_to_assets_raw', 'book_value_per_share_raw',
            'retained_earnings_ratio_raw', 'treasury_stock_effect_raw',
        ]

        for col in base_metrics:
            if col in df.columns:
                df[f'{col}_sector_rank'] = df.groupby(['fiscal_date_ending', 'sector'])[col].rank(pct=True)
                df[f'{col}_industry_rank'] = df.groupby(['fiscal_date_ending', 'industry'])[col].rank(pct=True)
        return df

    # def _merge_price_features(self, bs_df: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
    #     # Price data integration disabled - balance sheet features only
    #     return bs_df

    def _compute_risk_indicators(self, df: pd.DataFrame, income_df: pd.DataFrame) -> pd.DataFrame:
        """Compute risk indicators using safe division."""
        df = df.merge(
            income_df[['symbol', 'fiscal_date_ending', 'ebit', 'total_revenue']],
            on=['symbol', 'fiscal_date_ending'],
            how='left'
        )

        # Balance sheet-only risk indicators (raw versions)
        df['balance_sheet_leverage_raw'] = self._safe_div(df['total_liabilities'], df['total_assets'])
        df['financial_leverage_raw'] = self._safe_div(df['total_assets'], df['total_shareholder_equity'])
        df['interest_coverage_proxy_raw'] = self._safe_div(
            df['ebit'].fillna(0),
            (df['current_debt'].fillna(0) + df['long_term_debt'].fillna(0))
        )

        # Asset turnover proxy using revenue
        df['asset_turnover_raw'] = self._safe_div(df['total_revenue'].fillna(0), df['total_assets'])

        # Binary indicators (already normalized)
        df['liquidity_shock_flag'] = (df['current_ratio_raw_qoq_pct'] < -0.2).astype(int)

        return df

    def _normalize_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply comprehensive normalization to all raw features."""
        df = df.copy()
        # Ensure consistent sorting for rolling calculations
        df = df.sort_values(['symbol_id', 'fiscal_date_ending']).reset_index(drop=True)

        # Define raw ratio columns that need normalization
        raw_ratio_cols = [
            'current_ratio_raw', 'quick_ratio_raw', 'cash_ratio_raw',
            'debt_to_equity_raw', 'current_debt_ratio_raw', 'long_term_debt_ratio_raw',
            'debt_to_assets_raw', 'tangible_asset_ratio_raw', 'intangibles_share_raw',
            'ppe_intensity_raw', 'cash_to_assets_raw', 'book_value_per_share_raw',
            'retained_earnings_ratio_raw', 'treasury_stock_effect_raw',
            'balance_sheet_leverage_raw', 'financial_leverage_raw', 'interest_coverage_proxy_raw',
            'asset_turnover_raw'
        ]

        # Apply robust preprocessing first
        df = self._robust_ratio_preprocessing(df, raw_ratio_cols)

        # Normalize base ratios
        for col in raw_ratio_cols:
            if col in df.columns:
                col_base = col.replace('_raw', '')
                df[f'{col_base}_winsor'] = self._winsorize(df, col, self.rolling_window)
                df[f'{col_base}'] = self._zscore_rolling(df, f'{col_base}_winsor', self.rolling_window)

        # Normalize working capital (monetary amount)
        df['working_capital_normalized'] = self._zscore_rolling(df, 'working_capital_asinh', self.rolling_window)

        # Normalize growth rates (percentage changes)
        growth_cols = [col for col in df.columns if '_qoq_pct' in col or '_yoy_pct' in col]
        for col in growth_cols:
            if col in df.columns:
                col_base = col.replace('_raw_', '_').replace('_raw', '')
                df[f'{col_base}_winsor'] = self._winsorize(df, col, self.rolling_window)
                df[f'{col_base}_normalized'] = self._zscore_rolling(df, f'{col_base}_winsor', self.rolling_window)

        # Normalize volatility measures
        volatility_cols = [col for col in df.columns if '_volatility' in col]
        for col in volatility_cols:
            if col in df.columns:
                col_base = col.replace('_raw_', '_').replace('_raw', '')
                df[f'{col_base}_winsor'] = self._winsorize(df, col, self.rolling_window)
                df[f'{col_base}_normalized'] = self._zscore_rolling(df, f'{col_base}_winsor', self.rolling_window)

        return df

    # ------------------------------------------------------------------
    def run(self) -> pd.DataFrame:
        self.db.connect()
        try:
            print(f"BalanceSheetTransformer: Processing universe_id = {self.universe_id}")  # noqa: T201
            bs_df = self._fetch_balance_sheet()
            print(f"BalanceSheetTransformer: Fetched {len(bs_df)} balance sheet records")  # noqa: T201
            overview_df = self._fetch_overview()
            income_df = self._fetch_income_statement()
            # price_df = self._fetch_price_data()  # Disabled for balance sheet-only features

            df = bs_df.merge(overview_df, on='symbol', how='left')
            df = self._compute_base_ratios(df)
            df = self._compute_growth_and_momentum(df)
            df = self._compute_rankings(df)
            # df = self._merge_price_features(df, price_df)  # Disabled for balance sheet-only features
            df = self._compute_risk_indicators(df, income_df)
            df = self._normalize_features(df)

            self._write_output(df)
            return df
        finally:
            self.db.close()

    def _write_output(self, df: pd.DataFrame) -> None:
        self.db.execute_query("CREATE SCHEMA IF NOT EXISTS transformed")

        # Get all feature columns (excluding source data and intermediate processing columns)
        identifier_cols = {'symbol_id', 'symbol', 'fiscal_date_ending'}
        source_cols = {
            'total_assets', 'total_current_assets', 'cash_and_short_term_investments',
            'cash_and_cash_equivalents_at_carrying_value', 'current_net_receivables',
            'total_current_liabilities', 'total_liabilities', 'current_debt',
            'long_term_debt', 'total_shareholder_equity', 'retained_earnings',
            'treasury_stock', 'goodwill', 'intangible_assets', 'property_plant_equipment',
            'common_stock_shares_outstanding', 'ebit', 'total_revenue'
        }

        # Exclude intermediate processing features (winsorized, raw ratios, unprocessed amounts)
        intermediate_cols = {
            # Raw ratio features (unbounded, replaced by normalized versions)
            'current_ratio_raw', 'quick_ratio_raw', 'cash_ratio_raw',
            'debt_to_equity_raw', 'current_debt_ratio_raw', 'long_term_debt_ratio_raw',
            'debt_to_assets_raw', 'tangible_asset_ratio_raw', 'intangibles_share_raw',
            'ppe_intensity_raw', 'cash_to_assets_raw', 'book_value_per_share_raw',
            'retained_earnings_ratio_raw', 'treasury_stock_effect_raw',
            'balance_sheet_leverage_raw', 'financial_leverage_raw', 'interest_coverage_proxy_raw',
            'asset_turnover_raw',

            # Winsorized features (intermediate step)
            'current_ratio_winsor', 'quick_ratio_winsor', 'cash_ratio_winsor',
            'debt_to_equity_winsor', 'current_debt_ratio_winsor', 'long_term_debt_ratio_winsor',
            'debt_to_assets_winsor', 'tangible_asset_ratio_winsor', 'intangibles_share_winsor',
            'ppe_intensity_winsor', 'cash_to_assets_winsor', 'book_value_per_share_winsor',
            'retained_earnings_ratio_winsor', 'treasury_stock_effect_winsor',
            'balance_sheet_leverage_winsor', 'financial_leverage_winsor', 'interest_coverage_proxy_winsor',
            'asset_turnover_winsor',

            # Raw monetary amounts (keep only normalized versions)
            'working_capital', 'working_capital_asinh',

            # Raw growth/volatility features (keep only normalized versions)
        }

        # Add all winsorized growth and volatility features to exclusions
        intermediate_cols.update({
            col for col in df.columns
            if ('_qoq_pct' in col or '_yoy_pct' in col or '_volatility' in col)
            and ('_winsor' in col or ('_raw' in col))
        })

        # Metadata columns to exclude for pure model-ready table
        metadata_cols = {'sector', 'industry'}

        # Select only MODEL-READY feature columns (normalized, bounded, standardized)
        all_excluded = identifier_cols | source_cols | intermediate_cols | metadata_cols
        feature_cols = [c for c in df.columns if c not in all_excluded]

        # Add "fbs_" prefix to all balance sheet features to indicate fundamental balance sheet data
        feature_mapping = {}
        prefixed_feature_cols = []
        for col in feature_cols:
            new_col = f"fbs_{col}"
            feature_mapping[col] = new_col
            prefixed_feature_cols.append(new_col)

        # Rename columns in the dataframe
        df = df.rename(columns=feature_mapping)

        # Update feature_cols to use the new prefixed names
        feature_cols = prefixed_feature_cols

        # Log feature selection for transparency
        [c for c in df.columns if c in intermediate_cols]

        # Create table with individual columns (all numeric features only)
        column_definitions = []
        column_definitions.append("symbol_id BIGINT")
        column_definitions.append("symbol VARCHAR(20)")
        column_definitions.append("fiscal_date_ending DATE")

        # Add feature columns (all numeric features only)
        for col in feature_cols:
            if col in ['fbs_liquidity_shock_flag']:
                column_definitions.append(f"{col} INTEGER")
            else:
                column_definitions.append(f"{col} NUMERIC")

        column_definitions.append("created_at TIMESTAMPTZ DEFAULT NOW()")
        column_definitions.append("updated_at TIMESTAMPTZ DEFAULT NOW()")
        column_definitions.append("PRIMARY KEY (symbol_id, fiscal_date_ending)")

        create_sql = f"""
            DROP TABLE IF EXISTS transformed.balance_sheet_features;
            CREATE TABLE transformed.balance_sheet_features (
                {',\n                '.join(column_definitions)}
            )
        """
        self.db.execute_query(create_sql)

        # Prepare data for insertion - select only model-ready columns
        df_clean = df[['symbol_id', 'symbol', 'fiscal_date_ending'] + feature_cols].copy()

        # Filter out records with null fiscal_date_ending
        df_clean = df_clean.dropna(subset=['fiscal_date_ending'])

        for col in feature_cols:
            if df_clean[col].dtype in ['float64', 'int64']:
                # Replace infinite values with None
                df_clean[col] = df_clean[col].replace([float('inf'), float('-inf')], None)

        # Create column list for INSERT statement
        all_columns = ['symbol_id', 'symbol', 'fiscal_date_ending'] + feature_cols
        column_list = ', '.join(all_columns)
        placeholders = ', '.join(['%s'] * len(all_columns))

        # Prepare records for batch insert
        records = []
        for _, row in df_clean.iterrows():
            record = []
            for col in all_columns:
                value = row[col]
                # Convert pandas null types to Python None
                if pd.isna(value):
                    record.append(None)
                else:
                    record.append(value)
            records.append(tuple(record))

        insert_sql = f"""
            INSERT INTO transformed.balance_sheet_features (
                {column_list}
            ) VALUES ({placeholders})
            ON CONFLICT (symbol_id, fiscal_date_ending)
            DO UPDATE SET
                {', '.join([f'{col} = EXCLUDED.{col}' for col in feature_cols])},
                updated_at = NOW()
        """

        self.db.execute_many(insert_sql, records)


if __name__ == "__main__":  # pragma: no cover
    transformer = BalanceSheetTransformer()
    df = transformer.run()

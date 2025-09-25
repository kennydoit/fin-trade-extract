#!/usr/bin/env python3
"""Transform Cash Flow data into analytical features.

Creates table ``transformed.cash_flow_features`` containing cash flow strength,
investment intensity, financing strategy, cash flexibility and stability metrics
with proper normalization and preprocessing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class CashFlowTransformer:
    """Create analytical features from cash flow statement information."""

    def __init__(self, universe_id: str | None = None) -> None:
        self.db = PostgresDatabaseManager()
        self.universe_id = universe_id
        self.epsilon = 1e-6  # Increased from 1e-9 for more robust calculations
        self.min_std_threshold = 1e-3  # Minimum std threshold for z-score calculations
        self.rolling_window = 4  # Quarterly
        self.winsor_bounds = (-5.0, 5.0)
        self.winsor_percentiles = (1.0, 99.0)  # More aggressive winsorization
        self.final_bounds = (-50.0, 50.0)  # Sanity bounds on final normalized values

    # ------------------------------------------------------------------
    # Data fetching helpers
    # ------------------------------------------------------------------
    def _fetch_cash_flow(self) -> pd.DataFrame:
        if self.universe_id:
            query = """
                SELECT
                    cf.symbol_id,
                    cf.symbol,
                    cf.fiscal_date_ending,
                    cf.operating_cashflow,
                    cf.capital_expenditures,
                    cf.cashflow_from_investment,
                    cf.cashflow_from_financing,
                    cf.change_in_cash_and_cash_equivalents,
                    cf.change_in_exchange_rate,
                    cf.proceeds_from_operating_activities,
                    cf.proceeds_from_issuance_of_long_term_debt_and_capital_securities,
                    cf.proceeds_from_issuance_of_common_stock,
                    cf.proceeds_from_issuance_of_preferred_stock,
                    cf.dividend_payout,
                    cf.payments_for_repurchase_of_common_stock
                FROM cash_flow cf
                INNER JOIN transformed.symbol_universes su ON cf.symbol_id = su.symbol_id
                WHERE su.universe_id = %s
                    AND cf.report_type = 'quarterly'
                    AND cf.fiscal_date_ending IS NOT NULL
            """
            return self.db.fetch_dataframe(query, (self.universe_id,))
        else:
            query = """
                SELECT
                    cf.symbol_id,
                    cf.symbol,
                    cf.fiscal_date_ending,
                    cf.operating_cashflow,
                    cf.capital_expenditures,
                    cf.cashflow_from_investment,
                    cf.cashflow_from_financing,
                    cf.change_in_cash_and_cash_equivalents,
                    cf.change_in_exchange_rate,
                    cf.proceeds_from_operating_activities,
                    cf.proceeds_from_issuance_of_long_term_debt_and_capital_securities,
                    cf.proceeds_from_issuance_of_common_stock,
                    cf.proceeds_from_issuance_of_preferred_stock,
                    cf.dividend_payout,
                    cf.payments_for_repurchase_of_common_stock
                FROM cash_flow cf
                WHERE cf.report_type = 'quarterly'
                    AND cf.fiscal_date_ending IS NOT NULL
            """
            return self.db.fetch_dataframe(query)

    def _fetch_income_statement(self) -> pd.DataFrame:
        if self.universe_id:
            query = """
                SELECT i.symbol, i.fiscal_date_ending, i.net_income
                FROM income_statement i
                INNER JOIN transformed.symbol_universes su ON i.symbol_id = su.symbol_id
                WHERE su.universe_id = %s
                    AND i.report_type = 'quarterly'
                    AND i.fiscal_date_ending IS NOT NULL
            """
            return self.db.fetch_dataframe(query, (self.universe_id,))
        else:
            query = """
                SELECT symbol, fiscal_date_ending, net_income
                FROM income_statement
                WHERE report_type = 'quarterly'
                    AND fiscal_date_ending IS NOT NULL
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

    # ------------------------------------------------------------------
    # Utility functions for normalization
    # ------------------------------------------------------------------
    def _safe_div(self, a: pd.Series, b: pd.Series) -> pd.Series:
        """Safe division with epsilon to avoid division by zero."""
        # Handle null values first
        a_clean = a.fillna(0)
        b_clean = b.fillna(0)

        # Create result with proper null handling
        result = a_clean / (np.abs(b_clean) + self.epsilon)

        # Return null when both inputs are null (meaningful missingness)
        both_null = a.isna() & b.isna()
        result.loc[both_null] = None

        return result

    def _clipped(self, x: pd.Series, lower: float, upper: float) -> pd.Series:
        """Clip values to bounds."""
        return np.clip(x, lower, upper)

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

    def _rolling_median_abs_past(self, df: pd.DataFrame, col: str, window: int) -> pd.Series:
        """Rolling median of absolute values using past data only."""
        return df.groupby('symbol_id')[col].transform(
            lambda x: np.abs(x).shift(1).rolling(window=window, min_periods=1).median()
        )

    def _rolling_quantile_past(self, df: pd.DataFrame, col: str, window: int, q: float) -> pd.Series:
        """Rolling quantile using past data only."""
        return df.groupby('symbol_id')[col].transform(
            lambda x: x.shift(1).rolling(window=window, min_periods=1).quantile(q)
        )

    def _asinh_scaled(self, df: pd.DataFrame, col: str, window: int) -> pd.Series:
        """Sign-preserving scale transform using asinh."""
        scale = self._rolling_median_abs_past(df, col, window) + self.epsilon
        return np.arcsinh(df[col] / scale)

    def _zscore_rolling(self, df: pd.DataFrame, col: str, window: int) -> pd.Series:
        """Z-score using rolling statistics from past data with robust std handling."""
        mean_past = self._rolling_mean_past(df, col, window)
        std_past = self._rolling_std_past(df, col, window)
        # Use max of std and min_threshold to prevent explosion from tiny std values
        robust_std = np.maximum(std_past, self.min_std_threshold)
        z_scores = (df[col] - mean_past) / (robust_std + self.epsilon)
        # Apply final sanity bounds to prevent extreme values
        return np.clip(z_scores, self.final_bounds[0], self.final_bounds[1])

    def _winsorize(self, df: pd.DataFrame, col: str, window: int) -> pd.Series:
        """Winsorize using rolling percentiles with fallback to fixed bounds."""
        p_low = self._rolling_quantile_past(df, col, window, self.winsor_percentiles[0] / 100)
        p_high = self._rolling_quantile_past(df, col, window, self.winsor_percentiles[1] / 100)

        # Handle cases where rolling quantiles might be invalid
        # Use fixed bounds as fallback
        p_low_safe = np.where(pd.isna(p_low), self.winsor_bounds[0], p_low)
        p_high_safe = np.where(pd.isna(p_high), self.winsor_bounds[1], p_high)

        return np.clip(df[col], p_low_safe, p_high_safe)

    def _growth_period_over_period(self, df: pd.DataFrame, col: str) -> pd.Series:
        """Period-over-period growth rate."""
        prev_values = df.groupby('symbol_id')[col].shift(1)
        return (df[col] - prev_values) / (np.abs(prev_values) + self.epsilon)

    def _robust_ratio_preprocessing(self, df: pd.DataFrame, ratio_cols: list) -> pd.DataFrame:
        """Apply robust preprocessing to ratio columns before normalization."""
        df = df.copy()
        for col in ratio_cols:
            if col in df.columns:
                # Replace infinite values
                df[col] = df[col].replace([float('inf'), float('-inf')], None)

                # Apply aggressive outlier clipping at raw level (99.9th percentile)
                if df[col].notna().sum() > 0:
                    q001 = df[col].quantile(0.001)
                    q999 = df[col].quantile(0.999)
                    df[col] = np.clip(df[col], q001, q999)

        return df

    def _handle_missing_data_gaps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing data and time series gaps intelligently."""
        df = df.copy()
        df = df.sort_values(['symbol', 'fiscal_date_ending'])

        # Ensure fiscal_date_ending is datetime
        df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])

        # 1. Classify data gaps
        df['days_since_last'] = df.groupby('symbol')['fiscal_date_ending'].diff().dt.days
        df['gap_type'] = 'normal'  # 90-100 days (quarterly)
        df.loc[df['days_since_last'] > 120, 'gap_type'] = 'minor_gap'  # 4+ months
        df.loc[df['days_since_last'] > 200, 'gap_type'] = 'major_gap'  # 6+ months
        df.loc[df['days_since_last'] > 400, 'gap_type'] = 'data_break'  # 1+ year

        # 2. Handle rolling calculations with gap awareness
        def gap_aware_rolling_fill(group, col, window):
            """Forward fill only within reasonable gaps for rolling calculations."""
            result = group[col].copy()

            # Don't fill across major gaps (> 200 days)
            major_gaps = group['days_since_last'] > 200
            if major_gaps.any():
                # Reset sequences at major gaps
                gap_points = major_gaps.shift(1, fill_value=False)
                for i in range(1, len(result)):
                    if gap_points.iloc[i]:
                        # Don't use data before major gaps for rolling stats
                        break

            return result

        # 3. Apply smart missing value strategies by feature type
        feature_strategies = {
            # Raw ratios: Use limited forward fill (max 1 quarter)
            'ratio_features': ['operating_cashflow_margin_raw', 'capex_ratio_raw', 'fcf_to_net_income_raw'],
            # Growth rates: Don't forward fill (gaps break trend continuity)
            'growth_features': ['ocf_growth', 'fcf_growth'],
            # Volatility: Require minimum history, set null for insufficient data
            'volatility_features': ['ocf_volatility', 'fcf_volatility'],
            # Rankings: Require complete peer data for that period
            'ranking_features': ['debt_financing_ratio', 'equity_financing_ratio']
        }

        # Apply limited forward fill for ratios (max 1 period)
        for col in feature_strategies['ratio_features']:
            if col in df.columns:
                df[col] = df.groupby('symbol')[col].transform(
                    lambda x: x.ffill(limit=1)
                )

        # Mark data quality flags
        df['has_minor_gap'] = (df['gap_type'] == 'minor_gap').astype(int)
        df['has_major_gap'] = (df['gap_type'].isin(['major_gap', 'data_break'])).astype(int)
        df['rolling_window_complete'] = df.groupby('symbol').cumcount() >= (self.rolling_window - 1)

        return df

    def _count_positive_rolling(self, df: pd.DataFrame, col: str, window: int) -> pd.Series:
        """Count of positive values in rolling window (past data only)."""
        return df.groupby('symbol_id')[col].transform(
            lambda x: (x.shift(1) > 0).rolling(window=window, min_periods=1).sum()
        )

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------
    def _compute_cash_flow_strength_coverage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Section 1: Cash Flow Strength & Coverage."""
        df = df.copy()

        # Operating Cash Flow Margin
        df['operating_cashflow_margin_raw'] = self._safe_div(df['operating_cashflow'], df['net_income'])

        # Free Cash Flow
        df['free_cash_flow'] = df['operating_cashflow'] - df['capital_expenditures']

        # Free Cash Flow to Net Income
        df['fcf_to_net_income_raw'] = self._safe_div(df['free_cash_flow'], df['net_income'])

        # Cash Conversion Ratio
        df['cash_conversion_ratio_raw'] = self._safe_div(
            df['operating_cashflow'],
            df['proceeds_from_operating_activities']
        )

        return df

    def _compute_investment_intensity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Section 2: Investment Intensity."""
        df = df.copy()

        # Capex Ratio
        df['capex_ratio_raw'] = self._safe_div(df['capital_expenditures'], df['operating_cashflow'])

        # Net Investing Cash Flow to OCF
        df['net_investing_to_ocf_raw'] = self._safe_div(
            df['cashflow_from_investment'],
            df['operating_cashflow']
        )

        return df

    def _compute_financing_strategy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Section 3: Financing Strategy."""
        df = df.copy()

        # Debt Financing Ratio
        df['debt_financing_ratio_raw'] = self._safe_div(
            df['proceeds_from_issuance_of_long_term_debt_and_capital_securities'],
            df['cashflow_from_financing']
        )

        # Equity Financing Ratio
        equity_proceeds = (
            df['proceeds_from_issuance_of_common_stock'].fillna(0) +
            df['proceeds_from_issuance_of_preferred_stock'].fillna(0)
        )
        df['equity_financing_ratio_raw'] = self._safe_div(
            equity_proceeds,
            df['cashflow_from_financing']
        )

        # Shareholder Return Ratio
        shareholder_returns = (
            df['dividend_payout'].fillna(0) +
            df['payments_for_repurchase_of_common_stock'].fillna(0)
        )
        df['shareholder_return_ratio_raw'] = self._safe_div(
            shareholder_returns,
            df['cashflow_from_financing']
        )

        return df

    def _compute_cash_flexibility_changes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Section 4: Cash Flexibility & Changes."""
        df = df.copy()

        # Net Cash Flow
        df['net_cash_flow'] = (
            df['operating_cashflow'] +
            df['cashflow_from_investment'] +
            df['cashflow_from_financing']
        )

        # Change in Cash to OCF
        df['change_in_cash_to_ocf_raw'] = self._safe_div(
            df['change_in_cash_and_cash_equivalents'],
            df['operating_cashflow']
        )

        # FX Impact Ratio
        df['fx_impact_ratio_raw'] = self._safe_div(
            df['change_in_exchange_rate'],
            df['change_in_cash_and_cash_equivalents']
        )

        return df

    def _compute_stability_trends(self, df: pd.DataFrame) -> pd.DataFrame:
        """Section 5: Stability & Trends."""
        df = df.copy()

        # Ensure proper sorting by symbol_id and date for time series operations
        df = df.sort_values(['symbol_id', 'fiscal_date_ending']).reset_index(drop=True)

        # Cash Flow Volatility (Operating Cash Flow)
        df['ocf_volatility'] = self._rolling_std_past(df, 'operating_cashflow', self.rolling_window)

        # Free Cash Flow Volatility
        df['fcf_volatility'] = self._rolling_std_past(df, 'free_cash_flow', self.rolling_window)

        # Cash Flow Growth (Operating Cash Flow)
        df['ocf_growth'] = self._growth_period_over_period(df, 'operating_cashflow')

        # Free Cash Flow Growth
        df['fcf_growth'] = self._growth_period_over_period(df, 'free_cash_flow')

        # Sustainability Ratio
        ocf_positive_count = self._count_positive_rolling(df, 'operating_cashflow', self.rolling_window)
        df['sustainability_ratio'] = ocf_positive_count / self.rolling_window

        return df

    def _normalize_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Section 2: Normalization & Preprocessing."""
        df = df.copy()
        # Ensure consistent sorting for rolling calculations
        df = df.sort_values(['symbol_id', 'fiscal_date_ending']).reset_index(drop=True)

        # Apply robust preprocessing to raw ratio columns first
        raw_ratio_cols = [
            'operating_cashflow_margin_raw', 'fcf_to_net_income_raw', 'cash_conversion_ratio_raw',
            'capex_ratio_raw', 'net_investing_to_ocf_raw', 'debt_financing_ratio_raw',
            'equity_financing_ratio_raw', 'shareholder_return_ratio_raw', 'change_in_cash_to_ocf_raw',
            'fx_impact_ratio_raw'
        ]
        df = self._robust_ratio_preprocessing(df, raw_ratio_cols)

        # Cash Flow Strength & Coverage - normalized versions
        df['operating_cashflow_margin_winsor'] = self._winsorize(df, 'operating_cashflow_margin_raw', self.rolling_window)
        df['operating_cashflow_margin'] = self._zscore_rolling(df, 'operating_cashflow_margin_winsor', self.rolling_window)

        df['free_cash_flow_asinh'] = self._asinh_scaled(df, 'free_cash_flow', self.rolling_window)
        df['free_cash_flow_normalized'] = self._zscore_rolling(df, 'free_cash_flow_asinh', self.rolling_window)

        df['fcf_to_net_income_winsor'] = self._winsorize(df, 'fcf_to_net_income_raw', self.rolling_window)
        df['fcf_to_net_income'] = self._zscore_rolling(df, 'fcf_to_net_income_winsor', self.rolling_window)

        df['cash_conversion_ratio_winsor'] = self._winsorize(df, 'cash_conversion_ratio_raw', self.rolling_window)
        df['cash_conversion_ratio'] = self._zscore_rolling(df, 'cash_conversion_ratio_winsor', self.rolling_window)

        # Investment Intensity - normalized versions
        df['capex_ratio_winsor'] = self._winsorize(df, 'capex_ratio_raw', self.rolling_window)
        df['capex_ratio'] = self._zscore_rolling(df, 'capex_ratio_winsor', self.rolling_window)

        df['net_investing_to_ocf_winsor'] = self._winsorize(df, 'net_investing_to_ocf_raw', self.rolling_window)
        df['net_investing_to_ocf'] = self._zscore_rolling(df, 'net_investing_to_ocf_winsor', self.rolling_window)

        # Financing Strategy - normalized versions
        df['debt_financing_ratio_winsor'] = self._winsorize(df, 'debt_financing_ratio_raw', self.rolling_window)
        df['debt_financing_ratio'] = self._zscore_rolling(df, 'debt_financing_ratio_winsor', self.rolling_window)

        df['equity_financing_ratio_winsor'] = self._winsorize(df, 'equity_financing_ratio_raw', self.rolling_window)
        df['equity_financing_ratio'] = self._zscore_rolling(df, 'equity_financing_ratio_winsor', self.rolling_window)

        df['shareholder_return_ratio_winsor'] = self._winsorize(df, 'shareholder_return_ratio_raw', self.rolling_window)
        df['shareholder_return_ratio'] = self._zscore_rolling(df, 'shareholder_return_ratio_winsor', self.rolling_window)

        # Cash Flexibility & Changes - normalized versions
        df['net_cash_flow_asinh'] = self._asinh_scaled(df, 'net_cash_flow', self.rolling_window)
        df['net_cash_flow_normalized'] = self._zscore_rolling(df, 'net_cash_flow_asinh', self.rolling_window)

        df['change_in_cash_to_ocf_winsor'] = self._winsorize(df, 'change_in_cash_to_ocf_raw', self.rolling_window)
        df['change_in_cash_to_ocf'] = self._zscore_rolling(df, 'change_in_cash_to_ocf_winsor', self.rolling_window)

        df['fx_impact_ratio_winsor'] = self._winsorize(df, 'fx_impact_ratio_raw', self.rolling_window)
        df['fx_impact_ratio'] = self._zscore_rolling(df, 'fx_impact_ratio_winsor', self.rolling_window)

        # Stability & Trends - normalized versions
        df['ocf_volatility_normalized'] = self._zscore_rolling(df, 'ocf_volatility', self.rolling_window)
        df['fcf_volatility_normalized'] = self._zscore_rolling(df, 'fcf_volatility', self.rolling_window)

        df['ocf_growth_winsor'] = self._winsorize(df, 'ocf_growth', self.rolling_window)
        df['ocf_growth_normalized'] = self._zscore_rolling(df, 'ocf_growth_winsor', self.rolling_window)

        df['fcf_growth_winsor'] = self._winsorize(df, 'fcf_growth', self.rolling_window)
        df['fcf_growth_normalized'] = self._zscore_rolling(df, 'fcf_growth_winsor', self.rolling_window)

        # Sustainability ratio is already in [0,1] range, optionally normalize
        df['sustainability_ratio_normalized'] = self._zscore_rolling(df, 'sustainability_ratio', self.rolling_window)

        return df

    # ------------------------------------------------------------------
    def run(self) -> pd.DataFrame:
        self.db.connect()
        try:
            print(f"CashFlowTransformer: Processing universe_id = {self.universe_id}")  # noqa: T201
            cf_df = self._fetch_cash_flow()
            print(f"CashFlowTransformer: Fetched {len(cf_df)} cash flow records")  # noqa: T201
            income_df = self._fetch_income_statement()
            overview_df = self._fetch_overview()

            # Merge data
            df = cf_df.merge(
                income_df[['symbol', 'fiscal_date_ending', 'net_income']],
                on=['symbol', 'fiscal_date_ending'],
                how='left'
            )
            df = df.merge(overview_df, on='symbol', how='left')

            # Sort by symbol_id and fiscal_date_ending for consistent time series ordering
            df = df.sort_values(['symbol_id', 'fiscal_date_ending']).reset_index(drop=True)

            # TODO: Re-enable missing data handling after testing
            # df = self._handle_missing_data_gaps(df)

            # Compute features
            df = self._compute_cash_flow_strength_coverage(df)
            df = self._compute_investment_intensity(df)
            df = self._compute_financing_strategy(df)
            df = self._compute_cash_flexibility_changes(df)
            df = self._compute_stability_trends(df)
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
            'operating_cashflow', 'capital_expenditures', 'cashflow_from_investment',
            'cashflow_from_financing', 'change_in_cash_and_cash_equivalents',
            'change_in_exchange_rate', 'proceeds_from_operating_activities',
            'proceeds_from_issuance_of_long_term_debt_and_capital_securities',
            'proceeds_from_issuance_of_common_stock', 'proceeds_from_issuance_of_preferred_stock',
            'dividend_payout', 'payments_for_repurchase_of_common_stock', 'net_income'
        }

        # Exclude intermediate processing features (winsorized, asinh, raw ratios)
        intermediate_cols = {
            # Winsorized features (intermediate step)
            'operating_cashflow_margin_winsor', 'fcf_to_net_income_winsor', 'cash_conversion_ratio_winsor',
            'capex_ratio_winsor', 'net_investing_to_ocf_winsor', 'debt_financing_ratio_winsor',
            'equity_financing_ratio_winsor', 'shareholder_return_ratio_winsor', 'change_in_cash_to_ocf_winsor',
            'fx_impact_ratio_winsor', 'ocf_growth_winsor', 'fcf_growth_winsor',

            # Asinh transformed features (intermediate step)
            'free_cash_flow_asinh', 'net_cash_flow_asinh',

            # Raw ratio features (unbounded, replaced by normalized versions)
            'operating_cashflow_margin_raw', 'fcf_to_net_income_raw', 'cash_conversion_ratio_raw',
            'capex_ratio_raw', 'net_investing_to_ocf_raw', 'debt_financing_ratio_raw',
            'equity_financing_ratio_raw', 'shareholder_return_ratio_raw', 'change_in_cash_to_ocf_raw',
            'fx_impact_ratio_raw',

            # Raw volatility/growth (keep only normalized versions)
            'ocf_volatility', 'fcf_volatility', 'ocf_growth', 'fcf_growth',

            # Raw cash flow amounts (keep only normalized versions)
            'free_cash_flow', 'net_cash_flow',

            # Metadata columns (remove for pure model-ready table)
            'sector', 'industry'
        }

        # Select only MODEL-READY feature columns (normalized, bounded, standardized)
        all_excluded = identifier_cols | source_cols | intermediate_cols
        feature_cols = [c for c in df.columns if c not in all_excluded]

        # Add "fcf_" prefix to all cash flow features to indicate fundamental cash flow data
        # Avoid duplicating prefix for features that already contain fcf/ocf
        feature_mapping = {}
        prefixed_feature_cols = []
        for col in feature_cols:
            if col.startswith('fcf_'):
                # Already has fcf_ prefix, keep as is
                new_col = col
            elif 'fcf_' in col or 'ocf_' in col:
                # Contains fcf or ocf, just add fcf prefix without duplication
                if col.startswith('fcf_'):
                    new_col = col
                elif col.startswith('ocf_'):
                    new_col = f"fcf_{col}"
                else:
                    # Handle cases like 'fcf_to_net_income' -> 'fcf_to_net_income'
                    new_col = f"fcf_{col.replace('fcf_', '').replace('ocf_', '')}"
            else:
                # Standard case: add fcf_ prefix
                new_col = f"fcf_{col}"
            feature_mapping[col] = new_col
            prefixed_feature_cols.append(new_col)

        # Rename columns in the dataframe
        df = df.rename(columns=feature_mapping)

        # Update feature_cols to use the new prefixed names
        feature_cols = prefixed_feature_cols

        # Log feature selection for transparency
        [c for c in df.columns if c in intermediate_cols]

        # Create table with individual columns
        column_definitions = []
        column_definitions.append("symbol_id BIGINT")
        column_definitions.append("symbol VARCHAR(20)")
        column_definitions.append("fiscal_date_ending DATE")

        # Add feature columns (all numeric features only)
        for col in feature_cols:
            column_definitions.append(f"{col} NUMERIC")

        column_definitions.append("created_at TIMESTAMPTZ DEFAULT NOW()")
        column_definitions.append("updated_at TIMESTAMPTZ DEFAULT NOW()")
        column_definitions.append("PRIMARY KEY (symbol_id, fiscal_date_ending)")

        create_sql = f"""
            DROP TABLE IF EXISTS transformed.cash_flow_features;
            CREATE TABLE transformed.cash_flow_features (
                {',\n                '.join(column_definitions)}
            )
        """
        self.db.execute_query(create_sql)

        # Prepare data for insertion - select only model-ready columns
        df_clean = df[['symbol_id', 'symbol', 'fiscal_date_ending'] + feature_cols].copy()
        for col in feature_cols:
            if df_clean[col].dtype in ['float64', 'int64']:
                # Replace infinite values with None
                df_clean[col] = df_clean[col].replace([float('inf'), float('-inf')], None)

        # Create records for batch insert
        all_columns = ['symbol_id', 'symbol', 'fiscal_date_ending'] + feature_cols
        column_list = ', '.join(all_columns)
        placeholders = ', '.join(['%s'] * len(all_columns))

        records = []
        for _, row in df_clean.iterrows():
            record = []
            for col in all_columns:
                value = row[col]
                if pd.isna(value):
                    record.append(None)
                else:
                    record.append(value)
            records.append(tuple(record))

        insert_sql = f"""
            INSERT INTO transformed.cash_flow_features (
                {column_list}
            ) VALUES ({placeholders})
            ON CONFLICT (symbol_id, fiscal_date_ending)
            DO UPDATE SET
                {', '.join([f'{col} = EXCLUDED.{col}' for col in feature_cols])},
                updated_at = NOW()
        """

        self.db.execute_many(insert_sql, records)


if __name__ == "__main__":  # pragma: no cover
    transformer = CashFlowTransformer()
    df = transformer.run()

"""
Income Statement Feature Transformer

This module transforms raw income statement data into model-ready features with comprehensive
normalization and standardization. All features are prefixed with "fis_" (fundamental income statement)
to indicate their source and prevent naming conflicts.

Author: Financial Data Pipeline
Created: 2025-08-25
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from db.postgres_database_manager import PostgresDatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IncomeStatementTransformer:
    """
    Transforms raw income statement data into normalized, model-ready features.

    Features include:
    - Core profitability metrics (margins)
    - Expense control ratios
    - Leverage and interest coverage
    - Tax efficiency metrics
    - Growth and volatility measures
    - Cash proxy features

    All features are comprehensively normalized and prefixed with "fis_".
    """

    def __init__(self, universe_id: str | None = None):
        self.db = PostgresDatabaseManager()
        self.universe_id = universe_id

    def safe_divide(self, a: pd.Series, b: pd.Series, fillvalue: float = 0.0) -> pd.Series:
        """Safely divide two series, handling division by zero."""
        # Convert to numeric and fill NaN with 0
        a_clean = pd.to_numeric(a, errors='coerce').fillna(0)
        b_clean = pd.to_numeric(b, errors='coerce').fillna(0)

        # Perform safe division
        result = np.where(b_clean != 0, a_clean / b_clean, fillvalue)
        return pd.Series(result, index=a.index)

    def _fetch_income_statement(self) -> pd.DataFrame:
        """Fetch quarterly income statement data with overview information."""
        if self.universe_id:
            query = """
                SELECT
                    ism.symbol_id,
                    ism.symbol,
                    ism.fiscal_date_ending,
                    ism.total_revenue,
                    ism.gross_profit,
                    ism.operating_income,
                    ism.net_income,
                    ism.ebit,
                    ism.ebitda,
                    ism.selling_general_and_administrative,
                    ism.research_and_development,
                    ism.operating_expenses,
                    ism.interest_expense,
                    ism.interest_and_debt_expense,
                    ism.net_interest_income,
                    ism.income_tax_expense,
                    ism.income_before_tax,
                    ism.net_income_from_continuing_operations,
                    ism.comprehensive_income_net_of_tax,
                    ism.depreciation_and_amortization,
                    ism.other_non_operating_income,
                    ov.sector,
                    ov.industry
                FROM extracted.income_statement ism
                LEFT JOIN extracted.overview ov ON ism.symbol = ov.symbol
                INNER JOIN transformed.symbol_universes su ON ism.symbol_id = su.symbol_id
                WHERE su.universe_id = %s
                    AND ism.report_type = 'quarterly'
                    AND ism.fiscal_date_ending IS NOT NULL
                    AND ism.total_revenue IS NOT NULL
                ORDER BY ism.symbol, ism.fiscal_date_ending
            """
            return self.db.fetch_dataframe(query, (self.universe_id,))
        else:
            query = """
                SELECT
                    ism.symbol_id,
                    ism.symbol,
                    ism.fiscal_date_ending,
                    ism.total_revenue,
                    ism.gross_profit,
                    ism.operating_income,
                    ism.net_income,
                    ism.ebit,
                    ism.ebitda,
                    ism.selling_general_and_administrative,
                    ism.research_and_development,
                    ism.operating_expenses,
                    ism.interest_expense,
                    ism.interest_and_debt_expense,
                    ism.net_interest_income,
                    ism.income_tax_expense,
                    ism.income_before_tax,
                    ism.net_income_from_continuing_operations,
                    ism.comprehensive_income_net_of_tax,
                    ism.depreciation_and_amortization,
                    ism.other_non_operating_income,
                    ov.sector,
                    ov.industry
                FROM extracted.income_statement ism
                LEFT JOIN extracted.overview ov ON ism.symbol = ov.symbol
                WHERE ism.report_type = 'quarterly'
                    AND ism.fiscal_date_ending IS NOT NULL
                    AND ism.total_revenue IS NOT NULL
                ORDER BY ism.symbol, ism.fiscal_date_ending
            """
            return self.db.fetch_dataframe(query)

    def _create_core_profitability_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create core profitability margin features."""
        logger.info("Creating core profitability features...")

        # Core Profitability Features
        df['gross_margin'] = self.safe_divide(df['gross_profit'], df['total_revenue'])
        df['operating_margin'] = self.safe_divide(df['operating_income'], df['total_revenue'])
        df['net_margin'] = self.safe_divide(df['net_income'], df['total_revenue'])
        df['ebit_margin'] = self.safe_divide(df['ebit'], df['total_revenue'])
        df['ebitda_margin'] = self.safe_divide(df['ebitda'], df['total_revenue'])

        return df

    def _create_expense_control_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create expense control ratio features."""
        logger.info("Creating expense control features...")

        # Expense Control Features
        df['sga_ratio'] = self.safe_divide(df['selling_general_and_administrative'], df['total_revenue'])
        df['r_and_d_ratio'] = self.safe_divide(df['research_and_development'], df['total_revenue'])
        df['operating_expense_ratio'] = self.safe_divide(df['operating_expenses'], df['total_revenue'])

        return df

    def _create_leverage_interest_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create leverage and interest coverage features."""
        logger.info("Creating leverage and interest coverage features...")

        # Leverage & Interest Features
        df['interest_coverage'] = self.safe_divide(df['ebit'], df['interest_expense'])
        df['debt_burden_ratio'] = self.safe_divide(df['interest_and_debt_expense'], df['total_revenue'])
        df['net_interest_ratio'] = self.safe_divide(df['net_interest_income'], df['total_revenue'])

        return df

    def _create_tax_income_quality_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create tax efficiency and income quality features."""
        logger.info("Creating tax and income quality features...")

        # Tax & Income Quality Features
        df['effective_tax_rate'] = self.safe_divide(df['income_tax_expense'], df['income_before_tax'])
        df['continuing_ops_ratio'] = self.safe_divide(df['net_income_from_continuing_operations'], df['net_income'])
        df['comprehensive_income_ratio'] = self.safe_divide(df['comprehensive_income_net_of_tax'], df['net_income'])

        return df

    def _create_growth_volatility_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create growth and volatility features using time series analysis."""
        logger.info("Creating growth and volatility features...")

        # Sort by symbol and date for proper time series operations
        df = df.sort_values(['symbol', 'fiscal_date_ending'])

        # Growth Features (Quarter-over-Quarter)
        df['revenue_growth_qoq'] = df.groupby('symbol')['total_revenue'].pct_change()
        df['operating_income_growth_qoq'] = df.groupby('symbol')['operating_income'].pct_change()
        df['net_income_growth_qoq'] = df.groupby('symbol')['net_income'].pct_change()

        # Volatility Features (4-quarter rolling)
        def rolling_cv(series):
            """Calculate coefficient of variation (std/mean) for rolling window."""
            mean_val = series.mean()
            if pd.isna(mean_val) or mean_val == 0:
                return 0
            return series.std() / abs(mean_val)

        df['earnings_volatility_4q'] = df.groupby('symbol')['net_income'].rolling(
            window=4, min_periods=2
        ).apply(rolling_cv, raw=False).reset_index(0, drop=True)

        return df

    def _create_cash_proxy_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create cash flow proxy features from income statement."""
        logger.info("Creating cash proxy features...")

        # Cash Proxy Features (from IS itself)
        df['depreciation_ratio'] = self.safe_divide(df['depreciation_and_amortization'], df['total_revenue'])
        df['non_operating_income_ratio'] = self.safe_divide(df['other_non_operating_income'], df['total_revenue'])

        return df

    def _normalize_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply comprehensive normalization to all features."""
        logger.info("Applying comprehensive normalization...")

        # Identify feature columns (exclude identifiers and source columns)
        identifier_cols = {'symbol_id', 'symbol', 'fiscal_date_ending'}
        source_cols = {
            'total_revenue', 'gross_profit', 'operating_income', 'net_income', 'ebit', 'ebitda',
            'selling_general_and_administrative', 'research_and_development', 'operating_expenses',
            'interest_expense', 'interest_and_debt_expense', 'net_interest_income',
            'income_tax_expense', 'income_before_tax', 'net_income_from_continuing_operations',
            'comprehensive_income_net_of_tax', 'depreciation_and_amortization', 'other_non_operating_income',
            'sector', 'industry'
        }

        feature_cols = [c for c in df.columns if c not in identifier_cols and c not in source_cols]

        logger.info(f"Normalizing {len(feature_cols)} feature columns...")

        for col in feature_cols:
            if col in df.columns:
                # Step 1: Winsorization (handle outliers)
                df[f'{col}_winsor'] = df.groupby('symbol')[col].transform(
                    lambda x: x.clip(lower=x.quantile(0.05), upper=x.quantile(0.95))
                )

                # Step 2: Rolling Z-Score Normalization (cross-sectional, time-aware)
                # Use 12-quarter rolling window for cross-sectional standardization
                def rolling_zscore(group):
                    rolling_mean = group.rolling(window=12, min_periods=3).mean()
                    rolling_std = group.rolling(window=12, min_periods=3).std()
                    return (group - rolling_mean) / (rolling_std + 1e-8)

                df[f'{col}_zscore'] = df.groupby('symbol')[f'{col}_winsor'].transform(rolling_zscore)

                # Step 3: Asinh transformation for final scaling
                df[f'{col}_normalized'] = np.arcsinh(df[f'{col}_zscore'])

                # Step 4: Cross-sectional ranking (0-1 scale)
                df[f'{col}_rank'] = df.groupby('fiscal_date_ending')[f'{col}_normalized'].rank(pct=True)

        return df

    def _create_table(self, feature_cols: list) -> None:
        """Create the income statement features table with proper schema."""
        logger.info("Creating income statement features table...")

        # Ensure schema exists
        self.db.execute_query("CREATE SCHEMA IF NOT EXISTS transformed")

        column_definitions = [
            "symbol_id BIGINT NOT NULL",
            "symbol VARCHAR(10) NOT NULL",
            "fiscal_date_ending DATE NOT NULL"
        ]

        # Add feature columns
        for col in feature_cols:
            column_definitions.append(f"{col} NUMERIC")

        # Add metadata columns
        column_definitions.append("created_at TIMESTAMPTZ DEFAULT NOW()")
        column_definitions.append("updated_at TIMESTAMPTZ DEFAULT NOW()")
        column_definitions.append("PRIMARY KEY (symbol_id, fiscal_date_ending)")

        create_sql = f"""
            DROP TABLE IF EXISTS transformed.income_statement_features;
            CREATE TABLE transformed.income_statement_features (
                {',\n                '.join(column_definitions)}
            )
        """
        self.db.execute_query(create_sql)

    def _write_output(self, df: pd.DataFrame) -> None:
        """Write the transformed features to the database."""
        logger.info("Writing income statement features to database...")

        # Identify columns to exclude from model-ready output
        identifier_cols = {'symbol_id', 'symbol', 'fiscal_date_ending'}
        source_cols = {
            'total_revenue', 'gross_profit', 'operating_income', 'net_income', 'ebit', 'ebitda',
            'selling_general_and_administrative', 'research_and_development', 'operating_expenses',
            'interest_expense', 'interest_and_debt_expense', 'net_interest_income',
            'income_tax_expense', 'income_before_tax', 'net_income_from_continuing_operations',
            'comprehensive_income_net_of_tax', 'depreciation_and_amortization', 'other_non_operating_income'
        }

        # Intermediate calculation columns to exclude
        intermediate_cols = {col for col in df.columns if any(
            col.endswith(suffix) for suffix in ['_winsor', '_zscore', '_raw']
        )}
        intermediate_cols.update({'sector', 'industry'})

        # Select only MODEL-READY feature columns (normalized, bounded, standardized)
        all_excluded = identifier_cols | source_cols | intermediate_cols
        feature_cols = [c for c in df.columns if c not in all_excluded]

        # Add "fis_" prefix to all income statement features to indicate fundamental income statement data
        # Avoid duplicating prefix for features that already contain fis
        feature_mapping = {}
        prefixed_feature_cols = []
        for col in feature_cols:
            if col.startswith('fis_'):
                # Already has fis_ prefix, keep as is
                new_col = col
            elif 'fis_' in col:
                # Contains fis, just add fis prefix without duplication
                new_col = f"fis_{col.replace('fis_', '')}"
            else:
                # Standard case: add fis_ prefix
                new_col = f"fis_{col}"
            feature_mapping[col] = new_col
            prefixed_feature_cols.append(new_col)

        # Rename columns with prefixes
        df = df.rename(columns=feature_mapping)
        feature_cols = prefixed_feature_cols


        # Create table with proper schema
        self._create_table(feature_cols)

        # Prepare data for insertion
        output_cols = ['symbol_id', 'symbol', 'fiscal_date_ending'] + feature_cols
        output_df = df[output_cols].copy()

        # Remove rows with all NaN feature values
        feature_mask = output_df[feature_cols].notna().any(axis=1)
        output_df = output_df[feature_mask]

        # Convert to records for bulk insert
        records = []
        column_list = ', '.join(output_cols)
        placeholders = ', '.join(['%s'] * len(output_cols))

        for _, row in output_df.iterrows():
            record = []
            for col in output_cols:
                value = row[col]
                if pd.isna(value):
                    record.append(None)
                else:
                    record.append(float(value) if isinstance(value, int | float) else value)
            records.append(tuple(record))

        insert_sql = f"""
            INSERT INTO transformed.income_statement_features (
                {column_list}
            ) VALUES ({placeholders})
            ON CONFLICT (symbol_id, fiscal_date_ending)
            DO UPDATE SET
                {', '.join([f'{col} = EXCLUDED.{col}' for col in feature_cols])},
                updated_at = NOW()
        """

        self.db.execute_many(insert_sql, records)

    def run(self) -> pd.DataFrame:
        """Execute the complete income statement transformation pipeline."""
        logger.info("Starting income statement transformation...")
        print(f"IncomeStatementTransformer: Processing universe_id = {self.universe_id}")  # noqa: T201

        try:
            # Connect to database
            self.db.connect()

            # Fetch data
            df = self._fetch_income_statement()
            print(f"IncomeStatementTransformer: Fetched {len(df)} income statement records")  # noqa: T201
            logger.info(f"Fetched {len(df)} income statement records")

            if df.empty:
                logger.warning("No income statement data found")
                return df

            # Create features
            df = self._create_core_profitability_features(df)
            df = self._create_expense_control_features(df)
            df = self._create_leverage_interest_features(df)
            df = self._create_tax_income_quality_features(df)
            df = self._create_growth_volatility_features(df)
            df = self._create_cash_proxy_features(df)

            # Apply normalization
            df = self._normalize_features(df)

            # Write to database
            self._write_output(df)

            logger.info("Income statement transformation completed successfully")
            return df

        except Exception as e:
            logger.error(f"Income statement transformation failed: {e}")
            raise
        finally:
            self.db.close()


if __name__ == "__main__":  # pragma: no cover
    transformer = IncomeStatementTransformer()
    df = transformer.run()
    feature_cols = [c for c in df.columns if c.startswith('fis_')]

"""
Cash Flow Extractor using incremental ETL architecture.
Uses source schema, watermarks, and deterministic processing.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db and utils
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.adaptive_rate_limiter import AdaptiveRateLimiter, ExtractorType
from utils.incremental_etl import (
    ContentHasher,
    DateUtils,
    RunIdGenerator,
    WatermarkManager,
)

# API configuration
STOCK_API_FUNCTION = "CASH_FLOW"

# Schema-driven field mapping configuration
CASH_FLOW_FIELDS = {
    # Operating Activities
    'operating_cashflow': 'operatingCashflow',
    'payments_for_operating_activities': 'paymentsForOperatingActivities',
    'proceeds_from_operating_activities': 'proceedsFromOperatingActivities',
    'change_in_operating_liabilities': 'changeInOperatingLiabilities',
    'change_in_operating_assets': 'changeInOperatingAssets',
    'depreciation_depletion_and_amortization': 'depreciationDepletionAndAmortization',
    'change_in_receivables': 'changeInReceivables',
    'change_in_inventory': 'changeInInventory',
    'profit_loss': 'profitLoss',

    # Investing Activities
    'cashflow_from_investment': 'cashflowFromInvestment',
    'capital_expenditures': 'capitalExpenditures',

    # Financing Activities
    'cashflow_from_financing': 'cashflowFromFinancing',
    'proceeds_from_repayments_of_short_term_debt': 'proceedsFromRepaymentsOfShortTermDebt',
    'payments_for_repurchase_of_common_stock': 'paymentsForRepurchaseOfCommonStock',
    'payments_for_repurchase_of_equity': 'paymentsForRepurchaseOfEquity',
    'payments_for_repurchase_of_preferred_stock': 'paymentsForRepurchaseOfPreferredStock',
    'dividend_payout': 'dividendPayout',
    'dividend_payout_common_stock': 'dividendPayoutCommonStock',
    'dividend_payout_preferred_stock': 'dividendPayoutPreferredStock',
    'proceeds_from_issuance_of_common_stock': 'proceedsFromIssuanceOfCommonStock',
    'proceeds_from_issuance_of_long_term_debt_and_capital_securities_net': 'proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet',
    'proceeds_from_issuance_of_preferred_stock': 'proceedsFromIssuanceOfPreferredStock',
    'proceeds_from_repurchase_of_equity': 'proceedsFromRepurchaseOfEquity',
    'proceeds_from_sale_of_treasury_stock': 'proceedsFromSaleOfTreasuryStock',

    # Summary
    'change_in_cash_and_cash_equivalents': 'changeInCashAndCashEquivalents',
}


class CashFlowExtractor:
    """Cash flow extractor with adaptive rate limiting and incremental processing."""

    def __init__(self):
        """Initialize the extractor."""
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        self.table_name = "cash_flow"
        self.schema_name = "source"
        self.db_manager = None
        self.watermark_manager = None

        # Initialize adaptive rate limiter for fundamentals (light processing)
        self.rate_limiter = AdaptiveRateLimiter(ExtractorType.FUNDAMENTALS, verbose=True)

    def _get_db_manager(self):
        """Get database manager with context management."""
        if not self.db_manager:
            self.db_manager = PostgresDatabaseManager()
        return self.db_manager

    def _initialize_watermark_manager(self, db):
        """Initialize watermark manager with database connection."""
        if not self.watermark_manager:
            self.watermark_manager = WatermarkManager(db)
        return self.watermark_manager

    def _ensure_schema_exists(self, db):
        """Ensure the source schema and tables exist."""
        try:
            # Read and execute the source schema
            schema_file = Path(__file__).parent.parent.parent / "db" / "schema" / "source_schema.sql"
            if schema_file.exists():
                with schema_file.open() as f:
                    schema_sql = f.read()
                db.execute_script(schema_sql)
                print("✅ Source schema initialized")
            else:
                print(f"⚠️ Schema file not found: {schema_file}")
        except Exception as e:
            print(f"⚠️ Schema initialization error: {e}")

    def _fetch_api_data(self, symbol: str) -> tuple[dict[str, Any], str]:
        """
        Fetch cash flow data from Alpha Vantage API.

        Args:
            symbol: Stock symbol to fetch

        Returns:
            Tuple of (api_response, status)
        """
        url = "https://www.alphavantage.co/query"
        params = {
            "function": STOCK_API_FUNCTION,
            "symbol": symbol,
            "apikey": self.api_key
        }

        # Adaptive rate limiting - smart delay based on elapsed time and processing overhead
        self.rate_limiter.pre_api_call()

        try:
            print(f"Fetching data from: {url}?function={STOCK_API_FUNCTION}&symbol={symbol}&apikey={self.api_key}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Check for API errors
            if "Error Message" in data:
                return data, "error"
            if "Note" in data:
                return data, "rate_limited"
            if not data or len(data) == 0:
                return data, "empty"
            return data, "success"

        except Exception as e:
            print(f"API request failed for {symbol}: {e}")
            return {"error": str(e)}, "error"

    def _store_landing_record(self, db, symbol: str, symbol_id: int,
                            api_response: dict[str, Any], status: str, run_id: str) -> str:
        """
        Store raw API response in landing table.

        Args:
            db: Database manager
            symbol: Stock symbol
            symbol_id: Symbol ID
            api_response: Raw API response
            status: Response status
            run_id: Unique run ID

        Returns:
            Content hash of the response
        """
        content_hash = ContentHasher.calculate_api_response_hash(api_response)

        insert_query = """
            INSERT INTO source.api_responses_landing
            (table_name, symbol, symbol_id, api_function, api_response,
             content_hash, source_run_id, response_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        db.execute_query(insert_query, (
            self.table_name, symbol, symbol_id, STOCK_API_FUNCTION,
            json.dumps(api_response), content_hash, run_id, status
        ))

        return content_hash

    def _transform_data(self, symbol: str, symbol_id: int, api_response: dict[str, Any],
                       run_id: str) -> list[dict[str, Any]]:
        """
        Transform API response to standardized records.

        Args:
            symbol: Stock symbol
            symbol_id: Symbol ID
            api_response: Raw API response
            run_id: Unique run ID

        Returns:
            List of transformed records
        """
        records = []

        # Process quarterly reports
        quarterly_reports = api_response.get("quarterlyReports", [])
        for report in quarterly_reports:
            record = self._transform_single_report(
                symbol, symbol_id, report, "quarterly", run_id
            )
            if record:
                records.append(record)

        # Process annual reports
        annual_reports = api_response.get("annualReports", [])
        for report in annual_reports:
            record = self._transform_single_report(
                symbol, symbol_id, report, "annual", run_id
            )
            if record:
                records.append(record)

        return records

    def _transform_single_report(self, symbol: str, symbol_id: int,
                                report: dict[str, Any], report_type: str,
                                run_id: str) -> dict[str, Any] | None:
        """
        Transform a single cash flow report.

        Args:
            symbol: Stock symbol
            symbol_id: Symbol ID
            report: Single report data
            report_type: 'quarterly' or 'annual'
            run_id: Unique run ID

        Returns:
            Transformed record or None if invalid
        """
        try:
            # Initialize record with all fields as None
            record = dict.fromkeys(CASH_FLOW_FIELDS.keys())

            # Set known values
            record.update({
                "symbol_id": symbol_id,
                "symbol": symbol,
                "report_type": report_type,
                "api_response_status": "pass",
                "source_run_id": run_id,
                "fetched_at": datetime.now()
            })

            # Helper function to convert API values
            def convert_value(value):
                if value is None or value in {"None", ""}:
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None

            # Map API fields to database fields using schema-driven approach
            for db_field, api_field in CASH_FLOW_FIELDS.items():
                if api_field in report:
                    record[db_field] = convert_value(report.get(api_field))

            # Handle special fields not in the main mapping
            record["fiscal_date_ending"] = DateUtils.parse_fiscal_date(report.get("fiscalDateEnding"))
            record["reported_currency"] = report.get("reportedCurrency")

            # Validate required fields
            if not record["fiscal_date_ending"]:
                print(f"Missing fiscal_date_ending for {symbol} {report_type} report")
                return None

            # Calculate content hash for change detection
            record["content_hash"] = ContentHasher.calculate_business_content_hash(record)

            return record

        except Exception as e:
            print(f"Error transforming report for {symbol}: {e}")
            return None

    def _content_has_changed(self, db, symbol_id: int, content_hash: str) -> bool:
        """
        Check if content has changed based on hash comparison.

        Args:
            db: Database manager
            symbol_id: Symbol ID
            content_hash: New content hash

        Returns:
            True if content has changed or is new
        """
        query = """
            SELECT COUNT(*) FROM source.cash_flow
            WHERE symbol_id = %s AND content_hash = %s
        """

        result = db.fetch_query(query, (symbol_id, content_hash))
        return result[0][0] == 0 if result else True  # True if no matching hash found

    def _upsert_records(self, db, records: list[dict[str, Any]]) -> int:
        """
        Upsert records into the cash flow table.

        Args:
            db: Database manager
            records: List of records to upsert

        Returns:
            Number of rows affected
        """
        if not records:
            return 0

        # Get column names (excluding auto-generated ones)
        columns = [col for col in records[0]
                  if col not in ['cash_flow_id', 'created_at', 'updated_at']]

        # Build upsert query
        placeholders = ", ".join(["%s" for _ in columns])
        update_columns = [col for col in columns
                         if col not in ['symbol_id', 'fiscal_date_ending', 'report_type']]
        update_set = [f"{col} = EXCLUDED.{col}" for col in update_columns]
        update_set.append("updated_at = NOW()")

        upsert_query = f"""
            INSERT INTO source.cash_flow ({', '.join(columns)}, created_at, updated_at)
            VALUES ({placeholders}, NOW(), NOW())
            ON CONFLICT (symbol_id, fiscal_date_ending, report_type)
            DO UPDATE SET {', '.join(update_set)}
        """

        # Prepare record values
        record_values = []
        for record in records:
            values = [record[col] for col in columns]
            record_values.append(tuple(values))

        # Execute upsert
        with db.connection.cursor() as cursor:
            cursor.executemany(upsert_query, record_values)
            db.connection.commit()
            return cursor.rowcount

    def extract_symbol(self, symbol: str, symbol_id: int, db) -> dict[str, Any]:
        """
        Extract cash flow data for a single symbol with adaptive rate limiting.

        Args:
            symbol: Stock symbol
            symbol_id: Symbol ID
            db: Database manager (passed from caller)

        Returns:
            Processing result summary
        """
        run_id = RunIdGenerator.generate()

        # Start processing timer for adaptive rate limiter
        self.rate_limiter.start_processing()

        watermark_mgr = self._initialize_watermark_manager(db)

        # Fetch API data
        api_response, status = self._fetch_api_data(symbol)

        # Store in landing table (always)
        content_hash = self._store_landing_record(
            db, symbol, symbol_id, api_response, status, run_id
        )

        # Process if successful
        if status == "success":
            # Check if content has changed
            if not self._content_has_changed(db, symbol_id, content_hash):
                print(f"No changes detected for {symbol}, skipping transformation")
                watermark_mgr.update_watermark(self.table_name, symbol_id, success=True)
                return {
                    "symbol": symbol,
                    "status": "no_changes",
                    "records_processed": 0,
                    "run_id": run_id
                }

            # Transform data
            records = self._transform_data(symbol, symbol_id, api_response, run_id)

            if records:
                # Upsert records
                rows_affected = self._upsert_records(db, records)

                # Update watermark with latest fiscal date
                latest_fiscal_date = max(
                    r['fiscal_date_ending'] for r in records
                    if r['fiscal_date_ending']
                )
                watermark_mgr.update_watermark(
                    self.table_name, symbol_id, latest_fiscal_date, success=True
                )

                result = {
                    "symbol": symbol,
                    "status": "success",
                    "records_processed": len(records),
                    "rows_affected": rows_affected,
                    "latest_fiscal_date": latest_fiscal_date,
                    "run_id": run_id
                }
            else:
                # No valid records
                watermark_mgr.update_watermark(self.table_name, symbol_id, success=False)
                result = {
                    "symbol": symbol,
                    "status": "no_valid_records",
                    "records_processed": 0,
                    "run_id": run_id
                }
        else:
            # API failure (rate_limited, error, empty)
            watermark_mgr.update_watermark(self.table_name, symbol_id, success=False)
            result = {
                "symbol": symbol,
                "status": "api_failure",
                "error": status,
                "records_processed": 0,
                "run_id": run_id
            }

        # Notify rate limiter about processing result (enables optimization)
        self.rate_limiter.post_api_call(result["status"])

        return result

    def run_incremental_extraction(self, limit: int | None = None,
                                 staleness_hours: int = 24,
                                 quarterly_gap_detection: bool = True,
                                 enable_pre_screening: bool = True,
                                 use_dcs: bool = False,
                                 min_dcs: float = 0.0) -> dict[str, Any]:
        """
        Run incremental extraction for symbols that need processing.

        Args:
            limit: Maximum number of symbols to process
            staleness_hours: Hours before data is considered stale
            quarterly_gap_detection: Enable quarterly gap detection for financial statements
            enable_pre_screening: Enable symbol pre-screening to avoid likely failures
            use_dcs: Enable Data Coverage Score prioritization
            min_dcs: Minimum DCS score for symbol selection

        Returns:
            Processing summary
        """
        print("🚀 Starting incremental cash flow extraction with adaptive rate limiting...")
        print(f"Configuration: limit={limit}, staleness_hours={staleness_hours}, quarterly_gap_detection={quarterly_gap_detection}, pre_screening={enable_pre_screening}, use_dcs={use_dcs}, min_dcs={min_dcs}")

        with self._get_db_manager() as db:
            # Ensure schema exists
            self._ensure_schema_exists(db)

            watermark_mgr = self._initialize_watermark_manager(db)

            # Get symbols needing processing with DCS prioritization if requested
            if use_dcs:
                print(f"🎯 Using Data Coverage Score prioritization with min_dcs={min_dcs}")
                try:
                    symbols_to_process = watermark_mgr.get_symbols_needing_processing_with_dcs(
                        self.table_name,
                        staleness_hours=staleness_hours,
                        limit=limit,
                        quarterly_gap_detection=quarterly_gap_detection,
                        reporting_lag_days=45,  # Standard 45-day reporting lag for quarterly data
                        enable_pre_screening=enable_pre_screening,
                        min_dcs_threshold=min_dcs
                    )
                    print(f"✅ DCS prioritization successful: {len(symbols_to_process)} symbols selected")
                except Exception as e:
                    print(f"⚠️ DCS prioritization failed ({e}), falling back to standard method")
                    symbols_to_process = watermark_mgr.get_symbols_needing_processing(
                        self.table_name,
                        staleness_hours=staleness_hours,
                        limit=limit,
                        quarterly_gap_detection=quarterly_gap_detection,
                        reporting_lag_days=45,  # Standard 45-day reporting lag for quarterly data
                        enable_pre_screening=enable_pre_screening  # Enable enhanced symbol pre-screening
                    )
            else:
                # Standard processing without DCS
                symbols_to_process = watermark_mgr.get_symbols_needing_processing(
                    self.table_name,
                    staleness_hours=staleness_hours,
                    limit=limit,
                    quarterly_gap_detection=quarterly_gap_detection,
                    reporting_lag_days=45,  # Standard 45-day reporting lag for quarterly data
                    enable_pre_screening=enable_pre_screening  # Enable enhanced symbol pre-screening
                )

            print(f"Found {len(symbols_to_process)} symbols needing processing")

            if not symbols_to_process:
                print("✅ No symbols need processing")
                return {
                    "symbols_processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "no_changes": 0,
                    "total_records": 0
                }

            # Process symbols
            results = {
                "symbols_processed": 0,
                "successful": 0,
                "failed": 0,
                "no_changes": 0,
                "total_records": 0,
                "details": []
            }

            for i, symbol_data in enumerate(symbols_to_process, 1):
                symbol_id = symbol_data["symbol_id"]
                symbol = symbol_data["symbol"]

                print(f"Processing {symbol} (ID: {symbol_id}) [{i}/{len(symbols_to_process)}]")

                # Extract symbol data
                result = self.extract_symbol(symbol, symbol_id, db)
                results["details"].append(result)
                results["symbols_processed"] += 1

                if result["status"] == "success":
                    results["successful"] += 1
                    results["total_records"] += result["records_processed"]
                    print(f"✅ {symbol}: {result['records_processed']} records processed")
                elif result["status"] == "no_changes":
                    results["no_changes"] += 1
                    print(f"⚪ {symbol}: No changes detected")
                else:
                    results["failed"] += 1
                    print(f"❌ {symbol}: {result['status']}")

                # Show periodic performance updates
                if i % 10 == 0 or i == len(symbols_to_process):
                    self.rate_limiter.print_performance_summary()

            print("\n🎯 Incremental extraction completed:")
            print(f"  Symbols processed: {results['symbols_processed']}")
            print(f"  Successful: {results['successful']}")
            print(f"  No changes: {results['no_changes']}")
            print(f"  Failed: {results['failed']}")
            print(f"  Total records: {results['total_records']}")

            return results


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Cash Flow Extractor")
    parser.add_argument("--limit", type=int, help="Maximum number of symbols to process")
    parser.add_argument("--staleness-hours", type=int, default=24,
                       help="Hours before data is considered stale (default: 24)")
    parser.add_argument("--no-quarterly-gap-detection", action="store_true",
                       help="Disable quarterly gap detection (use only time-based staleness)")
    parser.add_argument("--no-pre-screening", action="store_true",
                       help="Disable symbol pre-screening (process all symbols regardless of type)")
    parser.add_argument("--use-dcs", action="store_true",
                       help="Enable Data Coverage Score (DCS) prioritization")
    parser.add_argument("--min-dcs", type=float, default=0.0,
                       help="Minimum DCS score for symbol selection (0.0-1.0, default: 0.0)")

    args = parser.parse_args()

    extractor = CashFlowExtractor()
    result = extractor.run_incremental_extraction(
        limit=args.limit,
        staleness_hours=args.staleness_hours,
        quarterly_gap_detection=not args.no_quarterly_gap_detection,
        enable_pre_screening=not args.no_pre_screening,
        use_dcs=args.use_dcs,
        min_dcs=args.min_dcs
    )

    # Exit with appropriate code
    if result["failed"] > 0 and result["successful"] == 0:
        sys.exit(1)  # All failed
    else:
        sys.exit(0)  # Some or all successful


if __name__ == "__main__":
    main()

# -----------------------------------------------------------------------------
# Example commands (PowerShell):
#
# Basic incremental extraction (process up to 50 symbols, 24-hour staleness):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --limit 50
#
# DCS-prioritized extraction (Core symbols with DCS ≥ 0.8):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --limit 50 --use-dcs --min-dcs 0.8
#
# DCS-prioritized extraction (Extended symbols with DCS ≥ 0.6):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --limit 100 --use-dcs --min-dcs 0.6
#
# Process only 10 symbols (useful for testing):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --limit 10
#
# Test DCS prioritization (small batch):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --limit 10 --use-dcs --min-dcs 0.7
#
# Aggressive refresh (1-hour staleness, process 25 symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --limit 25 --staleness-hours 1
#
# Weekly batch processing (7-day staleness, no limit):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --staleness-hours 168
#
# Large batch processing (process 100 symbols, 24-hour staleness):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --limit 100 --staleness-hours 24
#
# Force refresh of recent data (6-hour staleness, 50 symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --limit 50 --staleness-hours 6
#
# Process unprocessed symbols only (very long staleness period):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_cash_flow.py --limit 25 --staleness-hours 8760
# -----------------------------------------------------------------------------

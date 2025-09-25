"""
Insider Transactions Extractor using incremental ETL architecture.
Uses source schema, watermarks, and adaptive rate limiting for optimal performance.
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
STOCK_API_FUNCTION = "INSIDER_TRANSACTIONS"


class InsiderTransactionsExtractor:
    """Insider transactions extractor with adaptive rate limiting and incremental processing."""

    def __init__(self):
        """Initialize the extractor."""
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        self.table_name = "insider_transactions"
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
            # Read and execute the source schema from archive
            schema_file = Path(__file__).parent.parent.parent / "archive" / "sql" / "source_schema.sql"
            if schema_file.exists():
                with schema_file.open() as f:
                    schema_sql = f.read()
                db.execute_script(schema_sql)
                print("✅ Source schema initialized from archive")
            else:
                print(f"⚠️ Schema file not found: {schema_file}")
                # Create basic schema if file missing - just the minimum needed
                basic_schema = """
                    CREATE SCHEMA IF NOT EXISTS source;
                """
                db.execute_script(basic_schema)
                print("✅ Basic source schema created")

            # Always ensure insider_transactions table exists in source schema
            insider_transactions_sql = """
                CREATE TABLE IF NOT EXISTS source.insider_transactions (
                    transaction_id SERIAL PRIMARY KEY,
                    symbol_id BIGINT NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    transaction_date DATE,
                    executive VARCHAR(255),
                    executive_title VARCHAR(255),
                    security_type VARCHAR(100),
                    acquisition_or_disposal VARCHAR(1),
                    shares DECIMAL(20,4),
                    share_price DECIMAL(20,4),

                    -- ETL metadata
                    api_response_status VARCHAR(20) NOT NULL DEFAULT 'pass',
                    content_hash VARCHAR(32) NOT NULL,
                    source_run_id UUID NOT NULL,
                    fetched_at TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),

                    -- Natural key constraint for idempotent upserts
                    UNIQUE(symbol_id, transaction_date, executive, security_type, acquisition_or_disposal, shares, share_price),

                    FOREIGN KEY (symbol_id) REFERENCES source.listing_status(symbol_id) ON DELETE CASCADE
                );

                -- Indexes for performance
                CREATE INDEX IF NOT EXISTS idx_source_insider_transactions_symbol_id ON source.insider_transactions(symbol_id);
                CREATE INDEX IF NOT EXISTS idx_source_insider_transactions_date ON source.insider_transactions(transaction_date);
                CREATE INDEX IF NOT EXISTS idx_source_insider_transactions_content_hash ON source.insider_transactions(content_hash);
                CREATE INDEX IF NOT EXISTS idx_source_insider_transactions_fetched_at ON source.insider_transactions(fetched_at);
            """
            db.execute_script(insider_transactions_sql)
            print("✅ Source insider_transactions table created/verified")

        except Exception as e:
            print(f"⚠️ Schema initialization error: {e}")

    def _fetch_api_data(self, symbol: str) -> tuple[dict[str, Any], str]:
        """
        Fetch insider transactions data from Alpha Vantage API.

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

        # Process insider transactions - the API typically returns data under 'transactions' key
        transactions = api_response.get("transactions", [])
        if not transactions and isinstance(api_response, list):
            # Sometimes the API returns transactions directly as a list
            transactions = api_response
        elif not transactions and isinstance(api_response, dict):
            # Sometimes transactions are under other keys, try common variants
            for key in ["insiderTransactions", "insider_transactions", "data"]:
                if key in api_response:
                    transactions = api_response[key]
                    break

        for transaction in transactions:
            record = self._transform_single_transaction(
                symbol, symbol_id, transaction, run_id
            )
            if record:
                records.append(record)

        return records

    def _transform_single_transaction(self, symbol: str, symbol_id: int,
                                    transaction: dict[str, Any],
                                    run_id: str) -> dict[str, Any] | None:
        """
        Transform a single insider transaction.

        Args:
            symbol: Stock symbol
            symbol_id: Symbol ID
            transaction: Single transaction data
            run_id: Unique run ID

        Returns:
            Transformed record or None if invalid
        """
        try:
            # Helper function to convert values
            def convert_numeric_value(value):
                if value is None or value in {"None", ""}:
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None

            # Parse transaction date
            transaction_date = DateUtils.parse_fiscal_date(
                transaction.get("transactionDate") or
                transaction.get("transaction_date") or
                transaction.get("date")
            )

            if not transaction_date:
                print(f"Missing transaction_date for {symbol} transaction")
                return None

            # Extract and clean fields
            record = {
                "symbol_id": symbol_id,
                "symbol": symbol,
                "transaction_date": transaction_date,
                "executive": transaction.get("executive") or transaction.get("insider"),
                "executive_title": transaction.get("executiveTitle") or transaction.get("title"),
                "security_type": transaction.get("securityType") or transaction.get("security"),
                "acquisition_or_disposal": transaction.get("acquisitionOrDisposal") or
                                        transaction.get("transaction_type", "").upper()[:1] if
                                        transaction.get("transaction_type") else None,
                "shares": convert_numeric_value(
                    transaction.get("shares") or
                    transaction.get("quantity") or
                    transaction.get("amount")
                ),
                "share_price": convert_numeric_value(
                    transaction.get("sharePrice") or
                    transaction.get("price") or
                    transaction.get("pricePerShare")
                ),
                "api_response_status": "pass",
                "source_run_id": run_id,
                "fetched_at": datetime.now()
            }

            # Calculate content hash for change detection
            record["content_hash"] = ContentHasher.calculate_business_content_hash(record)

            return record

        except Exception as e:
            print(f"Error transforming transaction for {symbol}: {e}")
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
            SELECT COUNT(*) FROM source.insider_transactions
            WHERE symbol_id = %s AND content_hash = %s
        """

        result = db.fetch_query(query, (symbol_id, content_hash))
        return result[0][0] == 0 if result else True  # True if no matching hash found

    def _upsert_records(self, db, records: list[dict[str, Any]]) -> int:
        """
        Upsert records into the insider transactions table.

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
                  if col not in ['transaction_id', 'created_at', 'updated_at']]

        # Build upsert query - use unique constraint from schema
        placeholders = ", ".join(["%s" for _ in columns])
        update_columns = [col for col in columns
                         if col not in ['symbol_id', 'transaction_date', 'executive',
                                       'security_type', 'acquisition_or_disposal', 'shares', 'share_price']]
        update_set = [f"{col} = EXCLUDED.{col}" for col in update_columns]
        update_set.append("updated_at = NOW()")

        upsert_query = f"""
            INSERT INTO source.insider_transactions ({', '.join(columns)}, created_at, updated_at)
            VALUES ({placeholders}, NOW(), NOW())
            ON CONFLICT (symbol_id, transaction_date, executive, security_type, acquisition_or_disposal, shares, share_price)
            DO UPDATE SET {', '.join(update_set) if update_set else 'updated_at = NOW()'}
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
        Extract insider transactions data for a single symbol with adaptive rate limiting.

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

                # Update watermark with latest transaction date
                latest_transaction_date = max(
                    r['transaction_date'] for r in records
                    if r['transaction_date']
                )
                watermark_mgr.update_watermark(
                    self.table_name, symbol_id, latest_transaction_date, success=True
                )

                result = {
                    "symbol": symbol,
                    "status": "success",
                    "records_processed": len(records),
                    "rows_affected": rows_affected,
                    "latest_transaction_date": latest_transaction_date,
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
                                 quarterly_gap_detection: bool = False,  # Not applicable for insider transactions
                                 enable_pre_screening: bool = True,
                                 use_dcs: bool = False,
                                 min_dcs: float = 0.0) -> dict[str, Any]:
        """
        Run incremental extraction for symbols that need processing.

        Args:
            limit: Maximum number of symbols to process
            staleness_hours: Hours before data is considered stale
            quarterly_gap_detection: Not used for insider transactions (always False)
            enable_pre_screening: Enable symbol pre-screening to avoid likely failures
            use_dcs: Enable Data Coverage Score prioritization
            min_dcs: Minimum DCS score for symbol selection

        Returns:
            Processing summary
        """
        print("🚀 Starting incremental insider transactions extraction with adaptive rate limiting...")
        print(f"Configuration: limit={limit}, staleness_hours={staleness_hours}, pre_screening={enable_pre_screening}, use_dcs={use_dcs}, min_dcs={min_dcs}")

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
                        quarterly_gap_detection=False,  # Insider transactions don't use quarterly gaps
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
                        quarterly_gap_detection=False,  # Insider transactions don't use quarterly gaps
                        enable_pre_screening=enable_pre_screening
                    )
            else:
                # Standard processing without DCS
                symbols_to_process = watermark_mgr.get_symbols_needing_processing(
                    self.table_name,
                    staleness_hours=staleness_hours,
                    limit=limit,
                    quarterly_gap_detection=False,  # Insider transactions don't use quarterly gaps
                    enable_pre_screening=enable_pre_screening
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
                    error_detail = result.get("error", result["status"])
                    print(f"❌ {symbol}: {error_detail}")

                # Show periodic performance updates
                if i % 10 == 0 or i == len(symbols_to_process):
                    self.rate_limiter.print_performance_summary()

            print("\n🎯 Incremental extraction completed:")
            print(f"  Symbols processed: {results['symbols_processed']}")
            print(f"  Successful: {results['successful']}")
            print(f"  No changes: {results['no_changes']}")
            print(f"  Failed: {results['failed']}")
            print(f"  Total records: {results['total_records']}")

            # Final performance summary
            print("\n" + "="*60)
            self.rate_limiter.print_performance_summary()
            print("="*60)

            return results


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Insider Transactions Extractor")
    parser.add_argument("--limit", type=int, help="Maximum number of symbols to process")
    parser.add_argument("--staleness-hours", type=int, default=24,
                       help="Hours before data is considered stale (default: 24)")
    parser.add_argument("--no-pre-screening", action="store_true",
                       help="Disable symbol pre-screening (process all symbols regardless of type)")
    parser.add_argument("--use-dcs", action="store_true",
                       help="Enable Data Coverage Score (DCS) prioritization")
    parser.add_argument("--min-dcs", type=float, default=0.0,
                       help="Minimum DCS score for symbol selection (0.0-1.0, default: 0.0)")

    args = parser.parse_args()

    extractor = InsiderTransactionsExtractor()
    result = extractor.run_incremental_extraction(
        limit=args.limit,
        staleness_hours=args.staleness_hours,
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
# Basic incremental extraction with pre-screening (recommended):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_insider_transactions.py --limit 50
#
# Process only 10 symbols (useful for testing):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_insider_transactions.py --limit 10
#
# Disable pre-screening (process all symbols including problematic ones):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_insider_transactions.py --limit 25 --no-pre-screening
#
# Aggressive refresh with pre-screening (1-hour staleness, process 25 symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_insider_transactions.py --limit 25 --staleness-hours 1
#
# Weekly batch processing (7-day staleness, no limit):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_insider_transactions.py --staleness-hours 168
#
# Large batch processing with optimized screening (process 100 symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_insider_transactions.py --limit 100 --staleness-hours 24
#
# Force refresh of recent data (6-hour staleness, 50 symbols):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_insider_transactions.py --limit 50 --staleness-hours 6
#
# Process unprocessed symbols with pre-screening (very long staleness period):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_insider_transactions.py --limit 25 --staleness-hours 8760
# -----------------------------------------------------------------------------

"""
Extract company overview data from Alpha Vantage API and load into database.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.adaptive_rate_limiter import AdaptiveRateLimiter, ExtractorType

API_FUNCTION = "OVERVIEW"


class OverviewExtractor:
    """Extract and load company overview data from Alpha Vantage API."""

    def __init__(self):
        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")

        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        self.db_manager = PostgresDatabaseManager()
        self.base_url = "https://www.alphavantage.co/query"

        # Initialize adaptive rate limiter for fundamentals processing
        self.rate_limiter = AdaptiveRateLimiter(ExtractorType.FUNDAMENTALS, verbose=True)

    def load_valid_symbols(self, exchange_filter=None, limit=None, try_failed: str = "Y"):
        """Load valid stock symbols from the database with their symbol_ids.

        Args:
            exchange_filter: Exchange or list of exchanges to include.
            limit: Optional limit on number of symbols.
            try_failed: 'Y' or 'N'. When 'N', exclude symbols whose latest
                overview status is 'fail'. Defaults to 'Y' (include).
        """
        if try_failed not in {"Y", "N"}:
            raise ValueError("try_failed must be 'Y' or 'N'")

        with self.db_manager as db:
            params = []
            if try_failed == "N":
                # Exclude symbols whose latest overview has status='fail'
                base_query = (
                    """
                    SELECT ls.symbol_id, ls.symbol
                    FROM listing_status ls
                    LEFT JOIN LATERAL (
                        SELECT ov.status
                        FROM overview ov
                        WHERE ov.symbol_id = ls.symbol_id
                        ORDER BY ov.updated_at DESC
                        LIMIT 1
                    ) latest ON TRUE
                    WHERE ls.asset_type = 'Stock'
                      AND (latest.status IS DISTINCT FROM 'fail')
                    """
                )
            else:
                base_query = (
                    "SELECT symbol_id, symbol FROM listing_status WHERE asset_type = 'Stock'"
                )

            if exchange_filter:
                if isinstance(exchange_filter, list):
                    placeholders = ",".join(["%s" for _ in exchange_filter])
                    base_query += f" AND ls.exchange IN ({placeholders})" if try_failed == "N" else f" AND exchange IN ({placeholders})"
                    params.extend(exchange_filter)
                else:
                    base_query += " AND ls.exchange = %s" if try_failed == "N" else " AND exchange = %s"
                    params.append(exchange_filter)

            # Order by symbol to ensure consistent ordering
            if try_failed == "N":
                base_query += " ORDER BY ls.symbol"
            else:
                base_query += " ORDER BY symbol"

            if limit:
                base_query += " LIMIT %s"
                params.append(limit)

            result = db.fetch_query(base_query, params)
            # Return dict mapping symbol -> symbol_id
            return {row[1]: row[0] for row in result}

    def extract_single_overview(self, symbol):
        """Extract overview data for a single symbol."""
        print(f"Processing TICKER: {symbol}")

        url = f"{self.base_url}?function={API_FUNCTION}&symbol={symbol}&apikey={self.api_key}"
        print(f"Fetching data from: {url}")

        # Wait with adaptive rate limiting
        self.rate_limiter.pre_api_call()

        try:
            response = requests.get(url)
            response.raise_for_status()

            print(f"Response status: {response.status_code}")
            data = response.json()

            # Check if we got valid data or an empty response
            if not data or data == {} or "Symbol" not in data:
                print(f"Empty or invalid response for {symbol}: {data}")
                # Notify rate limiter of failed API call
                self.rate_limiter.post_api_call('error')
                return None, "fail"

            print(f"Successfully fetched data for {symbol}")
            # Notify rate limiter of successful API call
            self.rate_limiter.post_api_call('success')
            return data, "pass"

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            # Notify rate limiter of failed API call
            self.rate_limiter.post_api_call('error')
            return None, "fail"
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            # Notify rate limiter of failed API call
            self.rate_limiter.post_api_call('error')
            return None, "fail"

    def transform_overview_data(self, symbol, symbol_id, data, status):
        """Transform overview data to match database schema."""
        current_timestamp = datetime.now().isoformat()

        if status == "fail" or data is None:
            # Create a minimal record for failed fetches
            return {
                "symbol_id": symbol_id,
                "symbol": symbol,
                "assettype": None,
                "name": None,
                "description": None,
                "cik": None,
                "exchange": None,
                "currency": None,
                "country": None,
                "sector": None,
                "industry": None,
                "address": None,
                "officialsite": None,
                "fiscalyearend": None,
                "status": "fail",
                "created_at": current_timestamp,
                "updated_at": current_timestamp,
            }

        # Map API fields to database columns
        return {
            "symbol_id": symbol_id,
            "symbol": data.get("Symbol", symbol),
            "assettype": data.get("AssetType"),
            "name": data.get("Name"),
            "description": data.get("Description"),
            "cik": data.get("CIK"),
            "exchange": data.get("Exchange"),
            "currency": data.get("Currency"),
            "country": data.get("Country"),
            "sector": data.get("Sector"),
            "industry": data.get("Industry"),
            "address": data.get("Address"),
            "officialsite": data.get("OfficialSite"),
            "fiscalyearend": data.get("FiscalYearEnd"),
            "status": "pass",
            "created_at": current_timestamp,
            "updated_at": current_timestamp,
        }


    def load_overview_data_with_db(self, db_manager, records):
        """Load overview records into the database using provided database manager."""
        if not records:
            print("No records to load")
            return

        print(f"Loading {len(records)} records into database...")

        # Initialize schema if tables don't exist
        schema_path = (
            Path(__file__).parent.parent.parent
            / "db"
            / "schema"
            / "postgres_stock_db_schema.sql"
        )
        if not db_manager.table_exists("overview"):
            print("Initializing database schema...")
            db_manager.initialize_schema(schema_path)

        # Ensure the sequence for overview_id is synchronized to avoid PK collisions
        try:
            db_manager.execute_query(
                """
                SELECT setval(
                    pg_get_serial_sequence('overview','overview_id'),
                    COALESCE((SELECT MAX(overview_id) FROM overview), 0) + 1,
                    false
                )
                """
            )
        except Exception as e:
            # Non-fatal: continue even if sequence sync query fails
            print(f"Sequence sync skipped: {e}")

        # Use direct SQL INSERT (remove problematic ON CONFLICT)
        if not records:
            print("No records to load")
            return

        # Get the first record to determine columns
        first_record = records[0]
        columns = [col for col in first_record if col not in ['created_at', 'updated_at']]

        # Build the INSERT query
        placeholders = ", ".join(["%s" for _ in columns])

        insert_query = f"""
            INSERT INTO overview ({', '.join(columns)}, created_at, updated_at)
            VALUES ({placeholders}, NOW(), NOW())
        """

        # Prepare record values (excluding timestamp columns)
        record_values = []
        for record in records:
            values = [record[col] for col in columns]
            record_values.append(tuple(values))

        # Execute the insert
        try:
            with db_manager.connection.cursor() as cursor:
                cursor.executemany(insert_query, record_values)
                db_manager.connection.commit()
                rows_affected = cursor.rowcount

            print(f"Successfully loaded {rows_affected} records into overview table")

        except Exception as e:
            db_manager.connection.rollback()
            if "duplicate key" in str(e).lower():
                print("Duplicate key detected, skipping")
            else:
                raise Exception(f"Database error loading overview records: {str(e)}")

    def load_unprocessed_symbols_with_db(self, db, exchange_filter=None, limit=None):
        """Load symbols that haven't been processed yet using provided database connection."""
        base_query = (
            """
            SELECT ls.symbol_id, ls.symbol
            FROM listing_status ls
            WHERE ls.asset_type = 'Stock'
              AND NOT EXISTS (
                SELECT 1 FROM overview ov WHERE ov.symbol_id = ls.symbol_id
              )
            """
        )
        params = []

        if exchange_filter:
            if isinstance(exchange_filter, list):
                placeholders = ",".join(["%s" for _ in exchange_filter])
                base_query += f" AND ls.exchange IN ({placeholders})"
                params.extend(exchange_filter)
            else:
                base_query += " AND ls.exchange = %s"
                params.append(exchange_filter)

        # Order by symbol to ensure consistent ordering
        base_query += " ORDER BY ls.symbol"

        if limit:
            base_query += " LIMIT %s"
            params.append(limit)

        result = db.fetch_query(base_query, params)
        return {row[1]: row[0] for row in result}

    def load_unprocessed_symbols(self, exchange_filter=None, limit=None):
        """Load symbols that haven't been processed yet (not in overview table)."""
        with self.db_manager as db:
            return self.load_unprocessed_symbols_with_db(db, exchange_filter, limit)

    def run_etl(self, exchange_filter=None, limit=None, try_failed: str = "Y"):
        """Run the complete ETL process for overview data."""
        print("Starting Overview ETL process...")

        # Initialize adaptive rate limiting
        self.rate_limiter.start_processing()

        try:
            # Use a fresh database manager for this ETL run
            db_manager = PostgresDatabaseManager()
            with db_manager as db:
                # Load symbols to process (now returns dict: symbol -> symbol_id)
                symbol_mapping = self.load_valid_symbols(exchange_filter, limit, try_failed=try_failed)
                symbols = list(symbol_mapping.keys())
                print(f"Found {len(symbols)} symbols to process")

                if not symbols:
                    print("No symbols found to process")
                    return

                records = []

                for _i, symbol in enumerate(symbols):
                    symbol_id = symbol_mapping[symbol]

                    # Extract data for this symbol
                    data, status = self.extract_single_overview(symbol)

                    # Transform data
                    transformed_record = self.transform_overview_data(
                        symbol, symbol_id, data, status
                    )
                    records.append(transformed_record)

                    print(f"Processed {symbol} (ID: {symbol_id}) with status: {status}")

                # Load all records using the shared connection
                self.load_overview_data_with_db(db, records)

            # Print summary
            pass_count = sum(1 for r in records if r["status"] == "pass")
            fail_count = sum(1 for r in records if r["status"] == "fail")
            print("\nETL Summary:")
            print(f"  Total processed: {len(records)}")
            print(f"  Successful: {pass_count}")
            print(f"  Failed: {fail_count}")

            print("Overview ETL process completed successfully!")

        except Exception as e:
            print(f"Overview ETL process failed: {e}")
            raise

    def run_etl_incremental(self, exchange_filter=None, limit=None, try_failed: str = "Y"):
        """Run ETL only for symbols not yet processed."""
        print("Starting Incremental Overview ETL process...")

        # Initialize adaptive rate limiting
        self.rate_limiter.start_processing()

        try:
            # Use a fresh database manager for this ETL run
            db_manager = PostgresDatabaseManager()
            with db_manager as db:
                # Load only unprocessed symbols using the shared connection
                symbol_mapping = self.load_unprocessed_symbols_with_db(db, exchange_filter, limit)
                symbols = list(symbol_mapping.keys())
                print(f"Found {len(symbols)} unprocessed symbols")

                if not symbols:
                    print("No unprocessed symbols found")
                    return

                records = []

                for i, symbol in enumerate(symbols):
                    symbol_id = symbol_mapping[symbol]

                    # Extract data for this symbol
                    data, status = self.extract_single_overview(symbol)

                    # Transform data
                    transformed_record = self.transform_overview_data(
                        symbol, symbol_id, data, status
                    )
                    records.append(transformed_record)

                    print(
                        f"Processed {symbol} (ID: {symbol_id}) with status: {status} [{i+1}/{len(symbols)}]"
                    )

                # Load all records using the shared connection
                self.load_overview_data_with_db(db, records)

            # Print summary
            pass_count = sum(1 for r in records if r["status"] == "pass")
            fail_count = sum(1 for r in records if r["status"] == "fail")
            print("\nIncremental ETL Summary:")
            print(f"  Total processed: {len(records)}")
            print(f"  Successful: {pass_count}")
            print(f"  Failed: {fail_count}")

            print("Incremental Overview ETL process completed successfully!")

        except Exception as e:
            print(f"Incremental Overview ETL process failed: {e}")
            raise


def main():
    """Main function to run the overview extraction via CLI."""
    parser = argparse.ArgumentParser(description="Extract and load Alpha Vantage OVERVIEW data")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Run full ETL for all eligible symbols or incremental for unprocessed symbols",
    )
    parser.add_argument(
        "--exchange",
        action="append",
        help="Exchange to include (can be specified multiple times). Examples: NYSE, NASDAQ",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of symbols; omit to apply no limit",
    )
    parser.add_argument(
        "--try-failed",
        choices=["Y", "N"],
        default="N",
        help=(
            "When N, exclude symbols whose latest overview status is 'fail' (applies to full runs). "
            "When Y, include them."
        ),
    )
    parser.add_argument(
        "--force-restart",
        choices=["Y", "N"],
        default="N",
        help=(
            "When N (default), do not start from scratch—only fetch symbols not already in overview. "
            "When Y, ignore overview contents and pull all eligible symbols."
        ),
    )
    args = parser.parse_args()

    extractor = OverviewExtractor()

    exchanges = args.exchange or []
    if not exchanges:
        print("No exchanges specified with --exchange; processing all matching symbols")

    def _run_for_exchange(ex_val):
        if args.force_restart == "Y":
            mode_label = "FULL (force restart)"
            extractor.run_etl(exchange_filter=ex_val, limit=args.limit, try_failed=args.try_failed)
        else:
            mode_label = "INCREMENTAL (new symbols only)"
            extractor.run_etl_incremental(exchange_filter=ex_val, limit=args.limit, try_failed=args.try_failed)
        return mode_label

    if exchanges:
        for ex in exchanges:
            label = _run_for_exchange(ex)
            print(f"[{label}] Completed for exchange: {ex}")
    else:
        label = _run_for_exchange(None)
        print(f"[{label}] Completed for all exchanges")

    # Optional: quick remaining counts when exchanges provided
    if exchanges:
        print("Checking remaining symbols by exchange...")
        with PostgresDatabaseManager() as db:
            for ex in exchanges:
                remaining = db.fetch_query(
                    """
                    SELECT COUNT(*)
                    FROM listing_status ls
                    LEFT JOIN overview ov ON ls.symbol_id = ov.symbol_id
                    WHERE ls.asset_type = 'Stock' AND ov.symbol_id IS NULL
                    AND ls.exchange = %s
                """,
                    (ex,),
                )[0][0]
                print(f"Remaining {ex} symbols to process: {remaining}")


if __name__ == "__main__":
    main()

# -----------------------------------------------------------------------------
# Example command (PowerShell):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_overview.py \
#   --mode incremental \
#   --exchange NYSE \
#   --exchange NASDAQ \
#   --limit 10 \
#   --try-failed N \
#   --force-restart N
# -----------------------------------------------------------------------------

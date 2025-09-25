"""
Economic Indicators Extractor using incremental ETL architecture.
Uses source schema, watermarks, and deterministic processing.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
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
)

# API configuration
API_DELAY_SECONDS = 0.8  # Alpha Vantage rate limiting

# Economic indicator configurations: function_name -> (interval, display_name, maturity)
# For Treasury yields, maturity is specified; for others, it's None
ECONOMIC_INDICATOR_CONFIGS = {
    "REAL_GDP": ("quarterly", "Real GDP", None),
    "REAL_GDP_PER_CAPITA": ("quarterly", "Real GDP Per Capita", None),
    "TREASURY_YIELD_10YEAR": ("daily", "Treasury Yield 10 Year", "10year"),
    "TREASURY_YIELD_3MONTH": ("daily", "Treasury Yield 3 Month", "3month"),
    "TREASURY_YIELD_2YEAR": ("daily", "Treasury Yield 2 Year", "2year"),
    "TREASURY_YIELD_5YEAR": ("daily", "Treasury Yield 5 Year", "5year"),
    "TREASURY_YIELD_7YEAR": ("daily", "Treasury Yield 7 Year", "7year"),
    "TREASURY_YIELD_30YEAR": ("daily", "Treasury Yield 30 Year", "30year"),
    "FEDERAL_FUNDS_RATE": ("daily", "Federal Funds Rate", None),
    "CPI": ("monthly", "Consumer Price Index", None),
    "INFLATION": ("monthly", "Inflation Rate", None),
    "RETAIL_SALES": ("monthly", "Retail Sales", None),
    "DURABLES": ("monthly", "Durable Goods Orders", None),
    "UNEMPLOYMENT": ("monthly", "Unemployment Rate", None),
    "NONFARM_PAYROLL": ("monthly", "Total Nonfarm Payroll", None),
}

# Schema-driven field mapping configuration
ECONOMIC_INDICATORS_FIELDS = {
    'economic_indicator_id': 'economic_indicator_id',
    'indicator_name': 'indicator_name',
    'function_name': 'function_name',
    'maturity': 'maturity',
    'date': 'date',
    'interval': 'interval',
    'unit': 'unit',
    'value': 'value',
    'name': 'name',
    'api_response': 'api_response',
    'api_response_status': 'api_response_status',
    'run_id': 'run_id',
    'created_at': 'created_at',
    'updated_at': 'updated_at',
    'content_hash': 'content_hash',
}


class EconomicIndicatorsExtractor:
    """
    Extracts economic indicators data from Alpha Vantage API using modern ETL architecture.

    Features:
    - Source schema storage with landing table pattern
    - Incremental processing with content hash deduplication
    - Watermark management for tracking extraction progress
    - API response caching with deterministic hashing
    - Configurable batch processing with rate limiting
    """

    def __init__(self):
        # Load environment variables
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        # Initialize components
        self.db_manager = PostgresDatabaseManager()
        self.base_url = "https://www.alphavantage.co/query"

        # ETL utilities
        self.content_hasher = ContentHasher()
        self.date_utils = DateUtils()
        self.run_id_generator = RunIdGenerator()

        # Initialize adaptive rate limiter for time series processing
        self.rate_limiter = AdaptiveRateLimiter(ExtractorType.TIME_SERIES, verbose=True)

    def _safe_json_dumps(self, response_data, record_count):
        """Safely serialize JSON response, handling large datasets that may fail."""
        try:
            # For very large datasets (>10k records), store a summary instead of full response
            if record_count > 10000:
                summary = {
                    "data_summary": f"Large dataset with {record_count:,} records",
                    "metadata": response_data.get("Meta Data", {}) if isinstance(response_data, dict) else {},
                    "record_count": record_count,
                    "extraction_time": datetime.now().isoformat(),
                    "note": "Full response truncated due to size"
                }
                return json.dumps(summary)
            return json.dumps(response_data)
        except (TypeError, ValueError, MemoryError) as e:
            # If JSON serialization fails, store error info
            error_summary = {
                "error": "JSON serialization failed",
                "error_type": str(type(e).__name__),
                "record_count": record_count,
                "extraction_time": datetime.now().isoformat()
            }
            return json.dumps(error_summary)

    def _ensure_source_schema(self):
        """Create source schema tables if they don't exist."""
        with self.db_manager as db:
            # Create economic_indicators table in source schema
            create_table_sql = """
                CREATE SCHEMA IF NOT EXISTS source;

                CREATE TABLE IF NOT EXISTS source.economic_indicators (
                    economic_indicator_id BIGSERIAL PRIMARY KEY,
                    indicator_name VARCHAR(100) NOT NULL,
                    function_name VARCHAR(50) NOT NULL,
                    maturity VARCHAR(20),
                    date DATE,
                    interval VARCHAR(15) NOT NULL CHECK (interval IN ('daily','monthly','quarterly')),
                    unit VARCHAR(50),
                    value NUMERIC(15,6),
                    name VARCHAR(255),
                    api_response JSONB,
                    api_response_status VARCHAR(20) NOT NULL,
                    run_id VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    content_hash VARCHAR(64) NOT NULL,
                    UNIQUE(indicator_name, function_name, maturity, date, interval)
                );

                CREATE INDEX IF NOT EXISTS idx_economic_indicators_name
                    ON source.economic_indicators(indicator_name);
                CREATE INDEX IF NOT EXISTS idx_economic_indicators_date
                    ON source.economic_indicators(date);
                CREATE INDEX IF NOT EXISTS idx_economic_indicators_status
                    ON source.economic_indicators(api_response_status);
                CREATE INDEX IF NOT EXISTS idx_economic_indicators_hash
                    ON source.economic_indicators(content_hash);
                CREATE INDEX IF NOT EXISTS idx_economic_indicators_run_id
                    ON source.economic_indicators(run_id);
            """
            db.execute_query(create_table_sql)
            print("✓ Source schema tables ensured")

    def _extract_indicator_data(self, function_key: str) -> tuple[pd.DataFrame | None, str, str, dict]:
        """
        Extract data for a single economic indicator from Alpha Vantage API.

        Returns:
            tuple: (dataframe, status, message, raw_response)
        """
        interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]

        # For Treasury yields, we need to specify the function and maturity
        if function_key.startswith("TREASURY_YIELD_"):
            actual_function = "TREASURY_YIELD"
            params = {
                "function": actual_function,
                "interval": interval,
                "maturity": maturity,
                "datatype": "json",
                "apikey": self.api_key,
            }
        else:
            # For other indicators, use the function key directly
            actual_function = function_key
            params = {
                "function": actual_function,
                "interval": interval,
                "datatype": "json",
                "apikey": self.api_key,
            }

        try:
            print(f"Extracting {display_name} data...")

            # Wait with adaptive rate limiting
            self.rate_limiter.pre_api_call()

            response = requests.get(self.base_url, params=params)
            response.raise_for_status()

            data = response.json()

            # Check for API error messages
            if "Error Message" in data:
                print(f"API Error for {function_key}: {data['Error Message']}")
                # Notify rate limiter of error
                self.rate_limiter.post_api_call('error')
                return None, "error", data.get("Error Message", "Unknown API error"), data

            if "Note" in data:
                print(f"API Note for {function_key}: {data['Note']}")
                # Notify rate limiter of rate limiting
                self.rate_limiter.post_api_call('rate_limited')
                return None, "error", data.get("Note", "API rate limit or other note"), data

            if "Information" in data:
                print(f"API Information for {function_key}: {data['Information']}")
                # Notify rate limiter of error
                self.rate_limiter.post_api_call('error')
                return None, "error", data.get("Information", "API information message"), data

            # Check if we have data
            if "data" not in data:
                print(f"No 'data' field found in response for {function_key}")
                # Notify rate limiter (successful API call but no data)
                self.rate_limiter.post_api_call('success')
                return None, "empty", "No data field in API response", data

            indicator_data = data["data"]
            if not indicator_data:
                print(f"Empty data array for {function_key}")
                # Notify rate limiter (successful API call but no data)
                self.rate_limiter.post_api_call('success')
                return None, "empty", "Empty data array", data

            # Extract metadata
            name = data.get("name", display_name)
            unit = data.get("unit", "")

            # Convert to DataFrame
            df = pd.DataFrame(indicator_data)
            df["indicator_name"] = display_name
            df["function_name"] = actual_function
            df["maturity"] = maturity
            df["interval"] = interval
            df["unit"] = unit
            df["name"] = name
            df["api_response_status"] = "success"

            # Convert value to float, handling NaN properly
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

            # Convert date
            df["date"] = pd.to_datetime(df["date"]).dt.date

            print(f"Successfully extracted {len(df)} records for {display_name}")
            # Notify rate limiter of successful API call
            self.rate_limiter.post_api_call('success')
            return df, "success", f"Successfully extracted {len(df)} records", data

        except requests.exceptions.RequestException as e:
            error_msg = f"Request error for {function_key}: {str(e)}"
            print(error_msg)
            # Notify rate limiter of error
            self.rate_limiter.post_api_call('error')
            return None, "error", error_msg, {}
        except Exception as e:
            error_msg = f"Unexpected error for {function_key}: {str(e)}"
            print(error_msg)
            # Notify rate limiter of error
            self.rate_limiter.post_api_call('error')
            return None, "error", error_msg, {}

    def _prepare_records_for_insertion(self, df: pd.DataFrame, raw_response: dict, run_id: str) -> list[tuple]:
        """Prepare DataFrame records for database insertion with content hashing."""
        if df is None or df.empty:
            return []

        records = []

        # Store raw API response as JSONB
        # Convert NaN values to None for JSON serialization
        response_for_json = json.loads(json.dumps(raw_response, default=str))

        for _, row in df.iterrows():
            # Create deterministic content for hashing
            # Include all the data fields that matter for change detection
            content_data = {
                'indicator_name': row['indicator_name'],
                'function_name': row['function_name'],
                'maturity': row['maturity'],
                'date': str(row['date']) if pd.notna(row['date']) else None,
                'interval': row['interval'],
                'unit': row['unit'],
                'value': float(row['value']) if pd.notna(row['value']) else None,
                'name': row['name'],
                'api_response_status': row['api_response_status'],
            }

            # Generate content hash
            content_hash = self.content_hasher.calculate_business_content_hash(content_data)

            # Handle NaN values for database insertion
            value = None if pd.isna(row['value']) else float(row['value'])
            date_val = None if pd.isna(row['date']) else row['date']

            record = (
                row['indicator_name'],
                row['function_name'],
                row['maturity'],
                date_val,
                row['interval'],
                row['unit'],
                value,
                row['name'],
                self._safe_json_dumps(response_for_json, len(df)),  # Safe JSON serialization for large responses
                row['api_response_status'],
                run_id,
                content_hash,
            )
            records.append(record)

        return records

    def _upsert_records(self, db, records: list[tuple]) -> int:
        """Insert or update records using content hash for deduplication."""
        if not records:
            return 0

        # For large datasets, check existing hashes first to avoid unnecessary upserts
        if len(records) > 1000:
            return self._upsert_records_optimized(db, records)

        insert_query = """
            INSERT INTO source.economic_indicators
            (indicator_name, function_name, maturity, date, interval, unit, value, name,
             api_response, api_response_status, run_id, content_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (indicator_name, date, content_hash)
            DO UPDATE SET
                unit = EXCLUDED.unit,
                value = EXCLUDED.value,
                name = EXCLUDED.name,
                api_response = EXCLUDED.api_response,
                api_response_status = EXCLUDED.api_response_status,
                run_id = EXCLUDED.run_id,
                updated_at = NOW()
            WHERE source.economic_indicators.content_hash != EXCLUDED.content_hash
        """

        return db.execute_many(insert_query, records)

    def _upsert_records_optimized(self, db, records: list[tuple]) -> int:
        """Optimized upsert for large datasets - check existing data first."""
        print(f"  Optimizing upsert for {len(records):,} records...")

        # For very large datasets, check if this indicator already exists
        if records:
            sample_record = records[0]
            indicator_name = sample_record[0]
            function_name = sample_record[1]
            maturity = sample_record[2]
            interval = sample_record[4]

            # Quick check: do we have any existing data for this exact indicator?
            count_query = """
                SELECT COUNT(*) FROM source.economic_indicators
                WHERE indicator_name = %s AND function_name = %s
                AND (maturity = %s OR (maturity IS NULL AND %s IS NULL))
                AND interval = %s
            """
            existing_count = db.fetch_query(count_query, (indicator_name, function_name, maturity, maturity, interval))
            existing_count = existing_count[0][0] if existing_count else 0

            print(f"  Found {existing_count:,} existing records for {indicator_name}")

            # If we have roughly the same amount of data, assume it's the same and skip
            if existing_count > 0 and abs(existing_count - len(records)) < 100:
                print(f"  Skipping {len(records):,} records (data appears current)")
                return 0

        # If we get here, we need to do the upsert
        # For very large datasets, batch the operations
        batch_size = 5000
        total_inserted = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            print(f"  Processing batch {i//batch_size + 1}/{(len(records) + batch_size - 1)//batch_size} ({len(batch):,} records)")

            insert_query = """
                INSERT INTO source.economic_indicators
                (indicator_name, function_name, maturity, date, interval, unit, value, name,
                 api_response, api_response_status, run_id, content_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (indicator_name, date, content_hash)
                DO NOTHING
            """

            batch_inserted = db.execute_many(insert_query, batch)
            total_inserted += batch_inserted

        return total_inserted

    def _record_status_only(self, db, indicator_name: str, function_name: str, maturity: str,
                           interval: str, status: str, message: str, raw_response: dict, run_id: str):
        """Record extraction status (empty/error) without data."""
        # Create content hash for status record
        content_data = {
            'indicator_name': indicator_name,
            'function_name': function_name,
            'maturity': maturity,
            'interval': interval,
            'status': status,
            'message': message,
        }
        content_hash = self.content_hasher.calculate_business_content_hash(content_data)

        # Convert raw response for JSON storage
        response_for_json = json.loads(json.dumps(raw_response, default=str))

        # Check if status record already exists with same content
        check_query = """
            SELECT economic_indicator_id FROM source.economic_indicators
            WHERE indicator_name = %s AND function_name = %s
              AND interval = %s AND api_response_status = %s AND date IS NULL
              AND (maturity = %s OR (maturity IS NULL AND %s IS NULL))
              AND content_hash = %s
        """
        existing = db.fetch_query(
            check_query,
            (indicator_name, function_name, interval, status, maturity, maturity, content_hash),
        )

        if existing:
            print(f"Status record already exists for {indicator_name}: {status}")
            return

        # Insert new status record
        insert_query = """
            INSERT INTO source.economic_indicators
            (indicator_name, function_name, maturity, date, interval, unit, value, name,
             api_response, api_response_status, run_id, content_hash)
            VALUES (%s, %s, %s, NULL, %s, NULL, NULL, %s, %s, %s, %s, %s)
        """

        db.execute_query(
            insert_query,
            (indicator_name, function_name, maturity, interval, message,
             json.dumps(response_for_json), status, run_id, content_hash),
        )
        print(f"Recorded {status} status for {indicator_name}")

    def extract_single_indicator(self, function_key: str) -> tuple[int, str]:
        """Extract and load data for a single economic indicator."""
        if function_key not in ECONOMIC_INDICATOR_CONFIGS:
            raise ValueError(f"Unknown indicator: {function_key}")

        interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]
        actual_function = function_key if not function_key.startswith("TREASURY_YIELD_") else "TREASURY_YIELD"

        run_id = self.run_id_generator.generate()

        # Extract data
        df, status, message, raw_response = self._extract_indicator_data(function_key)

        # Process data in single database transaction
        inserted_count = 0
        with self.db_manager as db:
            if status == "success" and df is not None:
                # Prepare and insert data records
                records = self._prepare_records_for_insertion(df, raw_response, run_id)
                inserted_count = self._upsert_records(db, records)

                # Log summary
                total_records = len(records)
                updated_count = total_records - inserted_count
                print(f"✓ {display_name}: {inserted_count} new, {updated_count} updated ({total_records} total)")

            else:
                # Record status without data
                self._record_status_only(
                    db, display_name, actual_function, maturity, interval,
                    status, message, raw_response, run_id
                )

        return inserted_count, status

    def extract_batch(self, indicator_list: list[str] | None = None,
                     batch_size: int = 5) -> dict[str, Any]:
        """
        Extract multiple economic indicators with batch processing and rate limiting.

        Args:
            indicator_list: List of indicator function keys to extract (None = all)
            batch_size: Number of indicators to process before pausing

        Returns:
            Dict with extraction summary
        """
        # Ensure schema exists
        self._ensure_source_schema()

        if indicator_list is None:
            indicator_list = list(ECONOMIC_INDICATOR_CONFIGS.keys())

        print(f"Starting economic indicators extraction for {len(indicator_list)} indicators...")
        print(f"Batch size: {batch_size}")
        print("-" * 60)

        # Initialize adaptive rate limiting
        self.rate_limiter.start_processing()

        # Initialize tracking
        total_inserted = 0
        status_summary = {"success": 0, "empty": 0, "error": 0}
        start_time = datetime.now()

        for i, function_key in enumerate(indicator_list):
            if function_key not in ECONOMIC_INDICATOR_CONFIGS:
                print(f"⚠ Unknown indicator: {function_key}")
                continue

            display_name = ECONOMIC_INDICATOR_CONFIGS[function_key][1]
            print(f"Processing {i+1}/{len(indicator_list)}: {display_name}")

            try:
                # Extract each indicator individually with its own database connection
                inserted_count, status = self._extract_indicator_with_fresh_connection(function_key)
                total_inserted += inserted_count
                status_summary[status] += 1

            except Exception as e:
                print(f"✗ Error processing {display_name}: {str(e)}")
                status_summary["error"] += 1
                continue

            print("-" * 40)

        # Final summary
        duration = datetime.now() - start_time
        print("\n" + "=" * 60)
        print("ECONOMIC INDICATORS EXTRACTION SUMMARY")
        print("=" * 60)
        print(f"Total indicators processed: {len(indicator_list)}")
        print(f"Total records inserted: {total_inserted}")
        print(f"Duration: {duration}")
        print("Status breakdown:")
        for status, count in status_summary.items():
            print(f"  {status}: {count}")
        print("=" * 60)

        return {
            "total_processed": len(indicator_list),
            "total_inserted": total_inserted,
            "duration": duration,
            "status_summary": status_summary,
        }

    def _should_skip_api_call(self, function_key: str) -> bool:
        """
        Check if we should skip the API call for this indicator based on existing data.

        Args:
            function_key: The function key for the indicator

        Returns:
            bool: True if we should skip the API call
        """
        try:
            interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]

            # Only skip daily indicators (API calls are expensive and daily data is frequent)
            if interval != "daily":
                return False

            db_manager = PostgresDatabaseManager()
            with db_manager as db:
                # Check when we last got data for this indicator
                query = """
                    SELECT MAX(date) as latest_date, COUNT(*) as record_count
                    FROM source.economic_indicators
                    WHERE function_name = %s
                """
                result = db.fetch_query(query, (function_key,))

                if not result:
                    return False

                latest_date, record_count = result[0]

                if not latest_date or record_count == 0:
                    return False

                # For daily indicators, skip if we have data within the last 3 days
                from datetime import datetime
                if isinstance(latest_date, str):
                    latest_date = datetime.strptime(latest_date, '%Y-%m-%d').date()

                days_since_latest = (datetime.now().date() - latest_date).days

                if days_since_latest <= 3:
                    print(f"  Latest data: {latest_date} ({days_since_latest} days old, {record_count:,} records)")
                    return True

                return False

        except Exception as e:
            print(f"Error checking if should skip API call for {function_key}: {e}")
            return False
        """Check if we should skip the API call for this indicator based on recent data."""
        interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]
        actual_function = function_key if not function_key.startswith("TREASURY_YIELD_") else "TREASURY_YIELD"

        # Only check for daily indicators with large datasets to avoid unnecessary API calls
        if interval != "daily":
            return False

        try:
            db_manager = PostgresDatabaseManager()
            with db_manager as db:
                # Check if we have recent data (within last 3 days for daily indicators)
                recent_check_query = """
                    SELECT COUNT(*), MAX(date) as latest_date
                    FROM source.economic_indicators
                    WHERE indicator_name = %s AND function_name = %s
                    AND (maturity = %s OR (maturity IS NULL AND %s IS NULL))
                    AND interval = %s
                    AND date >= CURRENT_DATE - INTERVAL '3 days'
                """

                result = db.fetch_query(recent_check_query,
                    (display_name, actual_function, maturity, maturity, interval))

                if result and result[0][0] > 0:
                    recent_count, latest_date = result[0]
                    print(f"  Found recent data for {display_name}: {recent_count} records, latest: {latest_date}")
                    return True

        except Exception as e:
            print(f"  Error checking recent data: {e}")
            return False

        return False

    def _extract_indicator_with_fresh_connection(self, function_key: str) -> tuple[int, str]:
        """Extract and load data for a single economic indicator with fresh database connection."""
        if function_key not in ECONOMIC_INDICATOR_CONFIGS:
            raise ValueError(f"Unknown indicator: {function_key}")

        interval, display_name, maturity = ECONOMIC_INDICATOR_CONFIGS[function_key]
        actual_function = function_key if not function_key.startswith("TREASURY_YIELD_") else "TREASURY_YIELD"

        # Check if we should skip the API call for this indicator
        if self._should_skip_api_call(function_key):
            print(f"✓ {display_name}: Skipped (recent data already exists)")
            return 0, "success"

        run_id = self.run_id_generator.generate()

        # Extract data
        df, status, message, raw_response = self._extract_indicator_data(function_key)

        # Process data with fresh database connection
        inserted_count = 0
        db_manager = PostgresDatabaseManager()

        with db_manager as db:
            if status == "success" and df is not None:
                # Prepare and insert data records
                records = self._prepare_records_for_insertion(df, raw_response, run_id)
                inserted_count = self._upsert_records(db, records)

                # Log summary
                total_records = len(records)
                updated_count = total_records - inserted_count
                print(f"✓ {display_name}: {inserted_count} new, {updated_count} updated ({total_records} total)")

            else:
                # Record status without data
                self._record_status_only(
                    db, display_name, actual_function, maturity, interval,
                    status, message, raw_response, run_id
                )

        return inserted_count, status

    def get_database_summary(self) -> dict[str, Any]:
        """Get comprehensive summary of economic indicators data in the database."""
        # Use a fresh database connection for summary
        db_manager = PostgresDatabaseManager()

        with db_manager as db:
            # Total records by status
            status_query = """
                SELECT api_response_status, COUNT(*) as count
                FROM source.economic_indicators
                GROUP BY api_response_status
                ORDER BY api_response_status
            """
            status_results = db.fetch_query(status_query) or []

            # Data records by indicator
            indicator_query = """
                SELECT indicator_name, interval, COUNT(*) as count,
                       MIN(date) as earliest, MAX(date) as latest
                FROM source.economic_indicators
                WHERE api_response_status = 'success' AND date IS NOT NULL
                GROUP BY indicator_name, interval, maturity
                ORDER BY indicator_name, interval
            """
            indicator_results = db.fetch_query(indicator_query) or []

            # Latest values for each indicator
            latest_query = """
                SELECT e1.indicator_name, e1.interval, e1.date, e1.value, e1.unit
                FROM source.economic_indicators e1
                INNER JOIN (
                    SELECT indicator_name, function_name, maturity, interval, MAX(date) as max_date
                    FROM source.economic_indicators
                    WHERE api_response_status = 'success' AND date IS NOT NULL
                    GROUP BY indicator_name, function_name, maturity, interval
                ) e2 ON e1.indicator_name = e2.indicator_name
                     AND e1.function_name = e2.function_name
                     AND (e1.maturity = e2.maturity OR (e1.maturity IS NULL AND e2.maturity IS NULL))
                     AND e1.interval = e2.interval
                     AND e1.date = e2.max_date
                ORDER BY e1.indicator_name, e1.interval
            """
            latest_results = db.fetch_query(latest_query) or []

        print("\n" + "=" * 70)
        print("ECONOMIC INDICATORS DATABASE SUMMARY")
        print("=" * 70)

        print("\nRecords by Status:")
        total_records = 0
        for status, count in status_results:
            print(f"  {status}: {count:,}")
            total_records += count
        print(f"  Total: {total_records:,}")

        print("\nData Records by Indicator:")
        if indicator_results:
            for indicator, interval, count, earliest, latest in indicator_results:
                print(f"  {indicator} ({interval}): {count:,} records from {earliest} to {latest}")
        else:
            print("  No data records found")

        print("\nLatest Values:")
        if latest_results:
            for indicator, interval, date, value, unit in latest_results:
                if value is not None:
                    if unit and "%" in unit:
                        print(f"  {indicator} ({interval}): {value:.2f}% on {date}")
                    elif unit and ("thousands" in unit or "millions" in unit):
                        print(f"  {indicator} ({interval}): {value:,.0f} {unit} on {date}")
                    else:
                        print(f"  {indicator} ({interval}): {value:.2f} {unit} on {date}")
                else:
                    print(f"  {indicator} ({interval}): No value on {date}")
        else:
            print("  No latest values found")

        print("=" * 70)

        return {
            "total_records": total_records,
            "status_breakdown": dict(status_results),
            "data_indicators": len(indicator_results),
            "latest_values": len(latest_results),
        }


def main():
    """Command-line interface for economic indicators extraction."""
    parser = argparse.ArgumentParser(description="Extract economic indicators data from Alpha Vantage API")
    parser.add_argument(
        "--indicators",
        nargs="+",
        choices=list(ECONOMIC_INDICATOR_CONFIGS.keys()),
        help="Specific indicators to extract (default: all)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Number of indicators to process per batch (default: 5)"
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show database summary after extraction"
    )

    args = parser.parse_args()

    try:
        extractor = EconomicIndicatorsExtractor()

        # Run extraction
        if args.indicators:
            print(f"Extracting specific indicators: {', '.join(args.indicators)}")
        else:
            print("Extracting all configured indicators")

        extractor.extract_batch(
            indicator_list=args.indicators,
            batch_size=args.batch_size
        )

        # Show summary if requested
        if args.summary:
            print("\n" + "="*50)
            print("DATABASE SUMMARY")
            print("="*50)
            extractor.get_database_summary()

    except Exception as e:
        print(f"✗ Extraction failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

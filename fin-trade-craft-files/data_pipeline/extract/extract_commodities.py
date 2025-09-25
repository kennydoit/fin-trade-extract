"""
Commodities Extractor using incremental ETL architecture.
Uses source schema, watermarks, and deterministic processing.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db and utils
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.adaptive_rate_limiter import AdaptiveRateLimiter, ExtractorType
from utils.incremental_etl import (
    ContentHasher,
    RunIdGenerator,
    WatermarkManager,
)

# API configuration
API_DELAY_SECONDS = 0.8  # Alpha Vantage rate limiting

# Commodity configurations: function_name -> (interval, display_name)
COMMODITY_CONFIGS = {
    "WTI": ("daily", "Crude Oil WTI"),
    "BRENT": ("daily", "Crude Oil Brent"),
    "NATURAL_GAS": ("daily", "Natural Gas"),
    "COPPER": ("monthly", "Copper"),
    "ALUMINUM": ("monthly", "Aluminum"),
    "WHEAT": ("monthly", "Wheat"),
    "CORN": ("monthly", "Corn"),
    "COTTON": ("monthly", "Cotton"),
    "SUGAR": ("monthly", "Sugar"),
    "COFFEE": ("monthly", "Coffee"),
    "ALL_COMMODITIES": ("monthly", "Global Commodities Index"),
}

# Schema-driven field mapping configuration
COMMODITIES_FIELDS = {
    'commodity_id': 'commodity_id',
    'commodity_name': 'commodity_name',
    'function_name': 'function_name',
    'date': 'date',
    'interval': 'interval',
    'unit': 'unit',
    'value': 'value',
    'name': 'name',
}


class CommoditiesExtractor:
    """Commodities extractor with incremental processing."""

    def __init__(self):
        """Initialize the extractor."""
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        self.table_name = "commodities"
        self.schema_name = "source"
        self.db_manager = None
        self.watermark_manager = None
        self.base_url = "https://www.alphavantage.co/query"

        # Initialize adaptive rate limiter for time series processing
        self.rate_limiter = AdaptiveRateLimiter(ExtractorType.TIME_SERIES, verbose=True)

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
            # Read and execute the source schema first
            schema_file = Path(__file__).parent.parent.parent / "db" / "schema" / "source_schema.sql"
            if schema_file.exists():
                with schema_file.open() as f:
                    schema_sql = f.read()
                db.execute_script(schema_sql)
                print("✅ Source schema initialized")
            else:
                print(f"⚠️ Schema file not found: {schema_file}")

            # Add commodities table to source schema
            commodities_table_sql = """
                -- Make symbol_id nullable in landing table for non-stock data
                ALTER TABLE source.api_responses_landing
                ALTER COLUMN symbol_id DROP NOT NULL;

                -- Drop and recreate the foreign key constraint to allow NULLs
                ALTER TABLE source.api_responses_landing
                DROP CONSTRAINT IF EXISTS api_responses_landing_symbol_id_fkey;

                ALTER TABLE source.api_responses_landing
                ADD CONSTRAINT api_responses_landing_symbol_id_fkey
                FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE;

                -- Commodities table with modern schema
                CREATE TABLE IF NOT EXISTS source.commodities (
                    commodity_id SERIAL PRIMARY KEY,
                    commodity_name VARCHAR(100) NOT NULL,      -- Standardized display name
                    function_name VARCHAR(50) NOT NULL,        -- Alpha Vantage function name
                    date DATE,                                 -- Data date (NULL for status records)
                    interval VARCHAR(10) NOT NULL,             -- 'daily' or 'monthly'
                    unit VARCHAR(50),                          -- Price unit (e.g., 'USD per barrel')
                    value DECIMAL(15,6),                       -- Commodity price/value
                    name VARCHAR(255),                         -- Full name from API

                    -- ETL metadata
                    api_response_status VARCHAR(20) NOT NULL,  -- 'success', 'empty', 'error'
                    content_hash VARCHAR(32) NOT NULL,         -- For change detection
                    source_run_id UUID NOT NULL,               -- Links to landing table
                    fetched_at TIMESTAMPTZ NOT NULL,           -- When API was called
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),

                    -- Natural key constraint for idempotent upserts
                    UNIQUE(commodity_name, function_name, date, interval)
                );

                -- Indexes for commodities
                CREATE INDEX IF NOT EXISTS idx_source_commodities_name_date ON source.commodities(commodity_name, date);
                CREATE INDEX IF NOT EXISTS idx_source_commodities_fetched_at ON source.commodities(fetched_at);
                CREATE INDEX IF NOT EXISTS idx_source_commodities_content_hash ON source.commodities(content_hash);
                CREATE INDEX IF NOT EXISTS idx_source_commodities_interval ON source.commodities(interval);

                COMMENT ON TABLE source.commodities IS 'Clean commodities data with deterministic natural keys';
            """

            db.execute_script(commodities_table_sql)
            print("✅ Commodities table initialized in source schema")

        except Exception as e:
            print(f"⚠️ Schema initialization error: {e}")

    def _fetch_api_data(self, function_name: str) -> tuple[pd.DataFrame, str]:
        """
        Fetch commodities data from Alpha Vantage API.

        Args:
            function_name: Alpha Vantage function name

        Returns:
            Tuple of (dataframe, status)
        """
        interval, display_name = COMMODITY_CONFIGS[function_name]

        params = {
            "function": function_name,
            "interval": interval,
            "datatype": "json",
            "apikey": self.api_key,
        }

        try:
            print(f"Fetching {display_name} data...")

            # Wait with adaptive rate limiting
            self.rate_limiter.pre_api_call()

            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Check for API error messages
            if "Error Message" in data:
                print(f"API Error for {function_name}: {data['Error Message']}")
                # Notify rate limiter of error
                self.rate_limiter.post_api_call('error')
                return pd.DataFrame(), "error"

            if "Note" in data:
                print(f"API Note for {function_name}: {data['Note']}")
                # Notify rate limiter of rate limiting
                self.rate_limiter.post_api_call('rate_limited')
                return pd.DataFrame(), "rate_limited"

            if "Information" in data:
                print(f"API Information for {function_name}: {data['Information']}")
                # Notify rate limiter of error
                self.rate_limiter.post_api_call('error')
                return pd.DataFrame(), "error"

            # Check if we have data
            if "data" not in data:
                print(f"No 'data' field found in response for {function_name}")
                # Notify rate limiter (successful API call but no data)
                self.rate_limiter.post_api_call('success')
                return pd.DataFrame(), "empty"

            commodity_data = data["data"]
            if not commodity_data:
                print(f"Empty data array for {function_name}")
                # Notify rate limiter (successful API call but no data)
                self.rate_limiter.post_api_call('success')
                return pd.DataFrame(), "empty"

            # Extract metadata
            name = data.get("name", display_name)
            unit = data.get("unit", "")

            # Convert to DataFrame
            df = pd.DataFrame(commodity_data)

            # Add metadata columns
            df["commodity_name"] = display_name
            df["function_name"] = function_name
            df["interval"] = interval
            df["unit"] = unit
            df["name"] = name

            # Convert value to float
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

            # Convert date
            df["date"] = pd.to_datetime(df["date"]).dt.date

            print(f"Successfully fetched {len(df)} records for {display_name}")
            # Notify rate limiter of successful API call
            self.rate_limiter.post_api_call('success')
            return df, "success"

        except requests.exceptions.RequestException as e:
            print(f"Request error for {function_name}: {str(e)}")
            # Notify rate limiter of error
            self.rate_limiter.post_api_call('error')
            return pd.DataFrame(), "error"
        except Exception as e:
            print(f"Unexpected error for {function_name}: {str(e)}")
            # Notify rate limiter of error
            self.rate_limiter.post_api_call('error')
            return pd.DataFrame(), "error"

    def _store_landing_record(self, db, function_name: str, df: pd.DataFrame,
                            status: str, run_id: str) -> str:
        """
        Store raw API response in landing table.

        Args:
            db: Database manager
            function_name: Alpha Vantage function name
            df: DataFrame with API response
            status: Response status
            run_id: Unique run ID

        Returns:
            Content hash of the response
        """
        # Convert DataFrame to dict for storage
        if not df.empty:
            # Convert DataFrame copy to handle date serialization and NaN values
            df_for_json = df.copy()
            # Convert date objects to strings for JSON serialization
            if 'date' in df_for_json.columns:
                df_for_json['date'] = df_for_json['date'].astype(str)
            # Replace NaN values with None for JSON serialization
            df_for_json = df_for_json.replace({np.nan: None})

            api_response = {
                "data": df_for_json.to_dict("records"),
                "columns": list(df.columns),
                "row_count": len(df),
                "function_name": function_name
            }
        else:
            api_response = {"data": [], "columns": [], "row_count": 0, "function_name": function_name}

        content_hash = ContentHasher.calculate_api_response_hash(api_response)

        insert_query = """
            INSERT INTO source.api_responses_landing
            (table_name, symbol, symbol_id, api_function, api_response,
             content_hash, source_run_id, response_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # For commodities, we use function_name as symbol and NULL as symbol_id (no FK constraint)
        db.execute_query(insert_query, (
            self.table_name, function_name, None, function_name,
            json.dumps(api_response), content_hash, run_id, status
        ))

        return content_hash

    def _transform_data(self, function_name: str, df: pd.DataFrame,
                       run_id: str) -> list[dict[str, Any]]:
        """
        Transform API response to standardized records.

        Args:
            function_name: Alpha Vantage function name
            df: DataFrame with API response
            run_id: Unique run ID

        Returns:
            List of transformed records
        """
        if df.empty:
            return []

        records = []

        try:
            # Process each row
            for _, row in df.iterrows():
                # Initialize record with all fields as None
                record = dict.fromkeys(COMMODITIES_FIELDS.keys())

                # Set known values
                record.update({
                    "commodity_name": row["commodity_name"],
                    "function_name": row["function_name"],
                    "date": row["date"],
                    "interval": row["interval"],
                    "unit": row["unit"],
                    "value": row["value"],
                    "name": row["name"],
                    "api_response_status": "success",
                    "source_run_id": run_id,
                    "fetched_at": datetime.now()
                })

                # Validate required fields
                if not record["date"] or pd.isna(record["value"]):
                    print(f"Missing date or value for {function_name} record: {row['date']}, {row['value']}")
                    continue

                # Calculate content hash for change detection
                record["content_hash"] = ContentHasher.calculate_business_content_hash(record)

                records.append(record)

            print(f"Transformed {len(records)} records for {function_name}")
            return records

        except Exception as e:
            print(f"Error transforming data for {function_name}: {e}")
            return []

    def _content_has_changed(self, db, function_name: str, content_hash: str) -> bool:
        """
        Check if content has changed based on hash comparison.

        Args:
            db: Database manager
            function_name: Alpha Vantage function name
            content_hash: New content hash

        Returns:
            True if content has changed or is new
        """
        query = """
            SELECT COUNT(*) FROM source.commodities
            WHERE function_name = %s AND content_hash = %s
        """

        result = db.fetch_query(query, (function_name, content_hash))
        return result[0][0] == 0 if result else True  # True if no matching hash found

    def _upsert_records(self, db, records: list[dict[str, Any]]) -> int:
        """
        Upsert records into the commodities table.

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
                  if col not in ['commodity_id', 'created_at', 'updated_at']]

        # Build upsert query
        placeholders = ", ".join(["%s" for _ in columns])
        update_columns = [col for col in columns
                         if col not in ['commodity_name', 'function_name', 'date', 'interval']]
        update_set = [f"{col} = EXCLUDED.{col}" for col in update_columns]
        update_set.append("updated_at = NOW()")

        upsert_query = f"""
            INSERT INTO source.commodities ({', '.join(columns)}, created_at, updated_at)
            VALUES ({placeholders}, NOW(), NOW())
            ON CONFLICT (commodity_name, function_name, date, interval)
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

    def extract_commodity(self, function_name: str, db) -> dict[str, Any]:
        """
        Extract commodities data for a single function.

        Args:
            function_name: Alpha Vantage function name
            db: Database manager (passed from caller)

        Returns:
            Processing result summary
        """
        if function_name not in COMMODITY_CONFIGS:
            return {
                "function_name": function_name,
                "status": "invalid_function",
                "records_processed": 0,
                "run_id": None
            }

        run_id = RunIdGenerator.generate()
        interval, display_name = COMMODITY_CONFIGS[function_name]

        # Fetch API data
        df, status = self._fetch_api_data(function_name)

        # Store in landing table (always)
        content_hash = self._store_landing_record(
            db, function_name, df, status, run_id
        )

        # Process if successful
        if status == "success":
            # Check if content has changed
            if not self._content_has_changed(db, function_name, content_hash):
                print(f"No changes detected for {function_name}, skipping transformation")
                return {
                    "function_name": function_name,
                    "commodity_name": display_name,
                    "status": "no_changes",
                    "records_processed": 0,
                    "run_id": run_id
                }

            # Transform data
            records = self._transform_data(function_name, df, run_id)

            if records:
                # Upsert records
                rows_affected = self._upsert_records(db, records)

                # Get latest date for summary
                latest_date = max(
                    r['date'] for r in records
                    if r['date']
                )

                return {
                    "function_name": function_name,
                    "commodity_name": display_name,
                    "status": "success",
                    "records_processed": len(records),
                    "rows_affected": rows_affected,
                    "latest_date": latest_date,
                    "run_id": run_id
                }
            # No valid records
            return {
                "function_name": function_name,
                "commodity_name": display_name,
                "status": "no_valid_records",
                "records_processed": 0,
                "run_id": run_id
            }
        # API failure - store status record
        self._store_status_record(db, function_name, status, run_id)
        return {
            "function_name": function_name,
            "commodity_name": display_name,
            "status": "api_failure",
            "error": status,
            "records_processed": 0,
            "run_id": run_id
        }

    def _store_status_record(self, db, function_name: str, status: str, run_id: str):
        """Store a status record for failed extractions."""
        interval, display_name = COMMODITY_CONFIGS[function_name]

        record = {
            "commodity_name": display_name,
            "function_name": function_name,
            "date": None,  # NULL for status records
            "interval": interval,
            "unit": None,
            "value": None,
            "name": display_name,
            "api_response_status": status,
            "content_hash": ContentHasher.calculate_business_content_hash({"status": status}),
            "source_run_id": run_id,
            "fetched_at": datetime.now()
        }

        # Insert status record
        columns = [col for col in record
                  if col not in ['commodity_id', 'created_at', 'updated_at']]
        placeholders = ", ".join(["%s" for _ in columns])

        insert_query = f"""
            INSERT INTO source.commodities ({', '.join(columns)}, created_at, updated_at)
            VALUES ({placeholders}, NOW(), NOW())
        """

        values = [record[col] for col in columns]
        db.execute_query(insert_query, tuple(values))

    def run_incremental_extraction(self, function_list: list[str] | None = None,
                                 batch_size: int = 5) -> dict[str, Any]:
        """
        Run incremental extraction for commodities.

        Args:
            function_list: List of function names to process (default: all)
            batch_size: Number of functions to process in each batch

        Returns:
            Processing summary
        """
        if function_list is None:
            function_list = list(COMMODITY_CONFIGS.keys())

        print("🚀 Starting incremental commodities extraction...")
        print(f"Configuration: functions={len(function_list)}, batch_size={batch_size}")

        # Initialize adaptive rate limiting
        self.rate_limiter.start_processing()

        with self._get_db_manager() as db:
            # Ensure schema exists
            self._ensure_schema_exists(db)

            print(f"Found {len(function_list)} commodities to process")

            if not function_list:
                print("✅ No commodities to process")
                return {
                    "functions_processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "no_changes": 0,
                    "total_records": 0
                }

            # Process commodities
            results = {
                "functions_processed": 0,
                "successful": 0,
                "failed": 0,
                "no_changes": 0,
                "total_records": 0,
                "details": []
            }

            for i, function_name in enumerate(function_list, 1):
                interval, display_name = COMMODITY_CONFIGS.get(function_name, ("unknown", function_name))

                print(f"Processing {display_name} ({function_name}) [{i}/{len(function_list)}]")

                # Extract commodity data
                result = self.extract_commodity(function_name, db)
                results["details"].append(result)
                results["functions_processed"] += 1

                if result["status"] == "success":
                    results["successful"] += 1
                    results["total_records"] += result["records_processed"]
                    print(f"✅ {display_name}: {result['records_processed']} records processed")
                elif result["status"] == "no_changes":
                    results["no_changes"] += 1
                    print(f"⚪ {display_name}: No changes detected")
                else:
                    results["failed"] += 1
                    print(f"❌ {display_name}: {result['status']}")

            print("\n🎯 Incremental extraction completed:")
            print(f"  Functions processed: {results['functions_processed']}")
            print(f"  Successful: {results['successful']}")
            print(f"  No changes: {results['no_changes']}")
            print(f"  Failed: {results['failed']}")
            print(f"  Total records: {results['total_records']}")

            return results


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Commodities Extractor")
    parser.add_argument("--functions", nargs="+",
                       help=f"Specific functions to process. Available: {list(COMMODITY_CONFIGS.keys())}")
    parser.add_argument("--batch-size", type=int, default=5,
                       help="Number of functions to process in each batch (default: 5)")

    args = parser.parse_args()

    # Validate function names if provided
    if args.functions:
        invalid_functions = [f for f in args.functions if f not in COMMODITY_CONFIGS]
        if invalid_functions:
            print(f"Invalid function names: {invalid_functions}")
            print(f"Available functions: {list(COMMODITY_CONFIGS.keys())}")
            sys.exit(1)

    extractor = CommoditiesExtractor()
    result = extractor.run_incremental_extraction(
        function_list=args.functions,
        batch_size=args.batch_size
    )

    # Exit with appropriate code
    if result["failed"] > 0 and result["successful"] == 0:
        sys.exit(1)  # All failed
    else:
        sys.exit(0)  # Some or all successful


if __name__ == "__main__":
    main()


# Sample usage calls:
# python extract_commodities.py
# python extract_commodities.py --functions WTI BRENT NATURAL_GAS
# python extract_commodities.py --batch-size 3

# -----------------------------------------------------------------------------
# Example commands (PowerShell):
#
# Extract all commodities:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_commodities.py
#
# Extract specific commodities:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_commodities.py --functions WTI BRENT NATURAL_GAS
#
# Extract daily commodities only:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_commodities.py --functions WTI BRENT NATURAL_GAS
#
# Extract monthly commodities only:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_commodities.py --functions COPPER ALUMINUM WHEAT CORN COTTON SUGAR COFFEE ALL_COMMODITIES
#
# Smaller batch size for rate limiting:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_commodities.py --batch-size 3
# -----------------------------------------------------------------------------

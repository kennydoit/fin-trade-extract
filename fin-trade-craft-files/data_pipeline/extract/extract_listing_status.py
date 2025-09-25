"""
Extract listing status data from Alpha Vantage API and load into database.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Add the parent directories to the path so we can import from db
sys.path.append(str(Path(__file__).parent.parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager
from utils.adaptive_rate_limiter import (  # -----------------------------------------------------------------------------
    AdaptiveRateLimiter,
    ExtractorType,
)
from utils.database_safety import DatabaseSafetyManager

# Example commands (PowerShell):
#
# Incremental update (default):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_listing_status.py
#
# Or explicitly:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_listing_status.py --mode update
#
# Full replacement:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_listing_status.py --mode replace
# -----------------------------------------------------------------------------s.database_safety import DatabaseSafetyManager

STOCK_API_FUNCTION = "LISTING_STATUS"


class ListingStatusExtractor:
    """Extract and load listing status data from Alpha Vantage API."""

    def __init__(self, mode="update"):
        """Initialize the extractor with specified mode.

        Args:
            mode (str): Operation mode - "update" for incremental updates (default), "replace" for full replacement
        """
        if mode not in ["replace", "update"]:
            raise ValueError("Mode must be 'replace' or 'update'")

        self.mode = mode

        # Load ALPHAVANTAGE_API_KEY from .env file
        load_dotenv()
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")

        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        self.db_manager = PostgresDatabaseManager()
        self.base_url = "https://www.alphavantage.co/query"

        # Initialize adaptive rate limiter for fundamentals processing
        self.rate_limiter = AdaptiveRateLimiter(ExtractorType.FUNDAMENTALS, verbose=True)

    def extract_data(self):
        """Extract listing status data from Alpha Vantage API for both active and delisted stocks."""
        print("Extracting listing status data from Alpha Vantage API...")

        all_data = []
        states = ['active', 'delisted']

        for state in states:
            print(f"Extracting {state} stocks...")
            url = f"{self.base_url}?function={STOCK_API_FUNCTION}&state={state}&apikey={self.api_key}"

            # Wait with adaptive rate limiting
            self.rate_limiter.pre_api_call()

            try:
                response = requests.get(url)
                response.raise_for_status()

                # Read CSV data directly from the response
                df = pd.read_csv(url)

                # Add state column to track which API call this data came from
                df['api_state'] = state

                print(f"Successfully extracted {len(df)} {state} records")
                all_data.append(df)

                # Notify rate limiter of successful API call
                self.rate_limiter.post_api_call('success')

            except requests.exceptions.RequestException as e:
                print(f"Error fetching {state} data from API: {e}")
                # Notify rate limiter of failed API call
                self.rate_limiter.post_api_call('error')
                # Continue with other state if one fails
                continue
            except pd.errors.EmptyDataError:
                print(f"No {state} data received from API")
                # Notify rate limiter (treat as successful API call but no data)
                self.rate_limiter.post_api_call('success')
                continue

        if not all_data:
            raise Exception("No data received from any API calls")

        # Combine all dataframes
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"Successfully extracted {len(combined_df)} total records")
        return combined_df

    def transform_data(self, df):
        """Transform the extracted data to match database schema."""
        print("Transforming data...")

        # Map API column names to database column names
        column_mapping = {
            "symbol": "symbol",
            "name": "name",
            "exchange": "exchange",
            "assetType": "asset_type",
            "ipoDate": "ipo_date",
            "delistingDate": "delisting_date",
            "status": "status",
            # Note: api_state is not mapped - status column already captures this information
        }

        # Rename columns to match database schema
        df_transformed = df.rename(columns=column_mapping)

        # Clean data - remove rows with null or empty symbols (required field)
        if "symbol" in df_transformed.columns:
            initial_count = len(df_transformed)
            df_transformed = df_transformed.dropna(subset=["symbol"])
            df_transformed = df_transformed[df_transformed["symbol"].str.strip() != ""]
            final_count = len(df_transformed)
            if initial_count != final_count:
                print(
                    f"Removed {initial_count - final_count} rows with null/empty symbols"
                )

        # Handle date columns - convert to proper date format or NULL
        date_columns = ["ipo_date", "delisting_date"]
        for col in date_columns:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_datetime(
                    df_transformed[col], errors="coerce"
                )
                df_transformed[col] = df_transformed[col].dt.strftime("%Y-%m-%d")
                df_transformed[col] = df_transformed[col].where(
                    pd.notna(df_transformed[col]), None
                )

        # Add timestamp columns
        current_timestamp = datetime.now().isoformat()
        df_transformed["created_at"] = current_timestamp
        df_transformed["updated_at"] = current_timestamp

        # Select only columns that exist in our database schema
        required_columns = [
            "symbol",
            "name",
            "exchange",
            "asset_type",
            "ipo_date",
            "delisting_date",
            "status",  # Status column captures active/delisted state
            "created_at",
            "updated_at",
        ]

        # Keep only columns that exist in both dataframe and required columns
        available_columns = [
            col for col in required_columns if col in df_transformed.columns
        ]
        df_final = df_transformed[available_columns]

        # Handle duplicate symbols - keep the most recent record (using status column to determine priority)
        # First check for duplicates
        initial_count = len(df_transformed)
        duplicate_symbols = df_transformed[df_transformed.duplicated(subset=['symbol'], keep=False)]

        if len(duplicate_symbols) > 0:
            print(f"Found {len(duplicate_symbols)} duplicate symbol entries")
            # Use api_state for sorting since it's more reliable than status for this purpose
            if 'api_state' in df_transformed.columns:
                df_sorted = df_transformed.sort_values('api_state')  # 'active' comes before 'delisted'
                # Keep last occurrence (delisted if both exist)
                df_deduped = df_sorted.drop_duplicates(subset=['symbol'], keep='last')
                print(f"Removed {initial_count - len(df_deduped)} duplicate symbols (kept most recent status)")
            else:
                # If no api_state column, just remove duplicates keeping the last one
                df_deduped = df_transformed.drop_duplicates(subset=['symbol'], keep='last')
                print(f"Removed {initial_count - len(df_deduped)} duplicate symbols")
        else:
            df_deduped = df_transformed
            print("No duplicate symbols found")

        # Now select only the required columns
        df_final = df_deduped[available_columns]

        # Sort by symbol to ensure consistent ordering in the database
        df_final = df_final.sort_values('symbol')
        print("✅ Data sorted by symbol for consistent database ordering")

        # Final check for duplicates
        final_duplicates = df_final[df_final.duplicated(subset=['symbol'])]
        if len(final_duplicates) > 0:
            print(f"WARNING: Still have {len(final_duplicates)} duplicates in final data!")
            print(f"Sample duplicates: {final_duplicates['symbol'].head().tolist()}")

        print(f"Transformed data with columns: {list(df_final.columns)}")
        print(f"Final record count: {len(df_final)}")
        print("✅ Status column captures active/delisted state information")
        return df_final

    def load_data(self, df):
        """Load transformed data into the database using the specified mode."""
        if self.mode == "update":
            return self._load_data_update(df)
        return self._load_data_replace(df)

    def _load_data_replace(self, df):
        """Load transformed data into the database, replacing all existing records."""
        print("Loading data into database (REPLACE mode)...")

        # Initialize safety manager
        safety_manager = DatabaseSafetyManager(enable_backups=True, enable_checks=True)

        with self.db_manager as db:
            # Check if the table exists in the extracted schema
            table_exists_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'extracted'
                    AND table_name = 'listing_status'
                );
            """
            result = db.fetch_query(table_exists_query)
            table_exists = result[0][0] if result else False

            if not table_exists:
                print("Table extracted.listing_status does not exist!")
                print("Please run the schema initialization first or check your database setup.")
                raise Exception("Table extracted.listing_status not found")

            # SAFE data replacement using safety manager - with CASCADE protection
            print("🛡️ Performing SAFE data replacement...")
            print("⚠️ WARNING: This will replace all listing_status data while preserving symbol_id relationships")

            # Check if there are dependent records
            safety_manager = DatabaseSafetyManager(enable_backups=True, enable_checks=True)
            has_dependents = not safety_manager.verify_no_cascade_risk('listing_status')

            if has_dependents:
                print("🔄 Using UPSERT strategy to preserve symbol_id relationships...")
                # Use UPSERT approach instead of DELETE to preserve foreign key relationships
                self._perform_upsert_replacement(db, df)
            else:
                print("✅ No dependent data found - safe to use DELETE approach")
                if not safety_manager.safe_delete_table_data('listing_status', 'listing_status_replace'):
                    raise Exception("Safe deletion failed - operation aborted")
                self._perform_insert_replacement(db, df)

            # Verify integrity after operation
            safety_manager.verify_table_integrity('listing_status')

    def _load_data_update(self, df):
        """Update existing listing status records and insert new ones."""
        print("Loading data into database (UPDATE mode)...")

        with self.db_manager as db:
            # Check if the table exists in the extracted schema
            if not db.table_exists("listing_status", schema_name="extracted"):
                raise Exception("Table extracted.listing_status does not exist")

            # Fetch existing data
            existing_df = db.fetch_dataframe(
                """
                SELECT symbol_id, symbol, name, exchange, asset_type,
                       ipo_date, delisting_date, status,
                       created_at, updated_at
                FROM extracted.listing_status
                """
            )
            existing_df = existing_df.set_index("symbol")

            columns_to_compare = [
                "name",
                "exchange",
                "asset_type",
                "ipo_date",
                "delisting_date",
                "status",
            ]

            new_records = []
            update_records = []

            for _, row in df.iterrows():
                symbol = row["symbol"]
                current_values = row[columns_to_compare].to_dict()

                # Convert NaN values to None for proper comparison
                for key, value in current_values.items():
                    if pd.isna(value):
                        current_values[key] = None

                if symbol not in existing_df.index:
                    # New symbol - prepare for insert
                    record = {
                        "symbol": symbol,
                        **current_values,
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                    }
                    new_records.append(record)
                else:
                    # Existing symbol - check if update needed
                    existing_row = existing_df.loc[symbol]
                    changed = False
                    for col in columns_to_compare:
                        existing_val = existing_row[col]
                        if pd.isna(existing_val):
                            existing_val = None
                        if existing_val != current_values[col]:
                            changed = True
                            break
                    if changed:
                        record = {
                            "symbol": symbol,
                            **current_values,
                            "created_at": row["created_at"],
                            "updated_at": row["updated_at"],
                        }
                        update_records.append(record)

            # Insert new records (sorted by symbol for consistent ordering)
            if new_records:
                # Sort new records by symbol to maintain alphabetical order in database
                new_records_sorted = sorted(new_records, key=lambda x: x["symbol"])

                insert_cols = [
                    "symbol",
                    "name",
                    "exchange",
                    "asset_type",
                    "ipo_date",
                    "delisting_date",
                    "status",
                    "created_at",
                    "updated_at",
                ]
                placeholders = ", ".join(["%s"] * len(insert_cols))
                insert_query = f"""
                    INSERT INTO extracted.listing_status ({', '.join(insert_cols)})
                    VALUES ({placeholders})
                """
                insert_params = [
                    [rec[col] for col in insert_cols] for rec in new_records_sorted
                ]
                db.execute_many(insert_query, insert_params)
                print(f"✅ Inserted {len(new_records)} new symbols (ordered by symbol)")
            else:
                print("ℹ️ No new symbols to insert")

            # Update existing records
            if update_records:
                update_query = """
                    UPDATE extracted.listing_status
                    SET name = %s,
                        exchange = %s,
                        asset_type = %s,
                        ipo_date = %s,
                        delisting_date = %s,
                        status = %s,
                        created_at = %s,
                        updated_at = %s
                    WHERE symbol = %s
                """
                update_params = []
                for rec in update_records:
                    update_params.append(
                        [
                            rec["name"],
                            rec["exchange"],
                            rec["asset_type"],
                            rec.get("ipo_date"),
                            rec.get("delisting_date"),
                            rec["status"],
                            rec["created_at"],
                            rec["updated_at"],
                            rec["symbol"],
                        ]
                    )
                db.execute_many(update_query, update_params)
                print(f"✅ Updated {len(update_records)} existing symbols")
            else:
                print("ℹ️ No existing symbols required updates")

            print(f"✅ Successfully processed {len(df)} records in UPDATE mode")
            print(f"   - New symbols: {len(new_records)}")
            print(f"   - Updated symbols: {len(update_records)}")
            print(f"   - Unchanged symbols: {len(df) - len(new_records) - len(update_records)}")

    def _perform_upsert_replacement(self, db, df):
        """Replace data using UPSERT to preserve existing symbol_id values."""
        print("🔄 Performing UPSERT-based replacement (preserves symbol_id relationships)...")

        # Get existing symbol -> symbol_id mapping
        existing_mapping = {}
        try:
            existing_data = db.fetch_query("SELECT symbol, symbol_id FROM extracted.listing_status")
            existing_mapping = {row[0]: row[1] for row in existing_data}
            print(f"📊 Found {len(existing_mapping)} existing symbols with preserved IDs")
        except Exception as e:
            print(f"⚠️ Could not fetch existing mapping: {e}")

        # Track statistics
        preserved_ids = 0
        new_symbols = 0
        updated_symbols = 0

        # Process each record with UPSERT
        for _index, row in df.iterrows():
            symbol = row['symbol']
            data_dict = row.to_dict()

            # Convert NaN values to None
            for key, value in data_dict.items():
                if pd.isna(value):
                    data_dict[key] = None

            if symbol in existing_mapping:
                # Preserve existing symbol_id
                data_dict['symbol_id'] = existing_mapping[symbol]
                preserved_ids += 1
                updated_symbols += 1
            else:
                # New symbol - will get auto-generated symbol_id
                if 'symbol_id' in data_dict:
                    del data_dict['symbol_id']  # Let database auto-generate
                new_symbols += 1

            # Perform UPSERT
            try:
                if symbol in existing_mapping:
                    # Update existing record
                    update_cols = [col for col in data_dict if col != 'symbol_id']
                    update_set = ', '.join([f"{col} = %s" for col in update_cols])
                    update_query = f"""
                        UPDATE extracted.listing_status
                        SET {update_set}
                        WHERE symbol_id = %s
                    """
                    update_values = [data_dict[col] for col in update_cols] + [data_dict['symbol_id']]
                    db.execute_query(update_query, update_values)
                else:
                    # Insert new record
                    insert_cols = list(data_dict.keys())
                    placeholders = ', '.join(['%s'] * len(insert_cols))
                    insert_query = f"""
                        INSERT INTO extracted.listing_status ({', '.join(insert_cols)})
                        VALUES ({placeholders})
                    """
                    insert_values = [data_dict[col] for col in insert_cols]
                    db.execute_query(insert_query, insert_values)

            except Exception as e:
                print(f"❌ Error upserting {symbol}: {e}")
                raise

        # Remove symbols that are no longer in the new data
        if existing_mapping:
            new_symbols_set = set(df['symbol'].tolist())
            symbols_to_remove = set(existing_mapping.keys()) - new_symbols_set

            if symbols_to_remove:
                print(f"🗑️ Removing {len(symbols_to_remove)} symbols no longer in source data...")
                for symbol in symbols_to_remove:
                    symbol_id = existing_mapping[symbol]
                    db.execute_query("DELETE FROM extracted.listing_status WHERE symbol_id = %s", [symbol_id])

        print("✅ UPSERT replacement completed:")
        print(f"   - Preserved symbol_id relationships: {preserved_ids}")
        print(f"   - New symbols added: {new_symbols}")
        print(f"   - Existing symbols updated: {updated_symbols}")
        print(f"   - Symbols removed: {len(symbols_to_remove) if 'symbols_to_remove' in locals() else 0}")

    def _perform_insert_replacement(self, db, df):
        """Replace data using INSERT after DELETE (when no foreign key dependencies exist)."""
        print("📥 Performing INSERT-based replacement (no foreign key dependencies)...")

        # Reset the sequence for symbol_id if it exists
        try:
            db.execute_query("ALTER SEQUENCE IF EXISTS extracted.listing_status_symbol_id_seq RESTART WITH 1;")
        except Exception as e:
            print(f"Note: Could not reset sequence (this is normal if no sequence exists): {e}")

        # Prepare data for batch insert
        records = []
        for _index, row in df.iterrows():
            data_dict = row.to_dict()
            # Convert NaN values to None for proper NULL handling
            for key, value in data_dict.items():
                if pd.isna(value):
                    data_dict[key] = None
            records.append(data_dict)

        # Build the INSERT query
        if records:
            # Note: Records are already sorted by symbol from transform_data()
            columns = list(records[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)

            insert_query = f"""
                INSERT INTO extracted.listing_status ({columns_str})
                VALUES ({placeholders})
            """

            # Prepare data for batch insert (maintains symbol order from DataFrame)
            data_to_insert = []
            for record in records:
                row_values = [record[col] for col in columns]
                data_to_insert.append(row_values)

            # Execute batch insert
            db.execute_many(insert_query, data_to_insert)

        print(f"✅ Successfully replaced all records with {len(df)} new records in listing_status table (ordered by symbol)")

    def run_etl(self):
        """Run the complete ETL process."""
        print(f"Starting Listing Status ETL process ({self.mode.upper()} mode)...")

        # Initialize adaptive rate limiting
        self.rate_limiter.start_processing()

        try:
            # Extract
            raw_data = self.extract_data()

            # Transform
            transformed_data = self.transform_data(raw_data)

            # Load
            self.load_data(transformed_data)

            print(f"ETL process completed successfully ({self.mode.upper()} mode)!")

        except Exception as e:
            print(f"ETL process failed ({self.mode.upper()} mode): {e}")
            raise


def main():
    """Main function to run the listing status extraction via CLI."""
    parser = argparse.ArgumentParser(description="Extract/Update listing status data from Alpha Vantage")
    parser.add_argument(
        "--mode",
        choices=["replace", "update"],
        default="update",
        help="Operation mode: 'update' for incremental updates (default), 'replace' for full data replacement"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes (future enhancement)"
    )

    args = parser.parse_args()

    if args.dry_run:
        print("⚠️ Dry-run mode is not yet implemented")
        return

    print(f"🚀 Starting in {args.mode.upper()} mode")
    extractor = ListingStatusExtractor(mode=args.mode)
    extractor.run_etl()


if __name__ == "__main__":
    main()

# ----------------------------------------------------------------------------
# Example commands (PowerShell):
#
# Incremental update (default):
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_listing_status.py
# --mode update
#
# Or explicitly:
# & .\.venv\Scripts\python.exe data_pipeline\extract\extract_listing_status.py
# --mode replace
# -----------------------------------------------------------------------------

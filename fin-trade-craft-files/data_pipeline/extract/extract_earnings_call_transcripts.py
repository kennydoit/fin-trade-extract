"""
Earnings Call Transcripts Extractor using modern incremental ETL architecture.
Uses source schema, watermarks, content hashing, and adaptive rate limiting for optimal performance.
"""

import argparse
import json
import os
import sys
from datetime import date, datetime
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
    RunIdGenerator,
    WatermarkManager,
)

# API configuration
STOCK_API_FUNCTION = "EARNINGS_CALL_TRANSCRIPT"
TABLE_NAME = "earnings_call_transcripts"

class EarningsCallTranscriptsExtractor:
    """Modern earnings call transcripts extractor with adaptive rate limiting and incremental processing."""

    def __init__(self, db_manager: PostgresDatabaseManager):
        """
        Initialize the extractor.

        Args:
            db_manager: Database connection manager
        """
        self.db = db_manager
        self.watermark_manager = WatermarkManager(db_manager)
        self.run_id = RunIdGenerator.generate()

        # Load API key
        load_dotenv()
        self.api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY not found in environment variables")

        self.base_url = "https://www.alphavantage.co/query"

        # Initialize adaptive rate limiter for earnings calls (very heavy text processing)
        self.rate_limiter = AdaptiveRateLimiter(ExtractorType.EARNINGS_CALLS, verbose=True)

        # Ensure source table exists
        self._ensure_source_table_exists()

        # Ensure watermarks table exists
        self._ensure_watermarks_table_exists()

        # Precompute quarters from current quarter back to 2010Q1
        self.quarters = self._generate_quarters()
        print(f"Initialized with {len(self.quarters)} quarters from {self.quarters[0]} to {self.quarters[-1]}")

    def _ensure_source_table_exists(self):
        """Ensure the source.earnings_call_transcripts table exists."""
        create_table_sql = """
            CREATE SCHEMA IF NOT EXISTS source;

            CREATE TABLE IF NOT EXISTS source.earnings_call_transcripts (
                transcript_id       SERIAL PRIMARY KEY,
                symbol_id           INTEGER NOT NULL,
                symbol              VARCHAR(20) NOT NULL,
                quarter             VARCHAR(10) NOT NULL,  -- Format: YYYYQM (e.g., 2024Q1)
                speaker             VARCHAR(255) NOT NULL,
                title               VARCHAR(255),
                content             TEXT NOT NULL,
                content_hash        VARCHAR(32) NOT NULL,  -- MD5 hash of content for uniqueness
                sentiment           DECIMAL(5,3),  -- Sentiment score (e.g., 0.6, 0.7)
                api_response_status VARCHAR(20) DEFAULT 'pass',
                source_run_id       VARCHAR(36) NOT NULL,
                fetched_at          TIMESTAMP DEFAULT NOW(),
                created_at          TIMESTAMP DEFAULT NOW(),
                updated_at          TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (symbol_id) REFERENCES source.listing_status(symbol_id) ON DELETE CASCADE,
                UNIQUE(symbol_id, quarter, speaker, content_hash)  -- Use hash instead of full content
            );

            -- Create indexes for earnings call transcripts
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol_id ON source.earnings_call_transcripts(symbol_id);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol ON source.earnings_call_transcripts(symbol);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_quarter ON source.earnings_call_transcripts(quarter);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_speaker ON source.earnings_call_transcripts(speaker);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_sentiment ON source.earnings_call_transcripts(sentiment);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_content_hash ON source.earnings_call_transcripts(content_hash);
            CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_run_id ON source.earnings_call_transcripts(source_run_id);
        """

        # Create trigger for updated_at
        trigger_sql = """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_trigger
                    WHERE tgname = 'update_earnings_call_transcripts_updated_at'
                    AND tgrelid = 'source.earnings_call_transcripts'::regclass
                ) THEN
                    CREATE TRIGGER update_earnings_call_transcripts_updated_at
                    BEFORE UPDATE ON source.earnings_call_transcripts
                    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                END IF;
            END $$;
        """

        try:
            self.db.execute_query(create_table_sql)
            self.db.execute_query(trigger_sql)
            print("✓ Ensured source.earnings_call_transcripts table exists")
        except Exception as e:
            print(f"Warning: Could not create table/trigger: {e}")

    def _ensure_watermarks_table_exists(self):
        """Ensure the source.extraction_watermarks table exists."""
        create_watermarks_sql = """
            CREATE TABLE IF NOT EXISTS source.extraction_watermarks (
                watermark_id        SERIAL PRIMARY KEY,
                table_name          VARCHAR(100) NOT NULL,
                symbol_id           INTEGER NOT NULL,
                last_fiscal_date    DATE,
                last_successful_run TIMESTAMP,
                consecutive_failures INTEGER DEFAULT 0,
                created_at          TIMESTAMP DEFAULT NOW(),
                updated_at          TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (symbol_id) REFERENCES source.listing_status(symbol_id) ON DELETE CASCADE,
                UNIQUE(table_name, symbol_id)
            );

            CREATE INDEX IF NOT EXISTS idx_extraction_watermarks_table_symbol ON source.extraction_watermarks(table_name, symbol_id);
            CREATE INDEX IF NOT EXISTS idx_extraction_watermarks_last_run ON source.extraction_watermarks(last_successful_run);
        """

        trigger_sql = """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_trigger
                    WHERE tgname = 'update_extraction_watermarks_updated_at'
                    AND tgrelid = 'source.extraction_watermarks'::regclass
                ) THEN
                    CREATE TRIGGER update_extraction_watermarks_updated_at
                    BEFORE UPDATE ON source.extraction_watermarks
                    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                END IF;
            END $$;
        """

        try:
            self.db.execute_query(create_watermarks_sql)
            self.db.execute_query(trigger_sql)
            print("✓ Ensured source.extraction_watermarks table exists")
        except Exception as e:
            print(f"Warning: Could not create watermarks table/trigger: {e}")

    def _generate_quarters(self) -> list[str]:
        """Generate quarters from current quarter back to 2010Q1."""
        quarters = []
        current = datetime.now()
        current_year = current.year
        current_quarter = (current.month - 1) // 3 + 1

        for year in range(current_year, 2009, -1):
            if year == current_year:
                quarters_range = range(current_quarter, 0, -1)
            else:
                quarters_range = range(4, 0, -1)
            for quarter in quarters_range:
                quarters.append(f"{year}Q{quarter}")

        return quarters

    def get_symbols_needing_processing(self,
                                     staleness_hours: int = 24,
                                     max_failures: int = 3,
                                     limit: int | None = None,
                                     exchange_filter: list[str] | None = None) -> list[dict[str, Any]]:
        """
        Get symbols that need earnings call transcript processing.

        Args:
            staleness_hours: Hours before data is considered stale
            max_failures: Maximum consecutive failures before giving up
            limit: Maximum number of symbols to return
            exchange_filter: Filter by exchanges (e.g., ['NASDAQ', 'NYSE'])

        Returns:
            List of symbol data needing processing
        """
        # Use the basic query since watermark manager might need adjustment for schema
        query = """
            SELECT ls.symbol_id, ls.symbol,
                   ew.last_fiscal_date, ew.last_successful_run, ew.consecutive_failures
            FROM source.listing_status ls
            LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id
                                                       AND ew.table_name = %s
            WHERE ls.asset_type = 'Stock'
              AND LOWER(ls.status) = 'active'
              AND ls.symbol NOT LIKE '%%WS%%'   -- Exclude warrants
              AND ls.symbol NOT LIKE '%%R'     -- Exclude rights
              AND ls.symbol NOT LIKE '%%R%%'   -- Exclude rights variants
              AND ls.symbol NOT LIKE '%%P%%'   -- Exclude preferred shares
              AND ls.symbol NOT LIKE '%%U'      -- Exclude units (SPACs)
              AND ls.symbol NOT LIKE '%%U'     -- Exclude unit variants
              AND (
                  ew.last_successful_run IS NULL  -- Never processed
                  OR ew.last_successful_run < NOW() - INTERVAL '1 hour' * %s  -- Stale
              )
              AND COALESCE(ew.consecutive_failures, 0) < %s  -- Not permanently failed
        """

        params = [TABLE_NAME, staleness_hours, max_failures]

        if exchange_filter:
            if isinstance(exchange_filter, list):
                placeholders = ','.join(['%s' for _ in exchange_filter])
                query += f" AND ls.exchange IN ({placeholders})"
                params.extend(exchange_filter)
            else:
                query += " AND ls.exchange = %s"
                params.append(exchange_filter)

        query += """
            ORDER BY
                CASE WHEN ew.last_successful_run IS NULL THEN 0 ELSE 1 END,
                COALESCE(ew.last_successful_run, '1900-01-01'::timestamp) ASC,
                LENGTH(ls.symbol) ASC,
                ls.symbol ASC
        """

        if limit:
            query += " LIMIT %s"
            params.append(limit)

        results = self.db.fetch_query(query, params)

        return [
            {
                'symbol_id': row[0],
                'symbol': row[1],
                'last_fiscal_date': row[2],
                'last_successful_run': row[3],
                'consecutive_failures': row[4] or 0
            }
            for row in results
        ]

    def extract_api_data(self, symbol: str, quarter: str) -> tuple[dict | None, str]:
        """
        Extract earnings call transcript data from Alpha Vantage API.

        Args:
            symbol: Stock symbol
            quarter: Quarter in format YYYYQM (e.g., 2024Q1)

        Returns:
            Tuple of (api_data, status) where status is 'success', 'no_data', or 'error'
        """
        print(f"  Fetching {symbol} {quarter} from API...")

        url = f'{self.base_url}?function={STOCK_API_FUNCTION}&symbol={symbol}&quarter={quarter}&apikey={self.api_key}'

        # Adaptive rate limiting - smart delay based on elapsed time and processing overhead
        self.rate_limiter.pre_api_call()

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Check for API errors
            if 'Error Message' in data:
                print(f"    API Error: {data['Error Message']}")
                return None, 'error'

            if 'Note' in data:
                print(f"    API Note: {data['Note']}")
                return None, 'error'

            # Check if we have transcript data
            if 'transcript' not in data or not data['transcript']:
                print("    No transcript data available")
                return None, 'no_data'

            print(f"    ✓ Retrieved {len(data['transcript'])} transcript entries")
            return data, 'success'

        except requests.exceptions.RequestException as e:
            print(f"    ✗ Request failed: {e}")
            return None, 'error'
        except json.JSONDecodeError as e:
            print(f"    ✗ JSON decode failed: {e}")
            return None, 'error'
        except Exception as e:
            print(f"    ✗ Unexpected error: {e}")
            return None, 'error'

    def get_quarters_for_symbol(self, symbol_data: dict[str, Any]) -> list[str]:
        """
        Get quarters to process for a symbol based on IPO date and current date.

        Args:
            symbol_data: Symbol information including IPO date

        Returns:
            List of quarters to process in reverse chronological order
        """
        symbol_id = symbol_data['symbol_id']

        # Get IPO date from source.listing_status
        query = """
            SELECT ipo_date, delisting_date
            FROM source.listing_status
            WHERE symbol_id = %s
        """
        result = self.db.fetch_query(query, (symbol_id,))

        if not result:
            # Fallback to all quarters
            return self.quarters

        ipo_date, delisting_date = result[0]

        if not ipo_date:
            # No IPO date info, process all quarters
            return self.quarters

        # Parse dates
        def parse_date(d):
            if not d:
                return None
            if isinstance(d, str):
                try:
                    return datetime.strptime(d, "%Y-%m-%d").date()
                except ValueError:
                    return None
            if isinstance(d, datetime):
                return d.date()
            return d

        def to_year_quarter(dt: date):
            return dt.year, (dt.month - 1) // 3 + 1

        ipo_dt = parse_date(ipo_date)
        delist_dt = parse_date(delisting_date)
        start_dt = delist_dt or datetime.now().date()
        earliest_dt = max(date(2010, 1, 1), ipo_dt) if ipo_dt else date(2010, 1, 1)

        start_year, start_quarter = to_year_quarter(start_dt)
        end_year, end_quarter = to_year_quarter(earliest_dt)

        quarters = []
        year, quarter = start_year, start_quarter
        while year > end_year or (year == end_year and quarter >= end_quarter):
            quarters.append(f"{year}Q{quarter}")
            quarter -= 1
            if quarter == 0:
                quarter = 4
                year -= 1

        return quarters

    def get_existing_quarters(self, symbol_id: int) -> set:
        """
        Get quarters that already have data for a symbol.

        Args:
            symbol_id: Symbol ID

        Returns:
            Set of quarters already processed
        """
        query = """
            SELECT DISTINCT quarter
            FROM source.earnings_call_transcripts
            WHERE symbol_id = %s
        """

        result = self.db.fetch_query(query, (symbol_id,))
        return {row[0] for row in result} if result else set()

    def transform_transcript_data(self, symbol_data: dict[str, Any], quarter: str,
                                api_data: dict | None, status: str) -> list[dict[str, Any]]:
        """
        Transform API response data into database records.

        Args:
            symbol_data: Symbol information
            quarter: Quarter being processed
            api_data: Raw API response data
            status: Processing status ('success', 'no_data', 'error')

        Returns:
            List of transformed records
        """
        symbol_id = symbol_data['symbol_id']
        symbol = symbol_data['symbol']
        current_timestamp = datetime.now()

        # Handle non-success cases
        if status == 'no_data':
            content = 'No transcript data available'
            return [{
                'symbol_id': symbol_id,
                'symbol': symbol,
                'quarter': quarter,
                'speaker': 'NO_DATA',
                'title': None,
                'content': content,
                'content_hash': ContentHasher.calculate_business_content_hash({
                    'symbol_id': symbol_id,
                    'quarter': quarter,
                    'speaker': 'NO_DATA',
                    'content': content
                }),
                'sentiment': None,
                'api_response_status': 'no_data',
                'source_run_id': self.run_id,
                'fetched_at': current_timestamp,
                'created_at': current_timestamp,
                'updated_at': current_timestamp
            }]

        if status == 'error' or not api_data:
            content = 'API Error'
            return [{
                'symbol_id': symbol_id,
                'symbol': symbol,
                'quarter': quarter,
                'speaker': 'ERROR',
                'title': None,
                'content': content,
                'content_hash': ContentHasher.calculate_business_content_hash({
                    'symbol_id': symbol_id,
                    'quarter': quarter,
                    'speaker': 'ERROR',
                    'content': content
                }),
                'sentiment': None,
                'api_response_status': 'error',
                'source_run_id': self.run_id,
                'fetched_at': current_timestamp,
                'created_at': current_timestamp,
                'updated_at': current_timestamp
            }]

        # Transform successful transcript data
        records = []

        try:
            for transcript_entry in api_data['transcript']:
                # Helper function to convert sentiment values
                def convert_sentiment(value):
                    if value is None or value in {'', 'None'}:
                        return None
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None

                speaker = transcript_entry.get('speaker', 'Unknown')
                title = transcript_entry.get('title')
                content = transcript_entry.get('content', '')
                sentiment = convert_sentiment(transcript_entry.get('sentiment'))

                # Calculate business content hash
                business_data = {
                    'symbol_id': symbol_id,
                    'quarter': quarter,
                    'speaker': speaker,
                    'title': title,
                    'content': content,
                    'sentiment': sentiment
                }
                content_hash = ContentHasher.calculate_business_content_hash(business_data)

                record = {
                    'symbol_id': symbol_id,
                    'symbol': symbol,
                    'quarter': quarter,
                    'speaker': speaker,
                    'title': title,
                    'content': content,
                    'content_hash': content_hash,
                    'sentiment': sentiment,
                    'api_response_status': 'pass',
                    'source_run_id': self.run_id,
                    'fetched_at': current_timestamp,
                    'created_at': current_timestamp,
                    'updated_at': current_timestamp
                }

                records.append(record)

            print(f"    ✓ Transformed {len(records)} transcript records")
            return records

        except Exception as e:
            print(f"    ✗ Transform error: {e}")
            # Return error record on transform failure
            content = f'Transform Error: {str(e)}'
            return [{
                'symbol_id': symbol_id,
                'symbol': symbol,
                'quarter': quarter,
                'speaker': 'TRANSFORM_ERROR',
                'title': None,
                'content': content,
                'content_hash': ContentHasher.calculate_business_content_hash({
                    'symbol_id': symbol_id,
                    'quarter': quarter,
                    'speaker': 'TRANSFORM_ERROR',
                    'content': content
                }),
                'sentiment': None,
                'api_response_status': 'error',
                'source_run_id': self.run_id,
                'fetched_at': current_timestamp,
                'created_at': current_timestamp,
                'updated_at': current_timestamp
            }]

    def load_records(self, records: list[dict[str, Any]]) -> int:
        """
        Load transformed records into the database.

        Args:
            records: List of transformed records

        Returns:
            Number of records loaded
        """
        if not records:
            return 0

        # Prepare insert query with conflict handling
        columns = list(records[0].keys())
        placeholders = ', '.join(['%s' for _ in columns])

        insert_query = f"""
            INSERT INTO source.earnings_call_transcripts ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (symbol_id, quarter, speaker, content_hash)
            DO UPDATE SET
                title = EXCLUDED.title,
                content = EXCLUDED.content,
                sentiment = EXCLUDED.sentiment,
                api_response_status = EXCLUDED.api_response_status,
                source_run_id = EXCLUDED.source_run_id,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at
        """

        # Convert records to tuples
        record_tuples = [tuple(record[col] for col in columns) for record in records]

        # Execute bulk insert
        rows_affected = self.db.execute_many(insert_query, record_tuples)
        print(f"    ✓ Loaded {rows_affected} records to database")
        return rows_affected

    def process_symbol(self, symbol_data: dict[str, Any],
                      force_refresh: bool = False) -> dict[str, Any]:
        """
        Process earnings call transcripts for a single symbol.

        Args:
            symbol_data: Symbol information
            force_refresh: Force processing even if recently processed

        Returns:
            Processing statistics
        """
        symbol_id = symbol_data['symbol_id']
        symbol = symbol_data['symbol']

        print(f"Processing {symbol} (ID: {symbol_id})")

        # Check if processing is needed
        if not force_refresh and not self.watermark_manager.needs_processing(TABLE_NAME, symbol_id):
            print(f"  ↷ Skipping {symbol} - recently processed")
            return {
                'symbol': symbol,
                'symbol_id': symbol_id,
                    'status': 'skipped',
                    'api_calls': 0,
                    'records_loaded': 0,
                    'quarters_processed': 0
                }

        # Get quarters to process
        available_quarters = self.get_quarters_for_symbol(symbol_data)
        existing_quarters = self.get_existing_quarters(symbol_id)
        missing_quarters = [q for q in available_quarters if q not in existing_quarters]

        if not missing_quarters:
            print(f"  ✓ {symbol} already has data for all quarters")
            self.watermark_manager.update_watermark(TABLE_NAME, symbol_id, success=True)
            return {
                'symbol': symbol,
                'symbol_id': symbol_id,
                'status': 'complete',
                'api_calls': 0,
                'records_loaded': 0,
                'quarters_processed': 0
            }

        print(f"  Processing {len(missing_quarters)} missing quarters (out of {len(available_quarters)} total)")

        total_records = 0
        api_calls = 0
        quarters_processed = 0
        consecutive_no_data = 0
        has_success = False

        # Process missing quarters
        for quarter in missing_quarters:
            api_calls += 1
            quarters_processed += 1

            # Extract data from API
            api_data, status = self.extract_api_data(symbol, quarter)

            # Transform data
            records = self.transform_transcript_data(symbol_data, quarter, api_data, status)

            # Load records
            if records:
                loaded_count = self.load_records(records)
                total_records += loaded_count

                if status == 'success':
                    has_success = True
                    consecutive_no_data = 0
                elif status == 'no_data':
                    consecutive_no_data += 1
                else:
                    consecutive_no_data = 0

            # Early stopping for consecutive no_data
            if consecutive_no_data >= 4:
                print("  ↷ Stopping early after 4 consecutive no_data quarters")
                break

            # Notify rate limiter about API call result
            api_status = 'success' if status == 'success' else ('rate_limited' if 'rate' in str(status).lower() else 'error')
            self.rate_limiter.post_api_call(api_status)

        # Update watermark
        self.watermark_manager.update_watermark(TABLE_NAME, symbol_id, success=has_success)

        result_status = 'success' if has_success else 'no_data_or_error'
        print(f"  ✓ Completed {symbol}: {quarters_processed} quarters, {api_calls} API calls, {total_records} records")

        return {
            'symbol': symbol,
            'symbol_id': symbol_id,
            'status': result_status,
            'api_calls': api_calls,
            'records_loaded': total_records,
            'quarters_processed': quarters_processed
        }

    def run_extraction(self,
                      limit: int | None = None,
                      exchange_filter: list[str] | None = None,
                      staleness_hours: int = 24,
                      max_failures: int = 3,
                      force_refresh: bool = False,
                      dry_run: bool = False,
                      use_dcs: bool = False,
                      min_dcs: float = 0.0) -> dict[str, Any]:
        """
        Run the earnings call transcripts extraction process.

        Args:
            limit: Maximum number of symbols to process
            exchange_filter: Filter by exchanges (e.g., ['NASDAQ', 'NYSE'])
            staleness_hours: Hours before data is considered stale
            max_failures: Maximum consecutive failures before giving up
            force_refresh: Force processing even if recently processed
            dry_run: Only show what would be processed
            use_dcs: Enable Data Coverage Score prioritization
            min_dcs: Minimum DCS score for symbol selection

        Returns:
            Extraction statistics
        """
        print("🚀 Starting Earnings Call Transcripts Extraction")
        print(f"   Run ID: {self.run_id}")
        print(f"   Exchange filter: {exchange_filter or 'All'}")
        print(f"   Limit: {limit or 'No limit'}")
        print(f"   Staleness threshold: {staleness_hours} hours")
        print(f"   Max failures: {max_failures}")
        print(f"   Force refresh: {force_refresh}")
        print(f"   Dry run: {dry_run}")
        print(f"   Use DCS: {use_dcs}")
        print(f"   Min DCS: {min_dcs}")

        # Initialize adaptive rate limiting
        self.rate_limiter.start_processing()

        # Get symbols needing processing with DCS prioritization if requested
        if use_dcs:
            print(f"🎯 Using Data Coverage Score prioritization with min_dcs={min_dcs}")
            try:
                # Use watermark manager for DCS-based selection
                watermark_mgr = WatermarkManager(self.db)
                symbols_to_process = watermark_mgr.get_symbols_needing_processing_with_dcs(
                    TABLE_NAME,  # earnings_call_transcripts
                    staleness_hours=staleness_hours,
                    limit=limit,
                    quarterly_gap_detection=False,  # Not applicable for earnings transcripts
                    enable_pre_screening=True,
                    min_dcs_threshold=min_dcs
                )
                print(f"✅ DCS prioritization successful: {len(symbols_to_process)} symbols selected")
            except Exception as e:
                print(f"⚠️ DCS prioritization failed ({e}), falling back to standard method")
                symbols_to_process = self.get_symbols_needing_processing(
                    staleness_hours=staleness_hours,
                    max_failures=max_failures,
                    limit=limit,
                    exchange_filter=exchange_filter
                )
        else:
            # Standard processing without DCS
            symbols_to_process = self.get_symbols_needing_processing(
                staleness_hours=staleness_hours,
                max_failures=max_failures,
                limit=limit,
                exchange_filter=exchange_filter
            )

        print(f"\nFound {len(symbols_to_process)} symbols needing processing")

        if dry_run:
            print("\n🧮 DRY RUN - Would process:")
            for i, symbol_data in enumerate(symbols_to_process[:10]):  # Show first 10
                quarters = self.get_quarters_for_symbol(symbol_data)
                existing = self.get_existing_quarters(symbol_data['symbol_id'])
                missing = len([q for q in quarters if q not in existing])
                print(f"  {i+1:3d}. {symbol_data['symbol']:<8} - {missing:3d} missing quarters")

            if len(symbols_to_process) > 10:
                print(f"  ... and {len(symbols_to_process) - 10} more symbols")

            # Estimate API calls
            total_api_calls = 0
            for symbol_data in symbols_to_process:
                quarters = self.get_quarters_for_symbol(symbol_data)
                existing = self.get_existing_quarters(symbol_data['symbol_id'])
                missing = len([q for q in quarters if q not in existing])
                total_api_calls += min(missing, 20)  # Estimate with early stopping

            estimated_time = (total_api_calls * 0.5) / 60  # Estimated with adaptive rate limiting
            print(f"\n📊 Estimated API calls: {total_api_calls:,}")
            print(f"⏱️  Estimated time: {estimated_time:.1f} minutes")

            return {
                'dry_run': True,
                'symbols_found': len(symbols_to_process),
                'estimated_api_calls': total_api_calls,
                'estimated_time_minutes': estimated_time
            }

        if not symbols_to_process:
            print("✓ No symbols need processing")
            return {
                'symbols_processed': 0,
                'total_api_calls': 0,
                'total_records_loaded': 0,
                'total_quarters_processed': 0,
                'successful_symbols': 0,
                'failed_symbols': 0
            }

        # Process symbols
        stats = {
            'symbols_processed': 0,
            'total_api_calls': 0,
            'total_records_loaded': 0,
            'total_quarters_processed': 0,
            'successful_symbols': 0,
            'failed_symbols': 0
        }

        for i, symbol_data in enumerate(symbols_to_process):
            print(f"\n--- [{i+1}/{len(symbols_to_process)}] ---")

            try:
                result = self.process_symbol(symbol_data, force_refresh=force_refresh)

                stats['symbols_processed'] += 1
                stats['total_api_calls'] += result['api_calls']
                stats['total_records_loaded'] += result['records_loaded']
                stats['total_quarters_processed'] += result['quarters_processed']

                if result['status'] in ['success', 'complete']:
                    stats['successful_symbols'] += 1
                else:
                    stats['failed_symbols'] += 1

            except Exception as e:
                print(f"  ✗ Error processing {symbol_data['symbol']}: {e}")
                stats['symbols_processed'] += 1
                stats['failed_symbols'] += 1

                # Update watermark to track failure
                self.watermark_manager.update_watermark(
                    TABLE_NAME, symbol_data['symbol_id'], success=False
                )

        # Print summary
        print(f"\n{'='*60}")
        print("🎯 Earnings Call Transcripts Extraction Complete")
        print(f"   Symbols processed: {stats['symbols_processed']}")
        print(f"   Successful: {stats['successful_symbols']}")
        print(f"   Failed: {stats['failed_symbols']}")
        print(f"   Total API calls: {stats['total_api_calls']:,}")
        print(f"   Total records loaded: {stats['total_records_loaded']:,}")
        print(f"   Total quarters processed: {stats['total_quarters_processed']:,}")
        print(f"   Average records per symbol: {stats['total_records_loaded']/max(stats['successful_symbols'], 1):.1f}")
        print(f"{'='*60}")

        return stats


def main():
    """Main function to run the earnings call transcripts extraction with CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Modern earnings call transcripts extractor with watermarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process symbols needing updates
  python extract_earnings_call_transcripts.py

  # Process specific exchange with limit
  python extract_earnings_call_transcripts.py --exchange NASDAQ --limit 50

  # Dry run to see what would be processed
  python extract_earnings_call_transcripts.py --dry-run --limit 100

  # Force refresh all symbols (ignore watermarks)
  python extract_earnings_call_transcripts.py --force-refresh --limit 10

  # Process with custom staleness threshold
  python extract_earnings_call_transcripts.py --staleness-hours 48 --limit 20
        """
    )

    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of symbols to process"
    )

    parser.add_argument(
        "--exchange",
        action="append",
        help="Filter by exchange (can be used multiple times)"
    )

    parser.add_argument(
        "--staleness-hours",
        type=int,
        default=24,
        help="Hours before data is considered stale (default: 24)"
    )

    parser.add_argument(
        "--max-failures",
        type=int,
        default=3,
        help="Maximum consecutive failures before giving up (default: 3)"
    )

    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force processing even if recently processed"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without making API calls"
    )

    parser.add_argument(
        "--use-dcs",
        action="store_true",
        help="Enable Data Coverage Score (DCS) prioritization"
    )

    parser.add_argument(
        "--min-dcs",
        type=float,
        default=0.0,
        help="Minimum DCS score for symbol selection (0.0-1.0, default: 0.0)"
    )

    args = parser.parse_args()

    try:
        # Initialize database manager and extractor
        db_manager = PostgresDatabaseManager()

        with db_manager as db:
            extractor = EarningsCallTranscriptsExtractor(db)

            # Run extraction
            extractor.run_extraction(
                limit=args.limit,
                exchange_filter=args.exchange,
                staleness_hours=args.staleness_hours,
                max_failures=args.max_failures,
                force_refresh=args.force_refresh,
                dry_run=args.dry_run,
                use_dcs=args.use_dcs,
                min_dcs=args.min_dcs
            )

            if not args.dry_run:
                print("\n✅ Extraction completed successfully!")

    except Exception as e:
        print(f"\n❌ Extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

# -----------------------------------------------------------------------------
# RUN INSTRUCTIONS - Earnings Call Transcripts Extractor
# -----------------------------------------------------------------------------
#
# This is a modern, production-ready extractor for earnings call transcripts
# that uses incremental ETL with watermarks, content hashing, and the source schema.
#
# PREREQUISITES:
# 1. PostgreSQL database with source.listing_status table populated
# 2. ALPHAVANTAGE_API_KEY set in .env file
# 3. Python virtual environment activated: .\.venv\Scripts\Activate.ps1
#
# BASIC USAGE:
#
# 1. DRY RUN (recommended first step):
#    Shows what would be processed without making API calls
#    & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --dry-run --limit 10
#
# 2. INCREMENTAL EXTRACTION (default mode):
#    Processes only symbols that need updates based on watermarks
#    & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --limit 50
#
# 3. EXCHANGE-SPECIFIC PROCESSING:
#    Process only symbols from specific exchanges
#    & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --exchange NASDAQ --limit 25
#    & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --exchange NYSE --exchange AMEX --limit 40
#
# 4. FORCE REFRESH:
#    Ignore watermarks and process symbols even if recently updated
#    & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --force-refresh --limit 10
#
# 5. CUSTOM STALENESS THRESHOLD:
#    Change how long before data is considered stale (default: 24 hours)
#    & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --staleness-hours 48 --limit 20
#
# 6. PRODUCTION RUN:
#    Large batch processing for production updates
#    & .\.venv\Scripts\python.exe data_pipeline\extract\extract_earnings_call_transcripts.py --limit 500
#
# ADVANCED OPTIONS:
#
# --dry-run              : Show what would be processed (no API calls)
# --limit N              : Maximum number of symbols to process
# --exchange EXCHANGE    : Filter by exchange (NASDAQ, NYSE, AMEX, etc.)
# --staleness-hours N    : Hours before data considered stale (default: 24)
# --max-failures N       : Max consecutive failures before giving up (default: 3)
# --force-refresh        : Ignore watermarks, process even recently updated symbols
#
# MONITORING & DEBUGGING:
#
# 1. CHECK WATERMARK STATUS:
#    Query: SELECT * FROM source.extraction_watermarks WHERE table_name = 'earnings_call_transcripts' ORDER BY last_successful_run DESC LIMIT 10;
#
# 2. CHECK RECENT EXTRACTIONS:
#    Query: SELECT symbol, quarter, api_response_status, created_at FROM source.earnings_call_transcripts WHERE created_at > NOW() - INTERVAL '1 day' ORDER BY created_at DESC LIMIT 20;
#
# 3. COUNT RECORDS BY STATUS:
#    Query: SELECT api_response_status, COUNT(*) FROM source.earnings_call_transcripts GROUP BY api_response_status;
#
# API RATE LIMITING:
# - Alpha Vantage Premium: 75 requests/minute (0.8 second delay between calls)
# - Free tier: 500 requests/day, 5 requests/minute
# - The extractor automatically handles rate limiting
#
# DATA VOLUME ESTIMATES:
# - Each symbol has ~63 quarters (2010Q1 to current)
# - Each quarter may have 5-20 transcript entries (speakers)
# - Full extraction of 1000 symbols ≈ 60,000+ API calls
# - Estimated time: 13+ hours for full 1000 symbol extraction
#
# TYPICAL WORKFLOW:
# 1. Run dry-run with small limit to verify
# 2. Process in batches of 50-100 symbols
# 3. Monitor for errors and API limits
# 4. Use exchange filters to distribute load
# 5. Schedule regular incremental updates
#
# TROUBLESHOOTING:
# - If stuck on failed symbols, check consecutive_failures in watermarks table
# - Failed symbols are automatically skipped after max-failures threshold
# - Use --force-refresh to retry previously failed symbols
# - Check database constraints if getting unique key violations
#
# PERFORMANCE NOTES:
# - Watermarks prevent re-processing of existing data
# - Content hashing detects actual data changes vs metadata updates
# - Early stopping after 4 consecutive no-data quarters per symbol
# - Batch database inserts for efficiency
#
# -----------------------------------------------------------------------------

#!/usr/bin/env python3
"""
Bulk Company Overview Data Extractor for fin-trade-extract pipeline.

Features:
- Bulk processing of company overview data from Alpha Vantage
- Focuses ONLY on active common stocks (no ETFs, no delisted stocks)
- Intelligent incremental processing with watermark tracking
- Cross-exchange support for NASDAQ, NYSE, AMEX common stocks
- Rate limiting optimized for Alpha Vantage Premium (75 calls/minute)
- Comprehensive error handling and retry logic
- S3 upload with organized structure
- Snowflake integration for tracking processing status

This extractor is designed to process only companies that will have overview data,
avoiding wasted API calls on delisted stocks or ETFs that may not have overview records.
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
import time
import requests
import boto3
from io import StringIO
import pandas as pd
import snowflake.connector

# Add parent directories to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.incremental_etl import IncrementalETLManager, get_snowflake_config_from_env
from utils.symbol_screener import SymbolScreener, ScreeningCriteria, AssetType, ExchangeType

# Configure logging
log_dir = os.getenv('LOG_DIR', os.path.join(os.getcwd(), 'logs'))
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'company_overview_bulk_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file)
    ]
)
logger = logging.getLogger(__name__)


class CompanyOverviewExtractor:
    """Bulk company overview data extraction with incremental ETL intelligence."""

    def __init__(self):
        """Initialize extractor with configuration."""
        self.api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        self.s3_bucket = os.getenv('S3_BUCKET', 'fin-trade-craft-landing')
        self.s3_prefix = os.getenv('S3_COMPANY_OVERVIEW_PREFIX', 'company_overview/')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.load_date = os.getenv('LOAD_DATE', datetime.now().strftime('%Y%m%d_%H%M%S'))
        
        # Processing configuration - Overview only for active common stocks
        self.processing_mode = os.getenv('PROCESSING_MODE', 'incremental')
        self.universe_name = os.getenv('UNIVERSE_NAME', 'active_common_stocks')
        self.exchange_filter = os.getenv('EXCHANGE_FILTER', 'ALL')  # Will be filtered to major exchanges
        
        # COST PROTECTION: Default to small limits for safety
        cost_protection_mode = os.getenv('COST_PROTECTION', 'ON')
        if cost_protection_mode == 'ON' and not os.getenv('MAX_SYMBOLS'):
            logger.warning("🛡️ COST PROTECTION ACTIVE: Limiting to 10 symbols max")
            logger.warning("🛡️ Set MAX_SYMBOLS explicitly or COST_PROTECTION=OFF to override")
            self.max_symbols = 10
        elif cost_protection_mode == 'ON' and int(os.getenv('MAX_SYMBOLS', '0')) > 50:
            logger.warning("🛡️ COST PROTECTION: MAX_SYMBOLS > 50 detected, limiting to 50")
            logger.warning("🛡️ Set COST_PROTECTION=OFF to process more than 50 symbols")
            self.max_symbols = 50
        
        # Fixed filters for overview - only active common stocks
        self.asset_type_filter = 'Stock'  # Force to Stock only, no ETFs
        self.status_filter = 'Active'     # Force to Active only, no delisted
        
        # Rate limiting and batch configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', '50'))  # Standard batch size for overview
        self.max_batches = int(os.getenv('MAX_BATCHES', '0')) if os.getenv('MAX_BATCHES') else None
        self.max_symbols = int(os.getenv('MAX_SYMBOLS', '0')) if os.getenv('MAX_SYMBOLS') else None
        self.failure_threshold = float(os.getenv('FAILURE_THRESHOLD', '0.5'))
        
        # Rate limiting (overview calls are less frequent than time series)
        # Configurable delay - can be overridden via environment variable
        default_delay = 0.3  # Optimized for faster processing
        self.api_delay = float(os.getenv('API_DELAY_SECONDS', str(default_delay)))
        self.retry_delay = 5.0
        self.max_retries = 3
        
        # Initialize services
        self.s3_client = boto3.client('s3', region_name=self.aws_region)
        self.snowflake_config = get_snowflake_config_from_env()
        
        # Statistics tracking
        self.stats = {
            'total_symbols': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'api_calls': 0,
            'start_time': datetime.now(),
            'errors': []
        }
        
        logger.info(f"Initialized Company Overview Extractor")
        logger.info(f"Processing mode: {self.processing_mode}")
        logger.info(f"Universe: {self.universe_name}")
        logger.info(f"Exchange filter: {self.exchange_filter}")
        logger.info(f"Asset type filter: {self.asset_type_filter} (FIXED - no ETFs)")
        logger.info(f"Status filter: {self.status_filter} (FIXED - no delisted)")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"API delay: {self.api_delay}s ({60/self.api_delay:.1f} calls/minute max)")
        
        if not self.api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY environment variable is required")

    def get_symbols_to_process(self) -> List[str]:
        """Get list of active common stock symbols that need overview data processing."""
        logger.info("🔍 Getting symbols for company overview processing...")
        
        try:
            etl_manager = IncrementalETLManager(self.snowflake_config)
            
            if self.processing_mode == 'incremental':
                # Get active common stock symbols from the universe
                symbols = self._get_active_common_stock_symbols()
                
                if not symbols:
                    logger.warning("⚠️ No active common stock symbols found")
                    return []
                
                # Use incremental ETL logic to identify which ones need processing
                symbols_to_process = etl_manager.identify_symbols_to_process(
                    data_type='company_overview',
                    universe_symbols=symbols,
                    force_refresh=False,
                    max_symbols=self.max_symbols
                )
                
                logger.info(f"📊 Incremental processing: {len(symbols_to_process)} symbols need overview data")
                
            elif self.processing_mode == 'full_refresh':
                # Get all active common stock symbols regardless of processing status
                symbols_to_process = self._get_active_common_stock_symbols()
                if self.max_symbols:
                    symbols_to_process = symbols_to_process[:self.max_symbols]
                logger.info(f"🔄 Full refresh: processing {len(symbols_to_process)} symbols")
                
            elif self.processing_mode == 'universe':
                # Process entire active common stock universe
                symbols_to_process = self._get_active_common_stock_symbols()
                if self.max_symbols:
                    symbols_to_process = symbols_to_process[:self.max_symbols]
                logger.info(f"🌐 Universe processing: {len(symbols_to_process)} symbols")
                
            else:
                raise ValueError(f"Invalid processing_mode: {self.processing_mode}")
            
            etl_manager.close_connection()
            return symbols_to_process
            
        except Exception as e:
            logger.error(f"❌ Error getting symbols to process: {e}")
            raise

    def _get_active_common_stock_symbols(self) -> List[str]:
        """Get symbols for active common stocks only - no ETFs, no delisted stocks."""
        logger.info("📋 Fetching active common stock symbols...")
        
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Build query for active common stocks only
            where_clauses = [
                "status = 'Active'",               # Only active stocks
                "assetType = 'Stock'",             # Only common stocks, no ETFs
                "symbol IS NOT NULL", 
                "symbol != ''",
                "LEN(symbol) <= 5"                # Reasonable symbol length filter
            ]
            
            # Filter by exchange if specified
            if self.exchange_filter != 'ALL':
                where_clauses.append(f"exchange = '{self.exchange_filter}'")
            else:
                # Default to major exchanges for overview data
                where_clauses.append("exchange IN ('NASDAQ', 'NYSE', 'AMEX')")
            
            query = f"""
            SELECT DISTINCT symbol 
            FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
            WHERE {' AND '.join(where_clauses)}
            ORDER BY symbol
            """
            
            if self.max_symbols:
                query += f" LIMIT {self.max_symbols}"
            
            cursor.execute(query)
            results = cursor.fetchall()
            symbols = [row[0] for row in results]
            
            conn.close()
            
            logger.info(f"📊 Found {len(symbols)} active common stock symbols")
            if symbols:
                logger.info(f"📝 Sample symbols: {', '.join(symbols[:10])}" + 
                           (f" (and {len(symbols) - 10} more)" if len(symbols) > 10 else ""))
            
            return symbols
            
        except Exception as e:
            logger.error(f"❌ Error fetching active common stock symbols: {e}")
            raise

    def fetch_company_overview(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch company overview data for a single symbol."""
        url = "https://www.alphavantage.co/query"
        
        params = {
            'function': 'OVERVIEW',
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        for attempt in range(self.max_retries):
            try:
                self.stats['api_calls'] += 1
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Check for API errors
                if 'Error Message' in data:
                    logger.warning(f"❌ API error for {symbol}: {data['Error Message']}")
                    return None
                    
                if 'Note' in data:
                    logger.warning(f"⚠️ Rate limit note for {symbol}: {data['Note']}")
                    time.sleep(self.retry_delay)
                    continue
                
                # Check for empty response (common for delisted stocks or ETFs)
                if not data or len(data) == 0 or data.get('Symbol') != symbol:
                    logger.info(f"📝 No overview data available for {symbol} (likely normal for this symbol type)")
                    return None
                
                # Log key fiscal fields for verification
                fiscal_year_end = data.get('FiscalYearEnd', 'N/A')
                latest_quarter = data.get('LatestQuarter', 'N/A')
                logger.debug(f"✅ Successfully fetched overview data for {symbol} (FiscalYearEnd: {fiscal_year_end}, LatestQuarter: {latest_quarter})")
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ Request error for {symbol} (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"❌ Failed to fetch {symbol} after {self.max_retries} attempts")
                    return None
            except Exception as e:
                logger.error(f"❌ Unexpected error fetching {symbol}: {e}")
                return None
        
        return None

    def process_overview_data(self, symbol: str, data: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Process raw overview data into structured format."""
        try:
            # Add processing metadata
            processed_data = data.copy()
            processed_data['SYMBOL_ID'] = abs(hash(symbol)) % 1000000000
            processed_data['PROCESSED_DATE'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            # LOAD_DATE will be generated by Snowflake during data transfer
            
            # Clean up date fields that may have invalid values
            fiscal_year_end = processed_data.get('FiscalYearEnd', '')
            latest_quarter = processed_data.get('LatestQuarter', '')
            
            # Clean up LatestQuarter field - handle various invalid formats
            if latest_quarter:
                latest_quarter = self._clean_date_field(latest_quarter, 'LatestQuarter', symbol)
                processed_data['LatestQuarter'] = latest_quarter
            
            # Clean up FiscalYearEnd field if needed
            if fiscal_year_end:
                fiscal_year_end = self._clean_date_field(fiscal_year_end, 'FiscalYearEnd', symbol)
                processed_data['FiscalYearEnd'] = fiscal_year_end
            
            logger.debug(f"📊 Processing {symbol}: FiscalYearEnd='{fiscal_year_end}', LatestQuarter='{latest_quarter}'")
            
            # Convert to DataFrame
            df = pd.DataFrame([processed_data])
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error processing overview data for {symbol}: {e}")
            return None

    def _clean_date_field(self, date_value: str, field_name: str, symbol: str) -> str:
        """Clean and validate date field values from Alpha Vantage API."""
        if not date_value or date_value == 'None':
            return ''
        
        # Handle numeric-only values that aren't valid dates
        if date_value.isdigit() and len(date_value) < 6:
            logger.warning(f"Could not parse date string '{date_value}' for symbol {symbol}: appears to be invalid numeric value")
            return ''
        
        # Handle timestamp format like '20251008_162440' - extract just the date part
        if '_' in date_value and len(date_value) >= 8:
            date_part = date_value.split('_')[0]
            if len(date_part) == 8 and date_part.isdigit():
                try:
                    # Convert YYYYMMDD to YYYY-MM-DD
                    formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                    # Validate it's a real date
                    datetime.strptime(formatted_date, '%Y-%m-%d')
                    logger.warning(f"Could not parse date string '{date_value}' for symbol {symbol}: converted timestamp to date '{formatted_date}'")
                    return formatted_date
                except ValueError:
                    logger.warning(f"Could not parse date string '{date_value}' for symbol {symbol}: invalid timestamp format")
                    return ''
        
        # Handle various other formats and validate
        try:
            # Try parsing as YYYY-MM-DD first
            if len(date_value) == 10 and date_value.count('-') == 2:
                datetime.strptime(date_value, '%Y-%m-%d')
                return date_value
            
            # Try parsing as MM-DD format (fiscal year end)
            if len(date_value) == 5 and date_value.count('-') == 1:
                # This is probably a MM-DD format for fiscal year end, leave as-is
                return date_value
            
            # For other formats, try to parse and reformat
            parsed_date = datetime.strptime(date_value, '%Y-%m-%d')
            return parsed_date.strftime('%Y-%m-%d')
            
        except ValueError:
            logger.warning(f"Could not parse date string '{date_value}' for symbol {symbol}: time data '{date_value}' does not match format '%Y-%m-%d'")
            return ''

    def upload_to_s3(self, df: pd.DataFrame, symbol: str) -> bool:
        """Upload processed overview data to S3."""
        try:
            # Convert DataFrame to CSV (Snowflake will handle column mapping by name)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Create S3 key directly in company_overview folder (no date subfolders)
            s3_key = f"{self.s3_prefix}overview_{symbol}_{self.load_date}.csv"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=csv_content,
                ContentType='text/csv'
            )
            
            logger.debug(f"✅ Uploaded {symbol} overview data to s3://{self.s3_bucket}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to upload {symbol} to S3: {e}")
            return False

    def update_processing_status(self, symbol: str, success: bool, fiscal_date: str = None):
        """Update processing status in ETL watermarks table."""
        try:
            etl_manager = IncrementalETLManager(self.snowflake_config)
            etl_manager.update_processing_status(
                data_type='company_overview',
                symbol=symbol,
                success=success,
                fiscal_date=fiscal_date
            )
            etl_manager.close_connection()
            
        except Exception as e:
            logger.warning(f"⚠️ Could not update processing status for {symbol}: {e}")

    def process_batch(self, symbols: List[str]) -> Dict[str, int]:
        """Process a batch of symbols."""
        batch_stats = {'successful': 0, 'failed': 0, 'skipped': 0}
        
        for i, symbol in enumerate(symbols):
            try:
                logger.info(f"📊 Processing {symbol} ({i + 1}/{len(symbols)})")
                
                # Rate limiting
                if i > 0:
                    time.sleep(self.api_delay)
                
                # Fetch overview data
                overview_data = self.fetch_company_overview(symbol)
                
                if overview_data is None:
                    batch_stats['skipped'] += 1
                    self.stats['skipped'] += 1
                    self.update_processing_status(symbol, success=False)
                    continue
                
                # Process the data
                df = self.process_overview_data(symbol, overview_data)
                if df is None:
                    batch_stats['failed'] += 1
                    self.stats['failed'] += 1
                    self.update_processing_status(symbol, success=False)
                    continue
                
                # Upload to S3
                s3_success = self.upload_to_s3(df, symbol)
                if s3_success:
                    batch_stats['successful'] += 1
                    self.stats['successful'] += 1
                    # Update processing status with the overview data date (or current date as fallback)
                    fiscal_date = overview_data.get('LatestQuarter', datetime.now().strftime('%Y-%m-%d'))
                    self.update_processing_status(symbol, success=True, fiscal_date=fiscal_date)
                    logger.info(f"✅ Successfully processed and uploaded {symbol} to S3")
                else:
                    batch_stats['failed'] += 1
                    self.stats['failed'] += 1
                    self.update_processing_status(symbol, success=False)
                    logger.warning(f"❌ Failed to upload {symbol} to S3")
                    
            except Exception as e:
                logger.error(f"❌ Error processing {symbol}: {e}")
                batch_stats['failed'] += 1
                self.stats['failed'] += 1
                self.stats['errors'].append(f"{symbol}: {str(e)}")
                self.update_processing_status(symbol, success=False)
        
        return batch_stats

    def check_failure_threshold(self, batch_stats: Dict[str, int]) -> bool:
        """Check if failure rate exceeds threshold."""
        total_processed = batch_stats['successful'] + batch_stats['failed']
        if total_processed == 0:
            return False
        
        failure_rate = batch_stats['failed'] / total_processed
        if failure_rate > self.failure_threshold:
            logger.error(f"❌ Failure rate {failure_rate:.2%} exceeds threshold {self.failure_threshold:.2%}")
            return True
        return False

    def cleanup_s3_bucket(self):
        """Delete all existing files in the S3 bucket before processing."""
        logger.info("🧹 Cleaning up S3 bucket before extraction...")
        
        try:
            total_deleted = 0
            continuation_token = None
            
            while True:
                # List objects (handles pagination for large numbers of files)
                list_kwargs = {
                    'Bucket': self.s3_bucket,
                    'Prefix': self.s3_prefix
                }
                if continuation_token:
                    list_kwargs['ContinuationToken'] = continuation_token
                
                response = self.s3_client.list_objects_v2(**list_kwargs)
                
                if 'Contents' not in response:
                    if total_deleted == 0:
                        logger.info("📂 S3 bucket is already empty")
                    break
                
                # Get objects to delete (max 1000 per batch due to AWS limits)
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                
                if objects_to_delete:
                    logger.info(f"🗑️ Deleting batch of {len(objects_to_delete)} files from S3...")
                    
                    self.s3_client.delete_objects(
                        Bucket=self.s3_bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    
                    total_deleted += len(objects_to_delete)
                    logger.info(f"✅ Deleted {len(objects_to_delete)} files (total deleted: {total_deleted})")
                
                # Check if there are more objects to delete
                if not response.get('IsTruncated', False):
                    break
                    
                continuation_token = response.get('NextContinuationToken')
            
            if total_deleted > 0:
                logger.info(f"✅ Successfully deleted {total_deleted} files from s3://{self.s3_bucket}/{self.s3_prefix}")
            else:
                logger.info("📂 No files found to delete")
                
        except Exception as e:
            logger.warning(f"⚠️ Error cleaning S3 bucket: {e}")
            # Don't fail the whole process due to cleanup issues
            logger.info("🔄 Continuing with extraction despite cleanup error...")

    def run_bulk_extraction(self):
        """Run the complete bulk company overview extraction process."""
        logger.info("🚀 Starting bulk company overview extraction...")
        
        try:
            # Clean up S3 bucket first
            self.cleanup_s3_bucket()
            
            # Get symbols to process
            symbols = self.get_symbols_to_process()
            if not symbols:
                logger.warning("⚠️ No symbols found for processing")
                return
            
            self.stats['total_symbols'] = len(symbols)
            
            # Process in batches
            total_batches = (len(symbols) + self.batch_size - 1) // self.batch_size
            if self.max_batches:
                total_batches = min(total_batches, self.max_batches)
            
            logger.info(f"📋 Processing {len(symbols)} active common stock symbols in {total_batches} batches")
            logger.info(f"🎯 Focus: Active common stocks only (no ETFs, no delisted)")
            
            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(symbols))
                batch_symbols = symbols[start_idx:end_idx]
                
                logger.info(f"🔄 Processing batch {batch_num + 1}/{total_batches}: {len(batch_symbols)} symbols")
                
                batch_stats = self.process_batch(batch_symbols)
                
                # Check failure threshold
                if self.check_failure_threshold(batch_stats):
                    logger.error("❌ Stopping extraction due to high failure rate")
                    break
                
                logger.info(f"📊 Batch {batch_num + 1} complete: ✅{batch_stats['successful']} ⚠️{batch_stats['skipped']} ❌{batch_stats['failed']}")
            
            # Final summary
            self.print_final_summary()
            
        except Exception as e:
            logger.error(f"❌ Critical error in bulk extraction: {e}")
            raise

    def print_final_summary(self):
        """Print final processing summary."""
        duration = datetime.now() - self.stats['start_time']
        
        logger.info("=" * 60)
        logger.info("📋 COMPANY OVERVIEW EXTRACTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"🔄 Processing mode: {self.processing_mode}")
        logger.info(f"🌐 Universe: {self.universe_name}")
        logger.info(f"🏢 Exchange filter: {self.exchange_filter}")
        logger.info(f"💼 Asset type filter: {self.asset_type_filter} (FIXED)")
        logger.info(f"🎯 Status filter: {self.status_filter} (FIXED)")
        logger.info(f"📊 Total symbols: {self.stats['total_symbols']}")
        logger.info(f"🌐 API calls made: {self.stats['api_calls']}")
        logger.info(f"✅ Successful API + S3 uploads: {self.stats['successful']}")
        logger.info(f"⚠️ Skipped (no API data): {self.stats['skipped']}")
        logger.info(f"❌ Failed (API or S3 errors): {self.stats['failed']}")
        logger.info(f"⏱️ Duration: {duration}")
        
        if self.stats['api_calls'] > 0:
            calls_per_minute = (self.stats['api_calls'] / duration.total_seconds()) * 60
            logger.info(f"📈 API rate: {calls_per_minute:.1f} calls/minute")
        
        # Additional S3/data insights
        success_rate = (self.stats['successful'] / self.stats['api_calls']) * 100 if self.stats['api_calls'] > 0 else 0
        logger.info(f"📈 Success rate: {success_rate:.1f}% (API calls that resulted in S3 files)")
        logger.info(f"📁 S3 location: s3://{self.s3_bucket}/{self.s3_prefix}")
        logger.info(f"📝 Expected files in Snowflake staging: {self.stats['successful']}")
        
        # Save results for workflow reporting
        results = {
            'processing_mode': self.processing_mode,
            'universe_name': self.universe_name,
            'exchange_filter': self.exchange_filter,
            'asset_type_filter': self.asset_type_filter,
            'status_filter': self.status_filter,
            'total_symbols': self.stats['total_symbols'],
            'successful': self.stats['successful'],
            'skipped': self.stats['skipped'],
            'failed': self.stats['failed'],
            'api_calls': self.stats['api_calls'],
            'duration_seconds': duration.total_seconds()
        }
        
        try:
            with open('/tmp/company_overview_results.json', 'w') as f:
                json.dump(results, f, indent=2)
        except Exception as e:
            logger.warning(f"⚠️ Could not save results file: {e}")
        
        if self.stats['errors']:
            logger.warning(f"⚠️ Errors encountered during processing:")
            for error in self.stats['errors'][:10]:  # Show first 10 errors
                logger.warning(f"   {error}")
            if len(self.stats['errors']) > 10:
                logger.warning(f"   ... and {len(self.stats['errors']) - 10} more errors")
        
        logger.info("=" * 60)


def main():
    """Main execution function."""
    try:
        logger.info("🏢 Starting Company Overview Bulk Extraction")
        logger.info("🎯 Focus: Active common stocks only (no ETFs, no delisted)")
        
        extractor = CompanyOverviewExtractor()
        extractor.run_bulk_extraction()
        
        logger.info("✅ Company overview extraction completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
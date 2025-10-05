#!/usr/bin/env python3
"""
Bulk fetch Alpha Vantage TIME_SERIES_DAILY_ADJUSTED data for multiple symbols.
Automatically processes symbols from LISTING_STATUS table in batches with rate limiting.
Designed to handle thousands of symbols efficiently within GitHub Actions constraints.
"""

import os
import sys
import time
import json
from datetime import datetime
from io import StringIO
import csv
import logging
from typing import List, Dict, Optional

import boto3
import requests
import snowflake.connector

# Import incremental ETL utilities
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'utils'))
from incremental_etl import IncrementalETLManager, DataType
from symbol_screener import SymbolScreener, ScreeningCriteria
from universe_management import UniverseManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AlphaVantageRateLimiter:
    """Rate limiter for Alpha Vantage Premium API (75 calls/minute)."""
    
    def __init__(self, calls_per_minute: int = 75):
        self.calls_per_minute = calls_per_minute
        self.min_delay = 60.0 / calls_per_minute  # seconds between calls
        self.last_call_time = 0.0
        
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_call_time
        
        if time_since_last < self.min_delay:
            wait_time = self.min_delay - time_since_last
            logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
        
        self.last_call_time = time.time()


def get_snowflake_config() -> Dict[str, str]:
    """Get Snowflake configuration from environment variables."""
    return {
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
    }


def get_symbols_to_process(processing_mode: str = 'incremental', 
                          universe_name: str = 'nasdaq_composite',
                          max_symbols: int = None) -> List[str]:
    """
    Get symbols to process using incremental ETL management.
    
    Args:
        processing_mode: 'incremental', 'full_refresh', or 'universe'
        universe_name: Universe to process (e.g., 'nasdaq_composite', 'nasdaq_high_quality')
        max_symbols: Maximum number of symbols to return (for testing)
        
    Returns:
        List of symbols to process
    """
    logger.info(f"Getting symbols using {processing_mode} mode for universe '{universe_name}'...")
    
    try:
        snowflake_config = get_snowflake_config()
        etl_manager = IncrementalETLManager(snowflake_config)
        
        if processing_mode == 'incremental':
            # Get incremental processing plan
            logger.info(f"🔄 Generating incremental processing plan for {universe_name}...")
            plan = etl_manager.get_processing_plan(universe_name, DataType.TIME_SERIES_DAILY_ADJUSTED)
            
            if plan.processing_items:
                symbols = [item['symbol'] for item in plan.processing_items]
                logger.info(f"📋 Incremental plan: {len(symbols)} symbols need processing")
                logger.info(f"📊 Processing reasons: {plan.summary}")
            else:
                logger.info("✅ All symbols are up to date - no processing needed")
                return []
                
        elif processing_mode == 'full_refresh':
            # Get all symbols from universe regardless of processing status
            logger.info(f"🔄 Full refresh mode: getting all symbols from {universe_name}...")
            symbols = etl_manager.get_universe_symbols(universe_name)
            if not symbols:
                logger.warning(f"No symbols found in universe '{universe_name}' - falling back to NASDAQ query")
                symbols = etl_manager._get_nasdaq_symbols_fallback()
                
        elif processing_mode == 'universe':
            # Process entire universe (skip dependency/recency checks)
            logger.info(f"🌐 Universe mode: processing all symbols from {universe_name}...")
            symbols = etl_manager.get_universe_symbols(universe_name)
            if not symbols:
                logger.warning(f"No symbols found in universe '{universe_name}' - creating default universe")
                # Create default universe if it doesn't exist
                universe_mgr = UniverseManager(snowflake_config)
                universe_mgr.create_universe_table()
                universe_mgr.create_predefined_universes()
                symbols = etl_manager.get_universe_symbols('nasdaq_composite')
                
        else:
            raise ValueError(f"Invalid processing_mode: {processing_mode}")
        
        # Apply symbol limit if specified (for testing)
        if max_symbols and len(symbols) > max_symbols:
            logger.info(f"🔒 Limiting to first {max_symbols} symbols for testing")
            symbols = symbols[:max_symbols]
        
        logger.info(f"✅ Selected {len(symbols)} symbols for processing")
        etl_manager.close_connection()
        
        return symbols
        
    except Exception as e:
        logger.error(f"Failed to get symbols for processing: {e}")
        sys.exit(1)


def fetch_time_series_data(symbol: str, api_key: str, rate_limiter: AlphaVantageRateLimiter, 
                          max_retries: int = 3) -> Optional[str]:
    """
    Fetch TIME_SERIES_DAILY_ADJUSTED data from Alpha Vantage API with rate limiting.
    
    Returns:
        CSV string with time series data, or None if failed
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "datatype": "csv", 
        "outputsize": "full",
        "apikey": api_key
    }
    
    for attempt in range(1, max_retries + 1):
        try:
            # Respect rate limits
            rate_limiter.wait_if_needed()
            
            logger.info(f"Fetching {symbol} (attempt {attempt}/{max_retries})...")
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Check if response looks like CSV
            response_text = response.text.strip()
            if response_text.startswith('timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient'):
                logger.info(f"✅ Successfully fetched {symbol}")
                return response_text
            
            # Handle Alpha Vantage errors (JSON responses)
            try:
                error_data = response.json()
                if isinstance(error_data, dict):
                    for key in ("Note", "Information", "Error Message"):
                        if error_data.get(key):
                            logger.warning(f"❌ Alpha Vantage error for {symbol}: {error_data[key]}")
                            if "rate limit" in error_data[key].lower():
                                logger.info("Rate limit hit, waiting 60 seconds...")
                                time.sleep(60)
                                continue
                            else:
                                return None  # Skip this symbol
            except:
                pass
            
            logger.warning(f"❌ Unexpected response for {symbol}: {response.text[:100]}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed for {symbol}: {e}")
            
        if attempt < max_retries:
            sleep_time = 10 * attempt  # Exponential backoff
            logger.info(f"Retrying {symbol} in {sleep_time} seconds...")
            time.sleep(sleep_time)
    
    logger.error(f"❌ Failed to fetch {symbol} after {max_retries} attempts")
    return None


def upload_to_s3(csv_data: str, symbol: str, load_date: str, bucket: str, 
                s3_prefix: str, region: str) -> bool:
    """Upload CSV data to S3."""
    try:
        s3_key = f"{s3_prefix}time_series_daily_adjusted_{symbol}_{load_date}.csv"
        
        s3_client = boto3.client('s3', region_name=region)
        
        row_count = len(csv_data.strip().split('\n')) - 1
        
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_data.encode('utf-8'),
            ContentType='text/csv',
            Metadata={
                'symbol': symbol,
                'load_date': load_date,
                'row_count': str(row_count),
                'upload_timestamp': datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"✅ Uploaded {symbol} to S3: {row_count} rows")
        return True
        
    except Exception as e:
        logger.error(f"❌ S3 upload failed for {symbol}: {e}")
        return False


def process_symbols_in_batches(symbols: List[str], api_key: str, batch_size: int = 50, 
                             max_batches: int = None, failure_threshold: float = 0.5) -> Dict:
    """Process symbols in batches with comprehensive progress tracking and safety controls.
    
    Args:
        symbols: List of symbols to process
        api_key: Alpha Vantage API key
        batch_size: Number of symbols per batch
        max_batches: Maximum number of batches to process (None = no limit)
        failure_threshold: Stop processing if failure rate exceeds this (0.5 = 50%)
    """
    
    total_symbols = len(symbols)
    total_batch_count = (total_symbols + batch_size - 1) // batch_size  # Ceiling division
    
    # Apply batch limit if specified
    if max_batches is not None:
        batch_count = min(total_batch_count, max_batches)
        symbols = symbols[:batch_count * batch_size]  # Limit symbols to process
        total_symbols = len(symbols)
        logger.info(f"�️ SAFETY MODE: Limited to {max_batches} batches ({total_symbols} symbols)")
    else:
        batch_count = total_batch_count
    
    logger.info(f"�🚀 Processing {total_symbols} symbols in {batch_count} batches of {batch_size}")
    if max_batches:
        logger.info(f"📊 Total available: {len(symbols)} symbols, Processing: {total_symbols} symbols")
    
    # Environment variables
    bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = os.environ.get('S3_TIME_SERIES_PREFIX', 'time_series_daily_adjusted/')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    load_date = os.environ.get('LOAD_DATE', datetime.now().strftime('%Y%m%d'))
    
    # Initialize rate limiter and tracking
    rate_limiter = AlphaVantageRateLimiter(calls_per_minute=75)
    
    results = {
        'total_symbols': total_symbols,
        'successful': 0,
        'failed': 0,
        'skipped': 0,
        'failed_symbols': [],
        'start_time': datetime.now().isoformat(),
        'batches_completed': 0
    }
    
    start_time = time.time()
    
    # Process each batch
    for batch_num in range(batch_count):
        batch_start_idx = batch_num * batch_size
        batch_end_idx = min(batch_start_idx + batch_size, total_symbols)
        batch_symbols = symbols[batch_start_idx:batch_end_idx]
        
        batch_start_time = time.time()
        logger.info(f"\n📦 Batch {batch_num + 1}/{batch_count}: Processing symbols {batch_start_idx + 1}-{batch_end_idx}")
        
        batch_success = 0
        batch_failed = 0
        
        # Process each symbol in the batch
        for i, symbol in enumerate(batch_symbols):
            symbol_num = batch_start_idx + i + 1
            logger.info(f"[{symbol_num}/{total_symbols}] Processing {symbol}...")
            
            # Fetch data
            csv_data = fetch_time_series_data(symbol, api_key, rate_limiter)
            
            if csv_data:
                # Upload to S3
                if upload_to_s3(csv_data, symbol, load_date, bucket, s3_prefix, region):
                    results['successful'] += 1
                    batch_success += 1
                else:
                    results['failed'] += 1
                    results['failed_symbols'].append(f"{symbol} (S3 upload failed)")
                    batch_failed += 1
            else:
                results['failed'] += 1
                results['failed_symbols'].append(f"{symbol} (API fetch failed)")
                batch_failed += 1
        
        # Batch completion summary
        batch_time = time.time() - batch_start_time
        results['batches_completed'] += 1
        
        batch_success_rate = batch_success / len(batch_symbols) if batch_symbols else 0
        logger.info(f"✅ Batch {batch_num + 1} completed in {batch_time:.1f}s: {batch_success} success, {batch_failed} failed ({batch_success_rate:.1%})")
        
        # Check failure threshold after first few batches
        if results['batches_completed'] >= 3:  # Check after 3 batches
            overall_success_rate = results['successful'] / (results['successful'] + results['failed']) if (results['successful'] + results['failed']) > 0 else 0
            if overall_success_rate < failure_threshold:
                logger.error(f"🚨 STOPPING: Success rate {overall_success_rate:.1%} below threshold {failure_threshold:.1%}")
                logger.error(f"🚨 Processed {results['batches_completed']} batches with {results['successful']} successes")
                break
        
        # Check for consecutive batch failures
        if batch_success_rate < 0.1 and results['batches_completed'] >= 2:  # Less than 10% success in batch
            logger.error(f"🚨 STOPPING: Batch {batch_num + 1} had very low success rate ({batch_success_rate:.1%})")
            logger.error(f"🚨 This may indicate API issues or rate limiting problems")
            break
        
        # Progress summary
        elapsed_time = time.time() - start_time
        symbols_processed = batch_end_idx
        symbols_remaining = total_symbols - symbols_processed
        
        if symbols_processed > 0:
            avg_time_per_symbol = elapsed_time / symbols_processed
            estimated_remaining_time = avg_time_per_symbol * symbols_remaining
            
            logger.info(f"📊 Progress: {symbols_processed}/{total_symbols} ({symbols_processed/total_symbols*100:.1f}%)")
            logger.info(f"⏱️  Elapsed: {elapsed_time/60:.1f}m, Est. remaining: {estimated_remaining_time/60:.1f}m")
    
    # Final summary
    results['end_time'] = datetime.now().isoformat()
    results['total_time_minutes'] = (time.time() - start_time) / 60
    
    return results


# Add fallback method to IncrementalETLManager for when universe doesn't exist
def _add_fallback_method():
    """Add fallback method to IncrementalETLManager for NASDAQ symbols."""
    
    def _get_nasdaq_symbols_fallback(self) -> List[str]:
        """Fallback method to get NASDAQ symbols directly from LISTING_STATUS."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT symbol 
        FROM LISTING_STATUS 
        WHERE UPPER(exchange) LIKE '%NASDAQ%' 
          AND symbol IS NOT NULL 
          AND symbol != ''
          AND status = 'Active'
        ORDER BY symbol
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        return [row[0] for row in results if row[0]]
    
    # Add method to IncrementalETLManager class
    IncrementalETLManager._get_nasdaq_symbols_fallback = _get_nasdaq_symbols_fallback

# Add the fallback method
_add_fallback_method()


def main():
    """Main function for bulk time series processing with incremental ETL support."""
    
    logger.info("🚀 Starting bulk time series data extraction with incremental ETL")
    
    # Validate environment variables
    required_vars = [
        'ALPHAVANTAGE_API_KEY', 'S3_BUCKET',
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_WAREHOUSE'
    ]
    
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    # Get configuration with new incremental ETL options
    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    batch_size = int(os.environ.get('BATCH_SIZE', '50'))
    max_batches = int(os.environ.get('MAX_BATCHES')) if os.environ.get('MAX_BATCHES') else None
    failure_threshold = float(os.environ.get('FAILURE_THRESHOLD', '0.5'))  # 50% default
    
    # New incremental ETL configuration
    processing_mode = os.environ.get('PROCESSING_MODE', 'incremental')  # 'incremental', 'full_refresh', 'universe'
    universe_name = os.environ.get('UNIVERSE_NAME', 'nasdaq_composite')
    max_symbols = int(os.environ.get('MAX_SYMBOLS')) if os.environ.get('MAX_SYMBOLS') else None
    
    logger.info(f"📋 Configuration: Batch size = {batch_size}")
    if max_batches:
        logger.info(f"🛡️ Safety limit: Max {max_batches} batches")
    logger.info(f"⚠️ Failure threshold: {failure_threshold:.1%}")
    logger.info(f"🔄 Processing mode: {processing_mode}")
    logger.info(f"🌐 Universe: {universe_name}")
    if max_symbols:
        logger.info(f"🔒 Symbol limit: {max_symbols} symbols")
    
    # Step 1: Get symbols using incremental ETL management
    symbols = get_symbols_to_process(processing_mode, universe_name, max_symbols)
    
    if not symbols:
        logger.error("❌ No symbols found to process")
        sys.exit(1)
    
    # Step 2: Process symbols with safety controls
    results = process_symbols_in_batches(symbols, api_key, batch_size, max_batches, failure_threshold)
    
    # Step 3: Final summary
    total_processed = results['successful'] + results['failed'] 
    success_rate = results['successful']/total_processed*100 if total_processed > 0 else 0
    
    logger.info("\n" + "="*60)
    logger.info("🎯 BULK PROCESSING COMPLETED")
    logger.info("="*60)
    logger.info(f"📊 Total symbols attempted: {total_processed}")
    logger.info(f"✅ Successful: {results['successful']}")
    logger.info(f"❌ Failed: {results['failed']}")
    logger.info(f"📦 Batches completed: {results['batches_completed']}")
    logger.info(f"⏱️ Total time: {results['total_time_minutes']:.1f} minutes")
    logger.info(f"📈 Success rate: {success_rate:.1f}%")
    
    if results['failed_symbols']:
        logger.info(f"\n❌ Failed symbols ({len(results['failed_symbols'])}):")
        for failed_symbol in results['failed_symbols'][:10]:  # Show first 10
            logger.info(f"  - {failed_symbol}")
        if len(results['failed_symbols']) > 10:
            logger.info(f"  ... and {len(results['failed_symbols']) - 10} more")
    
    # Save results for workflow
    with open('/tmp/bulk_processing_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"\n🔄 Ready for Snowflake bulk loading of {results['successful']} files")
    
    if results['successful'] == 0:
        logger.error("❌ No data was successfully processed")
        sys.exit(1)


if __name__ == "__main__":
    main()
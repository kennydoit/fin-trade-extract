#!/usr/bin/env python3
"""
Watermark-Based Time Series ETL
Fetches TIME_SERIES_DAILY_ADJUSTED data using the ETL_WATERMARKS table for incremental processing.
"""

import os
import sys
import time
import json
from datetime import datetime, timedelta
from io import StringIO
import csv
import logging
from typing import List, Dict, Optional, Tuple

import boto3
import requests
import snowflake.connector

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class WatermarkETLManager:
    """Manages ETL processing using the ETL_WATERMARKS table."""
    
    def __init__(self, snowflake_config: Dict[str, str], table_name: str = 'TIME_SERIES_DAILY_ADJUSTED'):
        self.snowflake_config = snowflake_config
        self.table_name = table_name
        self.connection = None
        
    def connect(self):
        """Establish Snowflake connection."""
        if not self.connection:
            self.connection = snowflake.connector.connect(**self.snowflake_config)
            logger.info("✅ Connected to Snowflake")
            
    def close(self):
        """Close Snowflake connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("🔒 Snowflake connection closed")
    
    def get_symbols_to_process(self, exchange_filter: Optional[str] = None,
                               max_symbols: Optional[int] = None,
                               staleness_days: int = 5,
                               skip_recent_hours: Optional[int] = None) -> List[Dict]:
        """
        Get symbols to process from ETL_WATERMARKS table.
        
        Args:
            exchange_filter: Filter by exchange (NYSE, NASDAQ, etc.)
            max_symbols: Maximum number of symbols to return
            staleness_days: Days before data is considered stale
            skip_recent_hours: Skip symbols processed within this many hours (for incremental runs)
        
        Returns list of dicts with:
        - symbol: stock ticker
        - processing_mode: 'full' or 'compact'
        - last_fiscal_date: last data point date (or None)
        """
        self.connect()
        
        query = f"""
            SELECT 
                SYMBOL,
                EXCHANGE,
                ASSET_TYPE,
                STATUS,
                FIRST_FISCAL_DATE,
                LAST_FISCAL_DATE,
                LAST_SUCCESSFUL_RUN,
                CONSECUTIVE_FAILURES
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = '{self.table_name}'
              AND API_ELIGIBLE = 'YES'
        """
        
        # Skip recently processed symbols if requested
        if skip_recent_hours:
            query += f"""
              AND (LAST_SUCCESSFUL_RUN IS NULL 
                   OR LAST_SUCCESSFUL_RUN < DATEADD(hour, -{skip_recent_hours}, CURRENT_TIMESTAMP()))
            """
        
        if exchange_filter:
            query += f"\n              AND UPPER(EXCHANGE) = '{exchange_filter.upper()}'"
        
        query += "\n            ORDER BY SYMBOL"
        
        if max_symbols:
            query += f"\n            LIMIT {max_symbols}"
        
        logger.info(f"📊 Querying watermarks for {self.table_name}...")
        if exchange_filter:
            logger.info(f"🏢 Exchange filter: {exchange_filter}")
        if max_symbols:
            logger.info(f"🔒 Symbol limit: {max_symbols}")
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        symbols_to_process = []
        full_count = 0
        compact_count = 0
        
        for row in results:
            symbol = row[0]
            first_fiscal = row[4]
            last_fiscal = row[5]
            
            # Determine processing mode based on watermark logic
            if first_fiscal is None and last_fiscal is None:
                # Never processed - get full history
                processing_mode = 'full'
                full_count += 1
            elif first_fiscal is not None and last_fiscal is not None:
                # Previously processed - check staleness
                days_since_last = (datetime.now().date() - last_fiscal).days
                if days_since_last < staleness_days:
                    # Recent data - use compact mode
                    processing_mode = 'compact'
                    compact_count += 1
                else:
                    # Stale data - refresh full history
                    processing_mode = 'full'
                    full_count += 1
            else:
                # Partial data - get full history to be safe
                processing_mode = 'full'
                full_count += 1
            
            symbols_to_process.append({
                'symbol': symbol,
                'processing_mode': processing_mode,
                'last_fiscal_date': last_fiscal
            })
        
        logger.info(f"📋 Found {len(symbols_to_process)} symbols to process:")
        logger.info(f"   🔄 Full refresh: {full_count} symbols")
        logger.info(f"   ⚡ Compact update: {compact_count} symbols")
        
        return symbols_to_process
    
    def update_watermark(self, symbol: str, first_date: str, last_date: str, success: bool = True):
        """
        Update watermark for a symbol after processing.
        
        Args:
            symbol: Stock ticker
            first_date: Earliest date in the data (YYYY-MM-DD)
            last_date: Most recent date in the data (YYYY-MM-DD)
            success: Whether processing was successful
        """
        self.connect()
        
        cursor = self.connection.cursor()
        
        if success:
            # Check if symbol has a delisting date - if so, mark as delisted after successful pull
            update_sql = f"""
                UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                SET 
                    FIRST_FISCAL_DATE = COALESCE(FIRST_FISCAL_DATE, TO_DATE('{first_date}', 'YYYY-MM-DD')),
                    LAST_FISCAL_DATE = TO_DATE('{last_date}', 'YYYY-MM-DD'),
                    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                    CONSECUTIVE_FAILURES = 0,
                    API_ELIGIBLE = CASE 
                        WHEN DELISTING_DATE IS NOT NULL AND DELISTING_DATE <= CURRENT_DATE() 
                        THEN 'DEL'
                        ELSE API_ELIGIBLE 
                    END,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '{self.table_name}'
                  AND SYMBOL = '{symbol}'
            """
        else:
            update_sql = f"""
                UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                SET 
                    CONSECUTIVE_FAILURES = COALESCE(CONSECUTIVE_FAILURES, 0) + 1,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '{self.table_name}'
                  AND SYMBOL = '{symbol}'
            """
        
        cursor.execute(update_sql)
        self.connection.commit()
        cursor.close()


class AlphaVantageRateLimiter:
    """Rate limiter for Alpha Vantage API."""
    
    def __init__(self, calls_per_minute: int = 75):
        default_delay = 60.0 / calls_per_minute
        self.min_delay = float(os.getenv('API_DELAY_SECONDS', str(default_delay)))
        self.last_call_time = 0.0
        
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_call_time
        
        if time_since_last < self.min_delay:
            wait_time = self.min_delay - time_since_last
            time.sleep(wait_time)
        
        self.last_call_time = time.time()


def cleanup_s3_bucket(bucket: str, s3_prefix: str, s3_client) -> int:
    """Delete all existing files in the S3 bucket before processing."""
    logger.info("🧹 Cleaning up S3 bucket before extraction...")
    logger.info(f"📂 Target: s3://{bucket}/{s3_prefix}")
    
    try:
        total_deleted = 0
        continuation_token = None
        
        while True:
            # List objects (handles pagination for large numbers of files)
            list_kwargs = {
                'Bucket': bucket,
                'Prefix': s3_prefix
            }
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            
            response = s3_client.list_objects_v2(**list_kwargs)
            
            if 'Contents' not in response:
                if total_deleted == 0:
                    logger.info("✅ S3 bucket is already empty")
                break
            
            # Get objects to delete (max 1000 per batch due to AWS limits)
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            if objects_to_delete:
                logger.info(f"🗑️  Deleting batch of {len(objects_to_delete)} files from S3...")
                
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objects_to_delete}
                )
                
                total_deleted += len(objects_to_delete)
                logger.info(f"✅ Deleted {len(objects_to_delete)} files (total: {total_deleted})")
            
            # Check if there are more objects to delete
            if not response.get('IsTruncated', False):
                break
                
            continuation_token = response.get('NextContinuationToken')
        
        if total_deleted > 0:
            logger.info(f"🎉 Successfully deleted {total_deleted} files from s3://{bucket}/{s3_prefix}")
        
        return total_deleted
        
    except Exception as e:
        logger.warning(f"⚠️  Error cleaning S3 bucket: {e}")
        logger.info("🔄 Continuing with extraction despite cleanup error...")
        return 0


def fetch_time_series_data(symbol: str, api_key: str, output_size: str = 'full') -> Optional[Dict]:
    """
    Fetch time series data from Alpha Vantage.
    
    Args:
        symbol: Stock ticker
        api_key: Alpha Vantage API key
        output_size: 'full' or 'compact'
    
    Returns:
        Dict with time series data or None if error
    """
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY_ADJUSTED',
        'symbol': symbol,
        'apikey': api_key,
        'datatype': 'csv',
        'outputsize': output_size
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        # Check for API error messages
        if 'Error Message' in response.text or 'Invalid API call' in response.text:
            logger.warning(f"❌ API error for {symbol}: {response.text[:200]}")
            return None
        
        # Parse CSV response
        csv_data = StringIO(response.text)
        reader = csv.DictReader(csv_data)
        records = list(reader)
        
        if not records:
            logger.warning(f"⚠️  No data returned for {symbol}")
            return None
        
        # Get date range
        dates = [r['timestamp'] for r in records]
        first_date = min(dates)
        last_date = max(dates)
        
        return {
            'symbol': symbol,
            'records': records,
            'first_date': first_date,
            'last_date': last_date,
            'record_count': len(records)
        }
        
    except Exception as e:
        logger.error(f"❌ Error fetching {symbol}: {e}")
        return None


def upload_to_s3(data: Dict, s3_client, bucket: str, prefix: str) -> bool:
    """Upload time series data to S3."""
    try:
        symbol = data['symbol']
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{prefix}{symbol}_{timestamp}.csv"
        
        # Convert records back to CSV
        if not data['records']:
            return False
        
        csv_buffer = StringIO()
        fieldnames = data['records'][0].keys()
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data['records'])
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode('utf-8')
        )
        
        logger.info(f"✅ Uploaded {symbol} to s3://{bucket}/{s3_key} ({data['record_count']} records)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error uploading {data['symbol']} to S3: {e}")
        return False


def main():
    """Main ETL execution."""
    logger.info("🚀 Starting Watermark-Based Time Series ETL")
    
    # Get configuration from environment
    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    s3_bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = os.environ.get('S3_TIME_SERIES_PREFIX', 'time_series_daily_adjusted/')
    exchange_filter = os.environ.get('EXCHANGE_FILTER')  # NYSE, NASDAQ, or None for all
    max_symbols = int(os.environ['MAX_SYMBOLS']) if os.environ.get('MAX_SYMBOLS') else None
    staleness_days = int(os.environ.get('STALENESS_DAYS', '5'))
    skip_recent_hours = int(os.environ['SKIP_RECENT_HOURS']) if os.environ.get('SKIP_RECENT_HOURS') else None
    batch_size = int(os.environ.get('BATCH_SIZE', '50'))
    
    # Snowflake configuration
    snowflake_config = {
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
    }
    
    # STEP 1: Clean up S3 bucket (no Snowflake connection needed)
    logger.info("=" * 60)
    logger.info("🧹 STEP 1: Clean up existing S3 files")
    logger.info("=" * 60)
    s3_client = boto3.client('s3')
    deleted_count = cleanup_s3_bucket(s3_bucket, s3_prefix, s3_client)
    logger.info(f"✅ Cleanup complete: {deleted_count} old files removed")
    logger.info("")
    
    # STEP 2: Query watermarks - CLOSE connection immediately after
    logger.info("=" * 60)
    logger.info("🔍 STEP 2: Query watermarks for symbols to process")
    logger.info("=" * 60)
    
    watermark_manager = WatermarkETLManager(snowflake_config)
    try:
        symbols_to_process = watermark_manager.get_symbols_to_process(
            exchange_filter=exchange_filter,
            max_symbols=max_symbols,
            staleness_days=staleness_days,
            skip_recent_hours=skip_recent_hours
        )
    finally:
        # CRITICAL: Close Snowflake connection immediately after getting symbols
        watermark_manager.close()
        logger.info("🔌 Snowflake connection closed after watermark query")
    
    if not symbols_to_process:
        logger.warning("⚠️  No symbols to process")
        return
    
    logger.info("")
    
    # STEP 3: Extract from Alpha Vantage (NO Snowflake connection - prevents idle warehouse costs)
    logger.info("=" * 60)
    logger.info("🚀 STEP 3: Extract time series data from Alpha Vantage")
    logger.info("=" * 60)
    
    rate_limiter = AlphaVantageRateLimiter()
    
    results = {
        'total_symbols': len(symbols_to_process),
        'successful': 0,
        'failed': 0,
        'start_time': datetime.now().isoformat(),
        'details': [],
        'successful_updates': []
    }
    
    for i, symbol_info in enumerate(symbols_to_process, 1):
        symbol = symbol_info['symbol']
        mode = symbol_info['processing_mode']
        output_size = 'full' if mode == 'full' else 'compact'
        
        logger.info(f"📊 [{i}/{len(symbols_to_process)}] Processing {symbol} ({mode} mode)...")
        
        # Rate limit
        rate_limiter.wait_if_needed()
        
        # Fetch data
        data = fetch_time_series_data(symbol, api_key, output_size)
        
        if data:
            # Upload to S3
            if upload_to_s3(data, s3_client, s3_bucket, s3_prefix):
                results['successful'] += 1
                results['details'].append({
                    'symbol': symbol,
                    'status': 'success',
                    'mode': mode,
                    'records': data['record_count']
                })
                # Store successful symbols for watermark update
                results['successful_updates'].append({
                    'symbol': symbol,
                    'first_date': data['first_date'],
                    'last_date': data['last_date']
                })
            else:
                results['failed'] += 1
        else:
            results['failed'] += 1
            results['details'].append({
                'symbol': symbol,
                'status': 'failed',
                'mode': mode
            })
    
    results['end_time'] = datetime.now().isoformat()
    results['duration_minutes'] = (datetime.fromisoformat(results['end_time']) - 
                                  datetime.fromisoformat(results['start_time'])).total_seconds() / 60
    
    # STEP 4: Open NEW Snowflake connection to update watermarks
    logger.info("")
    logger.info("=" * 60)
    logger.info("🔄 STEP 4: Update watermarks for successful extractions")
    logger.info("=" * 60)
    
    watermark_manager = WatermarkETLManager(snowflake_config)
    try:
        # Update watermarks for successful symbols
        for update_info in results['successful_updates']:
            watermark_manager.update_watermark(
                update_info['symbol'],
                update_info['first_date'],
                update_info['last_date'],
                success=True
            )
        
        # Update watermarks for failed symbols
        failed_symbols = [d['symbol'] for d in results['details'] if d.get('status') == 'failed']
        for symbol in failed_symbols:
            watermark_manager.update_watermark(symbol, None, None, success=False)
        
        # Check how many delisted symbols were marked
        cursor = watermark_manager.connection.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = '{watermark_manager.table_name}'
              AND API_ELIGIBLE = 'DEL'
        """)
        delisted_count = cursor.fetchone()[0]
        cursor.close()
        results['delisted_marked'] = delisted_count
        
    finally:
        watermark_manager.close()
        logger.info("🔌 Snowflake connection closed after watermark updates")
    
    # Save results
    with open('/tmp/watermark_etl_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info("")
    logger.info("🎉 ETL processing complete!")
    logger.info(f"✅ Successful: {results['successful']}/{results['total_symbols']}")
    logger.info(f"❌ Failed: {results['failed']}/{results['total_symbols']}")
    if delisted_count > 0:
        logger.info(f"🔒 Delisted symbols marked as 'DEL': {delisted_count}")


if __name__ == '__main__':
    main()

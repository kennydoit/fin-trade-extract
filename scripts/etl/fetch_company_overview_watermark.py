#!/usr/bin/env python3
"""
Watermark-Based Company Overview ETL
Fetches COMPANY_OVERVIEW data using the ETL_WATERMARKS table for incremental processing.
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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class WatermarkETLManager:
    """Manages ETL processing using the ETL_WATERMARKS table."""
    
    def __init__(self, snowflake_config: Dict[str, str], table_name: str = 'COMPANY_OVERVIEW'):
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
                               skip_recent_hours: Optional[int] = None) -> List[Dict]:
        """
        Get symbols to process from ETL_WATERMARKS table.
        
        Args:
            exchange_filter: Filter by exchange (NYSE, NASDAQ, etc.)
            max_symbols: Maximum number of symbols to return
            skip_recent_hours: Skip symbols processed within this many hours (for incremental runs)
        
        Returns list of dicts with symbol information
        """
        self.connect()
        
        query = f"""
            SELECT 
                SYMBOL,
                EXCHANGE,
                ASSET_TYPE,
                STATUS,
                LAST_SUCCESSFUL_RUN,
                CONSECUTIVE_FAILURES
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = '{self.table_name}'
              AND API_ELIGIBLE = 'YES'
        """
        
        # COMPANY_OVERVIEW-SPECIFIC LOGIC: Only pull if NEVER pulled before (LAST_SUCCESSFUL_RUN IS NULL)
        # Company overview is semi-static data (sector, industry, description) - no need to refresh
        # Once pulled, the data is stored and only needs updates in rare cases (handled manually)
        # This ensures true incremental processing - only fetch new symbols
        query += """
              AND LAST_SUCCESSFUL_RUN IS NULL
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
        logger.info(f"📅 Company overview logic: Only symbols that have NEVER been pulled (LAST_SUCCESSFUL_RUN IS NULL)")
        if max_symbols:
            logger.info(f"🔒 Symbol limit: {max_symbols}")
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        symbols_to_process = []
        
        for row in results:
            symbol = row[0]
            symbols_to_process.append({
                'symbol': symbol,
                'exchange': row[1],
                'asset_type': row[2],
                'status': row[3]
            })
        
        logger.info(f"📈 Found {len(symbols_to_process)} symbols to process")
        
        return symbols_to_process
    
    def bulk_update_watermarks(self, successful_updates: List[Dict], failed_symbols: List[str]):
        """
        Bulk update watermarks using a single UPDATE statement.
        For company overview, we only update LAST_SUCCESSFUL_RUN (no fiscal dates).
        
        Args:
            successful_updates: List of dicts with {symbol}
            failed_symbols: List of symbols that failed processing
        """
        if not self.connection:
            raise RuntimeError("❌ No active Snowflake connection. Call connect() first.")
        
        cursor = self.connection.cursor()
        
        # Update successful symbols
        if successful_updates:
            logger.info(f"📝 Bulk updating {len(successful_updates)} successful watermarks...")
            
            # Extract symbol list
            symbols_list = "', '".join([u['symbol'] for u in successful_updates])
            
            # Single UPDATE to mark all symbols as successfully processed
            cursor.execute(f"""
                UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                SET 
                    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                    CONSECUTIVE_FAILURES = 0,
                    API_ELIGIBLE = CASE 
                        WHEN DELISTING_DATE IS NOT NULL AND DELISTING_DATE <= CURRENT_DATE() 
                        THEN 'DEL'
                        ELSE API_ELIGIBLE 
                    END,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '{self.table_name}'
                  AND SYMBOL IN ('{symbols_list}')
            """)
            
            logger.info(f"✅ Bulk updated {len(successful_updates)} successful watermarks")
        
        # Handle failed symbols (much smaller batch, can use simple UPDATE with IN clause)
        if failed_symbols:
            logger.info(f"📝 Updating {len(failed_symbols)} failed watermarks...")
            symbols_list = "', '".join(failed_symbols)
            cursor.execute(f"""
                UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                SET 
                    CONSECUTIVE_FAILURES = COALESCE(CONSECUTIVE_FAILURES, 0) + 1,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '{self.table_name}'
                  AND SYMBOL IN ('{symbols_list}')
            """)
            logger.info(f"✅ Updated {len(failed_symbols)} failed watermarks")
        
        cursor.close()
    
    def update_watermark(self, symbol: str, first_date: str, last_date: str, success: bool = True):
        """
        Update watermark for a symbol after processing.
        
        Args:
            symbol: Stock ticker
            first_date: Earliest fiscal date in the data (YYYY-MM-DD)
            last_date: Most recent fiscal date in the data (YYYY-MM-DD)
            success: Whether processing was successful
        """
        if not self.connection:
            raise RuntimeError("❌ No active Snowflake connection. Call connect() first.")
        
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
        cursor.close()
        # Note: Commit is handled by caller to allow batching multiple updates


class AlphaVantageRateLimiter:
    """Rate limiter for Alpha Vantage API."""
    
    def __init__(self, calls_per_minute: int = 75):
        default_delay = 60.0 / calls_per_minute
        self.min_delay = float(os.getenv('API_DELAY_SECONDS', str(default_delay)))
        self.last_call_time = 0.0
        
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        
        if time_since_last_call < self.min_delay:
            sleep_time = self.min_delay - time_since_last_call
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()


def cleanup_s3_bucket(bucket: str, prefix: str, s3_client) -> int:
    """
    Delete all existing files in the S3 prefix.
    This prevents duplicate loading when using COPY FROM s3://.../*.csv pattern.
    
    Returns: Number of files deleted
    """
    logger.info(f"🧹 Cleaning up S3 bucket: s3://{bucket}/{prefix}")
    
    deleted_count = 0
    continuation_token = None
    
    while True:
        # List objects
        list_params = {'Bucket': bucket, 'Prefix': prefix}
        if continuation_token:
            list_params['ContinuationToken'] = continuation_token
        
        response = s3_client.list_objects_v2(**list_params)
        
        # Delete objects if any exist
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )
            deleted_count += len(objects_to_delete)
            logger.info(f"   Deleted {len(objects_to_delete)} files (total: {deleted_count})")
        
        # Check if there are more objects to list
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    
    logger.info(f"✅ S3 cleanup complete: {deleted_count} files deleted")
    return deleted_count


def fetch_company_overview(symbol: str, api_key: str) -> Optional[Dict]:
    """
    Fetch company overview data from Alpha Vantage API.
    
    Returns dict with:
    - symbol: stock ticker
    - data: raw API response data
    - latest_quarter: most recent fiscal quarter date (for watermark tracking)
    """
    url = "https://www.alphavantage.co/query"
    params = {
        'function': 'OVERVIEW',
        'symbol': symbol,
        'apikey': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if 'Error Message' in data:
            logger.warning(f"⚠️  API error for {symbol}: {data['Error Message']}")
            return None
        
        if 'Note' in data:
            logger.warning(f"⚠️  API rate limit hit for {symbol}: {data['Note']}")
            return None
        
        # Check if we got any data (empty dict means no data available)
        if not data or 'Symbol' not in data:
            logger.warning(f"⚠️  No company overview data for {symbol}")
            return None
        
        # Get LatestQuarter for watermark tracking (this is the most recent fiscal data point)
        latest_quarter = data.get('LatestQuarter')
        
        if not latest_quarter:
            # Use current date as fallback if LatestQuarter not provided
            latest_quarter = datetime.now().strftime('%Y-%m-%d')
            logger.warning(f"⚠️  No LatestQuarter for {symbol}, using current date")
        
        logger.info(f"✅ Fetched {symbol}: LatestQuarter={latest_quarter}")
        
        return {
            'symbol': symbol,
            'data': data,
            'latest_quarter': latest_quarter
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request failed for {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error for {symbol}: {e}")
        return None


def upload_to_s3(data: Dict, s3_client, bucket: str, prefix: str) -> bool:
    """Upload company overview data to S3 as CSV."""
    symbol = data['symbol']
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"{prefix}{symbol}_{timestamp}.csv"
    
    try:
        # Convert to CSV format
        csv_buffer = StringIO()
        
        # Define CSV columns (essential fields for cost optimization)
        fieldnames = ['Symbol', 'AssetType', 'Name', 'Description', 'CIK', 'Exchange',
                     'Currency', 'Country', 'Sector', 'Industry', 'Address', 'OfficialSite',
                     'FiscalYearEnd', 'LatestQuarter', 'MarketCapitalization', 'EBITDA',
                     'PERatio', 'PEGRatio', 'BookValue', 'DividendPerShare', 'DividendYield',
                     'EPS', 'RevenuePerShareTTM', 'ProfitMargin', 'OperatingMarginTTM',
                     'ReturnOnAssetsTTM', 'ReturnOnEquityTTM', 'RevenueTTM', 'GrossProfitTTM',
                     'DilutedEPSTTM', 'QuarterlyEarningsGrowthYOY', 'QuarterlyRevenueGrowthYOY',
                     'AnalystTargetPrice', 'TrailingPE', 'ForwardPE', 'PriceToSalesRatioTTM',
                     'PriceToBookRatio', 'EVToRevenue', 'EVToEBITDA', 'Beta',
                     'Week52High', 'Week52Low', 'Day50MovingAverage', 'Day200MovingAverage',
                     'SharesOutstanding', 'DividendDate', 'ExDividendDate']
        
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        # Write single row with all company overview data
        writer.writerow(data['data'])
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode('utf-8')
        )
        
        logger.info(f"✅ Uploaded {symbol} to s3://{bucket}/{s3_key}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error uploading {data['symbol']} to S3: {e}")
        return False


def main():
    """Main ETL execution."""
    import argparse
    parser = argparse.ArgumentParser(description='Company Overview Watermark ETL')
    parser.add_argument('--max-symbols', type=int, default=None, help='Maximum number of symbols to process')
    args = parser.parse_args()
    
    logger.info("🚀 Starting Watermark-Based Company Overview ETL")
    
    # Get configuration from environment
    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    s3_bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = os.environ.get('S3_COMPANY_OVERVIEW_PREFIX', 'company_overview/')
    max_symbols = args.max_symbols  # Use argparse instead of environment variable
    
    # Snowflake configuration
    snowflake_config = {
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
    }
    
    # Initialize managers
    watermark_manager = WatermarkETLManager(snowflake_config)
    rate_limiter = AlphaVantageRateLimiter()
    s3_client = boto3.client('s3')
    
    # Clean up S3 bucket before extraction (critical for COPY FROM s3://.../*.csv)
    logger.info("=" * 60)
    logger.info("🧹 STEP 1: Clean up existing S3 files")
    logger.info("=" * 60)
    deleted_count = cleanup_s3_bucket(s3_bucket, s3_prefix, s3_client)
    logger.info(f"✅ Cleanup complete: {deleted_count} old files removed")
    logger.info("")
    
    # STEP 2: Get symbols to process from watermarks, then CLOSE connection
    logger.info("=" * 60)
    logger.info("🔍 STEP 2: Query watermarks for symbols to process")
    logger.info("=" * 60)
    try:
        symbols_to_process = watermark_manager.get_symbols_to_process(
            max_symbols=max_symbols
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
    logger.info("🚀 STEP 3: Extract company overview data from Alpha Vantage")
    logger.info("=" * 60)
    
    results = {
        'total_symbols': len(symbols_to_process),
        'successful': 0,
        'failed': 0,
        'start_time': datetime.now().isoformat(),
        'details': [],
        'successful_updates': []  # Track successful updates for bulk watermark update
    }
    
    for i, symbol_info in enumerate(symbols_to_process, 1):
        symbol = symbol_info['symbol']
        logger.info(f"📊 [{i}] Processing {symbol}...")
        
        # Rate limit
        rate_limiter.wait_if_needed()
        
        # Fetch data
        data = fetch_company_overview(symbol, api_key)
        
        if data:
            # Upload to S3
            if upload_to_s3(data, s3_client, s3_bucket, s3_prefix):
                # Track for bulk watermark update (don't update one-by-one)
                results['successful_updates'].append({
                    'symbol': symbol
                })
                results['successful'] += 1
                results['details'].append({
                    'symbol': symbol,
                    'status': 'success'
                })
            else:
                results['failed'] += 1
                results['details'].append({
                    'symbol': symbol,
                    'status': 'failed'
                })
        else:
            results['failed'] += 1
            results['details'].append({
                'symbol': symbol,
                'status': 'failed'
            })
    
    # Save results
    results['end_time'] = datetime.now().isoformat()
    results['duration_minutes'] = (datetime.fromisoformat(results['end_time']) - 
                                  datetime.fromisoformat(results['start_time'])).total_seconds() / 60
    
    # STEP 4: Open NEW Snowflake connection to update watermarks
    logger.info("")
    logger.info("=" * 60)
    logger.info("🔄 STEP 4: Update watermarks for successful extractions")
    logger.info("=" * 60)
    
    watermark_manager = WatermarkETLManager(snowflake_config)
    watermark_manager.connect()
    
    try:
        # Bulk update all watermarks in a single MERGE statement (100x faster!)
        failed_symbols = [d['symbol'] for d in results['details'] if d.get('status') == 'failed']
        watermark_manager.bulk_update_watermarks(results['successful_updates'], failed_symbols)
        
        # Commit all watermark updates at once
        logger.info("💾 Committing watermark updates...")
        watermark_manager.connection.commit()
        logger.info("✅ Watermark updates committed")
        
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

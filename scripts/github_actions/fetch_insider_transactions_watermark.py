#!/usr/bin/env python3
"""
Watermark-Based Insider Transactions ETL
Fetches INSIDER_TRANSACTIONS data using the ETL_WATERMARKS table for incremental processing.

API: https://www.alphavantage.co/documentation/#insider-transactions
Endpoint: function=INSIDER_TRANSACTIONS&symbol=IBM
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
    
    def __init__(self, snowflake_config: Dict[str, str], table_name: str = 'INSIDER_TRANSACTIONS'):
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
                FIRST_FISCAL_DATE,
                LAST_FISCAL_DATE,
                LAST_SUCCESSFUL_RUN,
                CONSECUTIVE_FAILURES
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = '{self.table_name}'
              AND API_ELIGIBLE = 'YES'
        """
        
        # NOTE: No staleness check for insider transactions
        # Unlike fundamentals, insider trading is sporadic and unpredictable
        # A company could have 0 transactions for months, then multiple in a day
        # Always fetch latest data when running
        
        # Skip recently processed symbols if requested (optional incremental behavior)
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
        if skip_recent_hours:
            logger.info(f"⏱️  Incremental mode: Skip symbols processed within {skip_recent_hours} hours")
        else:
            logger.info(f"🔄 Full mode: Process all eligible symbols")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            symbols = [dict(zip(columns, row)) for row in rows]
            
            logger.info(f"✅ Found {len(symbols)} symbols to process")
            return symbols
        finally:
            cursor.close()
    
    def bulk_update_watermarks(self, successful_updates: List[Dict], failed_symbols: List[str]):
        """
        Bulk update watermarks using temp table + MERGE (optimized for performance).
        
        Args:
            successful_updates: List of dicts with symbol, first_date, last_date
            failed_symbols: List of symbols that failed processing
        """
        self.connect()
        cursor = self.connection.cursor()
        
        try:
            # Create temp table for bulk updates
            cursor.execute("""
                CREATE TEMP TABLE WATERMARK_UPDATES (
                    SYMBOL VARCHAR(20),
                    FIRST_DATE DATE,
                    LAST_DATE DATE
                )
            """)
            
            # Build bulk INSERT
            if successful_updates:
                values_list = [
                    f"('{update['symbol']}', '{update['first_date']}', '{update['last_date']}')"
                    for update in successful_updates
                ]
                
                insert_query = f"""
                    INSERT INTO WATERMARK_UPDATES (SYMBOL, FIRST_DATE, LAST_DATE)
                    VALUES {','.join(values_list)}
                """
                cursor.execute(insert_query)
                logger.info(f"📝 Prepared {len(successful_updates)} successful watermark updates")
            
            # Update successful symbols
            if successful_updates:
                merge_query = f"""
                    MERGE INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS target
                    USING WATERMARK_UPDATES source
                    ON target.SYMBOL = source.SYMBOL AND target.TABLE_NAME = '{self.table_name}'
                    WHEN MATCHED THEN UPDATE SET
                        FIRST_FISCAL_DATE = COALESCE(target.FIRST_FISCAL_DATE, source.FIRST_DATE),
                        LAST_FISCAL_DATE = source.LAST_DATE,
                        LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                        CONSECUTIVE_FAILURES = 0,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                """
                cursor.execute(merge_query)
                logger.info(f"✅ Updated {len(successful_updates)} watermarks (successful)")
            
            # Update failed symbols (increment failure counter)
            if failed_symbols:
                symbols_str = "', '".join(failed_symbols)
                failure_query = f"""
                    UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                    SET CONSECUTIVE_FAILURES = CONSECUTIVE_FAILURES + 1,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE TABLE_NAME = '{self.table_name}'
                      AND SYMBOL IN ('{symbols_str}')
                """
                cursor.execute(failure_query)
                logger.info(f"⚠️  Updated {len(failed_symbols)} watermarks (failed)")
            
            self.connection.commit()
            logger.info(f"💾 Watermark updates committed to database")
            
        finally:
            cursor.close()


def cleanup_s3_bucket(bucket: str, prefix: str, s3_client) -> int:
    """
    Delete all existing files in S3 prefix (handles >1000 files with pagination).
    CRITICAL: Must run before extraction to prevent duplicate data in Snowflake.
    """
    logger.info(f"🧹 Cleaning up S3: s3://{bucket}/{prefix}")
    
    deleted_count = 0
    continuation_token = None
    
    while True:
        # List objects (handles pagination for >1000 files)
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
        
        # Check if there are more objects
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    
    logger.info(f"✅ S3 cleanup complete: {deleted_count} files deleted")
    return deleted_count


def fetch_insider_transactions(symbol: str, api_key: str) -> tuple:
    """
    Fetch insider transactions data from Alpha Vantage API.
    
    Returns: (data_dict, status_string)
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "INSIDER_TRANSACTIONS",
        "symbol": symbol,
        "apikey": api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if "Error Message" in data:
            return data, "error"
        if "Note" in data:  # Rate limit
            return data, "rate_limited"
        if "Information" in data:  # Info message (often rate limit or invalid symbol)
            return data, "info"
        
        # Check if we have actual transaction data
        if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
            return data, "success"
        elif not data or len(data) == 0:
            return {}, "empty"
        else:
            # Sometimes successful response but no transactions
            return data, "no_data"
            
    except Exception as e:
        logger.error(f"❌ API request failed for {symbol}: {e}")
        return {"error": str(e)}, "error"


def upload_to_s3(symbol: str, data: Dict, s3_client, bucket: str, prefix: str) -> bool:
    """Upload insider transactions data to S3 as CSV with explicit field mapping."""
    try:
        # Define CSV columns with explicit mapping (snake_case for Snowflake)
        fieldnames = [
            'symbol', 'transaction_date', 'filing_date', 'owner_name', 'owner_title',
            'transaction_type', 'acquisition_disposition', 'security_name', 
            'security_type', 'shares', 'value', 'shares_owned_following'
        ]
        
        # Convert to CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        # Extract transactions from API response
        # API structure: {"data": [{transaction1}, {transaction2}, ...]}
        transactions = data.get('data', [])
        if not transactions:
            logger.warning(f"⚠️  No transactions to upload for {symbol}")
            return False
        
        # Write transactions with explicit field mapping
        for txn in transactions:
            row = {
                'symbol': symbol,
                'transaction_date': txn.get('transactionDate'),
                'filing_date': txn.get('filingDate'),
                'owner_name': txn.get('ownerName'),
                'owner_title': txn.get('ownerTitle'),
                'transaction_type': txn.get('transactionType'),
                'acquisition_disposition': txn.get('acquistionOrDisposition'),  # Note: API may have typo
                'security_name': txn.get('securityName'),
                'security_type': txn.get('securityType'),
                'shares': txn.get('securitiesTransacted'),
                'value': txn.get('transactionPrice'),
                'shares_owned_following': txn.get('securitiesOwned')
            }
            writer.writerow(row)
        
        # Upload to S3
        csv_content = output.getvalue()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{prefix}{symbol}_{timestamp}.csv"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        
        logger.info(f"✅ Uploaded: s3://{bucket}/{s3_key} ({len(transactions)} transactions)")
        return True
    
    except Exception as e:
        logger.error(f"❌ S3 upload failed for {symbol}: {e}")
        return False


def main():
    """Main execution function."""
    logger.info("🚀 Starting Insider Transactions Watermark ETL")
    
    # Get environment variables
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    snowflake_config = {
        'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
        'user': os.environ.get('SNOWFLAKE_USER'),
        'password': os.environ.get('SNOWFLAKE_PASSWORD'),
        'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
        'database': os.environ.get('SNOWFLAKE_DATABASE'),
        'schema': os.environ.get('SNOWFLAKE_SCHEMA')
    }
    s3_bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = os.environ.get('S3_INSIDER_TRANSACTIONS_PREFIX', 'insider_transactions/')
    
    # Get workflow parameters
    exchange_filter = os.environ.get('EXCHANGE_FILTER', '').strip()
    max_symbols_str = os.environ.get('MAX_SYMBOLS', '').strip()
    skip_recent_hours_str = os.environ.get('SKIP_RECENT_HOURS', '').strip()
    batch_size = int(os.environ.get('BATCH_SIZE', '100'))
    
    max_symbols = int(max_symbols_str) if max_symbols_str else None
    skip_recent_hours = int(skip_recent_hours_str) if skip_recent_hours_str else None
    
    if not exchange_filter:
        exchange_filter = None
    
    logger.info(f"📋 Configuration:")
    logger.info(f"   Exchange filter: {exchange_filter or 'ALL'}")
    logger.info(f"   Max symbols: {max_symbols or 'unlimited'}")
    logger.info(f"   Skip recent hours: {skip_recent_hours or 'none (process all)'}")
    logger.info(f"   Batch size: {batch_size}")
    logger.info(f"   S3 destination: s3://{s3_bucket}/{s3_prefix}")
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # STEP 1: Clean up S3 bucket (CRITICAL - prevents duplicate data)
    cleanup_s3_bucket(s3_bucket, s3_prefix, s3_client)
    
    # STEP 2: Query watermarks and CLOSE connection immediately
    watermark_manager = WatermarkETLManager(snowflake_config, table_name='INSIDER_TRANSACTIONS')
    try:
        symbols = watermark_manager.get_symbols_to_process(
            exchange_filter=exchange_filter,
            max_symbols=max_symbols,
            skip_recent_hours=skip_recent_hours
        )
    finally:
        watermark_manager.close()  # CRITICAL: Close before API extraction
    
    if not symbols:
        logger.info("✅ No symbols to process")
        return
    
    # STEP 3: Extract from API and upload to S3 (NO Snowflake connection open)
    successful_updates = []
    failed_symbols = []
    api_calls = 0
    start_time = time.time()
    
    for i, symbol_data in enumerate(symbols, 1):
        symbol = symbol_data['SYMBOL']
        logger.info(f"[{i}/{len(symbols)}] Processing {symbol}...")
        
        # Fetch data from API
        data, status = fetch_insider_transactions(symbol, api_key)
        api_calls += 1
        
        if status == "success":
            # Upload to S3
            if upload_to_s3(symbol, data, s3_client, s3_bucket, s3_prefix):
                # Extract date range for watermark
                transactions = data.get('data', [])
                dates = [txn.get('transactionDate') for txn in transactions if txn.get('transactionDate')]
                
                if dates:
                    successful_updates.append({
                        'symbol': symbol,
                        'first_date': min(dates),
                        'last_date': max(dates)
                    })
                else:
                    # No valid dates, but upload succeeded
                    successful_updates.append({
                        'symbol': symbol,
                        'first_date': datetime.now().strftime('%Y-%m-%d'),
                        'last_date': datetime.now().strftime('%Y-%m-%d')
                    })
            else:
                failed_symbols.append(symbol)
        
        elif status == "no_data":
            # Successful API call but no insider transactions (this is normal)
            logger.info(f"   No insider transactions for {symbol}")
            successful_updates.append({
                'symbol': symbol,
                'first_date': datetime.now().strftime('%Y-%m-%d'),
                'last_date': datetime.now().strftime('%Y-%m-%d')
            })
        
        elif status == "rate_limited":
            logger.warning(f"⚠️  Rate limited on {symbol}, stopping processing")
            failed_symbols.append(symbol)
            break  # Stop processing to avoid more rate limits
        
        else:
            logger.error(f"❌ Failed to fetch {symbol}: {status}")
            failed_symbols.append(symbol)
        
        # Rate limiting: 75 calls/minute = 0.8 second delay
        if i < len(symbols):
            time.sleep(0.85)
    
    elapsed_time = time.time() - start_time
    
    # STEP 4: Open NEW connection and bulk update watermarks
    try:
        watermark_manager.bulk_update_watermarks(successful_updates, failed_symbols)
    finally:
        watermark_manager.close()
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("📊 INSIDER TRANSACTIONS ETL SUMMARY")
    logger.info("="*60)
    logger.info(f"✅ Successful: {len(successful_updates)}")
    logger.info(f"❌ Failed: {len(failed_symbols)}")
    logger.info(f"🌐 API calls: {api_calls}")
    logger.info(f"⏱️  Duration: {elapsed_time:.1f} seconds")
    logger.info(f"📊 Rate: {api_calls / (elapsed_time / 60):.1f} calls/minute")
    logger.info("="*60)
    
    if failed_symbols:
        logger.info(f"⚠️  Failed symbols: {', '.join(failed_symbols[:10])}")
        if len(failed_symbols) > 10:
            logger.info(f"   ... and {len(failed_symbols) - 10} more")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=True)
        sys.exit(1)

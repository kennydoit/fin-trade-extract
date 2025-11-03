#!/usr/bin/env python3
"""
Watermark-Based Balance Sheet ETL
Fetches BALANCE_SHEET data using the ETL_WATERMARKS table for incremental processing.
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
import cryptography.hazmat.primitives.serialization as serialization
from cryptography.hazmat.backends import default_backend

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WatermarkETLManager:
    """Manages ETL processing using the ETL_WATERMARKS table."""
    def __init__(self, snowflake_config: Dict[str, str], table_name: str = 'BALANCE_SHEET'):
        self.snowflake_config = snowflake_config
        self.table_name = table_name
        self.connection = None

    def connect(self):
        if not self.connection:
            self.connection = snowflake.connector.connect(**self.snowflake_config)
            logger.info("✅ Connected to Snowflake")

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("🔒 Snowflake connection closed")

    def get_symbols_to_process(self, exchange_filter: Optional[str] = None,
                               max_symbols: Optional[int] = None,
                               skip_recent_hours: Optional[int] = None,
                               consecutive_failure_threshold: Optional[int] = None) -> List[Dict]:
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
              AND (LAST_FISCAL_DATE IS NULL OR LAST_FISCAL_DATE < DATEADD(day, -135, CURRENT_DATE()))
        """
        if skip_recent_hours:
            query += f"""
              AND (LAST_SUCCESSFUL_RUN IS NULL 
                   OR LAST_SUCCESSFUL_RUN < DATEADD(hour, -{skip_recent_hours}, CURRENT_TIMESTAMP()))
            """
        if exchange_filter and exchange_filter != 'ALL':
            query += f"\n              AND UPPER(EXCHANGE) = '{exchange_filter.upper()}'"
        if consecutive_failure_threshold is not None:
            query += f"\n              AND (CONSECUTIVE_FAILURES IS NULL OR CONSECUTIVE_FAILURES < {consecutive_failure_threshold})"
        query += "\n            ORDER BY SYMBOL"
        if max_symbols:
            query += f"\n            LIMIT {max_symbols}"
        logger.info(f"📊 Querying watermarks for {self.table_name}...")
        logger.info(f"📅 Only symbols with LAST_FISCAL_DATE older than 135 days (or NULL)")
        if exchange_filter:
            logger.info(f"🏢 Exchange filter: {exchange_filter}")
        if max_symbols:
            logger.info(f"🔒 Symbol limit: {max_symbols}")
        if skip_recent_hours:
            logger.info(f"⏭️  Skip recent: {skip_recent_hours} hours")
        if consecutive_failure_threshold is not None:
            logger.info(f"🚫 Consecutive failure threshold: {consecutive_failure_threshold}")
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
        if not self.connection:
            raise RuntimeError("❌ No active Snowflake connection. Call connect() first.")
        cursor = self.connection.cursor()
        if successful_updates:
            logger.info(f"📝 Bulk updating {len(successful_updates)} successful watermarks...")
            cursor.execute("""
                CREATE TEMPORARY TABLE WATERMARK_UPDATES (
                    SYMBOL VARCHAR(20),
                    FIRST_DATE DATE,
                    LAST_DATE DATE
                )
            """)
            values_list = []
            for update in successful_updates:
                values_list.append(
                    f"('{update['symbol']}', "
                    f"TO_DATE('{update['first_date']}', 'YYYY-MM-DD'), "
                    f"TO_DATE('{update['last_date']}', 'YYYY-MM-DD'))"
                )
            values_clause = ',\n'.join(values_list)
            cursor.execute(f"""
                INSERT INTO WATERMARK_UPDATES (SYMBOL, FIRST_DATE, LAST_DATE)
                VALUES {values_clause}
            """)
            cursor.execute(f"""
                MERGE INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS target
                USING WATERMARK_UPDATES source
                ON target.TABLE_NAME = '{self.table_name}'
                   AND target.SYMBOL = source.SYMBOL
                WHEN MATCHED THEN UPDATE SET
                    FIRST_FISCAL_DATE = COALESCE(target.FIRST_FISCAL_DATE, source.FIRST_DATE),
                    LAST_FISCAL_DATE = source.LAST_DATE,
                    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                    CONSECUTIVE_FAILURES = 0,
                    API_ELIGIBLE = CASE 
                        WHEN target.DELISTING_DATE IS NOT NULL AND target.DELISTING_DATE <= CURRENT_DATE() 
                        THEN 'DEL'
                        ELSE target.API_ELIGIBLE 
                    END,
                    UPDATED_AT = CURRENT_TIMESTAMP()
            """)
            logger.info(f"✅ Bulk updated {len(successful_updates)} successful watermarks in single MERGE")
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

class RateLimiter:
    def __init__(self, calls_per_minute: int = 75):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = 0
    def wait_if_needed(self):
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        if time_since_last_call < self.min_interval:
            sleep_time = self.min_interval - time_since_last_call
            time.sleep(sleep_time)
        self.last_call_time = time.time()

def cleanup_s3_bucket(bucket: str, prefix: str, s3_client) -> int:
    logger.info(f"🧹 Cleaning up S3 bucket: s3://{bucket}/{prefix}")
    deleted_count = 0
    continuation_token = None
    while True:
        list_params = {'Bucket': bucket, 'Prefix': prefix}
        if continuation_token:
            list_params['ContinuationToken'] = continuation_token
        response = s3_client.list_objects_v2(**list_params)
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )
            deleted_count += len(objects_to_delete)
            logger.info(f"  🗑️  Deleted {len(objects_to_delete)} files")
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    return deleted_count

def fetch_balance_sheet(symbol: str, api_key: str, rate_limiter: RateLimiter) -> Optional[Dict]:
    rate_limiter.wait_if_needed()
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'BALANCE_SHEET',
        'symbol': symbol,
        'apikey': api_key
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if 'Error Message' in data:
            logger.warning(f"⚠️  API Error for {symbol}: {data['Error Message']}")
            return None
        if 'Note' in data:
            logger.warning(f"⚠️  API Rate Limit for {symbol}: {data['Note']}")
            return None
        if 'annualReports' not in data and 'quarterlyReports' not in data:
            logger.warning(f"⚠️  No balance sheet data for {symbol}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request failed for {symbol}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error for {symbol}: {e}")
        return None

def upload_to_s3(symbol: str, data: Dict, s3_client, bucket: str, prefix: str) -> bool:
    try:
        fieldnames = [
            'symbol', 'fiscal_date_ending', 'period_type', 'reported_currency',
            'total_assets', 'total_current_assets', 'cash_and_cash_equivalents_at_carrying_value',
            'cash_and_short_term_investments', 'inventory', 'current_net_receivables',
            'total_non_current_assets', 'property_plant_equipment', 'accumulated_depreciation_amortization_ppe',
            'intangible_assets', 'intangible_assets_excluding_goodwill', 'goodwill',
            'investments', 'long_term_investments', 'short_term_investments',
            'other_current_assets', 'other_non_current_assets', 'total_liabilities',
            'total_current_liabilities', 'current_accounts_payable', 'deferred_revenue',
            'current_debt', 'short_term_debt', 'total_non_current_liabilities',
            'capital_lease_obligations', 'long_term_debt', 'current_long_term_debt',
            'long_term_debt_noncurrent', 'short_long_term_debt_total', 'other_current_liabilities',
            'other_non_current_liabilities', 'total_shareholder_equity', 'treasury_stock',
            'retained_earnings', 'common_stock', 'common_stock_shares_outstanding'
        ]
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for report in data.get('annualReports', []):
            row = {'symbol': symbol, 'period_type': 'annual', 'reported_currency': report.get('reportedCurrency')}
            row.update({
                'fiscal_date_ending': report.get('fiscalDateEnding'),
                'total_assets': report.get('totalAssets'),
                'total_current_assets': report.get('totalCurrentAssets'),
                'cash_and_cash_equivalents_at_carrying_value': report.get('cashAndCashEquivalentsAtCarryingValue'),
                'cash_and_short_term_investments': report.get('cashAndShortTermInvestments'),
                'inventory': report.get('inventory'),
                'current_net_receivables': report.get('currentNetReceivables'),
                'total_non_current_assets': report.get('totalNonCurrentAssets'),
                'property_plant_equipment': report.get('propertyPlantEquipment'),
                'accumulated_depreciation_amortization_ppe': report.get('accumulatedDepreciationAmortizationPPE'),
                'intangible_assets': report.get('intangibleAssets'),
                'intangible_assets_excluding_goodwill': report.get('intangibleAssetsExcludingGoodwill'),
                'goodwill': report.get('goodwill'),
                'investments': report.get('investments'),
                'long_term_investments': report.get('longTermInvestments'),
                'short_term_investments': report.get('shortTermInvestments'),
                'other_current_assets': report.get('otherCurrentAssets'),
                'other_non_current_assets': report.get('otherNonCurrentAssets'),
                'total_liabilities': report.get('totalLiabilities'),
                'total_current_liabilities': report.get('totalCurrentLiabilities'),
                'current_accounts_payable': report.get('currentAccountsPayable'),
                'deferred_revenue': report.get('deferredRevenue'),
                'current_debt': report.get('currentDebt'),
                'short_term_debt': report.get('shortTermDebt'),
                'total_non_current_liabilities': report.get('totalNonCurrentLiabilities'),
                'capital_lease_obligations': report.get('capitalLeaseObligations'),
                'long_term_debt': report.get('longTermDebt'),
                'current_long_term_debt': report.get('currentLongTermDebt'),
                'long_term_debt_noncurrent': report.get('longTermDebtNoncurrent'),
                'short_long_term_debt_total': report.get('shortLongTermDebtTotal'),
                'other_current_liabilities': report.get('otherCurrentLiabilities'),
                'other_non_current_liabilities': report.get('otherNonCurrentLiabilities'),
                'total_shareholder_equity': report.get('totalShareholderEquity'),
                'treasury_stock': report.get('treasuryStock'),
                'retained_earnings': report.get('retainedEarnings'),
                'common_stock': report.get('commonStock'),
                'common_stock_shares_outstanding': report.get('commonStockSharesOutstanding')
            })
            writer.writerow(row)
        for report in data.get('quarterlyReports', []):
            row = {'symbol': symbol, 'period_type': 'quarterly', 'reported_currency': report.get('reportedCurrency')}
            row.update({
                'fiscal_date_ending': report.get('fiscalDateEnding'),
                'total_assets': report.get('totalAssets'),
                'total_current_assets': report.get('totalCurrentAssets'),
                'cash_and_cash_equivalents_at_carrying_value': report.get('cashAndCashEquivalentsAtCarryingValue'),
                'cash_and_short_term_investments': report.get('cashAndShortTermInvestments'),
                'inventory': report.get('inventory'),
                'current_net_receivables': report.get('currentNetReceivables'),
                'total_non_current_assets': report.get('totalNonCurrentAssets'),
                'property_plant_equipment': report.get('propertyPlantEquipment'),
                'accumulated_depreciation_amortization_ppe': report.get('accumulatedDepreciationAmortizationPPE'),
                'intangible_assets': report.get('intangibleAssets'),
                'intangible_assets_excluding_goodwill': report.get('intangibleAssetsExcludingGoodwill'),
                'goodwill': report.get('goodwill'),
                'investments': report.get('investments'),
                'long_term_investments': report.get('longTermInvestments'),
                'short_term_investments': report.get('shortTermInvestments'),
                'other_current_assets': report.get('otherCurrentAssets'),
                'other_non_current_assets': report.get('otherNonCurrentAssets'),
                'total_liabilities': report.get('totalLiabilities'),
                'total_current_liabilities': report.get('totalCurrentLiabilities'),
                'current_accounts_payable': report.get('currentAccountsPayable'),
                'deferred_revenue': report.get('deferredRevenue'),
                'current_debt': report.get('currentDebt'),
                'short_term_debt': report.get('shortTermDebt'),
                'total_non_current_liabilities': report.get('totalNonCurrentLiabilities'),
                'capital_lease_obligations': report.get('capitalLeaseObligations'),
                'long_term_debt': report.get('longTermDebt'),
                'current_long_term_debt': report.get('currentLongTermDebt'),
                'long_term_debt_noncurrent': report.get('longTermDebtNoncurrent'),
                'short_long_term_debt_total': report.get('shortLongTermDebtTotal'),
                'other_current_liabilities': report.get('otherCurrentLiabilities'),
                'other_non_current_liabilities': report.get('otherNonCurrentLiabilities'),
                'total_shareholder_equity': report.get('totalShareholderEquity'),
                'treasury_stock': report.get('treasuryStock'),
                'retained_earnings': report.get('retainedEarnings'),
                'common_stock': report.get('commonStock'),
                'common_stock_shares_outstanding': report.get('commonStockSharesOutstanding')
            })
            writer.writerow(row)
        csv_content = output.getvalue()
        s3_key = f"{prefix}{symbol}.csv"
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        total_reports = len(data.get('annualReports', [])) + len(data.get('quarterlyReports', []))
        logger.info(f"✅ Uploaded: s3://{bucket}/{s3_key} ({total_reports} reports)")
        return True
    except Exception as e:
        logger.error(f"❌ S3 upload failed for {symbol}: {e}")
        return False

def process_symbols_in_batches(symbols: List[Dict], api_key: str, s3_client, 
                               bucket: str, prefix: str, batch_size: int = 50) -> Dict:
    rate_limiter = RateLimiter(calls_per_minute=75)
    total = len(symbols)
    successful = []
    failed = []
    for i, symbol_info in enumerate(symbols, 1):
        symbol = symbol_info['symbol']
        logger.info(f"📊 [{i}] Processing {symbol}...")
        data = fetch_balance_sheet(symbol, api_key, rate_limiter)
        if data is None:
            failed.append(symbol)
            continue
        if upload_to_s3(symbol, data, s3_client, bucket, prefix):
            first_date = None
            last_date = None
            for report in data.get('annualReports', []) + data.get('quarterlyReports', []):
                fiscal_date = report.get('fiscalDateEnding')
                if fiscal_date:
                    if first_date is None or fiscal_date < first_date:
                        first_date = fiscal_date
                    if last_date is None or fiscal_date > last_date:
                        last_date = fiscal_date
            if first_date and last_date:
                successful.append({
                    'symbol': symbol,
                    'first_date': first_date,
                    'last_date': last_date
                })
        else:
            failed.append(symbol)
        if i % 10 == 0:
            logger.info(f"🔄 Progress: {i}/{total} symbols processed")
    return {
        'successful': successful,
        'failed': failed
    }

def main():
    start_time = datetime.now()
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    s3_bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = os.environ.get('S3_BALANCE_SHEET_PREFIX', 'balance_sheet/')
    exchange_filter = os.environ.get('EXCHANGE_FILTER')
    max_symbols = os.environ.get('MAX_SYMBOLS')
    skip_recent_hours = os.environ.get('SKIP_RECENT_HOURS')
    batch_size = int(os.environ.get('BATCH_SIZE', '50'))
    consecutive_failure_threshold = os.environ.get('CONSECUTIVE_FAILURE_THRESHOLD')
    if max_symbols:
        max_symbols = int(max_symbols)
    if skip_recent_hours:
        skip_recent_hours = int(skip_recent_hours)
    if consecutive_failure_threshold:
        consecutive_failure_threshold = int(consecutive_failure_threshold)
    if not api_key:
        logger.error("❌ ALPHAVANTAGE_API_KEY environment variable not set")
        sys.exit(1)
    # Load private key for Snowflake key-pair authentication
    private_key_path = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', 'snowflake_rsa_key.der')
    private_key_passphrase = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
    with open(private_key_path, 'rb') as key_file:
        p_key = key_file.read()
    private_key = serialization.load_der_private_key(
        p_key,
        password=private_key_passphrase.encode() if private_key_passphrase else None,
        backend=default_backend()
    )
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    snowflake_config = {
        'user': os.environ.get('SNOWFLAKE_USER'),
        'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
        'private_key': pkb,
        'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
        'database': os.environ.get('SNOWFLAKE_DATABASE'),
        'schema': os.environ.get('SNOWFLAKE_SCHEMA')
    }
    watermark_manager = WatermarkETLManager(snowflake_config)
    s3_client = boto3.client('s3')
    logger.info("=" * 60)
    logger.info("🧹 STEP 1: Clean up existing S3 files")
    logger.info("=" * 60)
    deleted_count = cleanup_s3_bucket(s3_bucket, s3_prefix, s3_client)
    logger.info(f"✅ Cleanup complete: {deleted_count} old files removed")
    logger.info("")
    logger.info("=" * 60)
    logger.info("🔍 STEP 2: Query watermarks for symbols to process")
    logger.info("=" * 60)
    try:
        symbols_to_process = watermark_manager.get_symbols_to_process(
            exchange_filter=exchange_filter,
            max_symbols=max_symbols,
            skip_recent_hours=skip_recent_hours,
            consecutive_failure_threshold=consecutive_failure_threshold
        )
    finally:
        watermark_manager.close()
        logger.info("🔌 Snowflake connection closed after watermark query")
    if not symbols_to_process:
        logger.warning("⚠️  No symbols to process")
        return
    logger.info("")
    logger.info("=" * 60)
    logger.info("🚀 STEP 3: Extract balance sheet data from Alpha Vantage")
    logger.info("=" * 60)
    results = process_symbols_in_batches(
        symbols_to_process,
        api_key,
        s3_client,
        s3_bucket,
        s3_prefix,
        batch_size
    )
    logger.info("")
    logger.info("=" * 60)
    logger.info("📊 EXTRACTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Successful: {len(results['successful'])} symbols")
    logger.info(f"❌ Failed: {len(results['failed'])} symbols")
    if results['failed']:
        logger.info(f"Failed symbols: {', '.join(results['failed'][:10])}")
        if len(results['failed']) > 10:
            logger.info(f"  ... and {len(results['failed']) - 10} more")
    logger.info("")
    logger.info("=" * 60)
    logger.info("💾 STEP 4: Update watermarks in Snowflake")
    logger.info("=" * 60)
    try:
        watermark_manager.connect()
        watermark_manager.bulk_update_watermarks(
            results['successful'],
            results['failed']
        )
    finally:
        watermark_manager.close()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60
    results_summary = {
        'total_symbols': len(symbols_to_process),
        'successful': len(results['successful']),
        'failed': len(results['failed']),
        'duration_minutes': round(duration, 2),
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'failed_symbols': results['failed']
    }
    with open('/tmp/watermark_etl_results.json', 'w') as f:
        json.dump(results_summary, f, indent=2)
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ BALANCE SHEET ETL COMPLETE!")
    logger.info("=" * 60)
    logger.info(f"⏱️  Total duration: {duration:.2f} minutes")
    logger.info(f"📊 Processing rate: {len(symbols_to_process)/duration:.1f} symbols/minute")
    logger.info("")

if __name__ == '__main__':
    main()
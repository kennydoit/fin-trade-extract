#!/usr/bin/env python3
"""
Bulk Balance Sheet Data Extractor for fin-trade-extract pipeline.

Features:
- Bulk processing of balance sheet fundamentals data from Alpha Vantage
- Intelligent incremental processing with dependency tracking
- Cross-exchange support for all fundamentals-eligible symbols
- Rate limiting optimized for Alpha Vantage Premium (75 calls/minute)
- Comprehensive error handling and retry logic
- S3 upload with organized structure
- Snowflake integration for tracking processing status

Designed to handle ~4,000 fundamentals symbols across all exchanges.
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
from utils.incremental_etl import IncrementalETLManager
from utils.symbol_screener import SymbolScreener, ScreeningCriteria, AssetType, ExchangeType
from utils.universe_management import UniverseManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BalanceSheetExtractor:
    """Bulk balance sheet data extraction with incremental ETL intelligence."""

    def __init__(self):
        """Initialize extractor with configuration."""
        self.api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        self.s3_bucket = os.getenv('S3_BUCKET', 'fin-trade-craft-landing')
        self.s3_prefix = os.getenv('S3_BALANCE_SHEET_PREFIX', 'balance_sheet/')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.load_date = os.getenv('LOAD_DATE', datetime.now().strftime('%Y%m%d_%H%M%S'))
        
        # Processing configuration
        self.processing_mode = os.getenv('PROCESSING_MODE', 'incremental')
        self.universe_name = os.getenv('UNIVERSE_NAME', 'fundamentals_eligible')
        self.exchange_filter = os.getenv('EXCHANGE_FILTER', 'ALL')
        self.asset_type_filter = os.getenv('ASSET_TYPE_FILTER', 'ALL')
        self.batch_size = int(os.getenv('BATCH_SIZE', '25'))  # Smaller batches for fundamentals
        self.max_batches = int(os.getenv('MAX_BATCHES', '0')) if os.getenv('MAX_BATCHES') else None
        self.max_symbols = int(os.getenv('MAX_SYMBOLS', '0')) if os.getenv('MAX_SYMBOLS') else None
        self.failure_threshold = float(os.getenv('FAILURE_THRESHOLD', '0.5'))
        
        # Rate limiting (fundamentals are less frequent than time series)
        self.api_delay = 0.8  # 75 calls/minute = 0.8s between calls
        self.retry_delay = 5.0
        self.max_retries = 3
        
        # Initialize services
        self.s3_client = boto3.client('s3', region_name=self.aws_region)
        self.snowflake_config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        }
        
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
        
        logger.info(f"Initialized Balance Sheet Extractor")
        logger.info(f"Processing mode: {self.processing_mode}")
        logger.info(f"Universe: {self.universe_name}")
        logger.info(f"Exchange filter: {self.exchange_filter}")
        logger.info(f"Asset type filter: {self.asset_type_filter}")
        logger.info(f"Batch size: {self.batch_size}")

    def get_symbols_to_process(self) -> List[str]:
        """Get list of symbols that need balance sheet data processing."""
        logger.info("🔍 Identifying symbols for balance sheet processing...")
        
        try:
            # Initialize incremental ETL manager
            etl_manager = IncrementalETLManager(self.snowflake_config)
            
            if self.processing_mode == 'universe':
                # Process specific universe
                logger.info(f"📋 Processing universe: {self.universe_name}")
                try:
                    universe_manager = UniverseManager(self.snowflake_config)
                    universe_def = universe_manager.load_universe(self.universe_name)
                    symbols = universe_def.symbols
                    universe_manager.close_connection()
                except Exception as e:
                    logger.warning(f"⚠️ Universe {self.universe_name} not found, falling back to fundamentals screening: {e}")
                    symbols = self._get_fundamentals_eligible_symbols()
                
            elif self.processing_mode == 'full_refresh':
                # Force refresh all eligible symbols
                logger.info("🔄 Full refresh mode - processing all fundamentals-eligible symbols")
                symbols = self._get_fundamentals_eligible_symbols()
                
            else:  # incremental mode (default)
                # Get symbols needing updates based on staleness
                logger.info("⚡ Incremental mode - identifying stale symbols")
                staleness_days = 30  # Fundamentals update monthly/quarterly
                
                # Get fundamentals-eligible symbols
                all_symbols = self._get_fundamentals_eligible_symbols()
                
                # Filter to symbols that need updates
                symbols = etl_manager.get_symbols_needing_update(
                    symbols=all_symbols,
                    data_type='BALANCE_SHEET',
                    staleness_hours=staleness_days * 24,
                    max_symbols=self.max_symbols
                )
            
            # Apply exchange/asset type filtering if specified
            if self.exchange_filter != 'ALL' or self.asset_type_filter != 'ALL':
                symbols = self._apply_filters(symbols)
            
            # Apply limits
            if self.max_symbols and len(symbols) > self.max_symbols:
                logger.info(f"🔒 Limiting to {self.max_symbols} symbols for testing")
                symbols = symbols[:self.max_symbols]
            
            logger.info(f"📊 Found {len(symbols)} symbols needing balance sheet processing")
            return symbols
            
        except Exception as e:
            logger.error(f"❌ Error getting symbols to process: {e}")
            raise
    
    def _get_fundamentals_eligible_symbols(self) -> List[str]:
        """Get symbols eligible for fundamentals data (typically larger companies)."""
        # For now, use the basic symbol list approach since advanced screening is having issues
        # TODO: Re-enable advanced screening once asset type mapping is confirmed
        logger.info("📈 Using basic symbol screening for fundamentals eligibility")
        try:
            return self._get_basic_symbol_list()
        except Exception as e:
            logger.error(f"❌ Error getting basic symbol list: {e}")
            return []
    
    def _get_basic_symbol_list(self) -> List[str]:
        """Fallback method to get basic symbol list from listing status."""
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            query = """
            SELECT DISTINCT symbol 
            FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
            WHERE status = 'Active' 
                AND assetType LIKE '%Stock%'  -- More flexible matching
                AND exchange IN ('NASDAQ', 'NYSE', 'AMEX')
                AND symbol IS NOT NULL 
                AND symbol != ''
                AND LEN(symbol) <= 5  -- Reasonable symbol length filter
            ORDER BY symbol
            LIMIT 1000  -- Limit for testing phase
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            symbols = [row[0] for row in results]
            
            conn.close()
            
            logger.info(f"📋 Fallback query found {len(symbols)} symbols")
            return symbols
            
        except Exception as e:
            logger.error(f"❌ Error in fallback symbol query: {e}")
            raise

    def _apply_filters(self, symbols: List[str]) -> List[str]:
        """Apply exchange and asset type filters to symbol list."""
        if self.exchange_filter == 'ALL' and self.asset_type_filter == 'ALL':
            return symbols
        
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Build filter query
            symbol_list = "', '".join(symbols)
            where_clauses = [f"symbol IN ('{symbol_list}')"]
            
            if self.exchange_filter != 'ALL':
                where_clauses.append(f"exchange = '{self.exchange_filter}'")
                
            if self.asset_type_filter != 'ALL':
                where_clauses.append(f"assetType = '{self.asset_type_filter}'")
            
            query = f"""
            SELECT DISTINCT symbol 
            FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
            WHERE {' AND '.join(where_clauses)}
            ORDER BY symbol
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            filtered_symbols = [row[0] for row in results]
            
            conn.close()
            
            logger.info(f"🔍 Applied filters: {len(symbols)} → {len(filtered_symbols)} symbols")
            return filtered_symbols
            
        except Exception as e:
            logger.warning(f"⚠️ Error applying filters, using unfiltered list: {e}")
            return symbols

    def fetch_balance_sheet(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch balance sheet data for a single symbol."""
        url = "https://www.alphavantage.co/query"
        
        params = {
            'function': 'BALANCE_SHEET',
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
                
                if 'Information' in data:
                    logger.warning(f"ℹ️ API info for {symbol}: {data['Information']}")
                    return None
                
                # Validate response structure
                if 'annualReports' not in data and 'quarterlyReports' not in data:
                    logger.warning(f"⚠️ No balance sheet data available for {symbol}")
                    return None
                
                logger.debug(f"✅ Successfully fetched balance sheet for {symbol}")
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

    def process_balance_sheet_data(self, symbol: str, data: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Process and normalize balance sheet data into DataFrame."""
        try:
            records = []
            
            # Process annual reports
            if 'annualReports' in data:
                for report in data['annualReports']:
                    record = {
                        'symbol': symbol,
                        'report_type': 'annual',
                        'fiscal_date_ending': report.get('fiscalDateEnding', ''),
                        'reported_currency': report.get('reportedCurrency', 'USD'),
                        
                        # Assets
                        'total_assets': self._safe_numeric(report.get('totalAssets')),
                        'current_assets': self._safe_numeric(report.get('totalCurrentAssets')),
                        'cash_and_equivalents': self._safe_numeric(report.get('cashAndCashEquivalentsAtCarryingValue')),
                        'cash_and_short_term_investments': self._safe_numeric(report.get('cashAndShortTermInvestments')),
                        'inventory': self._safe_numeric(report.get('inventory')),
                        'current_net_receivables': self._safe_numeric(report.get('currentNetReceivables')),
                        'non_current_assets': self._safe_numeric(report.get('totalNonCurrentAssets')),
                        'property_plant_equipment': self._safe_numeric(report.get('propertyPlantEquipment')),
                        'accumulated_depreciation_amortization_ppe': self._safe_numeric(report.get('accumulatedDepreciationAmortizationPPE')),
                        'intangible_assets': self._safe_numeric(report.get('intangibleAssets')),
                        'intangible_assets_excluding_goodwill': self._safe_numeric(report.get('intangibleAssetsExcludingGoodwill')),
                        'goodwill': self._safe_numeric(report.get('goodwill')),
                        'investments': self._safe_numeric(report.get('investments')),
                        'long_term_investments': self._safe_numeric(report.get('longTermInvestments')),
                        'short_term_investments': self._safe_numeric(report.get('shortTermInvestments')),
                        'other_current_assets': self._safe_numeric(report.get('otherCurrentAssets')),
                        'other_non_current_assets': self._safe_numeric(report.get('otherNonCurrentAssets')),
                        
                        # Liabilities
                        'total_liabilities': self._safe_numeric(report.get('totalLiabilities')),
                        'current_liabilities': self._safe_numeric(report.get('totalCurrentLiabilities')),
                        'current_accounts_payable': self._safe_numeric(report.get('currentAccountsPayable')),
                        'deferred_revenue': self._safe_numeric(report.get('deferredRevenue')),
                        'current_debt': self._safe_numeric(report.get('currentDebt')),
                        'short_term_debt': self._safe_numeric(report.get('shortTermDebt')),
                        'non_current_liabilities': self._safe_numeric(report.get('totalNonCurrentLiabilities')),
                        'capital_lease_obligations': self._safe_numeric(report.get('capitalLeaseObligations')),
                        'long_term_debt': self._safe_numeric(report.get('longTermDebt')),
                        'current_long_term_debt': self._safe_numeric(report.get('currentLongTermDebt')),
                        'long_term_debt_noncurrent': self._safe_numeric(report.get('longTermDebtNoncurrent')),
                        'short_long_term_debt_total': self._safe_numeric(report.get('shortLongTermDebtTotal')),
                        'other_current_liabilities': self._safe_numeric(report.get('otherCurrentLiabilities')),
                        'other_non_current_liabilities': self._safe_numeric(report.get('otherNonCurrentLiabilities')),
                        'total_shareholder_equity': self._safe_numeric(report.get('totalShareholderEquity')),
                        'treasury_stock': self._safe_numeric(report.get('treasuryStock')),
                        'retained_earnings': self._safe_numeric(report.get('retainedEarnings')),
                        'common_stock': self._safe_numeric(report.get('commonStock')),
                        'common_stock_shares_outstanding': self._safe_numeric(report.get('commonStockSharesOutstanding')),
                        
                        # Additional metadata
                        'load_date': self.load_date,
                        'fetched_at': datetime.now(timezone.utc).isoformat()
                    }
                    records.append(record)
            
            # Process quarterly reports  
            if 'quarterlyReports' in data:
                for report in data['quarterlyReports']:
                    record = {
                        'symbol': symbol,
                        'report_type': 'quarterly',
                        'fiscal_date_ending': report.get('fiscalDateEnding', ''),
                        'reported_currency': report.get('reportedCurrency', 'USD'),
                        
                        # Assets (same structure as annual)
                        'total_assets': self._safe_numeric(report.get('totalAssets')),
                        'current_assets': self._safe_numeric(report.get('totalCurrentAssets')),
                        'cash_and_equivalents': self._safe_numeric(report.get('cashAndCashEquivalentsAtCarryingValue')),
                        'cash_and_short_term_investments': self._safe_numeric(report.get('cashAndShortTermInvestments')),
                        'inventory': self._safe_numeric(report.get('inventory')),
                        'current_net_receivables': self._safe_numeric(report.get('currentNetReceivables')),
                        'non_current_assets': self._safe_numeric(report.get('totalNonCurrentAssets')),
                        'property_plant_equipment': self._safe_numeric(report.get('propertyPlantEquipment')),
                        'accumulated_depreciation_amortization_ppe': self._safe_numeric(report.get('accumulatedDepreciationAmortizationPPE')),
                        'intangible_assets': self._safe_numeric(report.get('intangibleAssets')),
                        'intangible_assets_excluding_goodwill': self._safe_numeric(report.get('intangibleAssetsExcludingGoodwill')),
                        'goodwill': self._safe_numeric(report.get('goodwill')),
                        'investments': self._safe_numeric(report.get('investments')),
                        'long_term_investments': self._safe_numeric(report.get('longTermInvestments')),
                        'short_term_investments': self._safe_numeric(report.get('shortTermInvestments')),
                        'other_current_assets': self._safe_numeric(report.get('otherCurrentAssets')),
                        'other_non_current_assets': self._safe_numeric(report.get('otherNonCurrentAssets')),
                        
                        # Liabilities (same structure as annual)
                        'total_liabilities': self._safe_numeric(report.get('totalLiabilities')),
                        'current_liabilities': self._safe_numeric(report.get('totalCurrentLiabilities')),
                        'current_accounts_payable': self._safe_numeric(report.get('currentAccountsPayable')),
                        'deferred_revenue': self._safe_numeric(report.get('deferredRevenue')),
                        'current_debt': self._safe_numeric(report.get('currentDebt')),
                        'short_term_debt': self._safe_numeric(report.get('shortTermDebt')),
                        'non_current_liabilities': self._safe_numeric(report.get('totalNonCurrentLiabilities')),
                        'capital_lease_obligations': self._safe_numeric(report.get('capitalLeaseObligations')),
                        'long_term_debt': self._safe_numeric(report.get('longTermDebt')),
                        'current_long_term_debt': self._safe_numeric(report.get('currentLongTermDebt')),
                        'long_term_debt_noncurrent': self._safe_numeric(report.get('longTermDebtNoncurrent')),
                        'short_long_term_debt_total': self._safe_numeric(report.get('shortLongTermDebtTotal')),
                        'other_current_liabilities': self._safe_numeric(report.get('otherCurrentLiabilities')),
                        'other_non_current_liabilities': self._safe_numeric(report.get('otherNonCurrentLiabilities')),
                        'total_shareholder_equity': self._safe_numeric(report.get('totalShareholderEquity')),
                        'treasury_stock': self._safe_numeric(report.get('treasuryStock')),
                        'retained_earnings': self._safe_numeric(report.get('retainedEarnings')),
                        'common_stock': self._safe_numeric(report.get('commonStock')),
                        'common_stock_shares_outstanding': self._safe_numeric(report.get('commonStockSharesOutstanding')),
                        
                        # Additional metadata
                        'load_date': self.load_date,
                        'fetched_at': datetime.now(timezone.utc).isoformat()
                    }
                    records.append(record)
            
            if records:
                df = pd.DataFrame(records)
                logger.debug(f"✅ Processed {len(records)} balance sheet records for {symbol}")
                return df
            else:
                logger.warning(f"⚠️ No valid balance sheet records found for {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error processing balance sheet data for {symbol}: {e}")
            return None
    
    def _safe_numeric(self, value: Any) -> Optional[float]:
        """Safely convert value to numeric, handling None and 'None' strings."""
        if value is None or value == 'None' or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def upload_to_s3(self, symbol: str, df: pd.DataFrame) -> bool:
        """Upload balance sheet data to S3."""
        try:
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            s3_key = f"{self.s3_prefix}BALANCE_SHEET_{symbol}_{timestamp}.csv"
            
            # Convert DataFrame to CSV
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=csv_content,
                ContentType='text/csv'
            )
            
            logger.debug(f"✅ Uploaded balance sheet data for {symbol} to s3://{self.s3_bucket}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading {symbol} to S3: {e}")
            return False

    def update_processing_status(self, symbol: str, success: bool, error_msg: Optional[str] = None):
        """Update incremental ETL processing status."""
        try:
            etl_manager = IncrementalETLManager(self.snowflake_config)
            etl_manager.update_processing_status(
                symbol=symbol,
                data_type='BALANCE_SHEET',
                success=success,
                error_message=error_msg,
                processing_mode=self.processing_mode
            )
        except Exception as e:
            logger.warning(f"⚠️ Could not update processing status for {symbol}: {e}")

    def process_batch(self, symbols: List[str]) -> Dict[str, int]:
        """Process a batch of symbols."""
        batch_stats = {'successful': 0, 'failed': 0, 'skipped': 0}
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"📊 Processing {symbol} ({i}/{len(symbols)} in batch)")
                
                # Add rate limiting delay
                if i > 1:  # Don't delay before first symbol
                    time.sleep(self.api_delay)
                
                # Fetch balance sheet data
                data = self.fetch_balance_sheet(symbol)
                if not data:
                    batch_stats['skipped'] += 1
                    self.update_processing_status(symbol, False, "No data available")
                    continue
                
                # Process data
                df = self.process_balance_sheet_data(symbol, data)
                if df is None or df.empty:
                    batch_stats['skipped'] += 1
                    self.update_processing_status(symbol, False, "Failed to process data")
                    continue
                
                # Upload to S3
                if self.upload_to_s3(symbol, df):
                    batch_stats['successful'] += 1
                    self.update_processing_status(symbol, True)
                    logger.info(f"✅ Successfully processed {symbol}")
                else:
                    batch_stats['failed'] += 1
                    self.update_processing_status(symbol, False, "S3 upload failed")
                    
            except Exception as e:
                logger.error(f"❌ Unexpected error processing {symbol}: {e}")
                batch_stats['failed'] += 1
                self.update_processing_status(symbol, False, str(e))
        
        return batch_stats

    def run_bulk_extraction(self):
        """Run the complete bulk balance sheet extraction process."""
        logger.info("🚀 Starting bulk balance sheet extraction...")
        
        try:
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
            
            logger.info(f"📋 Processing {len(symbols)} symbols in {total_batches} batches")
            
            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(symbols))
                batch_symbols = symbols[start_idx:end_idx]
                
                logger.info(f"🔄 Processing batch {batch_num + 1}/{total_batches} ({len(batch_symbols)} symbols)")
                
                batch_stats = self.process_batch(batch_symbols)
                
                # Update overall stats
                self.stats['successful'] += batch_stats['successful']
                self.stats['failed'] += batch_stats['failed'] 
                self.stats['skipped'] += batch_stats['skipped']
                
                # Check failure threshold
                total_processed = self.stats['successful'] + self.stats['failed']
                if total_processed > 0:
                    failure_rate = self.stats['failed'] / total_processed
                    if failure_rate > self.failure_threshold:
                        logger.error(f"❌ Failure rate ({failure_rate:.2%}) exceeds threshold ({self.failure_threshold:.2%})")
                        logger.error("❌ Stopping processing to prevent further issues")
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
        logger.info("📈 BALANCE SHEET EXTRACTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"🔄 Processing mode: {self.processing_mode}")
        logger.info(f"🌐 Universe: {self.universe_name}")
        logger.info(f"🏢 Exchange filter: {self.exchange_filter}")
        logger.info(f"💼 Asset type filter: {self.asset_type_filter}")
        logger.info(f"📊 Total symbols: {self.stats['total_symbols']}")
        logger.info(f"✅ Successful: {self.stats['successful']}")
        logger.info(f"⚠️ Skipped: {self.stats['skipped']}")
        logger.info(f"❌ Failed: {self.stats['failed']}")
        logger.info(f"🌐 API calls made: {self.stats['api_calls']}")
        logger.info(f"⏱️ Duration: {duration}")
        
        if self.stats['api_calls'] > 0:
            calls_per_minute = (self.stats['api_calls'] / duration.total_seconds()) * 60
            logger.info(f"📈 API rate: {calls_per_minute:.1f} calls/minute")
        
        # Save results for workflow reporting
        results = {
            'processing_mode': self.processing_mode,
            'universe_name': self.universe_name,
            'exchange_filter': self.exchange_filter,
            'asset_type_filter': self.asset_type_filter,
            'total_symbols': self.stats['total_symbols'],
            'successful': self.stats['successful'],
            'skipped': self.stats['skipped'],
            'failed': self.stats['failed'],
            'api_calls': self.stats['api_calls'],
            'duration_seconds': duration.total_seconds()
        }
        
        try:
            with open('/tmp/balance_sheet_results.json', 'w') as f:
                json.dump(results, f, indent=2)
        except Exception as e:
            logger.warning(f"⚠️ Could not save results file: {e}")


def main():
    """Main execution function."""
    try:
        extractor = BalanceSheetExtractor()
        extractor.run_bulk_extraction()
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
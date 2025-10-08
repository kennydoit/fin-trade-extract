#!/usr/bin/env python3
"""
Incremental ETL Management System for fin-trade-extract pipeline.

This module manages which symbols need processing based on:
- Last update timestamps per symbol per data type
- Business rules for refresh frequency  
- Data availability and quality checks
- Universe management (active symbols, exchanges, etc.)

Designed to prevent duplicate work and optimize API usage across all data extraction processes.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple
import logging
import json
from enum import Enum

import snowflake.connector
from dataclasses import dataclass

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataType(Enum):
    """Enum for supported data types."""
    LISTING_STATUS = "listing_status"
    TIME_SERIES_DAILY_ADJUSTED = "time_series_daily_adjusted"
    COMPANY_OVERVIEW = "company_overview"
    EARNINGS = "earnings"
    BALANCE_SHEET = "balance_sheet"


@dataclass
class DataTypeConfig:
    """Configuration for each data type's refresh rules."""
    name: str
    table_name: str
    refresh_frequency_days: int
    priority: int  # 1=highest, 10=lowest
    dependency_types: List[str]  # Must exist before processing this type
    

# Data type configurations
DATA_TYPES = {
    'listing_status': DataTypeConfig(
        name='listing_status',
        table_name='LISTING_STATUS', 
        refresh_frequency_days=7,  # Weekly refresh
        priority=1,  # Highest priority - foundation data
        dependency_types=[]
    ),
    'time_series_daily_adjusted': DataTypeConfig(
        name='time_series_daily_adjusted',
        table_name='TIME_SERIES_DAILY_ADJUSTED',
        refresh_frequency_days=1,  # Daily refresh
        priority=2,  # High priority - core price data
        dependency_types=['listing_status']
    ),
    'company_overview': DataTypeConfig(
        name='company_overview', 
        table_name='COMPANY_OVERVIEW',
        refresh_frequency_days=30,  # Monthly refresh
        priority=3,  # Medium priority - fundamental data
        dependency_types=['listing_status']
    ),
    'earnings': DataTypeConfig(
        name='earnings',
        table_name='EARNINGS',
        refresh_frequency_days=90,  # Quarterly refresh  
        priority=4,  # Lower priority - periodic data
        dependency_types=['listing_status']
    ),
    'balance_sheet': DataTypeConfig(
        name='balance_sheet',
        table_name='BALANCE_SHEET',
        refresh_frequency_days=30,  # Monthly refresh for fundamentals
        priority=3,  # Medium priority - fundamental data
        dependency_types=['listing_status']
    )
}


class IncrementalETLManager:
    """Manages incremental processing logic for all data types."""
    
    def __init__(self, snowflake_config: Dict[str, str]):
        """Initialize with Snowflake connection details."""
        self.snowflake_config = snowflake_config
        self._connection = None
        
    def get_connection(self):
        """Get or create Snowflake connection."""
        if not self._connection:
            self._connection = snowflake.connector.connect(**self.snowflake_config)
        return self._connection
    
    def close_connection(self):
        """Close Snowflake connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def get_universe_symbols(self, exchange_filter: Optional[str] = None, 
                           asset_type_filter: Optional[str] = None,
                           status_filter: str = 'Active') -> List[str]:
        """
        Get list of symbols in the investment universe.
        
        Args:
            exchange_filter: Filter by exchange (e.g., 'NASDAQ', 'NYSE')  
            asset_type_filter: Filter by asset type (e.g., 'Equity', 'ETF')
            status_filter: Filter by status (default: 'Active')
            
        Returns:
            List of symbols to consider for processing
        """
        logger.info(f"Getting universe symbols with filters: exchange={exchange_filter}, asset_type={asset_type_filter}, status={status_filter}")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Build dynamic query based on filters
        where_conditions = ["symbol IS NOT NULL", "symbol != ''"]
        
        if status_filter:
            where_conditions.append(f"status = '{status_filter}'")
            
        if exchange_filter and exchange_filter != 'ALL':
            where_conditions.append(f"UPPER(exchange) LIKE '%{exchange_filter.upper()}%'")
            
        if asset_type_filter:
            where_conditions.append(f"UPPER(assetType) = '{asset_type_filter.upper()}'")
        
        query = f"""
        SELECT DISTINCT symbol
        FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
        WHERE {' AND '.join(where_conditions)}
        ORDER BY symbol
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        symbols = [row[0] for row in results if row[0]]
        logger.info(f"Found {len(symbols)} symbols in universe")
        
        return symbols

    def get_last_update_timestamps(self, data_type: str, symbols: List[str] = None) -> Dict[str, datetime]:
        """
        Get last update timestamp for each symbol for a specific data type.
        
        Args:
            data_type: Type of data (e.g., 'time_series_daily_adjusted')
            symbols: Optional list to filter to specific symbols
            
        Returns:
            Dict mapping symbol -> last update timestamp
        """
        if data_type not in DATA_TYPES:
            raise ValueError(f"Unknown data type: {data_type}")
            
        config = DATA_TYPES[data_type]
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Build query based on data type
        if data_type == 'listing_status':
            query = """
            SELECT symbol, MAX(load_date) as last_update
            FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
            WHERE symbol IS NOT NULL
            GROUP BY symbol
            """
        elif data_type == 'time_series_daily_adjusted':
            query = """
            SELECT symbol, MAX(load_date) as last_update  
            FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED
            WHERE symbol IS NOT NULL
            GROUP BY symbol
            """
        elif data_type == 'balance_sheet':
            query = """
            SELECT symbol, MAX(COALESCE(load_date, created_at::DATE)) as last_update
            FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET
            WHERE symbol IS NOT NULL
            GROUP BY symbol
            """
        else:
            # Generic pattern for other data types
            query = f"""
            SELECT symbol, MAX(load_date) as last_update
            FROM FIN_TRADE_EXTRACT.RAW.{config.table_name}
            WHERE symbol IS NOT NULL
            GROUP BY symbol
            """
        
        # Add symbol filter if provided
        if symbols:
            symbol_list = "', '".join(symbols)
            query += f" HAVING symbol IN ('{symbol_list}')"
        
        try:
            cursor.execute(query)
            results = cursor.fetchall()
        except Exception as e:
            if "does not exist" in str(e).lower() or "invalid identifier" in str(e).lower():
                logger.info(f"Table {config.table_name} does not exist yet, returning empty timestamps for first run")
                return {}
            else:
                raise e
        
        last_updates = {}
        for symbol, last_update in results:
            if last_update:
                last_updates[symbol] = last_update
                
        logger.info(f"Found last update timestamps for {len(last_updates)} symbols in {data_type}")
        return last_updates

    def identify_symbols_to_process(self, data_type: str, 
                                  universe_symbols: List[str],
                                  force_refresh: bool = False,
                                  max_symbols: Optional[int] = None) -> List[str]:
        """
        Identify which symbols need processing based on refresh rules.
        For time series data, excludes delisted stocks that have already been processed.
        
        Args:
            data_type: Type of data to check
            universe_symbols: List of all symbols in universe
            force_refresh: If True, include all symbols regardless of last update
            max_symbols: Optional limit on number of symbols to return
            
        Returns:
            List of symbols that need processing
        """
        if data_type not in DATA_TYPES:
            raise ValueError(f"Unknown data type: {data_type}")
            
        config = DATA_TYPES[data_type]
        
        logger.info(f"Identifying symbols to process for {data_type}")
        logger.info(f"Universe size: {len(universe_symbols)}, Force refresh: {force_refresh}")
        
        # For time series data, exclude delisted stocks that have been processed
        # This prevents wasted API calls on stocks that won't have new data
        working_universe = universe_symbols[:]
        if data_type == 'time_series_daily_adjusted':
            delisted_processed = self.get_delisted_and_processed_symbols(data_type, universe_symbols)
            working_universe = [symbol for symbol in universe_symbols if symbol not in delisted_processed]
            
            if len(delisted_processed) > 0:
                logger.info(f"Excluded {len(delisted_processed)} delisted symbols that have been processed")
                logger.info(f"Working universe reduced to {len(working_universe)} symbols")
        
        if force_refresh:
            # Return all symbols from working universe (up to max_symbols)
            symbols_to_process = working_universe[:max_symbols] if max_symbols else working_universe
            logger.info(f"Force refresh enabled: processing {len(symbols_to_process)} symbols")
            return symbols_to_process
        
        # Get last update timestamps for working universe symbols
        last_updates = self.get_last_update_timestamps(data_type, working_universe)
        
        # Calculate cutoff date based on refresh frequency
        cutoff_datetime = datetime.now() - timedelta(days=config.refresh_frequency_days)
        
        symbols_to_process = []
        symbols_never_processed = []
        symbols_stale = []
        
        for symbol in working_universe:
            last_update = last_updates.get(symbol)
            
            if not last_update:
                # Never processed
                symbols_never_processed.append(symbol)
            else:
                # Normalize both timestamps for comparison
                # Convert last_update to datetime if it's a date
                if hasattr(last_update, 'date'):
                    # It's already a datetime
                    last_update_dt = last_update
                else:
                    # It's a date, convert to datetime at start of day
                    last_update_dt = datetime.combine(last_update, datetime.min.time())
                
                if last_update_dt < cutoff_datetime:
                    # Data is stale
                    symbols_stale.append(symbol)
                # else: symbol is up to date, skip
        
        # Prioritize never-processed symbols, then stale symbols
        symbols_to_process = symbols_never_processed + symbols_stale
        
        # Apply max_symbols limit if specified
        if max_symbols and len(symbols_to_process) > max_symbols:
            symbols_to_process = symbols_to_process[:max_symbols]
            
        logger.info(f"Identified symbols needing {data_type} processing:")
        logger.info(f"  - Never processed: {len(symbols_never_processed)}")
        logger.info(f"  - Stale (>{config.refresh_frequency_days} days): {len(symbols_stale)}")
        logger.info(f"  - Total to process: {len(symbols_to_process)}")
        
        return symbols_to_process

    def get_delisted_and_processed_symbols(self, data_type: str, 
                                         universe_symbols: List[str]) -> List[str]:
        """
        Get symbols that are delisted AND have been successfully processed.
        These symbols should be excluded from future processing since they
        won't generate new time series data.
        
        Args:
            data_type: Type of data to check
            universe_symbols: List of symbols to check
            
        Returns:
            List of symbols that are delisted and already processed
        """
        if not universe_symbols:
            return []
            
        if data_type not in DATA_TYPES:
            raise ValueError(f"Unknown data type: {data_type}")
            
        config = DATA_TYPES[data_type]
        
        # Build query to find delisted symbols that have been successfully processed
        # We check the ETL_WATERMARKS table for symbols with:
        # 1. DELISTING_DATE is not null (symbol is delisted)
        # 2. LAST_SUCCESSFUL_RUN is not null (has been processed successfully)
        symbol_list = "', '".join(universe_symbols)
        query = f"""
        SELECT DISTINCT w.symbol
        FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
        WHERE w.table_name = '{config.table_name}'
          AND w.symbol IN ('{symbol_list}')
          AND w.delisting_date IS NOT NULL
          AND w.last_successful_run IS NOT NULL
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
        except Exception as e:
            if "does not exist" in str(e).lower() or "invalid identifier" in str(e).lower():
                logger.info(f"ETL_WATERMARKS table does not exist yet, no delisted symbols to exclude")
                return []
            else:
                logger.error(f"Error querying delisted symbols: {e}")
                return []
        
        delisted_processed_symbols = [row[0] for row in results]
        
        logger.info(f"Found {len(delisted_processed_symbols)} delisted symbols that have been processed for {data_type}")
        if delisted_processed_symbols:
            logger.info(f"Excluding delisted symbols: {', '.join(delisted_processed_symbols[:10])}" + 
                       (f" and {len(delisted_processed_symbols) - 10} more" if len(delisted_processed_symbols) > 10 else ""))
        
        return delisted_processed_symbols

    def get_symbols_needing_update(self, symbols: List[str], data_type: str, 
                                 staleness_hours: int, max_symbols: Optional[int] = None) -> List[str]:
        """
        Get symbols that need updates based on staleness criteria.
        
        Args:
            symbols: List of symbols to check
            data_type: Type of data (e.g., 'BALANCE_SHEET')
            staleness_hours: Hours after which data is considered stale
            max_symbols: Optional limit on number of symbols to return
            
        Returns:
            List of symbols needing updates
        """
        # Convert to the data type format expected by our existing methods
        data_type_key = data_type.lower().replace('_', '_') if data_type.isupper() else data_type.lower()
        
        if data_type_key not in DATA_TYPES:
            logger.warning(f"Unknown data type: {data_type_key}, using staleness-based filtering")
            # Fallback: return first max_symbols if data type not recognized
            return symbols[:max_symbols] if max_symbols else symbols
        
        # Use existing identify_symbols_to_process method
        symbols_to_process = self.identify_symbols_to_process(
            data_type=data_type_key,
            universe_symbols=symbols,
            force_refresh=False,
            max_symbols=max_symbols
        )
        
        return symbols_to_process

    def initialize_watermarks_from_listing_status(self):
        """
        Initialize watermark table with all symbols from listing status table.
        This ensures we have a complete universe of symbols with their IPO/delisting dates.
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            initialize_query = """
            INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
                (TABLE_NAME, SYMBOL_ID, SYMBOL, IPO_DATE, DELISTING_DATE, CREATED_AT, UPDATED_AT)
            SELECT DISTINCT
                'LISTING_STATUS' as TABLE_NAME,
                ABS(HASH(ls.symbol)) % 1000000000 as SYMBOL_ID,
                ls.symbol as SYMBOL,
                TRY_TO_DATE(ls.ipoDate, 'YYYY-MM-DD') as IPO_DATE,
                TRY_TO_DATE(ls.delistingDate, 'YYYY-MM-DD') as DELISTING_DATE,
                CURRENT_TIMESTAMP() as CREATED_AT,
                CURRENT_TIMESTAMP() as UPDATED_AT
            FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS ls
            WHERE ls.symbol IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w 
                  WHERE w.TABLE_NAME = 'LISTING_STATUS' 
                    AND w.SYMBOL_ID = ABS(HASH(ls.symbol)) % 1000000000
              )
            """
            
            cursor.execute(initialize_query)
            rows_inserted = cursor.rowcount
            conn.commit()
            
            logger.info(f"Initialized watermarks for {rows_inserted} symbols from listing status")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Failed to initialize watermarks from listing status: {e}")
            return 0

    def update_processing_status(self, symbol: str, data_type: str, success: bool, 
                               error_message: Optional[str] = None, processing_mode: str = 'incremental',
                               fiscal_date: Optional[str] = None, first_fiscal_date: Optional[str] = None):
        """
        Update processing status for a symbol and data type.
        
        Args:
            symbol: Symbol that was processed
            data_type: Type of data processed
            success: Whether processing was successful
            error_message: Optional error message if processing failed
            processing_mode: Processing mode used
            fiscal_date: Latest fiscal/data date processed (for LAST_FISCAL_DATE)
            first_fiscal_date: Earliest fiscal/data date available (for FIRST_FISCAL_DATE, time series only)
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Update or insert into ETL_WATERMARKS for enhanced tracking
            # Calculate symbol_id (consistent with other tables)
            symbol_id = abs(hash(symbol)) % 1000000000
            
            # Map data_type to table_name
            table_name = data_type.upper().replace('_', '_')  # Keep as-is for now
            
            # Get listing dates for new watermark entries
            listing_query = """
            SELECT TRY_TO_DATE(ipoDate, 'YYYY-MM-DD') as ipo_date,
                   TRY_TO_DATE(delistingDate, 'YYYY-MM-DD') as delisting_date
            FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS 
            WHERE symbol = %s 
            LIMIT 1
            """
            
            cursor.execute(listing_query, (symbol,))
            listing_result = cursor.fetchone()
            ipo_date = listing_result[0] if listing_result else None
            delisting_date = listing_result[1] if listing_result else None
            
            watermark_query = """
            MERGE INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS AS target
            USING (SELECT %s as table_name, %s as symbol_id, %s as symbol) AS source
            ON target.TABLE_NAME = source.table_name AND target.SYMBOL_ID = source.symbol_id
            WHEN MATCHED THEN UPDATE SET
                target.FIRST_FISCAL_DATE = CASE 
                    WHEN %s IS NOT NULL AND (target.FIRST_FISCAL_DATE IS NULL OR %s < target.FIRST_FISCAL_DATE) 
                    THEN %s 
                    ELSE target.FIRST_FISCAL_DATE 
                END,
                target.LAST_FISCAL_DATE = CASE WHEN %s IS NOT NULL THEN %s ELSE target.LAST_FISCAL_DATE END,
                target.LAST_SUCCESSFUL_RUN = CASE WHEN %s THEN %s ELSE target.LAST_SUCCESSFUL_RUN END,
                target.CONSECUTIVE_FAILURES = CASE WHEN %s THEN 0 ELSE target.CONSECUTIVE_FAILURES + 1 END,
                target.UPDATED_AT = %s
            WHEN NOT MATCHED THEN INSERT (
                TABLE_NAME, SYMBOL_ID, SYMBOL, IPO_DATE, DELISTING_DATE, 
                FIRST_FISCAL_DATE, LAST_FISCAL_DATE, LAST_SUCCESSFUL_RUN, CONSECUTIVE_FAILURES, CREATED_AT, UPDATED_AT
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            """
            
            now = datetime.now()
            # Calculate INSERT values
            insert_last_successful_run = now if success else None
            insert_consecutive_failures = 0 if success else 1
            
            cursor.execute(watermark_query, (
                # USING clause
                table_name, symbol_id, symbol,
                # WHEN MATCHED SET values for FIRST_FISCAL_DATE
                first_fiscal_date, first_fiscal_date, first_fiscal_date,
                # WHEN MATCHED SET values for LAST_FISCAL_DATE
                fiscal_date, fiscal_date,
                # WHEN MATCHED SET values for processing status
                success, now, success, now,
                # WHEN NOT MATCHED INSERT values  
                table_name, symbol_id, symbol, ipo_date, delisting_date,
                first_fiscal_date, fiscal_date, insert_last_successful_run, insert_consecutive_failures, now, now
            ))
            
            conn.commit()
            
        except Exception as e:
            logger.warning(f"Could not update processing status for {symbol}: {e}")

    def check_dependencies(self, data_type: str, symbols: List[str]) -> Tuple[List[str], List[str]]:
        """
        Check if dependency data exists for symbols before processing.
        
        Args:
            data_type: Type of data to check dependencies for
            symbols: List of symbols to check
            
        Returns:
            Tuple of (symbols_ready, symbols_missing_deps)
        """
        if data_type not in DATA_TYPES:
            raise ValueError(f"Unknown data type: {data_type}")
            
        config = DATA_TYPES[data_type]
        
        if not config.dependency_types:
            # No dependencies, all symbols ready
            return symbols, []
        
        symbols_ready = []
        symbols_missing_deps = []
        
        # Check each dependency type
        for dep_type in config.dependency_types:
            dep_last_updates = self.get_last_update_timestamps(dep_type, symbols)
            
            for symbol in symbols:
                if symbol in dep_last_updates:
                    if symbol not in symbols_ready:
                        symbols_ready.append(symbol)
                else:
                    if symbol not in symbols_missing_deps:
                        symbols_missing_deps.append(symbol)
        
        logger.info(f"Dependency check for {data_type}:")
        logger.info(f"  - Symbols ready: {len(symbols_ready)}")
        logger.info(f"  - Missing dependencies: {len(symbols_missing_deps)}")
        
        return symbols_ready, symbols_missing_deps

    def get_processing_plan(self, data_types: List[str], 
                          exchange_filter: str = 'NASDAQ',
                          max_symbols_per_type: Optional[int] = None,
                          force_refresh: bool = False) -> Dict[str, List[str]]:
        """
        Generate comprehensive processing plan for multiple data types.
        
        Args:
            data_types: List of data types to include in plan
            exchange_filter: Exchange filter for universe
            max_symbols_per_type: Max symbols per data type
            force_refresh: Force refresh all symbols
            
        Returns:
            Dict mapping data_type -> list of symbols to process
        """
        logger.info(f"Generating processing plan for data types: {data_types}")
        
        # Get universe of symbols
        universe_symbols = self.get_universe_symbols(exchange_filter=exchange_filter)
        
        processing_plan = {}
        
        # Sort data types by priority
        sorted_types = sorted(data_types, key=lambda dt: DATA_TYPES[dt].priority)
        
        for data_type in sorted_types:
            logger.info(f"\n--- Planning {data_type} ---")
            
            # Identify symbols needing processing
            symbols_needed = self.identify_symbols_to_process(
                data_type=data_type,
                universe_symbols=universe_symbols,
                force_refresh=force_refresh,
                max_symbols=max_symbols_per_type
            )
            
            # Check dependencies
            symbols_ready, symbols_missing = self.check_dependencies(data_type, symbols_needed)
            
            processing_plan[data_type] = symbols_ready
            
            if symbols_missing:
                logger.warning(f"Skipping {len(symbols_missing)} symbols for {data_type} due to missing dependencies")
        
        # Summary
        total_symbols = sum(len(symbols) for symbols in processing_plan.values())
        logger.info(f"\n📋 PROCESSING PLAN SUMMARY:")
        for data_type, symbols in processing_plan.items():
            logger.info(f"  - {data_type}: {len(symbols)} symbols")
        logger.info(f"  - TOTAL: {total_symbols} symbol-datatype combinations")
        
        return processing_plan

    def save_processing_plan(self, plan: Dict[str, List[str]], filename: str = '/tmp/processing_plan.json'):
        """Save processing plan to file for use by extraction scripts."""
        with open(filename, 'w') as f:
            json.dump(plan, f, indent=2)
        logger.info(f"Processing plan saved to {filename}")

    def load_processing_plan(self, filename: str = '/tmp/processing_plan.json') -> Dict[str, List[str]]:
        """Load processing plan from file."""
        with open(filename, 'r') as f:
            plan = json.load(f)
        logger.info(f"Processing plan loaded from {filename}")
        return plan


def get_snowflake_config_from_env() -> Dict[str, str]:
    """Get Snowflake configuration from environment variables."""
    required_vars = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_WAREHOUSE'
    ]
    
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    return {
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
    }


def main():
    """CLI interface for incremental ETL management."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Incremental ETL Management')
    parser.add_argument('--data-types', nargs='+', 
                       choices=list(DATA_TYPES.keys()),
                       default=['time_series_daily_adjusted'],
                       help='Data types to analyze')
    parser.add_argument('--exchange', default='NASDAQ',
                       help='Exchange filter')
    parser.add_argument('--max-symbols', type=int,
                       help='Max symbols per data type')
    parser.add_argument('--force-refresh', action='store_true',
                       help='Force refresh all symbols')
    parser.add_argument('--output-plan', default='/tmp/processing_plan.json',
                       help='Output file for processing plan')
    
    args = parser.parse_args()
    
    try:
        # Initialize ETL manager
        snowflake_config = get_snowflake_config_from_env()
        etl_manager = IncrementalETLManager(snowflake_config)
        
        # Generate processing plan
        plan = etl_manager.get_processing_plan(
            data_types=args.data_types,
            exchange_filter=args.exchange,
            max_symbols_per_type=args.max_symbols,
            force_refresh=args.force_refresh
        )
        
        # Save plan
        etl_manager.save_processing_plan(plan, args.output_plan)
        
        etl_manager.close_connection()
        
        logger.info("✅ Processing plan generated successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error generating processing plan: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
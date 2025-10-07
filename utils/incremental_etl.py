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
            SELECT symbol, MAX(created_at) as last_update
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
        
        cursor.execute(query)
        results = cursor.fetchall()
        
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
        
        if force_refresh:
            # Return all symbols (up to max_symbols)
            symbols_to_process = universe_symbols[:max_symbols] if max_symbols else universe_symbols
            logger.info(f"Force refresh enabled: processing {len(symbols_to_process)} symbols")
            return symbols_to_process
        
        # Get last update timestamps for universe symbols
        last_updates = self.get_last_update_timestamps(data_type, universe_symbols)
        
        # Calculate cutoff date based on refresh frequency
        cutoff_date = datetime.now().date() - timedelta(days=config.refresh_frequency_days)
        
        symbols_to_process = []
        symbols_never_processed = []
        symbols_stale = []
        
        for symbol in universe_symbols:
            last_update = last_updates.get(symbol)
            
            if not last_update:
                # Never processed
                symbols_never_processed.append(symbol)
            elif last_update < cutoff_date:
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

    def update_processing_status(self, symbol: str, data_type: str, success: bool, 
                               error_message: Optional[str] = None, processing_mode: str = 'incremental'):
        """
        Update processing status for a symbol and data type.
        
        Args:
            symbol: Symbol that was processed
            data_type: Type of data processed
            success: Whether processing was successful
            error_message: Optional error message if processing failed
            processing_mode: Processing mode used
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Create or update processing status in INCREMENTAL_ETL_STATUS table
            # This is a simple implementation - you may want to create this table if it doesn't exist
            query = """
            INSERT INTO FIN_TRADE_EXTRACT.RAW.INCREMENTAL_ETL_STATUS 
            (symbol, data_type, last_processed_at, success, error_message, processing_mode, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                symbol,
                data_type,
                datetime.now(),
                success,
                error_message,
                processing_mode,
                datetime.now()
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
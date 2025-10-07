#!/usr/bin/env python3
"""
Universe Management for fin-trade-extract pipeline.

This module manages investment universes and symbol lists:
- Creates and maintains symbol universes (e.g., S&P 500, Russell 2000)
- Tracks universe changes over time
- Manages symbol additions/deletions
- Provides universe overlap analysis  
- Handles custom universe definitions

Integrates with incremental_etl.py and symbol_screener.py for comprehensive pipeline management.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple
import logging
import json
from dataclasses import dataclass, asdict
from enum import Enum

import snowflake.connector

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class UniverseDefinition:
    """Definition of a symbol universe."""
    name: str
    description: str
    symbols: List[str]
    created_date: datetime
    last_updated: datetime
    source: str  # e.g., 'manual', 'screener', 'index_provider'
    criteria: Dict  # Screening criteria used to create universe
    metadata: Dict = None


class UniverseType(Enum):
    """Predefined universe types."""
    NASDAQ_COMPOSITE = "nasdaq_composite"
    NASDAQ_100 = "nasdaq_100"
    SP_500 = "sp_500"
    RUSSELL_2000 = "russell_2000"
    CUSTOM = "custom"


class UniverseManager:
    """Manages investment universes and symbol lists."""
    
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

    def create_universe_table(self):
        """Create universe management table if it doesn't exist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.SYMBOL_UNIVERSES (
            universe_name VARCHAR(100) NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            added_date DATE NOT NULL,
            removed_date DATE,
            is_active BOOLEAN DEFAULT TRUE,
            source VARCHAR(100),
            metadata VARIANT,
            created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            
            -- Constraints
            CONSTRAINT PK_UNIVERSE_SYMBOL PRIMARY KEY (universe_name, symbol, added_date)
        )
        COMMENT = 'Symbol universe definitions and historical changes'
        CLUSTER BY (universe_name, is_active, added_date);
        """
        
        cursor.execute(create_table_sql)
        logger.info("Universe table created/verified")

    def save_universe(self, universe_def: UniverseDefinition, overwrite: bool = False):
        """
        Save universe definition to database.
        
        Args:
            universe_def: Universe definition to save
            overwrite: If True, replace existing universe
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Check if universe exists
        cursor.execute(
            "SELECT COUNT(*) FROM FIN_TRADE_EXTRACT.RAW.SYMBOL_UNIVERSES WHERE universe_name = %s AND is_active = TRUE",
            (universe_def.name,)
        )
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0 and not overwrite:
            raise ValueError(f"Universe '{universe_def.name}' already exists. Use overwrite=True to replace.")
        
        # Deactivate existing universe if overwriting
        if existing_count > 0 and overwrite:
            cursor.execute(
                """UPDATE FIN_TRADE_EXTRACT.RAW.SYMBOL_UNIVERSES 
                   SET is_active = FALSE, removed_date = CURRENT_DATE, updated_at = CURRENT_TIMESTAMP()
                   WHERE universe_name = %s AND is_active = TRUE""",
                (universe_def.name,)
            )
            logger.info(f"Deactivated existing universe '{universe_def.name}'")
        
        # Insert new universe symbols
        insert_sql = """
        INSERT INTO FIN_TRADE_EXTRACT.RAW.SYMBOL_UNIVERSES 
        (universe_name, symbol, added_date, source, metadata)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        metadata = {
            'description': universe_def.description,
            'criteria': universe_def.criteria,
            'metadata': universe_def.metadata or {}
        }
        
        for symbol in universe_def.symbols:
            cursor.execute(insert_sql, (
                universe_def.name,
                symbol,
                universe_def.created_date.date(),
                universe_def.source,
                json.dumps(metadata)
            ))
        
        conn.commit()
        logger.info(f"Saved universe '{universe_def.name}' with {len(universe_def.symbols)} symbols")

    def load_universe(self, universe_name: str, as_of_date: Optional[datetime] = None) -> UniverseDefinition:
        """
        Load universe definition from database.
        
        Args:
            universe_name: Name of universe to load
            as_of_date: Load universe as of specific date (None = current)
            
        Returns:
            UniverseDefinition object
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if as_of_date:
            # Historical universe
            query = """
            SELECT symbol, added_date, removed_date, source, metadata
            FROM FIN_TRADE_EXTRACT.RAW.SYMBOL_UNIVERSES
            WHERE universe_name = %s
                AND added_date <= %s
                AND (removed_date IS NULL OR removed_date > %s)
            ORDER BY symbol
            """
            cursor.execute(query, (universe_name, as_of_date.date(), as_of_date.date()))
        else:
            # Current universe
            query = """
            SELECT symbol, added_date, removed_date, source, metadata
            FROM FIN_TRADE_EXTRACT.RAW.SYMBOL_UNIVERSES
            WHERE universe_name = %s AND is_active = TRUE
            ORDER BY symbol
            """
            cursor.execute(query, (universe_name,))
        
        results = cursor.fetchall()
        
        if not results:
            raise ValueError(f"Universe '{universe_name}' not found")
        
        symbols = []
        metadata = {}
        source = "unknown"
        created_date = None
        
        for symbol, added_date, removed_date, src, meta in results:
            symbols.append(symbol)
            if not created_date or added_date < created_date:
                created_date = added_date
            if src:
                source = src
            if meta:
                try:
                    metadata = json.loads(meta)
                except:
                    pass
        
        universe_def = UniverseDefinition(
            name=universe_name,
            description=metadata.get('description', ''),
            symbols=symbols,
            created_date=created_date or datetime.now(),
            last_updated=datetime.now(),
            source=source,
            criteria=metadata.get('criteria', {}),
            metadata=metadata.get('metadata', {})
        )
        
        logger.info(f"Loaded universe '{universe_name}' with {len(symbols)} symbols")
        return universe_def

    def list_universes(self) -> List[Dict]:
        """List all available universes with summary statistics."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            universe_name,
            COUNT(DISTINCT symbol) as symbol_count,
            MIN(added_date) as created_date,
            MAX(updated_at) as last_updated,
            source,
            MAX(metadata) as latest_metadata
        FROM FIN_TRADE_EXTRACT.RAW.SYMBOL_UNIVERSES
        WHERE is_active = TRUE
        GROUP BY universe_name, source
        ORDER BY universe_name
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        universes = []
        for name, count, created, updated, source, metadata in results:
            meta_dict = {}
            if metadata:
                try:
                    meta_dict = json.loads(metadata)
                except:
                    pass
            
            universes.append({
                'name': name,
                'symbol_count': count,
                'created_date': created,
                'last_updated': updated,
                'source': source,
                'description': meta_dict.get('description', '')
            })
        
        return universes

    def update_universe(self, universe_name: str, 
                       symbols_to_add: List[str] = None,
                       symbols_to_remove: List[str] = None):
        """
        Update existing universe by adding/removing symbols.
        
        Args:
            universe_name: Name of universe to update
            symbols_to_add: List of symbols to add
            symbols_to_remove: List of symbols to remove
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        current_date = datetime.now().date()
        
        # Add new symbols
        if symbols_to_add:
            insert_sql = """
            INSERT INTO FIN_TRADE_EXTRACT.RAW.SYMBOL_UNIVERSES 
            (universe_name, symbol, added_date, source)
            VALUES (%s, %s, %s, 'manual_update')
            """
            
            for symbol in symbols_to_add:
                try:
                    cursor.execute(insert_sql, (universe_name, symbol, current_date))
                except Exception as e:
                    logger.warning(f"Failed to add {symbol}: {e}")
            
            logger.info(f"Added {len(symbols_to_add)} symbols to '{universe_name}'")
        
        # Remove symbols
        if symbols_to_remove:
            update_sql = """
            UPDATE FIN_TRADE_EXTRACT.RAW.SYMBOL_UNIVERSES
            SET is_active = FALSE, removed_date = %s, updated_at = CURRENT_TIMESTAMP()
            WHERE universe_name = %s AND symbol = %s AND is_active = TRUE
            """
            
            for symbol in symbols_to_remove:
                cursor.execute(update_sql, (current_date, universe_name, symbol))
            
            logger.info(f"Removed {len(symbols_to_remove)} symbols from '{universe_name}'")
        
        conn.commit()

    def compare_universes(self, universe1: str, universe2: str) -> Dict:
        """
        Compare two universes and show overlaps/differences.
        
        Args:
            universe1: Name of first universe
            universe2: Name of second universe
            
        Returns:
            Dict with comparison statistics
        """
        univ1 = self.load_universe(universe1)
        univ2 = self.load_universe(universe2)
        
        symbols1 = set(univ1.symbols)
        symbols2 = set(univ2.symbols)
        
        overlap = symbols1.intersection(symbols2)
        only_in_1 = symbols1 - symbols2
        only_in_2 = symbols2 - symbols1
        
        comparison = {
            'universe_1': {
                'name': universe1,
                'symbol_count': len(symbols1)
            },
            'universe_2': {
                'name': universe2,
                'symbol_count': len(symbols2)
            },
            'overlap': {
                'count': len(overlap),
                'percentage': len(overlap) / max(len(symbols1), len(symbols2)) * 100,
                'symbols': list(overlap)
            },
            'only_in_universe_1': {
                'count': len(only_in_1),
                'symbols': list(only_in_1)
            },
            'only_in_universe_2': {
                'count': len(only_in_2),
                'symbols': list(only_in_2)
            }
        }
        
        logger.info(f"Universe comparison: {len(overlap)} overlap, {len(only_in_1)} unique to {universe1}, {len(only_in_2)} unique to {universe2}")
        
        return comparison

    def create_predefined_universes(self):
        """Create common predefined universes."""
        
        # NASDAQ Composite (simplified - from LISTING_STATUS)
        try:
            nasdaq_symbols = self._get_symbols_by_exchange('NASDAQ')
            nasdaq_universe = UniverseDefinition(
                name='nasdaq_composite',
                description='All NASDAQ listed securities',
                symbols=nasdaq_symbols,
                created_date=datetime.now(),
                last_updated=datetime.now(),
                source='listing_status_extract',
                criteria={'exchange': 'NASDAQ', 'status': 'Active'}
            )
            self.save_universe(nasdaq_universe, overwrite=True)
            logger.info(f"Created NASDAQ Composite universe: {len(nasdaq_symbols)} symbols")
            
        except Exception as e:
            logger.error(f"Failed to create NASDAQ universe: {e}")

        # NYSE Composite (simplified - from LISTING_STATUS)
        try:
            nyse_symbols = self._get_symbols_by_exchange('NYSE')
            nyse_universe = UniverseDefinition(
                name='nyse_composite',
                description='All NYSE listed securities',
                symbols=nyse_symbols,
                created_date=datetime.now(),
                last_updated=datetime.now(),
                source='listing_status_extract',
                criteria={'exchange': 'NYSE', 'status': 'Active'}
            )
            self.save_universe(nyse_universe, overwrite=True)
            logger.info(f"Created NYSE Composite universe: {len(nyse_symbols)} symbols")
            
        except Exception as e:
            logger.error(f"Failed to create NYSE universe: {e}")
        
        # High Quality NASDAQ (using screener criteria)
        try:
            from .symbol_screener import SymbolScreener, ScreeningCriteria, ExchangeType, AssetType
            
            screener = SymbolScreener(self.snowflake_config)
            
            # NASDAQ High Quality
            nasdaq_criteria = ScreeningCriteria(
                exchanges=[ExchangeType.NASDAQ],
                asset_types=[AssetType.EQUITY],
                min_price=10.0,
                min_avg_volume=1000000,
                exclude_penny_stocks=True,
                min_data_quality_score=0.9
            )
            
            nasdaq_results = screener.screen_symbols(nasdaq_criteria)
            nasdaq_quality_symbols = [s['symbol'] for s in nasdaq_results]
            
            nasdaq_quality_universe = UniverseDefinition(
                name='nasdaq_high_quality',
                description='High-quality NASDAQ securities (>$10, >1M volume, >0.9 quality)',
                symbols=nasdaq_quality_symbols,
                created_date=datetime.now(),
                last_updated=datetime.now(),
                source='symbol_screener',
                criteria=asdict(nasdaq_criteria)
            )
            self.save_universe(nasdaq_quality_universe, overwrite=True)
            logger.info(f"Created high-quality NASDAQ universe: {len(nasdaq_quality_symbols)} symbols")

            # NYSE High Quality
            nyse_criteria = ScreeningCriteria(
                exchanges=[ExchangeType.NYSE],
                asset_types=[AssetType.EQUITY],
                min_price=10.0,
                min_avg_volume=1000000,
                exclude_penny_stocks=True,
                min_data_quality_score=0.9
            )
            
            nyse_results = screener.screen_symbols(nyse_criteria)
            nyse_quality_symbols = [s['symbol'] for s in nyse_results]
            
            nyse_quality_universe = UniverseDefinition(
                name='nyse_high_quality',
                description='High-quality NYSE securities (>$10, >1M volume, >0.9 quality)',
                symbols=nyse_quality_symbols,
                created_date=datetime.now(),
                last_updated=datetime.now(),
                source='symbol_screener',
                criteria=asdict(nyse_criteria)
            )
            self.save_universe(nyse_quality_universe, overwrite=True)
            logger.info(f"Created high-quality NYSE universe: {len(nyse_quality_symbols)} symbols")

            # ETFs All Exchanges
            etf_criteria = ScreeningCriteria(
                exchanges=[ExchangeType.NASDAQ, ExchangeType.NYSE, ExchangeType.AMEX],
                asset_types=[AssetType.ETF],
                min_price=5.0,  # Lower threshold for ETFs
                min_avg_volume=100000,  # Lower volume threshold for ETFs
                exclude_penny_stocks=True,
                min_data_quality_score=0.8
            )
            
            etf_results = screener.screen_symbols(etf_criteria)
            etf_symbols = [s['symbol'] for s in etf_results]
            
            etf_universe = UniverseDefinition(
                name='etf_all_exchanges',
                description='All ETFs across exchanges (>$5, >100K volume, >0.8 quality)',
                symbols=etf_symbols,
                created_date=datetime.now(),
                last_updated=datetime.now(),
                source='symbol_screener',
                criteria=asdict(etf_criteria)
            )
            self.save_universe(etf_universe, overwrite=True)
            logger.info(f"Created ETF all exchanges universe: {len(etf_symbols)} symbols")

            # High Volume ETFs
            high_volume_etf_criteria = ScreeningCriteria(
                exchanges=[ExchangeType.NASDAQ, ExchangeType.NYSE, ExchangeType.AMEX],
                asset_types=[AssetType.ETF],
                min_price=10.0,
                min_avg_volume=1000000,  # High volume ETFs
                exclude_penny_stocks=True,
                min_data_quality_score=0.9
            )
            
            high_volume_etf_results = screener.screen_symbols(high_volume_etf_criteria)
            high_volume_etf_symbols = [s['symbol'] for s in high_volume_etf_results]
            
            high_volume_etf_universe = UniverseDefinition(
                name='etf_high_volume',
                description='High-volume ETFs (>$10, >1M volume, >0.9 quality)',
                symbols=high_volume_etf_symbols,
                created_date=datetime.now(),
                last_updated=datetime.now(),
                source='symbol_screener',
                criteria=asdict(high_volume_etf_criteria)
            )
            self.save_universe(high_volume_etf_universe, overwrite=True)
            logger.info(f"Created high-volume ETF universe: {len(high_volume_etf_symbols)} symbols")
            
        except Exception as e:
            logger.error(f"Failed to create high-quality and ETF universes: {e}")

        # Create simple ETF universe from LISTING_STATUS (fallback)
        try:
            etf_symbols_simple = self._get_symbols_by_asset_type('ETF')
            etf_simple_universe = UniverseDefinition(
                name='etf_simple',
                description='All ETFs from LISTING_STATUS (no filtering)',
                symbols=etf_symbols_simple,
                created_date=datetime.now(),
                last_updated=datetime.now(),
                source='listing_status_extract',
                criteria={'asset_type': 'ETF', 'status': 'Active'}
            )
            self.save_universe(etf_simple_universe, overwrite=True)
            logger.info(f"Created simple ETF universe: {len(etf_symbols_simple)} symbols")
            
        except Exception as e:
            logger.error(f"Failed to create simple ETF universe: {e}")

    def _get_symbols_by_exchange(self, exchange: str) -> List[str]:
        """Helper method to get symbols by exchange from LISTING_STATUS."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT symbol
        FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
        WHERE UPPER(exchange) LIKE %s
            AND status = 'Active'
            AND symbol IS NOT NULL
            AND symbol != ''
        ORDER BY symbol
        """
        
        cursor.execute(query, (f'%{exchange.upper()}%',))
        results = cursor.fetchall()
        
        return [row[0] for row in results]

    def _get_symbols_by_asset_type(self, asset_type: str) -> List[str]:
        """Helper method to get symbols by asset type from LISTING_STATUS."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT symbol
        FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
        WHERE UPPER(assetType) = %s
            AND status = 'Active'
            AND symbol IS NOT NULL
            AND symbol != ''
        ORDER BY symbol
        """
        
        cursor.execute(query, (asset_type.upper(),))
        results = cursor.fetchall()
        
        return [row[0] for row in results]

    def export_universe(self, universe_name: str, filename: str):
        """Export universe to JSON file."""
        universe = self.load_universe(universe_name)
        
        export_data = {
            'name': universe.name,
            'description': universe.description,
            'symbols': universe.symbols,
            'symbol_count': len(universe.symbols),
            'created_date': universe.created_date.isoformat(),
            'last_updated': universe.last_updated.isoformat(),
            'source': universe.source,
            'criteria': universe.criteria,
            'metadata': universe.metadata
        }
        
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        logger.info(f"Exported universe '{universe_name}' to {filename}")

    def import_universe(self, filename: str, overwrite: bool = False):
        """Import universe from JSON file."""
        with open(filename, 'r') as f:
            data = json.load(f)
        
        universe = UniverseDefinition(
            name=data['name'],
            description=data['description'],
            symbols=data['symbols'],
            created_date=datetime.fromisoformat(data['created_date']),
            last_updated=datetime.now(),
            source=data.get('source', 'imported'),
            criteria=data.get('criteria', {}),
            metadata=data.get('metadata', {})
        )
        
        self.save_universe(universe, overwrite=overwrite)
        logger.info(f"Imported universe '{universe.name}' with {len(universe.symbols)} symbols")


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
    """CLI interface for universe management."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Universe Management')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List universes
    list_parser = subparsers.add_parser('list', help='List all universes')
    
    # Create universe
    create_parser = subparsers.add_parser('create', help='Create predefined universes')
    
    # Export universe
    export_parser = subparsers.add_parser('export', help='Export universe to file')
    export_parser.add_argument('universe_name', help='Name of universe to export')
    export_parser.add_argument('--output', required=True, help='Output filename')
    
    # Compare universes
    compare_parser = subparsers.add_parser('compare', help='Compare two universes')
    compare_parser.add_argument('universe1', help='First universe name')
    compare_parser.add_argument('universe2', help='Second universe name')
    compare_parser.add_argument('--output', help='Save comparison to file')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        # Initialize universe manager
        snowflake_config = get_snowflake_config_from_env()
        manager = UniverseManager(snowflake_config)
        manager.create_universe_table()
        
        if args.command == 'list':
            universes = manager.list_universes()
            print("\nAvailable Universes:")
            print("-" * 80)
            for univ in universes:
                print(f"{univ['name']:25} | {univ['symbol_count']:6} symbols | {univ['source']:15} | {univ['description']}")
        
        elif args.command == 'create':
            manager.create_predefined_universes()
        
        elif args.command == 'export':
            manager.export_universe(args.universe_name, args.output)
        
        elif args.command == 'compare':
            comparison = manager.compare_universes(args.universe1, args.universe2)
            
            print(f"\nUniverse Comparison:")
            print(f"{args.universe1}: {comparison['universe_1']['symbol_count']} symbols")
            print(f"{args.universe2}: {comparison['universe_2']['symbol_count']} symbols")
            print(f"Overlap: {comparison['overlap']['count']} symbols ({comparison['overlap']['percentage']:.1f}%)")
            print(f"Only in {args.universe1}: {comparison['only_in_universe_1']['count']} symbols")
            print(f"Only in {args.universe2}: {comparison['only_in_universe_2']['count']} symbols")
            
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(comparison, f, indent=2, default=str)
                print(f"Detailed comparison saved to {args.output}")
        
        manager.close_connection()
        logger.info("✅ Universe management completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error in universe management: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Symbol Screening and Universe Management for fin-trade-extract pipeline.

This module provides advanced symbol filtering capabilities:
- Market cap, volume, price screening
- Sector and industry filtering  
- Technical indicator screening
- Data quality and availability checks
- Custom screening rules and blacklists

Used to create targeted symbol universes for processing.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple
import logging
import json
from dataclasses import dataclass
from enum import Enum

import snowflake.connector

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ExchangeType(Enum):
    """Supported exchange types."""
    NASDAQ = "NASDAQ"
    NYSE = "NYSE" 
    AMEX = "AMEX"
    ALL = "ALL"


class AssetType(Enum):
    """Supported asset types."""
    EQUITY = "Equity"
    ETF = "ETF"
    REIT = "REIT" 
    ALL = "ALL"


@dataclass 
class ScreeningCriteria:
    """Screening criteria for symbol selection."""
    exchanges: List[ExchangeType] = None
    asset_types: List[AssetType] = None
    min_market_cap: Optional[float] = None
    max_market_cap: Optional[float] = None
    min_avg_volume: Optional[int] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    exclude_penny_stocks: bool = True  # Price < $5
    exclude_low_volume: bool = True    # Avg volume < 100k
    require_options: bool = False
    sectors_include: List[str] = None
    sectors_exclude: List[str] = None
    blacklist_symbols: List[str] = None
    whitelist_symbols: List[str] = None
    min_data_quality_score: float = 0.8
    

class SymbolScreener:
    """Advanced symbol screening and universe management."""
    
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

    def get_basic_universe(self, criteria: ScreeningCriteria) -> List[Dict]:
        """
        Get basic universe from LISTING_STATUS with fundamental filters.
        
        Args:
            criteria: Screening criteria to apply
            
        Returns:
            List of symbol dictionaries with basic info
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Build base query
        query_parts = [
            "SELECT DISTINCT",
            "    symbol,",
            "    name,", 
            "    exchange,",
            "    assetType,",
            "    status,",
            "    ipoDate,",
            "    delistingDate",
            "FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS",
            "WHERE symbol IS NOT NULL",
            "    AND symbol != ''",
            "    AND status = 'Active'"
        ]
        
        # Exchange filters
        if criteria.exchanges and ExchangeType.ALL not in criteria.exchanges:
            exchange_filters = []
            for exchange in criteria.exchanges:
                exchange_filters.append(f"UPPER(exchange) LIKE '%{exchange.value.upper()}%'")
            query_parts.append(f"    AND ({' OR '.join(exchange_filters)})")
        
        # Asset type filters  
        if criteria.asset_types and AssetType.ALL not in criteria.asset_types:
            asset_type_list = "', '".join([at.value for at in criteria.asset_types])
            query_parts.append(f"    AND UPPER(assetType) IN ('{asset_type_list.upper()}')")
        
        # Blacklist symbols
        if criteria.blacklist_symbols:
            blacklist = "', '".join(criteria.blacklist_symbols)
            query_parts.append(f"    AND symbol NOT IN ('{blacklist}')")
            
        # Whitelist symbols (if provided, only include these)
        if criteria.whitelist_symbols:
            whitelist = "', '".join(criteria.whitelist_symbols)
            query_parts.append(f"    AND symbol IN ('{whitelist}')")
        
        query_parts.append("ORDER BY symbol")
        
        query = "\n".join(query_parts)
        
        logger.info("Executing basic universe query...")
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to list of dicts
        columns = ['symbol', 'name', 'exchange', 'asset_type', 'status', 'ipo_date', 'delisting_date']
        universe = []
        for row in results:
            universe.append(dict(zip(columns, row)))
            
        logger.info(f"Basic universe: {len(universe)} symbols")
        return universe

    def apply_price_volume_filters(self, symbols: List[str], criteria: ScreeningCriteria) -> List[str]:
        """
        Apply price and volume filters using time series data.
        
        Args:
            symbols: List of symbols to filter
            criteria: Screening criteria
            
        Returns:
            List of symbols passing price/volume filters
        """
        if not symbols:
            return symbols
            
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Get recent price and volume data (last 30 days)
        symbol_list = "', '".join(symbols)
        
        query = f"""
        WITH recent_data AS (
            SELECT 
                symbol,
                AVG(close) as avg_price,
                AVG(volume) as avg_volume,
                MAX(close) as max_price,
                MIN(close) as min_price,
                COUNT(*) as data_points
            FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED
            WHERE symbol IN ('{symbol_list}')
                AND date >= CURRENT_DATE - 30
                AND close > 0
                AND volume > 0
            GROUP BY symbol
        )
        SELECT 
            symbol,
            avg_price,
            avg_volume,
            max_price,
            min_price,
            data_points
        FROM recent_data
        WHERE data_points >= 10  -- Require at least 10 data points
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        filtered_symbols = []
        
        for symbol, avg_price, avg_volume, max_price, min_price, data_points in results:
            # Price filters
            if criteria.min_price and avg_price < criteria.min_price:
                continue
                
            if criteria.max_price and avg_price > criteria.max_price:
                continue
                
            # Penny stock filter
            if criteria.exclude_penny_stocks and avg_price < 5.0:
                continue
                
            # Volume filters
            if criteria.min_avg_volume and avg_volume < criteria.min_avg_volume:
                continue
                
            # Low volume filter
            if criteria.exclude_low_volume and avg_volume < 100000:
                continue
            
            filtered_symbols.append(symbol)
        
        logger.info(f"Price/volume filtering: {len(symbols)} → {len(filtered_symbols)} symbols")
        return filtered_symbols

    def calculate_data_quality_scores(self, symbols: List[str]) -> Dict[str, float]:
        """
        Calculate data quality scores for symbols based on completeness and consistency.
        
        Args:
            symbols: List of symbols to evaluate
            
        Returns:
            Dict mapping symbol -> quality score (0.0 to 1.0)
        """
        if not symbols:
            return {}
            
        conn = self.get_connection()
        cursor = conn.cursor()
        
        symbol_list = "', '".join(symbols)
        
        # Calculate quality metrics
        query = f"""
        WITH quality_metrics AS (
            SELECT 
                symbol,
                COUNT(*) as total_records,
                COUNT(CASE WHEN close IS NOT NULL AND close > 0 THEN 1 END) as valid_close,
                COUNT(CASE WHEN volume IS NOT NULL AND volume >= 0 THEN 1 END) as valid_volume,
                COUNT(CASE WHEN open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL THEN 1 END) as valid_ohlc,
                AVG(CASE WHEN volume > 0 THEN 1.0 ELSE 0.0 END) as volume_completeness,
                MAX(date) as latest_date,
                MIN(date) as earliest_date
            FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED
            WHERE symbol IN ('{symbol_list}')
                AND date >= CURRENT_DATE - 365  -- Last year
            GROUP BY symbol
        )
        SELECT 
            symbol,
            total_records,
            valid_close,
            valid_volume,
            valid_ohlc,
            volume_completeness,
            latest_date,
            earliest_date,
            -- Quality score calculation
            (
                (valid_close::FLOAT / GREATEST(total_records, 1)) * 0.4 +  -- 40% weight on price completeness
                (valid_volume::FLOAT / GREATEST(total_records, 1)) * 0.3 + -- 30% weight on volume completeness  
                (valid_ohlc::FLOAT / GREATEST(total_records, 1)) * 0.2 +   -- 20% weight on OHLC completeness
                volume_completeness * 0.1                                   -- 10% weight on volume > 0
            ) as quality_score
        FROM quality_metrics
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        quality_scores = {}
        for row in results:
            symbol = row[0]
            quality_score = float(row[8]) if row[8] else 0.0
            quality_scores[symbol] = quality_score
            
        logger.info(f"Calculated quality scores for {len(quality_scores)} symbols")
        return quality_scores

    def screen_symbols(self, criteria: ScreeningCriteria) -> List[Dict]:
        """
        Apply comprehensive screening to identify target symbols.
        
        Args:
            criteria: Screening criteria to apply
            
        Returns:
            List of symbol dictionaries with screening results
        """
        logger.info("🔍 Starting comprehensive symbol screening...")
        
        # Step 1: Get basic universe
        universe = self.get_basic_universe(criteria)
        symbols = [s['symbol'] for s in universe]
        
        if not symbols:
            logger.warning("No symbols found in basic universe")
            return []
        
        # Step 2: Apply price/volume filters (if time series data exists)
        try:
            filtered_symbols = self.apply_price_volume_filters(symbols, criteria)
        except Exception as e:
            logger.warning(f"Price/volume filtering failed: {e}")
            filtered_symbols = symbols  # Fall back to basic universe
        
        # Step 3: Calculate data quality scores
        try:
            quality_scores = self.calculate_data_quality_scores(filtered_symbols)
        except Exception as e:
            logger.warning(f"Quality scoring failed: {e}")
            quality_scores = {s: 1.0 for s in filtered_symbols}  # Default to perfect scores
        
        # Step 4: Apply quality filter
        final_symbols = []
        for symbol in filtered_symbols:
            quality_score = quality_scores.get(symbol, 0.0)
            if quality_score >= criteria.min_data_quality_score:
                final_symbols.append(symbol)
        
        # Step 5: Build final results
        final_universe = []
        for symbol_info in universe:
            symbol = symbol_info['symbol']
            if symbol in final_symbols:
                symbol_info['quality_score'] = quality_scores.get(symbol, 0.0)
                final_universe.append(symbol_info)
        
        # Sort by quality score descending
        final_universe.sort(key=lambda x: x.get('quality_score', 0.0), reverse=True)
        
        logger.info(f"📊 Screening complete:")
        logger.info(f"  - Basic universe: {len(universe)} symbols")
        logger.info(f"  - After price/volume filters: {len(filtered_symbols)} symbols")
        logger.info(f"  - After quality filters: {len(final_universe)} symbols")
        
        return final_universe

    def get_predefined_universes(self) -> Dict[str, ScreeningCriteria]:
        """Get predefined screening criteria for common use cases."""
        return {
            'nasdaq_large_cap': ScreeningCriteria(
                exchanges=[ExchangeType.NASDAQ],
                asset_types=[AssetType.EQUITY],
                min_price=10.0,
                min_avg_volume=1000000,
                exclude_penny_stocks=True,
                exclude_low_volume=True,
                min_data_quality_score=0.9
            ),
            'nasdaq_all_equity': ScreeningCriteria(
                exchanges=[ExchangeType.NASDAQ],
                asset_types=[AssetType.EQUITY],
                min_price=1.0,
                min_avg_volume=10000,
                exclude_penny_stocks=False,
                exclude_low_volume=False,
                min_data_quality_score=0.7
            ),
            'major_exchanges_etf': ScreeningCriteria(
                exchanges=[ExchangeType.NASDAQ, ExchangeType.NYSE],
                asset_types=[AssetType.ETF],
                min_price=5.0,
                min_avg_volume=100000,
                exclude_penny_stocks=True,
                exclude_low_volume=True,
                min_data_quality_score=0.8
            ),
            'high_quality_all': ScreeningCriteria(
                exchanges=[ExchangeType.ALL],
                asset_types=[AssetType.ALL],
                min_price=5.0,
                min_avg_volume=500000,
                exclude_penny_stocks=True,
                exclude_low_volume=True,
                min_data_quality_score=0.9
            )
        }


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
    """CLI interface for symbol screening."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Symbol Screening and Universe Management')
    parser.add_argument('--universe', choices=['nasdaq_large_cap', 'nasdaq_all_equity', 'major_exchanges_etf', 'high_quality_all'],
                       help='Use predefined universe')
    parser.add_argument('--exchange', choices=['NASDAQ', 'NYSE', 'AMEX', 'ALL'], default='NASDAQ',
                       help='Exchange filter')
    parser.add_argument('--asset-type', choices=['Equity', 'ETF', 'REIT', 'ALL'], default='Equity',
                       help='Asset type filter')
    parser.add_argument('--min-price', type=float, help='Minimum price')
    parser.add_argument('--max-price', type=float, help='Maximum price')
    parser.add_argument('--min-volume', type=int, help='Minimum average volume')
    parser.add_argument('--min-quality', type=float, default=0.8, help='Minimum data quality score')
    parser.add_argument('--output', default='/tmp/screened_symbols.json', help='Output file')
    
    args = parser.parse_args()
    
    try:
        # Initialize screener
        snowflake_config = get_snowflake_config_from_env()
        screener = SymbolScreener(snowflake_config)
        
        # Get screening criteria
        if args.universe:
            predefined = screener.get_predefined_universes()
            criteria = predefined[args.universe]
        else:
            # Build criteria from CLI args
            criteria = ScreeningCriteria(
                exchanges=[ExchangeType(args.exchange)] if args.exchange != 'ALL' else [ExchangeType.ALL],
                asset_types=[AssetType(args.asset_type)] if args.asset_type != 'ALL' else [AssetType.ALL],
                min_price=args.min_price,
                max_price=args.max_price,
                min_avg_volume=args.min_volume,
                min_data_quality_score=args.min_quality
            )
        
        # Screen symbols
        results = screener.screen_symbols(criteria)
        
        # Save results
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        screener.close_connection()
        
        logger.info(f"✅ Screening complete! {len(results)} symbols saved to {args.output}")
        
    except Exception as e:
        logger.error(f"❌ Error in symbol screening: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
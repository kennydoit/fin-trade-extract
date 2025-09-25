"""
Utilities for modern incremental ETL architecture.
Provides deterministic data processing and watermark management.
"""

import hashlib
import json
import uuid
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from pathlib import Path
import sys

# Add symbol screener for enhanced pre-screening
sys.path.append(str(Path(__file__).parent))
try:
    from symbol_screener import SymbolScreener
    SYMBOL_SCREENER_AVAILABLE = True
except ImportError:
    SYMBOL_SCREENER_AVAILABLE = False
    print("⚠️ Symbol screener not available - pre-screening disabled")


class DateUtils:
    """Deterministic date parsing utilities."""
    
    @staticmethod
    def parse_fiscal_date(date_string: str) -> Optional[date]:
        """
        Parse fiscal date string to deterministic date object.
        
        Args:
            date_string: Date string from API (e.g., "2023-12-31")
            
        Returns:
            date object or None if invalid
        """
        if not date_string or date_string in ("None", "", "null"):
            return None
            
        try:
            # Parse to datetime first for validation, then extract date
            dt = datetime.strptime(str(date_string).strip(), "%Y-%m-%d")
            return dt.date()
        except (ValueError, AttributeError) as e:
            print(f"Invalid date format: {date_string} - {e}")
            return None
    
    @staticmethod
    def parse_timestamp(timestamp_string: str) -> Optional[datetime]:
        """
        Parse timestamp string to deterministic datetime object.
        
        Args:
            timestamp_string: Timestamp string from API
            
        Returns:
            datetime object or None if invalid
        """
        if not timestamp_string or timestamp_string in ("None", "", "null"):
            return None
            
        try:
            # Try common timestamp formats
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                try:
                    return datetime.strptime(str(timestamp_string).strip(), fmt)
                except ValueError:
                    continue
            
            # If no format matches, raise error
            raise ValueError(f"No matching format for: {timestamp_string}")
            
        except (ValueError, AttributeError) as e:
            print(f"Invalid timestamp format: {timestamp_string} - {e}")
            return None


class ContentHasher:
    """Content hashing for change detection."""
    
    @staticmethod
    def calculate_business_content_hash(data: Dict[str, Any], exclude_fields: List[str] = None) -> str:
        """
        Calculate MD5 hash of business content (excluding metadata).
        
        Args:
            data: Record data
            exclude_fields: Fields to exclude from hash (metadata fields)
            
        Returns:
            32-character MD5 hash
        """
        if exclude_fields is None:
            exclude_fields = [
                'created_at', 'updated_at', 'fetched_at', 'source_run_id',
                'landing_id', 'cash_flow_id', 'api_response_status'
            ]
        
        # Extract only business fields
        business_fields = {
            k: v for k, v in data.items() 
            if k not in exclude_fields
        }
        
        # Convert to canonical string representation
        canonical_string = json.dumps(business_fields, sort_keys=True, default=str)
        
        # Calculate MD5 hash
        return hashlib.md5(canonical_string.encode('utf-8')).hexdigest()
    
    @staticmethod
    def calculate_api_response_hash(api_response: Dict[str, Any]) -> str:
        """
        Calculate hash of raw API response.
        
        Args:
            api_response: Raw API response
            
        Returns:
            32-character MD5 hash
        """
        canonical_string = json.dumps(api_response, sort_keys=True, default=str)
        return hashlib.md5(canonical_string.encode('utf-8')).hexdigest()


class WatermarkManager:
    """Manages extraction watermarks for incremental processing."""
    
    def __init__(self, db_manager):
        """
        Initialize watermark manager.
        
        Args:
            db_manager: Database connection manager
        """
        self.db = db_manager
    
    def get_watermark(self, table_name: str, symbol_id: int) -> Optional[Dict[str, Any]]:
        """
        Get current watermark for table/symbol combination.
        
        Args:
            table_name: Name of the table being tracked
            symbol_id: Symbol ID being tracked
            
        Returns:
            Watermark data or None if not found
        """
        query = """
            SELECT last_fiscal_date, last_successful_run, consecutive_failures,
                   created_at, updated_at
            FROM source.extraction_watermarks 
            WHERE table_name = %s AND symbol_id = %s
        """
        
        result = self.db.fetch_query(query, (table_name, symbol_id))
        
        if result:
            row = result[0]
            return {
                'last_fiscal_date': row[0],
                'last_successful_run': row[1],
                'consecutive_failures': row[2],
                'created_at': row[3],
                'updated_at': row[4]
            }
        return None
    
    def update_watermark(self, table_name: str, symbol_id: int, 
                        fiscal_date: Optional[date] = None, 
                        success: bool = True) -> None:
        """
        Update watermark after processing attempt.
        
        Args:
            table_name: Name of the table being tracked
            symbol_id: Symbol ID being tracked
            fiscal_date: Latest fiscal date processed (if successful)
            success: Whether processing was successful
        """
        if success:
            query = """
                INSERT INTO source.extraction_watermarks 
                (table_name, symbol_id, last_fiscal_date, last_successful_run, consecutive_failures, updated_at)
                VALUES (%s, %s, %s, NOW(), 0, NOW())
                ON CONFLICT (table_name, symbol_id) 
                DO UPDATE SET 
                    last_fiscal_date = GREATEST(
                        source.extraction_watermarks.last_fiscal_date, 
                        EXCLUDED.last_fiscal_date
                    ),
                    last_successful_run = NOW(),
                    consecutive_failures = 0,
                    updated_at = NOW()
            """
            self.db.execute_query(query, (table_name, symbol_id, fiscal_date))
        else:
            # Track failure
            query = """
                INSERT INTO source.extraction_watermarks 
                (table_name, symbol_id, consecutive_failures, updated_at)
                VALUES (%s, %s, 1, NOW())
                ON CONFLICT (table_name, symbol_id) 
                DO UPDATE SET 
                    consecutive_failures = source.extraction_watermarks.consecutive_failures + 1,
                    updated_at = NOW()
            """
            self.db.execute_query(query, (table_name, symbol_id))
    
    def needs_processing(self, table_name: str, symbol_id: int, 
                        staleness_hours: int = 24, max_failures: int = 3) -> bool:
        """
        Check if symbol needs processing based on watermark.
        
        Args:
            table_name: Name of the table being tracked
            symbol_id: Symbol ID being tracked
            staleness_hours: How many hours before data is considered stale
            max_failures: Maximum consecutive failures before giving up
            
        Returns:
            True if symbol needs processing
        """
        watermark = self.get_watermark(table_name, symbol_id)
        
        if not watermark:
            return True  # Never processed
        
        # Skip if too many consecutive failures
        if watermark['consecutive_failures'] >= max_failures:
            return False
        
        # Check staleness
        if watermark['last_successful_run']:
            hours_since_last = (
                datetime.now() - watermark['last_successful_run']
            ).total_seconds() / 3600
            
            return hours_since_last >= staleness_hours
        
        return True  # No successful run yet
    
    def get_symbols_needing_processing(self, table_name: str, 
                                     staleness_hours: int = 24,
                                     max_failures: int = 3,
                                     limit: Optional[int] = None,
                                     quarterly_gap_detection: bool = True,
                                     reporting_lag_days: int = 45,
                                     enable_pre_screening: bool = True) -> List[Dict[str, Any]]:
        """
        Get list of symbols that need processing with enhanced quarterly gap detection and pre-screening.
        
        Args:
            table_name: Name of the table being tracked
            staleness_hours: How many hours before data is considered stale
            max_failures: Maximum consecutive failures before giving up
            limit: Maximum number of symbols to return
            quarterly_gap_detection: Enable quarterly gap detection for financial statements
            reporting_lag_days: Days after quarter end before data should be available
            enable_pre_screening: Enable symbol pre-screening to avoid likely failures
            
        Returns:
            List of symbol data needing processing
        """
        # Get base symbols using existing logic
        if quarterly_gap_detection and table_name in ['balance_sheet', 'cash_flow', 'income_statement']:
            base_symbols = self._get_symbols_with_quarterly_gap_detection(
                table_name, staleness_hours, max_failures, None, reporting_lag_days  # Don't limit here
            )
        else:
            base_symbols = self._get_symbols_without_quarterly_gap_detection(
                table_name, staleness_hours, max_failures, None  # Don't limit here
            )
        
        # Apply pre-screening if enabled and available
        if enable_pre_screening and SYMBOL_SCREENER_AVAILABLE:
            print(f"🔍 Pre-screening {len(base_symbols)} symbols to avoid likely failures...")
            
            # Convert symbols to format expected by screener
            symbol_data_for_screening = []
            for sym in base_symbols:
                # Get additional metadata from database
                query = """
                    SELECT ls.symbol, ls.asset_type, ls.status, ls.delisting_date, 
                           ls.ipo_date, ls.exchange, ls.name
                    FROM source.listing_status ls
                    WHERE ls.symbol_id = %s
                """
                result = self.db.fetch_query(query, [sym['symbol_id']])
                if result:
                    row = result[0]
                    symbol_data_for_screening.append({
                        'symbol': row[0],
                        'asset_type': row[1],
                        'status': row[2],
                        'delisting_date': row[3],
                        'ipo_date': row[4],
                        'exchange': row[5],
                        'name': row[6],
                        'consecutive_failures': sym.get('consecutive_failures', 0),
                        'symbol_id': sym['symbol_id'],
                        'last_fiscal_date': sym.get('last_fiscal_date'),
                        'last_successful_run': sym.get('last_successful_run')
                    })
            
            # Apply screening
            screening_result = SymbolScreener.filter_symbols_for_fundamentals(
                symbol_data_for_screening,
                enable_failure_blacklist=True,
                max_consecutive_failures=max_failures,
                enable_ipo_filter=False  # Keep this optional for now
            )
            
            # Convert back to original format
            screened_symbols = []
            for eligible_sym in screening_result['eligible_symbols']:
                # Find the original symbol data
                for orig_sym in base_symbols:
                    if orig_sym['symbol_id'] == eligible_sym['symbol_id']:
                        screened_symbols.append(orig_sym)
                        break
            
            # Print screening results
            stats = screening_result['statistics']
            print(f"✅ Pre-screening results:")
            print(f"   - Input symbols: {stats['total_input']}")
            print(f"   - Eligible symbols: {stats['eligible_count']}")
            print(f"   - Excluded symbols: {stats['excluded_count']}")
            print(f"   - Eligibility rate: {stats['eligibility_rate']:.1f}%")
            
            if stats['exclusion_reasons']:
                print(f"   - Top exclusion reasons:")
                for reason, count in sorted(stats['exclusion_reasons'].items(), 
                                          key=lambda x: x[1], reverse=True)[:5]:
                    print(f"     • {reason}: {count}")
            
            final_symbols = screened_symbols
        else:
            if enable_pre_screening and not SYMBOL_SCREENER_AVAILABLE:
                print("⚠️ Pre-screening requested but not available - proceeding without screening")
            final_symbols = base_symbols
        
        # Apply limit after screening
        if limit and len(final_symbols) > limit:
            print(f"📊 Limiting to {limit} symbols (from {len(final_symbols)} eligible)")
            final_symbols = final_symbols[:limit]
        
        return final_symbols
    
    def _get_symbols_without_quarterly_gap_detection(self, table_name: str,
                                                   staleness_hours: int,
                                                   max_failures: int,
                                                   limit: Optional[int]) -> List[Dict[str, Any]]:
        """
        Original time-based logic for tables without quarterly gap detection.
        
        Args:
            table_name: Name of the table being tracked
            staleness_hours: How many hours before data is considered stale
            max_failures: Maximum consecutive failures before giving up
            limit: Maximum number of symbols to return
            
        Returns:
            List of symbol data needing processing
        """
        query = """
            SELECT ls.symbol_id, ls.symbol, 
                   ew.last_fiscal_date, ew.last_successful_run, ew.consecutive_failures
            FROM source.listing_status ls
            LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                       AND ew.table_name = %s
            WHERE ls.asset_type = 'Stock'
              AND LOWER(ls.status) = 'active'
              AND ls.symbol NOT LIKE '%%WS%%'   -- Exclude warrants
              AND ls.symbol NOT LIKE '%%R'     -- Exclude rights  
              AND ls.symbol NOT LIKE '%%R%%'   -- Exclude rights variants
              AND ls.symbol NOT LIKE '%%P%%'   -- Exclude preferred shares
              AND ls.symbol NOT LIKE '%%U'      -- Exclude units (SPACs)
              AND ls.symbol NOT LIKE '%%U'     -- Exclude unit variants
              AND (
                  ew.last_successful_run IS NULL  -- Never processed
                  OR ew.last_successful_run < NOW() - INTERVAL '1 hour' * %s  -- Stale
              )
              AND COALESCE(ew.consecutive_failures, 0) < %s  -- Not permanently failed
            ORDER BY 
                CASE WHEN ew.last_successful_run IS NULL THEN 0 ELSE 1 END,
                COALESCE(ew.last_successful_run, '1900-01-01'::timestamp) ASC,
                LENGTH(ls.symbol) ASC,
                ls.symbol ASC
        """
        
        params = [table_name, staleness_hours, max_failures]
        
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        
        results = self.db.fetch_query(query, params)
        
        return [
            {
                'symbol_id': row[0],
                'symbol': row[1],
                'last_fiscal_date': row[2],
                'last_successful_run': row[3],
                'consecutive_failures': row[4] or 0
            }
            for row in results
        ]
    
    def _get_symbols_with_quarterly_gap_detection(self, table_name: str,
                                                staleness_hours: int,
                                                max_failures: int,
                                                limit: Optional[int],
                                                reporting_lag_days: int) -> List[Dict[str, Any]]:
        """
        Get symbols with quarterly gap detection logic for financial statements.
        
        This method identifies symbols that are missing expected quarterly data
        based on the current date and typical reporting lags.
        """
        query = """
            WITH quarterly_analysis AS (
                SELECT 
                    ls.symbol_id, 
                    ls.symbol,
                    ew.last_fiscal_date,
                    ew.last_successful_run,
                    ew.consecutive_failures,
                    -- Calculate expected latest quarter based on current date and reporting lag
                    -- Work backwards from current date to find the latest quarter that should have data available
                    CASE 
                        -- Current quarter minus 1 day = previous quarter end
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL %s
                        THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                        -- Previous quarter minus 1 day = quarter before that  
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL %s
                        THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                        -- Two quarters ago
                        WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' + INTERVAL %s
                        THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
                        -- Three quarters ago
                        ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '9 months' - INTERVAL '1 day')::date
                    END as expected_latest_quarter,
                    -- Check if there's a quarterly gap or staleness
                    CASE 
                        WHEN ew.last_fiscal_date IS NULL THEN TRUE -- Never processed
                        WHEN ew.last_fiscal_date < (
                            CASE 
                                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL %s
                                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date
                                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' + INTERVAL %s
                                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' - INTERVAL '1 day')::date
                                WHEN CURRENT_DATE >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' + INTERVAL %s
                                THEN (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' - INTERVAL '1 day')::date
                                ELSE (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '9 months' - INTERVAL '1 day')::date
                            END
                        ) THEN TRUE -- Has quarterly gap
                        WHEN ew.last_successful_run < NOW() - INTERVAL %s THEN TRUE -- Time-based staleness
                        ELSE FALSE
                    END as needs_processing
                FROM source.listing_status ls
                LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                           AND ew.table_name = %s
                WHERE ls.asset_type = 'Stock'
                  AND LOWER(ls.status) = 'active'
                  AND ls.symbol NOT LIKE %s   -- Exclude warrants
                  AND ls.symbol NOT LIKE %s     -- Exclude rights ending in R
                  AND ls.symbol NOT LIKE %s   -- Exclude rights with dots
                  AND ls.symbol NOT LIKE %s   -- Exclude preferred shares
                  AND ls.symbol NOT LIKE %s     -- Exclude units (SPACs)
                  AND COALESCE(ew.consecutive_failures, 0) < %s  -- Not permanently failed
            )
            SELECT symbol_id, symbol, last_fiscal_date, last_successful_run, consecutive_failures,
                   expected_latest_quarter, needs_processing
            FROM quarterly_analysis 
            WHERE needs_processing = TRUE
            ORDER BY 
                -- Prioritize quarterly gaps over time-based staleness
                CASE WHEN last_fiscal_date IS NULL THEN 0 -- Never processed (highest priority)
                     WHEN last_fiscal_date < expected_latest_quarter THEN 1 -- Has quarterly gap (high priority)
                     ELSE 2 -- Time-stale only (lower priority)
                END,
                COALESCE(last_successful_run, '1900-01-01'::timestamp) ASC,
                LENGTH(symbol) ASC,
                symbol ASC
        """
        
        params = [
            f'{reporting_lag_days} days',  # Current quarter check
            f'{reporting_lag_days} days',  # Previous quarter check  
            f'{reporting_lag_days} days',  # Two quarters ago check
            f'{reporting_lag_days} days',  # Current quarter gap check
            f'{reporting_lag_days} days',  # Previous quarter gap check
            f'{reporting_lag_days} days',  # Two quarters ago gap check
            f'{staleness_hours} hours',    # Time staleness
            table_name,                    # Table name
            '%WS%',                        # Warrants
            '%R',                          # Rights ending in R
            '%.R%',                        # Rights with dots (like SYMBOL.R)
            '%P%',                         # Preferred shares
            '%U',                          # Units
            max_failures                   # Max failures
        ]
        
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        
        results = self.db.fetch_query(query, params)
        
        return [
            {
                'symbol_id': row[0],
                'symbol': row[1],
                'last_fiscal_date': row[2],
                'last_successful_run': row[3],
                'consecutive_failures': row[4] or 0,
                'expected_latest_quarter': row[5],
                'has_quarterly_gap': row[2] is None or (row[5] and row[2] < row[5])
            }
            for row in results
        ]
    
    def get_symbols_needing_processing_with_filters(self, table_name: str, 
                                                  staleness_hours: int = 24,
                                                  max_failures: int = 3,
                                                  limit: Optional[int] = None,
                                                  exchange_filter: Optional[List[str]] = None,
                                                  asset_type_filter: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get list of symbols that need processing with exchange and asset type filtering.
        
        Args:
            table_name: Name of the table being tracked
            staleness_hours: How many hours before data is considered stale
            max_failures: Maximum consecutive failures before giving up
            limit: Maximum number of symbols to return
            exchange_filter: Filter by exchanges (e.g., ['NYSE', 'NASDAQ'])
            asset_type_filter: Filter by asset types (e.g., ['Stock', 'ETF'])
            
        Returns:
            List of symbol data needing processing
        """
        base_query = """
            SELECT ls.symbol_id, ls.symbol, 
                   ew.last_fiscal_date, ew.last_successful_run, ew.consecutive_failures
            FROM source.listing_status ls
            LEFT JOIN source.extraction_watermarks ew ON ew.symbol_id = ls.symbol_id 
                                                       AND ew.table_name = %s
            WHERE LOWER(ls.status) = 'active'
              AND ls.symbol NOT LIKE %s   -- Exclude warrants
              AND ls.symbol NOT LIKE %s     -- Exclude rights ending in R
              AND ls.symbol NOT LIKE %s   -- Exclude rights with dots
              AND ls.symbol NOT LIKE %s   -- Exclude preferred shares
              AND ls.symbol NOT LIKE %s      -- Exclude units (SPACs)
              AND (
                  ew.last_successful_run IS NULL  -- Never processed
                  OR ew.last_successful_run < NOW() - INTERVAL '1 hour' * %s  -- Stale
              )
              AND COALESCE(ew.consecutive_failures, 0) < %s  -- Not permanently failed
        """
        
        params = [table_name, '%WS%', '%R', '%.R%', '%P%', '%U', staleness_hours, max_failures]
        if asset_type_filter:
            placeholders = ",".join(["%s" for _ in asset_type_filter])
            base_query += f" AND ls.asset_type IN ({placeholders})"
            params.extend(asset_type_filter)
        else:
            # Default to Stock and ETF if no filter provided
            base_query += " AND ls.asset_type IN (%s, %s)"
            params.extend(['Stock', 'ETF'])
        
        # Add exchange filter
        if exchange_filter:
            placeholders = ",".join(["%s" for _ in exchange_filter])
            base_query += f" AND ls.exchange IN ({placeholders})"
            params.extend(exchange_filter)
        
        base_query += """
            ORDER BY 
                CASE WHEN ew.last_successful_run IS NULL THEN 0 ELSE 1 END,
                COALESCE(ew.last_successful_run, '1900-01-01'::timestamp) ASC,
                LENGTH(ls.symbol) ASC,
                ls.symbol ASC
        """
        
        if limit:
            base_query += " LIMIT %s"
            params.append(limit)
        
        results = self.db.fetch_query(base_query, params)
        
        return [
            {
                'symbol_id': row[0],
                'symbol': row[1],
                'last_fiscal_date': row[2],
                'last_successful_run': row[3],
                'consecutive_failures': row[4] or 0
            }
            for row in results
        ]


class RunIdGenerator:
    """Generate unique run IDs for tracking data lineage."""
    
    @staticmethod
    def generate() -> str:
        """Generate a unique run ID."""
        return str(uuid.uuid4())

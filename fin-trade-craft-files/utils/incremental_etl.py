"""
Utilities for modern incremental ETL architecture.
Provides deterministic data processing and watermark management.
"""

import hashlib
import json
import sys
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

# Add symbol screener for enhanced pre-screening
sys.path.append(str(Path(__file__).parent))
try:
    from symbol_screener import SymbolScreener
    SYMBOL_SCREENER_AVAILABLE = True
except ImportError:
    SYMBOL_SCREENER_AVAILABLE = False


class DateUtils:
    """Deterministic date parsing utilities."""

    @staticmethod
    def parse_fiscal_date(date_string: str) -> date | None:
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
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def parse_timestamp(timestamp_string: str) -> datetime | None:
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

        except (ValueError, AttributeError):
            return None


class ContentHasher:
    """Content hashing for change detection."""

    @staticmethod
    def calculate_business_content_hash(data: dict[str, Any], exclude_fields: list[str] = None) -> str:
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
    def calculate_api_response_hash(api_response: dict[str, Any]) -> str:
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

    def get_watermark(self, table_name: str, symbol_id: int) -> dict[str, Any] | None:
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
                        fiscal_date: date | None = None,
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
                                     limit: int | None = None,
                                     quarterly_gap_detection: bool = True,
                                     reporting_lag_days: int = 45,
                                     enable_pre_screening: bool = True) -> list[dict[str, Any]]:
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

            if stats['exclusion_reasons']:
                for _reason, _count in sorted(stats['exclusion_reasons'].items(),
                                          key=lambda x: x[1], reverse=True)[:5]:
                    pass

            final_symbols = screened_symbols
        else:
            if enable_pre_screening and not SYMBOL_SCREENER_AVAILABLE:
                pass
            final_symbols = base_symbols

        # Apply limit after screening
        if limit and len(final_symbols) > limit:
            final_symbols = final_symbols[:limit]

        return final_symbols

    def _symbol_needs_processing(self, table_name: str, symbol_id: int,
                               staleness_hours: int, max_failures: int) -> bool:
        """
        Check if a specific symbol needs processing.

        Args:
            table_name: Name of the table being tracked
            symbol_id: Symbol ID to check
            staleness_hours: Hours before data is considered stale
            max_failures: Maximum consecutive failures before giving up

        Returns:
            True if symbol needs processing
        """
        # Check watermark status
        watermark_query = """
            SELECT last_successful_run, consecutive_failures
            FROM source.extraction_watermarks
            WHERE table_name = %s AND symbol_id = %s
        """

        result = self.db.fetch_query(watermark_query, (table_name, symbol_id))

        if not result:
            # No watermark record - needs processing
            return True

        last_successful_run, consecutive_failures = result[0]

        # Check if too many failures
        if consecutive_failures and consecutive_failures >= max_failures:
            return False

        # Check staleness
        if last_successful_run:
            hours_since_last = (datetime.now() - last_successful_run).total_seconds() / 3600
            return hours_since_last >= staleness_hours

        return True  # No successful run yet

    def get_symbols_needing_processing_with_dcs(self, table_name: str,
                                              staleness_hours: int = 24,
                                              max_failures: int = 3,
                                              limit: int | None = None,
                                              quarterly_gap_detection: bool = True,
                                              reporting_lag_days: int = 45,
                                              enable_pre_screening: bool = True,
                                              min_dcs_threshold: float = 0.3) -> list[dict[str, Any]]:
        """
        Get symbols needing processing with DCS-based prioritization.

        This method integrates the Data Coverage Score system to prioritize extraction
        based on data quality and completeness metrics.

        Args:
            table_name: Name of the table being tracked
            staleness_hours: How many hours before data is considered stale
            max_failures: Maximum consecutive failures before giving up
            limit: Maximum number of symbols to return
            quarterly_gap_detection: Enable quarterly gap detection for financial statements
            reporting_lag_days: Days after quarter end before data should be available
            enable_pre_screening: Enable symbol pre-screening to avoid likely failures
            min_dcs_threshold: Minimum DCS score required for inclusion

        Returns:
            List of symbol data ordered by extraction priority (DCS + staleness)
        """
        try:
            from .universe_management import UniverseManager
            universe_manager = UniverseManager()

            # Get DCS-prioritized symbols
            prioritized_symbols = universe_manager.get_extraction_priority_list(
                extractor_type=table_name,
                limit=limit or 1000,  # Get larger set first for filtering
                min_dcs=min_dcs_threshold
            )

            if not prioritized_symbols:
                return self.get_symbols_needing_processing(
                    table_name, staleness_hours, max_failures, limit,
                    quarterly_gap_detection, reporting_lag_days, enable_pre_screening
                )

            # Filter prioritized symbols through standard processing logic
            symbols_needing_processing = []

            for symbol_info in prioritized_symbols:
                symbol_id = symbol_info['symbol_id']
                symbol = symbol_info['symbol']

                # Check if symbol needs processing using standard logic
                if self._symbol_needs_processing(table_name, symbol_id, staleness_hours, max_failures):

                    # Add DCS metadata to symbol info
                    enhanced_symbol_info = {
                        'symbol_id': symbol_id,
                        'symbol': symbol,
                        'dcs': symbol_info['dcs'],
                        'current_tier': symbol_info['current_tier'],
                        'hours_since_last_run': symbol_info['hours_since_last_run'],
                        'extraction_priority_score': self._calculate_extraction_priority_score(
                            symbol_info['dcs'],
                            symbol_info['hours_since_last_run'],
                            symbol_info['current_tier']
                        )
                    }

                    symbols_needing_processing.append(enhanced_symbol_info)

                    # Apply limit if specified
                    if limit and len(symbols_needing_processing) >= limit:
                        break

            # Apply quarterly gap detection if enabled
            if quarterly_gap_detection and table_name in ['balance_sheet', 'cash_flow', 'income_statement']:
                symbols_needing_processing = self._apply_quarterly_gap_filtering(
                    symbols_needing_processing, table_name, reporting_lag_days
                )

            # Apply pre-screening if enabled
            if enable_pre_screening and SYMBOL_SCREENER_AVAILABLE:
                symbols_needing_processing = self._apply_pre_screening_to_dcs_symbols(symbols_needing_processing)

            if symbols_needing_processing:
                sum(s['dcs'] for s in symbols_needing_processing) / len(symbols_needing_processing)
                tier_counts = {}
                for s in symbols_needing_processing:
                    tier = s['current_tier']
                    tier_counts[tier] = tier_counts.get(tier, 0) + 1


            return symbols_needing_processing

        except ImportError:
            return self.get_symbols_needing_processing(
                table_name, staleness_hours, max_failures, limit,
                quarterly_gap_detection, reporting_lag_days, enable_pre_screening
            )

    def _calculate_extraction_priority_score(self, dcs: float, hours_since_last_run: float,
                                           current_tier: str) -> float:
        """
        Calculate extraction priority score combining DCS, staleness, and tier.

        Args:
            dcs: Data Coverage Score (0-1)
            hours_since_last_run: Hours since last successful run
            current_tier: Universe tier (core, extended, long_tail)

        Returns:
            Priority score (higher = more important)
        """
        # Base score from DCS (40% weight)
        dcs_component = 0.4 * dcs

        # Staleness component (40% weight) - normalize hours to 0-1 scale
        max_staleness_hours = 168  # 1 week
        staleness_normalized = min(hours_since_last_run / max_staleness_hours, 1.0)
        staleness_component = 0.4 * staleness_normalized

        # Tier priority component (20% weight)
        tier_weights = {'core': 1.0, 'extended': 0.7, 'long_tail': 0.3}
        tier_component = 0.2 * tier_weights.get(current_tier, 0.3)

        return dcs_component + staleness_component + tier_component

    def _apply_quarterly_gap_filtering(self, symbols: list[dict[str, Any]],
                                     table_name: str, reporting_lag_days: int) -> list[dict[str, Any]]:
        """Apply quarterly gap detection filtering to symbols."""
        # This would integrate with existing quarterly gap detection logic
        # For now, return symbols as-is since quarterly gap detection is applied elsewhere
        return symbols

    def _apply_pre_screening_to_dcs_symbols(self, symbols: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply pre-screening to filter out likely failures from DCS symbols."""
        if not SYMBOL_SCREENER_AVAILABLE:
            return symbols

        screener = SymbolScreener()
        symbol_list = [{'symbol_id': s['symbol_id'], 'symbol': s['symbol']} for s in symbols]

        # Get screening results
        screening_results = screener.screen_symbols(symbol_list)
        eligible_symbol_ids = {s['symbol_id'] for s in screening_results['eligible_symbols']}

        # Filter symbols
        return [s for s in symbols if s['symbol_id'] in eligible_symbol_ids]



    def _get_symbols_without_quarterly_gap_detection(self, table_name: str,
                                                   staleness_hours: int,
                                                   max_failures: int,
                                                   limit: int | None) -> list[dict[str, Any]]:
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
                                                limit: int | None,
                                                reporting_lag_days: int) -> list[dict[str, Any]]:
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
                    -- Check if there's a quarterly gap or staleness WITH COOLING-OFF PERIOD
                    CASE
                        WHEN ew.last_fiscal_date IS NULL THEN TRUE -- Never processed
                        -- Has quarterly gap AND hasn't been processed recently (7-day cooling-off period)
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
                        ) AND (
                            ew.last_successful_run IS NULL
                            OR ew.last_successful_run < NOW() - INTERVAL '7 days'
                        ) THEN TRUE -- Has quarterly gap but with 7-day cooling-off period
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
                                                  limit: int | None = None,
                                                  exchange_filter: list[str] | None = None,
                                                  asset_type_filter: list[str] | None = None) -> list[dict[str, Any]]:
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

"""
Data Coverage Score (DCS) and Tiered Universe Management System

This module implements intelligent symbol prioritization based on data completeness,
freshness, and quality metrics. It enables efficient resource allocation by focusing
on symbols with the highest data coverage and trading potential.

The system provides:
1. Data Coverage Score calculation across multiple data sources
2. Three-tier universe management (Core, Extended, Long Tail)
3. Promotion/demotion rules based on liquidity and coverage
4. Smart scheduling prioritization for extractors
"""

import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

# Add the parent directories to the path so we can import from db and utils
sys.path.append(str(Path(__file__).parent.parent))
from db.postgres_database_manager import PostgresDatabaseManager


class UniverseTier(Enum):
    """Universe tiers for symbol classification."""
    CORE = "core"           # 400-800 active trading symbols
    EXTENDED = "extended"   # 3-5k symbols with rich data
    LONG_TAIL = "long_tail" # 10k+ symbols with minimal data


@dataclass
class DataCoverageMetrics:
    """Metrics used to calculate Data Coverage Score."""
    fundamentals_completeness: float  # 0-1, recent quarters coverage
    insider_timeliness: float         # 0-1, recent transaction freshness
    transcript_availability: float    # 0-1, earnings calls coverage
    news_velocity: float             # 0-1, news flow consistency
    ohlcv_freshness: float          # 0-1, price data recency

    def calculate_dcs(self) -> float:
        """Calculate weighted Data Coverage Score."""
        return (
            0.35 * self.fundamentals_completeness +
            0.20 * self.insider_timeliness +
            0.15 * self.transcript_availability +
            0.15 * self.news_velocity +
            0.15 * self.ohlcv_freshness
        )


@dataclass
class SymbolProfile:
    """Complete symbol profile with metrics and classification."""
    symbol_id: int
    symbol: str
    name: str
    exchange: str
    market_cap: float | None
    avg_daily_volume: float | None
    avg_daily_dollar_volume: float | None
    sector: str | None
    current_tier: UniverseTier
    dcs: float
    coverage_metrics: DataCoverageMetrics
    last_updated: datetime
    quarters_of_fundamentals: int
    liquidity_score: float


class DataCoverageAnalyzer:
    """Analyzes data coverage and calculates DCS for symbols."""

    def __init__(self):
        self.db_manager = None

    def _get_db_manager(self):
        """Get database manager with context management."""
        if not self.db_manager:
            self.db_manager = PostgresDatabaseManager()
        return self.db_manager

    def calculate_fundamentals_completeness(self, db, symbol_id: int) -> float:
        """
        Calculate fundamentals completeness score (0-1).

        Looks at recent 8 quarters for balance sheet, income statement, and cash flow.
        Returns weighted average of data availability across statements.
        """
        cutoff_date = date.today() - timedelta(days=2*365)  # 2 years back

        # Count quarters with data for each statement type
        queries = {
            'balance_sheet': """
                SELECT COUNT(DISTINCT fiscal_date_ending)
                FROM source.balance_sheet
                WHERE symbol_id = %s AND fiscal_date_ending >= %s
                AND report_type = 'quarterly'
                AND api_response_status = 'pass'
            """,
            'income_statement': """
                SELECT COUNT(DISTINCT fiscal_date_ending)
                FROM source.income_statement
                WHERE symbol_id = %s AND fiscal_date_ending >= %s
                AND report_type = 'quarterly'
                AND api_response_status = 'pass'
            """,
            'cash_flow': """
                SELECT COUNT(DISTINCT fiscal_date_ending)
                FROM source.cash_flow
                WHERE symbol_id = %s AND fiscal_date_ending >= %s
                AND report_type = 'quarterly'
                AND api_response_status = 'pass'
            """
        }

        scores = []
        for _statement_type, query in queries.items():
            result = db.fetch_query(query, (symbol_id, cutoff_date))
            quarters_available = result[0][0] if result else 0
            # Score as fraction of expected 8 quarters, capped at 1.0
            score = min(quarters_available / 8.0, 1.0)
            scores.append(score)

        # Weighted average (balance sheet most important)
        return 0.4 * scores[0] + 0.3 * scores[1] + 0.3 * scores[2]

    def calculate_insider_timeliness(self, db, symbol_id: int) -> float:
        """
        Calculate insider transaction timeliness score (0-1).

        Based on recency and consistency of insider transaction data.
        """
        # Check for transactions in last 180 days
        recent_cutoff = date.today() - timedelta(days=180)

        query = """
            SELECT
                COUNT(*) as transaction_count,
                MAX(transaction_date) as latest_transaction,
                COUNT(DISTINCT DATE_TRUNC('month', transaction_date)) as active_months
            FROM source.insider_transactions
            WHERE symbol_id = %s AND transaction_date >= %s
            AND api_response_status = 'pass'
        """

        result = db.fetch_query(query, (symbol_id, recent_cutoff))
        if not result:
            return 0.0

        transaction_count, latest_transaction, active_months = result[0]

        if transaction_count == 0:
            return 0.0

        # Base score from having any transactions
        base_score = 0.5

        # Bonus for recent activity (last 30 days)
        if latest_transaction and latest_transaction >= date.today() - timedelta(days=30):
            base_score += 0.3

        # Bonus for consistent activity across months
        consistency_bonus = min(active_months / 6.0, 0.2)  # Up to 0.2 for 6+ months

        return min(base_score + consistency_bonus, 1.0)

    def calculate_transcript_availability(self, db, symbol_id: int) -> float:
        """
        Calculate earnings call transcript availability score (0-1).

        Based on recent quarters with transcript coverage.
        """
        cutoff_date = date.today() - timedelta(days=2*365)  # 2 years back

        query = """
            SELECT COUNT(DISTINCT quarter)
            FROM source.earnings_call_transcripts
            WHERE symbol_id = %s
            AND created_at >= %s
            AND api_response_status = 'pass'
        """

        result = db.fetch_query(query, (symbol_id, cutoff_date))
        quarters_with_transcripts = result[0][0] if result else 0

        # Score as fraction of expected 8 quarters
        return min(quarters_with_transcripts / 8.0, 1.0)

    def calculate_news_velocity(self, db, symbol_id: int) -> float:
        """
        Calculate news velocity score (0-1).

        For now, return 1.0 as suggested in the strategy (can be enhanced later).
        """
        # TODO: Implement when news extraction is added
        return 1.0

    def calculate_ohlcv_freshness(self, db, symbol_id: int) -> float:
        """
        Calculate OHLCV data freshness score (0-1).

        Based on how recent the latest price data is.
        """
        query = """
            SELECT MAX(date) as latest_date
            FROM source.time_series_daily_adjusted
            WHERE symbol_id = %s
        """

        result = db.fetch_query(query, (symbol_id,))
        if not result or not result[0][0]:
            return 0.0

        latest_date = result[0][0]
        days_stale = (date.today() - latest_date).days

        # Perfect score for data within 3 business days, degrading afterwards
        if days_stale <= 3:
            return 1.0
        if days_stale <= 7:
            return 0.8
        if days_stale <= 30:
            return 0.5
        if days_stale <= 90:
            return 0.2
        return 0.0

    def calculate_symbol_dcs(self, db, symbol_id: int) -> DataCoverageMetrics:
        """Calculate complete data coverage metrics for a symbol."""
        return DataCoverageMetrics(
            fundamentals_completeness=self.calculate_fundamentals_completeness(db, symbol_id),
            insider_timeliness=self.calculate_insider_timeliness(db, symbol_id),
            transcript_availability=self.calculate_transcript_availability(db, symbol_id),
            news_velocity=self.calculate_news_velocity(db, symbol_id),
            ohlcv_freshness=self.calculate_ohlcv_freshness(db, symbol_id)
        )



class UniverseManager:
    """Manages symbol universe tiers and promotion/demotion rules."""

    def __init__(self):
        self.coverage_analyzer = DataCoverageAnalyzer()
        self.db_manager = None

    def _get_db_manager(self):
        """Get database manager with context management."""
        if not self.db_manager:
            self.db_manager = PostgresDatabaseManager()
        return self.db_manager

    def _ensure_universe_table_exists(self, db):
        """Ensure the symbol_universe table exists."""
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS source.symbol_universe (
                symbol_id BIGINT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                current_tier VARCHAR(20) NOT NULL,
                dcs DECIMAL(5,3) NOT NULL,
                fundamentals_completeness DECIMAL(5,3),
                insider_timeliness DECIMAL(5,3),
                transcript_availability DECIMAL(5,3),
                news_velocity DECIMAL(5,3),
                ohlcv_freshness DECIMAL(5,3),
                avg_daily_dollar_volume DECIMAL(20,2),
                quarters_of_fundamentals INTEGER,
                liquidity_score DECIMAL(5,3),
                last_updated TIMESTAMPTZ DEFAULT NOW(),
                created_at TIMESTAMPTZ DEFAULT NOW(),

                FOREIGN KEY (symbol_id) REFERENCES source.listing_status(symbol_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_symbol_universe_tier ON source.symbol_universe(current_tier);
            CREATE INDEX IF NOT EXISTS idx_symbol_universe_dcs ON source.symbol_universe(dcs DESC);
            CREATE INDEX IF NOT EXISTS idx_symbol_universe_liquidity ON source.symbol_universe(avg_daily_dollar_volume DESC);
        """

        db.execute_script(create_table_sql)

    def calculate_liquidity_metrics(self, db, symbol_id: int) -> tuple[float, float]:
        """
        Calculate liquidity metrics for a symbol.

        Returns:
            (avg_daily_dollar_volume, liquidity_score)
        """
        # Calculate 60-day average daily dollar volume
        query = """
            SELECT
                AVG(close * volume) as avg_daily_dollar_volume,
                COUNT(*) as trading_days,
                STDDEV(close * volume) as volume_volatility
            FROM source.time_series_daily_adjusted
            WHERE symbol_id = %s
            AND date >= %s
            AND volume > 0
        """

        cutoff_date = date.today() - timedelta(days=90)  # 90 days to get ~60 trading days
        result = db.fetch_query(query, (symbol_id, cutoff_date))

        if not result or not result[0][0]:
            return 0.0, 0.0

        avg_dollar_volume, trading_days, volume_volatility = result[0]

        # Liquidity score based on volume and consistency
        liquidity_score = 0.0

        # Base score from dollar volume (log scale)
        if avg_dollar_volume > 0:
            import math
            volume_score = min(math.log10(avg_dollar_volume) / math.log10(100_000_000), 1.0)  # $100M = 1.0
            liquidity_score += 0.7 * volume_score

        # Consistency bonus (lower volatility is better)
        if volume_volatility and avg_dollar_volume > 0:
            cv = float(volume_volatility) / float(avg_dollar_volume)  # Coefficient of variation
            consistency_score = max(0, 1.0 - cv)  # Lower CV = higher score
            liquidity_score += 0.3 * consistency_score

        return float(avg_dollar_volume or 0.0), min(liquidity_score, 1.0)

    def count_quarters_of_fundamentals(self, db, symbol_id: int) -> int:
        """Count total quarters of fundamental data available."""
        query = """
            SELECT COUNT(DISTINCT fiscal_date_ending)
            FROM source.balance_sheet
            WHERE symbol_id = %s
            AND report_type = 'quarterly'
            AND api_response_status = 'pass'
        """

        result = db.fetch_query(query, (symbol_id,))
        return result[0][0] if result else 0

    def determine_tier(self, dcs: float, avg_daily_dollar_volume: float,
                      quarters_of_fundamentals: int, current_tier: UniverseTier | None = None) -> UniverseTier:
        """
        Determine appropriate tier based on metrics and promotion rules.

        Promotion to CORE requirements:
        - Liquidity > $5M daily average
        - DCS >= 0.8
        - At least 8 quarters of fundamentals

        Promotion to EXTENDED requirements:
        - DCS >= 0.6
        - At least 4 quarters of fundamentals
        """

        # Core tier requirements
        if (avg_daily_dollar_volume >= 5_000_000 and
            dcs >= 0.8 and
            quarters_of_fundamentals >= 8):
            return UniverseTier.CORE

        # Extended tier requirements
        if (dcs >= 0.6 and
            quarters_of_fundamentals >= 4):
            return UniverseTier.EXTENDED

        # Everything else goes to long tail
        return UniverseTier.LONG_TAIL

    def refresh_universe_classification(self, limit: int | None = None) -> dict[str, Any]:
        """
        Refresh universe classification for all symbols.

        Args:
            limit: Optional limit on number of symbols to process

        Returns:
            Summary of classification changes
        """

        with self._get_db_manager() as db:
            # Ensure universe table exists
            self._ensure_universe_table_exists(db)

            # Get all active symbols
            query = """
                SELECT symbol_id, symbol, name, exchange
                FROM source.listing_status
                WHERE status = 'Active'
                ORDER BY symbol_id
            """
            if limit:
                query += f" LIMIT {limit}"

            symbols = db.fetch_query(query)

            results = {
                'processed': 0,
                'tier_changes': 0,
                'new_classifications': 0,
                'core_count': 0,
                'extended_count': 0,
                'long_tail_count': 0
            }

            for i, (symbol_id, symbol, _name, _exchange) in enumerate(symbols, 1):
                if i % 100 == 0:
                    pass

                # Calculate DCS metrics
                coverage_metrics = self.coverage_analyzer.calculate_symbol_dcs(db, symbol_id)
                dcs = coverage_metrics.calculate_dcs()

                # Calculate liquidity metrics
                avg_daily_dollar_volume, liquidity_score = self.calculate_liquidity_metrics(db, symbol_id)

                # Count quarters of fundamentals
                quarters_of_fundamentals = self.count_quarters_of_fundamentals(db, symbol_id)

                # Get current tier if exists
                current_tier_query = """
                    SELECT current_tier FROM source.symbol_universe WHERE symbol_id = %s
                """
                current_result = db.fetch_query(current_tier_query, (symbol_id,))
                current_tier_str = current_result[0][0] if current_result else None
                current_tier = UniverseTier(current_tier_str) if current_tier_str else None

                # Determine new tier
                new_tier = self.determine_tier(dcs, avg_daily_dollar_volume,
                                             quarters_of_fundamentals, current_tier)

                # Upsert classification
                upsert_query = """
                    INSERT INTO source.symbol_universe (
                        symbol_id, symbol, current_tier, dcs,
                        fundamentals_completeness, insider_timeliness,
                        transcript_availability, news_velocity, ohlcv_freshness,
                        avg_daily_dollar_volume, quarters_of_fundamentals,
                        liquidity_score, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (symbol_id) DO UPDATE SET
                        current_tier = EXCLUDED.current_tier,
                        dcs = EXCLUDED.dcs,
                        fundamentals_completeness = EXCLUDED.fundamentals_completeness,
                        insider_timeliness = EXCLUDED.insider_timeliness,
                        transcript_availability = EXCLUDED.transcript_availability,
                        news_velocity = EXCLUDED.news_velocity,
                        ohlcv_freshness = EXCLUDED.ohlcv_freshness,
                        avg_daily_dollar_volume = EXCLUDED.avg_daily_dollar_volume,
                        quarters_of_fundamentals = EXCLUDED.quarters_of_fundamentals,
                        liquidity_score = EXCLUDED.liquidity_score,
                        last_updated = NOW()
                """

                db.execute_query(upsert_query, (
                    symbol_id, symbol, new_tier.value, dcs,
                    coverage_metrics.fundamentals_completeness,
                    coverage_metrics.insider_timeliness,
                    coverage_metrics.transcript_availability,
                    coverage_metrics.news_velocity,
                    coverage_metrics.ohlcv_freshness,
                    avg_daily_dollar_volume,
                    quarters_of_fundamentals,
                    liquidity_score
                ))

                # Track changes
                results['processed'] += 1

                if current_tier != new_tier:
                    if current_tier is None:
                        results['new_classifications'] += 1
                    else:
                        results['tier_changes'] += 1

                # Count by tier
                if new_tier == UniverseTier.CORE:
                    results['core_count'] += 1
                elif new_tier == UniverseTier.EXTENDED:
                    results['extended_count'] += 1
                else:
                    results['long_tail_count'] += 1


            return results

    def get_symbols_by_tier(self, tier: UniverseTier, limit: int | None = None,
                           min_dcs: float | None = None) -> list[dict[str, Any]]:
        """
        Get symbols in a specific tier, ordered by DCS.

        Args:
            tier: Universe tier to retrieve
            limit: Optional limit on results
            min_dcs: Optional minimum DCS threshold

        Returns:
            List of symbol dictionaries with metrics
        """
        with self._get_db_manager() as db:
            query = """
                SELECT
                    u.symbol_id, u.symbol, u.dcs, u.avg_daily_dollar_volume,
                    u.fundamentals_completeness, u.insider_timeliness,
                    u.transcript_availability, u.news_velocity, u.ohlcv_freshness,
                    u.quarters_of_fundamentals, u.liquidity_score,
                    ls.name, ls.exchange
                FROM source.symbol_universe u
                JOIN source.listing_status ls ON u.symbol_id = ls.symbol_id
                WHERE u.current_tier = %s
            """

            params = [tier.value]

            if min_dcs is not None:
                query += " AND u.dcs >= %s"
                params.append(min_dcs)

            query += " ORDER BY u.dcs DESC, u.avg_daily_dollar_volume DESC"

            if limit:
                query += f" LIMIT {limit}"

            results = db.fetch_query(query, params)

            symbols = []
            for row in results:
                symbols.append({
                    'symbol_id': row[0],
                    'symbol': row[1],
                    'dcs': float(row[2]),
                    'avg_daily_dollar_volume': float(row[3]) if row[3] else 0.0,
                    'fundamentals_completeness': float(row[4]) if row[4] else 0.0,
                    'insider_timeliness': float(row[5]) if row[5] else 0.0,
                    'transcript_availability': float(row[6]) if row[6] else 0.0,
                    'news_velocity': float(row[7]) if row[7] else 0.0,
                    'ohlcv_freshness': float(row[8]) if row[8] else 0.0,
                    'quarters_of_fundamentals': row[9] if row[9] else 0,
                    'liquidity_score': float(row[10]) if row[10] else 0.0,
                    'name': row[11],
                    'exchange': row[12]
                })

            return symbols

    def get_extraction_priority_list(self, extractor_type: str, limit: int = 100,
                                   min_dcs: float = 0.3) -> list[dict[str, Any]]:
        """
        Get prioritized list of symbols for extraction based on DCS and staleness.

        Args:
            extractor_type: Type of extractor ('balance_sheet', 'cash_flow', etc.)
            limit: Maximum number of symbols to return
            min_dcs: Minimum DCS threshold

        Returns:
            Ordered list of symbols by extraction priority
        """
        with self._get_db_manager() as db:
            # Query symbols with DCS and watermark info
            query = """
                SELECT
                    u.symbol_id, u.symbol, u.dcs, u.current_tier,
                    u.avg_daily_dollar_volume, u.liquidity_score,
                    w.last_successful_run,
                    EXTRACT(EPOCH FROM (NOW() - COALESCE(w.last_successful_run, '1970-01-01'::timestamptz))) / 3600 as hours_since_last_run
                FROM source.symbol_universe u
                LEFT JOIN source.extraction_watermarks w ON u.symbol_id = w.symbol_id AND w.table_name = %s
                WHERE u.dcs >= %s
                ORDER BY
                    -- Tier priority (CORE > EXTENDED > LONG_TAIL)
                    CASE u.current_tier
                        WHEN 'core' THEN 1
                        WHEN 'extended' THEN 2
                        ELSE 3
                    END,
                    -- Staleness priority (older = higher priority)
                    hours_since_last_run DESC,
                    -- DCS priority (higher DCS = higher priority)
                    u.dcs DESC
                LIMIT %s
            """

            results = db.fetch_query(query, (extractor_type, min_dcs, limit))

            symbols = []
            for row in results:
                symbols.append({
                    'symbol_id': row[0],
                    'symbol': row[1],
                    'dcs': float(row[2]),
                    'current_tier': row[3],
                    'avg_daily_dollar_volume': float(row[4]) if row[4] else 0.0,
                    'liquidity_score': float(row[5]) if row[5] else 0.0,
                    'last_successful_run': row[6],
                    'hours_since_last_run': float(row[7]) if row[7] else float('inf')
                })

            return symbols


def main():
    """Demo/test function for the universe management system."""
    import argparse

    parser = argparse.ArgumentParser(description="Universe Management System")
    parser.add_argument("--refresh", action="store_true",
                       help="Refresh universe classification")
    parser.add_argument("--limit", type=int, default=100,
                       help="Limit number of symbols to process")
    parser.add_argument("--show-core", action="store_true",
                       help="Show core tier symbols")
    parser.add_argument("--show-extended", action="store_true",
                       help="Show extended tier symbols")
    parser.add_argument("--priority", type=str,
                       help="Show extraction priority for extractor type")

    args = parser.parse_args()

    manager = UniverseManager()

    if args.refresh:
        manager.refresh_universe_classification(limit=args.limit)

    if args.show_core:
        symbols = manager.get_symbols_by_tier(UniverseTier.CORE, limit=10)
        for _symbol in symbols:
            pass

    if args.show_extended:
        symbols = manager.get_symbols_by_tier(UniverseTier.EXTENDED, limit=10)
        for _symbol in symbols:
            pass

    if args.priority:
        symbols = manager.get_extraction_priority_list(args.priority, limit=10)
        for symbol in symbols:
            hours = symbol['hours_since_last_run']
            "Never" if hours == float('inf') else f"{hours:.1f}h ago"


if __name__ == "__main__":
    main()

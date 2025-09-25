"""
Enhanced symbol pre-screening utilities for fundamental data extractors.
Filters out symbols that are unlikely to have fundamental data.
"""

import re
from datetime import datetime, timedelta
from typing import Any


class SymbolScreener:
    """Pre-screen symbols to avoid unnecessary API calls for symbols without fundamentals."""

    # Patterns for symbols that typically don't have fundamental data
    WARRANT_PATTERNS = [
        r'.*WS.*',      # Warrants containing WS
        r'.*\.WS$',     # Warrants ending with .WS
        r'.*-WS$',      # Warrants ending with -WS
        r'.*W$'         # Some warrants end with W (but be careful with regular stocks)
    ]

    RIGHTS_PATTERNS = [
        r'^.+R$',       # Rights typically end in R (but exclude common stocks)
        r'.*\.R$',      # Rights with dot notation
        r'.*-R$',       # Rights with dash notation
    ]

    PREFERRED_PATTERNS = [
        r'.*\.P.*',     # Preferred with dot notation
        r'.*-P.*',      # Preferred with dash notation
        r'.*PR.*',      # Preferred containing PR
    ]

    UNIT_PATTERNS = [
        r'^.+U$',       # Units typically end in U (SPACs)
        r'.*\.U$',      # Units with dot notation
        r'.*-U$',       # Units with dash notation
    ]

    # Common exceptions that should NOT be filtered (legitimate stocks)
    STOCK_EXCEPTIONS = {
        # Common stock patterns that might match warrant/rights/preferred patterns
        'CAR', 'AAR', 'BAR', 'EAR', 'FAR', 'GAR', 'HAR', 'JAR', 'KAR', 'LAR', 'MAR', 'NAR', 'PAR', 'QAR', 'RAR', 'SAR', 'TAR', 'VAR', 'WAR',
        'ABER', 'APER', 'ATER', 'BPER', 'CPER', 'DPER', 'EPER', 'FPER', 'GPER', 'HPER', 'JPER', 'KPER', 'LPER', 'MPER', 'NPER', 'OPER', 'PPER', 'QPER', 'RPER', 'SPER', 'TPER', 'UPER', 'VPER', 'WPER',
        'AU', 'BU', 'CU', 'FU', 'GU', 'HU', 'JU', 'KU', 'LU', 'MU', 'NU', 'PU', 'QU', 'RU', 'SU', 'TU', 'VU', 'WU', 'XU', 'YU', 'ZU',
        # Well-known legitimate stocks with dots/dashes (class shares)
        'BRK.A', 'BRK.B', 'GOOGL', 'GOOG'
    }

    @classmethod
    def is_likely_warrant(cls, symbol: str) -> bool:
        """Check if symbol is likely a warrant."""
        if symbol in cls.STOCK_EXCEPTIONS:
            return False

        for pattern in cls.WARRANT_PATTERNS:
            if re.match(pattern, symbol, re.IGNORECASE):
                # Additional check: if it's just a single letter + W, might be legitimate
                return not (len(symbol) == 2 and symbol.endswith('W'))
        return False

    @classmethod
    def is_likely_rights(cls, symbol: str) -> bool:
        """Check if symbol is likely rights."""
        if symbol in cls.STOCK_EXCEPTIONS:
            return False

        for pattern in cls.RIGHTS_PATTERNS:
            if re.match(pattern, symbol, re.IGNORECASE):
                # Exclude common legitimate stocks ending in R
                return symbol.upper() not in ['CAR', 'AAR', 'BAR', 'UBER', 'PIER', 'BKIR', 'CHIR', 'HAIR']
        return False

    @classmethod
    def is_likely_preferred(cls, symbol: str) -> bool:
        """Check if symbol is likely preferred stock."""
        if symbol in cls.STOCK_EXCEPTIONS:
            return False

        for pattern in cls.PREFERRED_PATTERNS:
            if re.match(pattern, symbol, re.IGNORECASE):
                # Exclude common legitimate stocks with P
                return not any(common in symbol.upper() for common in ['APP', 'EPP', 'IPP', 'OPP', 'UPP'])
        return False

    @classmethod
    def is_likely_unit(cls, symbol: str) -> bool:
        """Check if symbol is likely a unit (SPAC)."""
        if symbol in cls.STOCK_EXCEPTIONS:
            return False

        for pattern in cls.UNIT_PATTERNS:
            if re.match(pattern, symbol, re.IGNORECASE):
                # Exclude common legitimate stocks ending in U
                return symbol.upper() not in ['AU', 'BU', 'CU', 'FU', 'MU', 'NU']
        return False

    @classmethod
    def has_complex_symbol(cls, symbol: str) -> bool:
        """Check if symbol has complex notation (dots, dashes) that suggests derivative instruments."""
        if symbol in cls.STOCK_EXCEPTIONS:
            return False
        return '.' in symbol or '-' in symbol

    @classmethod
    def is_too_long(cls, symbol: str, max_length: int = 5) -> bool:
        """Check if symbol is unusually long (often indicates derivative/complex instrument)."""
        return len(symbol) > max_length

    @classmethod
    def has_recent_ipo(cls, ipo_date: datetime | None, days_threshold: int = 90) -> bool:
        """Check if IPO is very recent (may not have enough fundamentals yet)."""
        if not ipo_date:
            return False

        cutoff_date = datetime.now() - timedelta(days=days_threshold)
        return ipo_date > cutoff_date

    @classmethod
    def is_fundamentals_eligible(cls, symbol_data: dict[str, Any],
                               enable_ipo_filter: bool = False,
                               ipo_days_threshold: int = 90,
                               enable_failure_blacklist: bool = True,
                               max_consecutive_failures: int = 3) -> dict[str, Any]:
        """
        Comprehensive screening to determine if symbol is eligible for fundamentals extraction.

        Args:
            symbol_data: Dictionary containing symbol metadata
            enable_ipo_filter: Filter out very recent IPOs
            ipo_days_threshold: Days after IPO before attempting fundamentals
            enable_failure_blacklist: Filter out symbols with too many failures
            max_consecutive_failures: Max failures before blacklisting

        Returns:
            Dict with eligibility decision and reasons
        """
        symbol = symbol_data.get('symbol', '')
        asset_type = symbol_data.get('asset_type', '')
        status = symbol_data.get('status', '').lower()
        delisting_date = symbol_data.get('delisting_date')
        ipo_date = symbol_data.get('ipo_date')
        consecutive_failures = symbol_data.get('consecutive_failures', 0)
        symbol_data.get('exchange', '')

        exclusion_reasons = []

        # 1. Basic asset type and status checks
        if asset_type != 'Stock':
            exclusion_reasons.append(f"Non-stock asset type: {asset_type}")

        if status != 'active':
            exclusion_reasons.append(f"Non-active status: {status}")

        if delisting_date:
            exclusion_reasons.append(f"Delisted on: {delisting_date}")

        # 2. Derivative instrument checks
        if cls.is_likely_warrant(symbol):
            exclusion_reasons.append("Likely warrant")

        if cls.is_likely_rights(symbol):
            exclusion_reasons.append("Likely rights")

        if cls.is_likely_preferred(symbol):
            exclusion_reasons.append("Likely preferred stock")

        if cls.is_likely_unit(symbol):
            exclusion_reasons.append("Likely unit/SPAC")

        if cls.has_complex_symbol(symbol):
            exclusion_reasons.append("Complex symbol notation (contains dots/dashes)")

        if cls.is_too_long(symbol):
            exclusion_reasons.append(f"Unusually long symbol ({len(symbol)} chars)")

        # 3. Recent IPO filter (optional)
        if enable_ipo_filter and ipo_date and cls.has_recent_ipo(ipo_date, ipo_days_threshold):
            exclusion_reasons.append(f"Recent IPO ({ipo_date}, less than {ipo_days_threshold} days)")

        # 4. Failure blacklist (optional but recommended)
        if enable_failure_blacklist and consecutive_failures >= max_consecutive_failures:
            exclusion_reasons.append(f"Too many consecutive failures ({consecutive_failures})")

        # 5. Single character symbols (often problematic)
        if len(symbol) == 1:
            exclusion_reasons.append("Single character symbol")

        # Determine eligibility
        is_eligible = len(exclusion_reasons) == 0

        return {
            'symbol': symbol,
            'is_eligible': is_eligible,
            'exclusion_reasons': exclusion_reasons,
            'screening_summary': {
                'is_stock': asset_type == 'Stock',
                'is_active': status == 'active',
                'is_listed': delisting_date is None,
                'is_simple_symbol': not cls.has_complex_symbol(symbol),
                'is_reasonable_length': not cls.is_too_long(symbol),
                'not_derivative': not any([
                    cls.is_likely_warrant(symbol),
                    cls.is_likely_rights(symbol),
                    cls.is_likely_preferred(symbol),
                    cls.is_likely_unit(symbol)
                ]),
                'failure_count': consecutive_failures
            }
        }

    @classmethod
    def filter_symbols_for_fundamentals(cls, symbols: list[dict[str, Any]],
                                      **screening_options) -> dict[str, Any]:
        """
        Filter a list of symbols for fundamentals extraction.

        Args:
            symbols: List of symbol dictionaries
            **screening_options: Options for screening (passed to is_fundamentals_eligible)

        Returns:
            Dictionary with eligible symbols, excluded symbols, and statistics
        """
        eligible_symbols = []
        excluded_symbols = []
        exclusion_stats = {}

        for symbol_data in symbols:
            result = cls.is_fundamentals_eligible(symbol_data, **screening_options)

            if result['is_eligible']:
                eligible_symbols.append(symbol_data)
            else:
                excluded_symbols.append({
                    **symbol_data,
                    'exclusion_reasons': result['exclusion_reasons']
                })

                # Track exclusion statistics
                for reason in result['exclusion_reasons']:
                    exclusion_stats[reason] = exclusion_stats.get(reason, 0) + 1

        return {
            'eligible_symbols': eligible_symbols,
            'excluded_symbols': excluded_symbols,
            'statistics': {
                'total_input': len(symbols),
                'eligible_count': len(eligible_symbols),
                'excluded_count': len(excluded_symbols),
                'eligibility_rate': len(eligible_symbols) / len(symbols) * 100 if symbols else 0,
                'exclusion_reasons': exclusion_stats
            }
        }


def test_symbol_screener():
    """Test the symbol screener with known examples."""
    test_symbols = [
        {'symbol': 'AAPL', 'asset_type': 'Stock', 'status': 'Active', 'delisting_date': None, 'ipo_date': None, 'consecutive_failures': 0},
        {'symbol': 'METBV', 'asset_type': 'Stock', 'status': 'Active', 'delisting_date': None, 'ipo_date': None, 'consecutive_failures': 3},
        {'symbol': 'GOOGL', 'asset_type': 'Stock', 'status': 'Active', 'delisting_date': None, 'ipo_date': None, 'consecutive_failures': 0},
        {'symbol': 'SPY-W', 'asset_type': 'Stock', 'status': 'Active', 'delisting_date': None, 'ipo_date': None, 'consecutive_failures': 0},
        {'symbol': 'BRK.A', 'asset_type': 'Stock', 'status': 'Active', 'delisting_date': None, 'ipo_date': None, 'consecutive_failures': 0},
        {'symbol': 'TSLAW', 'asset_type': 'Stock', 'status': 'Active', 'delisting_date': None, 'ipo_date': None, 'consecutive_failures': 0},
    ]

    screener = SymbolScreener()
    results = screener.filter_symbols_for_fundamentals(test_symbols)


    for _symbol in results['eligible_symbols']:
        pass

    for _symbol in results['excluded_symbols']:
        pass

    for _reason, _count in results['statistics']['exclusion_reasons'].items():
        pass


if __name__ == "__main__":
    test_symbol_screener()

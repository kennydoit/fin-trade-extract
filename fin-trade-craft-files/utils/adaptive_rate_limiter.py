"""
Adaptive Rate Limiter for Alpha Vantage API calls.
Automatically optimizes delays based on processing overhead and API response patterns.
"""

import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class ExtractorType(Enum):
    """Different extractor types with different processing characteristics."""
    TIME_SERIES = "time_series"
    EARNINGS_CALLS = "earnings_calls"
    INSIDER_TRANSACTIONS = "insider_transactions"
    FUNDAMENTALS = "fundamentals"
    ECONOMIC_INDICATORS = "economic_indicators"
    COMMODITIES = "commodities"


@dataclass
class RateLimitConfig:
    """Configuration for different extractor types."""
    base_delay: float
    min_delay: float
    max_delay: float
    adjustment_factor: float


class AdaptiveRateLimiter:
    """
    Smart rate limiter that adapts to processing overhead and API responses.

    Features:
    - Automatic delay optimization based on processing time
    - Rate limit detection and recovery
    - Per-extractor type optimization
    - Performance monitoring and reporting
    """

    # Optimized configurations for different extractor types
    CONFIGURATIONS = {
        ExtractorType.TIME_SERIES: RateLimitConfig(
            base_delay=0.4,      # Heavy processing, can be aggressive
            min_delay=0.2,       # Minimum safe delay
            max_delay=2.0,       # Maximum delay for recovery
            adjustment_factor=0.1
        ),
        ExtractorType.EARNINGS_CALLS: RateLimitConfig(
            base_delay=0.3,      # Very heavy processing, most aggressive
            min_delay=0.2,
            max_delay=2.0,
            adjustment_factor=0.1
        ),
        ExtractorType.INSIDER_TRANSACTIONS: RateLimitConfig(
            base_delay=0.5,      # Medium processing
            min_delay=0.3,
            max_delay=2.0,
            adjustment_factor=0.1
        ),
        ExtractorType.FUNDAMENTALS: RateLimitConfig(
            base_delay=0.6,      # Light processing
            min_delay=0.4,
            max_delay=2.0,
            adjustment_factor=0.1
        ),
        ExtractorType.ECONOMIC_INDICATORS: RateLimitConfig(
            base_delay=0.7,      # Minimal processing
            min_delay=0.5,
            max_delay=2.0,
            adjustment_factor=0.1
        ),
        ExtractorType.COMMODITIES: RateLimitConfig(
            base_delay=0.6,      # Light processing
            min_delay=0.4,
            max_delay=2.0,
            adjustment_factor=0.1
        )
    }

    def __init__(self, extractor_type: ExtractorType, verbose: bool = True):
        """
        Initialize adaptive rate limiter.

        Args:
            extractor_type: Type of extractor for optimization
            verbose: Whether to print optimization messages
        """
        self.extractor_type = extractor_type
        self.config = self.CONFIGURATIONS[extractor_type]
        self.verbose = verbose

        # Current state
        self.current_delay = self.config.base_delay
        self.last_api_call_time = None
        self.last_processing_start = None

        # Performance tracking
        self.total_calls = 0
        self.rate_limited_count = 0
        self.successful_calls = 0
        self.total_processing_time = 0.0
        self.total_api_time = 0.0
        self.adjustment_history = []

        # Rate limit recovery
        self.consecutive_rate_limits = 0
        self.last_rate_limit_time = None

        if self.verbose:
            pass

    def start_processing(self) -> None:
        """Mark the start of symbol processing (before API call)."""
        self.last_processing_start = time.time()

    def pre_api_call(self) -> None:
        """Call this right before making an API call."""
        if self.last_api_call_time is not None:
            # Wait for the current delay
            elapsed = time.time() - self.last_api_call_time
            remaining = self.current_delay - elapsed

            if remaining > 0:
                if self.verbose and remaining > 0.1:
                    pass
                time.sleep(remaining)

        # Record API call start time
        self.api_call_start = time.time()
        self.last_api_call_time = time.time()
        self.total_calls += 1

    def post_api_call(self, status: str, processing_time: float | None = None) -> None:
        """
        Call this after API call and processing is complete.

        Args:
            status: API response status ('success', 'rate_limited', 'error', 'empty')
            processing_time: Total processing time (optional, will calculate if not provided)
        """
        current_time = time.time()

        # Calculate times
        api_time = current_time - self.api_call_start
        self.total_api_time += api_time

        if processing_time is None and self.last_processing_start:
            processing_time = current_time - self.last_processing_start

        if processing_time:
            self.total_processing_time += processing_time

        # Handle different response types
        if status == "rate_limited":
            self._handle_rate_limit()
        elif status in ["success", "empty", "no_changes"]:
            self._handle_success(api_time, processing_time)
        elif status == "error":
            self._handle_error()

        # Reset processing start
        self.last_processing_start = None

    def _handle_rate_limit(self) -> None:
        """Handle rate-limited response."""
        self.rate_limited_count += 1
        self.consecutive_rate_limits += 1
        self.last_rate_limit_time = time.time()

        # Increase delay more aggressively for consecutive rate limits
        increase_factor = min(1.5 ** self.consecutive_rate_limits, 3.0)
        old_delay = self.current_delay
        self.current_delay = min(
            self.current_delay * increase_factor,
            self.config.max_delay
        )

        self.adjustment_history.append({
            'timestamp': datetime.now(),
            'reason': 'rate_limit',
            'old_delay': old_delay,
            'new_delay': self.current_delay,
            'consecutive_limits': self.consecutive_rate_limits
        })

        if self.verbose:
            pass

    def _handle_success(self, api_time: float, processing_time: float | None) -> None:
        """Handle successful response."""
        self.successful_calls += 1
        self.consecutive_rate_limits = 0

        # Only optimize if we have processing time data
        if processing_time is None:
            return

        # Calculate optimal delay based on processing overhead
        # If processing takes much longer than API call, we can be more aggressive
        processing_to_api_ratio = processing_time / max(api_time, 0.1)

        # Adaptive logic:
        # - High processing overhead (ratio > 3): Can reduce delay significantly
        # - Medium processing overhead (ratio 1.5-3): Moderate reduction
        # - Low processing overhead (ratio < 1.5): Conservative approach

        if processing_to_api_ratio > 3.0:
            # Heavy processing, can be aggressive
            target_delay = max(self.config.min_delay, self.config.base_delay * 0.7)
        elif processing_to_api_ratio > 1.5:
            # Medium processing
            target_delay = max(self.config.min_delay, self.config.base_delay * 0.85)
        else:
            # Light processing, be conservative
            target_delay = self.config.base_delay

        # Gradual adjustment toward target
        if self.current_delay > target_delay:
            old_delay = self.current_delay
            self.current_delay = max(
                self.current_delay - self.config.adjustment_factor,
                target_delay
            )

            # Log significant adjustments
            if abs(old_delay - self.current_delay) > 0.05:
                self.adjustment_history.append({
                    'timestamp': datetime.now(),
                    'reason': 'optimization',
                    'old_delay': old_delay,
                    'new_delay': self.current_delay,
                    'processing_ratio': processing_to_api_ratio
                })

                if self.verbose and len(self.adjustment_history) % 10 == 0:
                    pass

    def _handle_error(self) -> None:
        """Handle error response (no delay adjustment)."""
        # Reset consecutive rate limits on errors (they're different from rate limits)
        self.consecutive_rate_limits = min(self.consecutive_rate_limits, 0)

    def get_performance_summary(self) -> dict:
        """Get performance summary and statistics."""
        if self.total_calls == 0:
            return {"message": "No API calls made yet"}

        avg_processing_time = (
            self.total_processing_time / self.successful_calls
            if self.successful_calls > 0 else 0
        )
        avg_api_time = self.total_api_time / self.total_calls
        rate_limit_percentage = (self.rate_limited_count / self.total_calls) * 100
        success_rate = (self.successful_calls / self.total_calls) * 100

        # Calculate throughput improvement
        base_throughput = 3600 / (self.config.base_delay + avg_processing_time)
        current_throughput = 3600 / (self.current_delay + avg_processing_time)
        throughput_improvement = ((current_throughput - base_throughput) / base_throughput) * 100

        return {
            "extractor_type": self.extractor_type.value,
            "total_calls": self.total_calls,
            "successful_calls": self.successful_calls,
            "rate_limited_count": self.rate_limited_count,
            "success_rate": f"{success_rate:.1f}%",
            "rate_limit_percentage": f"{rate_limit_percentage:.1f}%",
            "current_delay": f"{self.current_delay:.2f}s",
            "base_delay": f"{self.config.base_delay:.2f}s",
            "avg_api_time": f"{avg_api_time:.2f}s",
            "avg_processing_time": f"{avg_processing_time:.2f}s",
            "throughput_improvement": f"{throughput_improvement:+.1f}%",
            "estimated_symbols_per_hour": int(current_throughput),
            "total_adjustments": len(self.adjustment_history)
        }

    def print_performance_summary(self) -> None:
        """Print a formatted performance summary."""
        summary = self.get_performance_summary()

        if "message" in summary:
            return


        if len(self.adjustment_history) > 0:
            pass


# Convenience function for easy integration
def create_rate_limiter(extractor_name: str, verbose: bool = True) -> AdaptiveRateLimiter:
    """
    Create rate limiter based on extractor name.

    Args:
        extractor_name: Name of the extractor file or type
        verbose: Whether to show optimization messages

    Returns:
        Configured AdaptiveRateLimiter
    """
    # Map extractor names to types
    name_mapping = {
        "time_series": ExtractorType.TIME_SERIES,
        "earnings_call": ExtractorType.EARNINGS_CALLS,
        "insider": ExtractorType.INSIDER_TRANSACTIONS,
        "balance_sheet": ExtractorType.FUNDAMENTALS,
        "income_statement": ExtractorType.FUNDAMENTALS,
        "cash_flow": ExtractorType.FUNDAMENTALS,
        "economic": ExtractorType.ECONOMIC_INDICATORS,
        "commodities": ExtractorType.COMMODITIES,
    }

    # Find matching type
    extractor_type = ExtractorType.FUNDAMENTALS  # Default
    for key, etype in name_mapping.items():
        if key in extractor_name.lower():
            extractor_type = etype
            break

    return AdaptiveRateLimiter(extractor_type, verbose)

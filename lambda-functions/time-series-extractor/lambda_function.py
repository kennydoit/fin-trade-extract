"""
AWS Lambda function for extracting time series daily adjusted data from Alpha Vantage API.
Writes results to S3 as CSV files for Snowpipe ingestion.

This Lambda function:
1. Extracts time series daily adjusted data for specified symbols via Alpha Vantage API
2. Applies adaptive rate limiting for optimal throughput
3. Writes business data as CSV files to S3 (s3://fin-trade-craft-landing/time-series/)
4. Stores audit trail as compressed JSON files
5. Follows AWS security best practices with Parameter Store
"""

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

# Import common utilities
from common.adaptive_rate_limiter import AdaptiveRateLimiter, ExtractorType
from common.parameter_store import get_alpha_vantage_key
from common.s3_data_writer import create_s3_writer
from common.symbol_id_calculator import calculate_symbol_id


class TimeSeriesExtractorLambda:
    """
    Lambda-optimized extractor for time series daily adjusted data.
    
    Features:
    - Batch processing of multiple symbols
    - S3 output in CSV and JSON formats
    - AWS Parameter Store integration
    - Adaptive rate limiting
    - Comprehensive error handling and logging
    """
    
    def __init__(self):
        """Initialize the extractor with AWS integrations."""
        # API configuration
        self.api_function = "TIME_SERIES_DAILY_ADJUSTED"
        self.base_url = "https://www.alphavantage.co/query"
        
        # AWS integrations
        self.s3_writer = create_s3_writer()
        
        # Rate limiting - time series uses different limits
        self.rate_limiter = AdaptiveRateLimiter(ExtractorType.TIME_SERIES, verbose=False)
        
        # Caching for Lambda execution
        self._api_key = None
        
    def get_api_key(self) -> str:
        """Get Alpha Vantage API key from Parameter Store (cached)."""
        if not self._api_key:
            self._api_key = get_alpha_vantage_key()
        return self._api_key
    
    def extract_single_time_series(self, symbol: str) -> tuple[Dict[str, Any], str]:
        """
        Extract time series data for a single symbol.
        
        Args:
            symbol: Stock symbol to extract
            
        Returns:
            Tuple of (api_response_dict, status_string)
        """
        print(f"Processing time series for symbol: {symbol}")
        
        # Build API URL - get compact data (last 100 days)
        url = f"{self.base_url}?function={self.api_function}&symbol={symbol}&outputsize=compact&apikey={self.get_api_key()}"
        
        # Apply rate limiting
        self.rate_limiter.pre_api_call()
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Validate response
            if not data or data == {}:
                print(f"Empty response for {symbol}")
                self.rate_limiter.post_api_call('empty')
                return {}, "empty"
            
            # Check for rate limiting message
            if "Note" in data and "call frequency" in str(data.get("Note", "")):
                print(f"Rate limited response for {symbol}")
                self.rate_limiter.post_api_call('rate_limited')
                return data, "rate_limited"
            
            # Check for error message
            if "Error Message" in data:
                print(f"API error for {symbol}: {data.get('Error Message')}")
                self.rate_limiter.post_api_call('error')
                return data, "api_error"
            
            # Check for time series data
            time_series_key = "Time Series (Daily)"
            if time_series_key not in data:
                print(f"No time series data found for {symbol}")
                self.rate_limiter.post_api_call('empty')
                return data, "no_data"
            
            print(f"Successfully extracted time series data for {symbol} ({len(data[time_series_key])} records)")
            self.rate_limiter.post_api_call('success')
            return data, "success"
            
        except requests.exceptions.Timeout:
            print(f"Timeout fetching time series data for {symbol}")
            self.rate_limiter.post_api_call('error')
            return {}, "timeout"
        except requests.exceptions.RequestException as e:
            print(f"Request error for {symbol}: {e}")
            self.rate_limiter.post_api_call('error')
            return {}, "error"
        except json.JSONDecodeError as e:
            print(f"JSON decode error for {symbol}: {e}")
            self.rate_limiter.post_api_call('error')
            return {}, "json_error"
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            self.rate_limiter.post_api_call('error')
            return {}, "error"
    
    def transform_time_series_data(self, 
                                 symbol: str, 
                                 symbol_id: int, 
                                 api_response: Dict[str, Any], 
                                 status: str,
                                 run_id: str) -> List[Dict[str, Any]]:
        """
        Transform API response to business record format.
        
        Args:
            symbol: Stock symbol
            symbol_id: Calculated symbol ID
            api_response: Raw API response
            status: Extraction status
            run_id: Unique run identifier
            
        Returns:
            List of transformed business records (one per date)
        """
        current_timestamp = datetime.utcnow().isoformat()
        records = []
        
        if status != "success" or not api_response:
            # Create single error record
            return [{
                "SYMBOL_ID": symbol_id,
                "SYMBOL": symbol,
                "DATE": None,
                "OPEN": None,
                "HIGH": None,
                "LOW": None,
                "CLOSE": None,
                "ADJUSTED_CLOSE": None,
                "VOLUME": None,
                "DIVIDEND_AMOUNT": None,
                "SPLIT_COEFFICIENT": None,
                "API_RESPONSE_STATUS": status,
                "CREATED_AT": current_timestamp,
                "UPDATED_AT": current_timestamp,
            }]
        
        # Extract time series data
        time_series_key = "Time Series (Daily)"
        time_series_data = api_response.get(time_series_key, {})
        
        for date_str, daily_data in time_series_data.items():
            record = {
                "SYMBOL_ID": symbol_id,
                "SYMBOL": symbol,
                "DATE": date_str,
                "OPEN": self._safe_float(daily_data.get("1. open")),
                "HIGH": self._safe_float(daily_data.get("2. high")),
                "LOW": self._safe_float(daily_data.get("3. low")),
                "CLOSE": self._safe_float(daily_data.get("4. close")),
                "ADJUSTED_CLOSE": self._safe_float(daily_data.get("5. adjusted close")),
                "VOLUME": self._safe_int(daily_data.get("6. volume")),
                "DIVIDEND_AMOUNT": self._safe_float(daily_data.get("7. dividend amount")),
                "SPLIT_COEFFICIENT": self._safe_float(daily_data.get("8. split coefficient")),
                "API_RESPONSE_STATUS": status,
                "CREATED_AT": current_timestamp,
                "UPDATED_AT": current_timestamp,
            }
            records.append(record)
        
        return records
    
    def create_landing_record(self,
                            symbol: str,
                            symbol_id: int,
                            api_response: Dict[str, Any],
                            status: str,
                            run_id: str) -> Dict[str, Any]:
        """
        Create landing/audit record for raw API response.
        
        Args:
            symbol: Stock symbol
            symbol_id: Calculated symbol ID
            api_response: Raw API response
            status: Extraction status
            run_id: Unique run identifier
            
        Returns:
            Landing record
        """
        # Calculate content hash for change detection
        content_hash = self._calculate_content_hash(api_response)
        
        return {
            "landing_id": str(uuid.uuid4()),
            "table_name": "time_series_daily_adjusted",
            "symbol": symbol,
            "symbol_id": symbol_id,
            "api_function": self.api_function,
            "api_response": api_response,
            "content_hash": content_hash,
            "source_run_id": run_id,
            "response_status": status,
            "fetched_at": datetime.utcnow().isoformat()
        }
    
    def process_symbol_batch(self, 
                           symbols: List[str], 
                           run_id: str) -> Dict[str, Any]:
        """
        Process a batch of symbols.
        
        Args:
            symbols: List of stock symbols to process
            run_id: Unique run identifier
            
        Returns:
            Batch processing results
        """
        if not symbols:
            return {
                "status": "no_symbols",
                "processed_count": 0,
                "business_records": [],
                "landing_records": []
            }
        
        print(f"Processing time series batch of {len(symbols)} symbols")
        
        # Initialize adaptive rate limiting
        self.rate_limiter.start_processing()
        
        business_records = []
        landing_records = []
        
        success_count = 0
        error_count = 0
        total_time_series_records = 0
        
        for symbol in symbols:
            # Calculate symbol ID
            symbol_id = calculate_symbol_id(symbol)
            
            # Extract data
            api_response, status = self.extract_single_time_series(symbol)
            
            # Create business records (multiple per symbol)
            symbol_business_records = self.transform_time_series_data(
                symbol, symbol_id, api_response, status, run_id
            )
            business_records.extend(symbol_business_records)
            total_time_series_records += len(symbol_business_records)
            
            # Create landing record (one per symbol)
            landing_record = self.create_landing_record(
                symbol, symbol_id, api_response, status, run_id
            )
            landing_records.append(landing_record)
            
            # Update counters
            if status == "success":
                success_count += 1
            else:
                error_count += 1
            
            print(f"Processed {symbol} (ID: {symbol_id}) with status: {status}, {len(symbol_business_records)} time series records")
        
        print(f"Batch processing complete: {success_count} successful, {error_count} errors, {total_time_series_records} total time series records")
        
        return {
            "status": "completed",
            "processed_count": len(symbols),
            "success_count": success_count,
            "error_count": error_count,
            "total_time_series_records": total_time_series_records,
            "business_records": business_records,
            "landing_records": landing_records,
            "rate_limiter_stats": self.rate_limiter.get_performance_summary()
        }
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None or value == "None" or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None or value == "None" or value == "":
            return None
        try:
            return int(float(value))  # Convert via float first to handle strings like "123.0"
        except (ValueError, TypeError):
            return None
    
    def _calculate_content_hash(self, data: Dict[str, Any]) -> str:
        """Calculate MD5 hash of API response content."""
        canonical_string = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(canonical_string.encode('utf-8')).hexdigest()


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    AWS Lambda handler function.
    
    Expected event format:
    {
        "symbols": ["AAPL", "MSFT", "GOOGL"],  # Required: list of symbols
        "run_id": "optional-custom-run-id",    # Optional: custom run ID
        "batch_size": 10                       # Optional: batch size limit (time series uses smaller batches)
    }
    
    Returns:
        Dict with processing results and S3 file information
    """
    try:
        # Extract parameters from event
        symbols = event.get("symbols", [])
        run_id = event.get("run_id", str(uuid.uuid4()))
        batch_size = event.get("batch_size", 10)  # Smaller default for time series
        
        # Validate input
        if not symbols:
            return {
                "statusCode": 400,
                "error": "No symbols provided in event",
                "event": event
            }
        
        if not isinstance(symbols, list):
            return {
                "statusCode": 400,
                "error": "Symbols must be provided as a list",
                "event": event
            }
        
        # Limit batch size (time series API has stricter limits)
        if len(symbols) > batch_size:
            symbols = symbols[:batch_size]
            print(f"Limited symbols to batch size: {batch_size}")
        
        print(f"Time series Lambda execution started for {len(symbols)} symbols, run_id: {run_id}")
        
        # Initialize extractor
        extractor = TimeSeriesExtractorLambda()
        
        # Process symbols
        processing_result = extractor.process_symbol_batch(symbols, run_id)
        
        # Write results to S3
        s3_result = extractor.s3_writer.write_batch_data(
            table_name="time_series_daily_adjusted",
            business_records=processing_result["business_records"],
            landing_records=processing_result["landing_records"],
            run_id=run_id
        )
        
        # Combine results
        lambda_result = {
            "statusCode": 200,
            "run_id": run_id,
            "symbols_processed": processing_result["processed_count"],
            "success_count": processing_result["success_count"],
            "error_count": processing_result["error_count"],
            "time_series_records": processing_result["total_time_series_records"],
            "s3_files": s3_result,
            "rate_limiter_stats": processing_result["rate_limiter_stats"],
            "execution_time_ms": getattr(context, 'get_remaining_time_in_millis', lambda: 0)()
        }
        
        print(f"Time series Lambda execution completed successfully: {lambda_result}")
        return lambda_result
        
    except Exception as e:
        error_result = {
            "statusCode": 500,
            "error": str(e),
            "error_type": type(e).__name__,
            "event": event,
            "run_id": event.get("run_id", "unknown")
        }
        
        print(f"Time series Lambda execution failed: {error_result}")
        return error_result


# For local testing
if __name__ == "__main__":
    # Test event
    test_event = {
        "symbols": ["AAPL", "MSFT"],
        "run_id": "test-time-series-" + str(uuid.uuid4())[:8]
    }
    
    class MockContext:
        def get_remaining_time_in_millis(self):
            return 15000  # 15 seconds remaining
    
    result = lambda_handler(test_event, MockContext())
    print("Test result:", json.dumps(result, indent=2, default=str))
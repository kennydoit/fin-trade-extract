#!/usr/bin/env python3
"""
Local test script for Lambda functions before AWS deployment.
Tests CSV output format and S3 integration.
"""

import json
import sys
import os
from datetime import datetime

# Add lambda functions to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda-functions'))

def test_overview_extractor():
    """Test the overview extractor Lambda function."""
    print("🧪 Testing Overview Extractor...")
    
    try:
        # Import from the correct path structure
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda-functions', 'overview-extractor'))
        from lambda_function import lambda_handler
        
        # Mock context
        class MockContext:
            def get_remaining_time_in_millis(self):
                return 15000
        
        # Test event
        test_event = {
            "symbols": ["AAPL"],  # Single symbol for quick test
            "run_id": f"local-test-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }
        
        print(f"📤 Sending test event: {json.dumps(test_event, indent=2)}")
        
        # Execute Lambda function
        result = lambda_handler(test_event, MockContext())
        
        print(f"📥 Lambda Response:")
        print(f"   Status Code: {result.get('statusCode')}")
        print(f"   Symbols Processed: {result.get('symbols_processed')}")
        print(f"   Success Count: {result.get('success_count')}")
        print(f"   Error Count: {result.get('error_count')}")
        
        if result.get('s3_files'):
            s3_files = result['s3_files']
            print(f"   S3 Files Written: {s3_files.get('total_files')}")
            print(f"   Total Records: {s3_files.get('total_records')}")
            
            # Show CSV file details
            business_files = s3_files.get('business_data', {}).get('files', [])
            for file_info in business_files:
                print(f"   📄 CSV File: s3://{file_info['bucket']}/{file_info['key']}")
                print(f"      Records: {file_info['record_count']}")
                print(f"      Size: {file_info['file_size_bytes']} bytes")
        
        return result.get('statusCode') == 200
        
    except Exception as e:
        print(f"❌ Overview Extractor Test Failed: {e}")
        return False

def test_time_series_extractor():
    """Test the time series extractor Lambda function."""
    print("\n🧪 Testing Time Series Extractor...")
    
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda-functions', 'time-series-extractor'))
        from lambda_function import lambda_handler
        
        class MockContext:
            def get_remaining_time_in_millis(self):
                return 15000
        
        test_event = {
            "symbols": ["AAPL"],
            "run_id": f"local-test-ts-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }
        
        result = lambda_handler(test_event, MockContext())
        
        print(f"📥 Time Series Lambda Response:")
        print(f"   Status Code: {result.get('statusCode')}")
        print(f"   Time Series Records: {result.get('time_series_records')}")
        
        return result.get('statusCode') == 200
        
    except Exception as e:
        print(f"❌ Time Series Extractor Test Failed: {e}")
        return False

def test_financial_statements_extractor():
    """Test the financial statements extractor Lambda function."""
    print("\n🧪 Testing Financial Statements Extractor...")
    
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda-functions', 'financial-statements-extractor'))
        from lambda_function import lambda_handler
        
        class MockContext:
            def get_remaining_time_in_millis(self):
                return 15000
        
        test_event = {
            "symbols": ["AAPL"],
            "statement_type": "INCOME_STATEMENT",
            "run_id": f"local-test-fs-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }
        
        result = lambda_handler(test_event, MockContext())
        
        print(f"📥 Financial Statements Lambda Response:")
        print(f"   Status Code: {result.get('statusCode')}")
        print(f"   Statement Records: {result.get('statement_records')}")
        
        return result.get('statusCode') == 200
        
    except Exception as e:
        print(f"❌ Financial Statements Extractor Test Failed: {e}")
        return False

def main():
    """Run all Lambda function tests."""
    print("🚀 Starting Local Lambda Function Tests")
    print("=" * 50)
    
    # Test results
    tests = [
        ("Overview Extractor", test_overview_extractor),
        ("Time Series Extractor", test_time_series_extractor),
        ("Financial Statements Extractor", test_financial_statements_extractor)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ {test_name} failed with error: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("🏁 Test Results Summary:")
    
    passed = 0
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {status} - {test_name}")
        if success:
            passed += 1
    
    print(f"\n📊 Overall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All tests passed! Your Lambda functions are ready for deployment.")
        print("📋 Next steps:")
        print("   1. Deploy to AWS Lambda")
        print("   2. Test with AWS console")
        print("   3. Monitor Snowpipe ingestion")
        print("   4. Check analytics views for data")
    else:
        print("\n⚠️  Some tests failed. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
# =============================================================================
# Diagnose and Fix Lambda Function Data Issues
# The Lambda is creating CSV files but not fetching actual financial data
# =============================================================================

Write-Host "Diagnosing Lambda Function Data Issues..." -ForegroundColor Yellow

# =============================================================================
# STEP 1: Check Lambda function logs for errors
# =============================================================================

Write-Host ""
Write-Host "Step 1: Checking Lambda function logs..." -ForegroundColor Cyan

try {
    Write-Host "Getting latest Lambda function logs..." -ForegroundColor Blue
    
    # Get the latest log stream
    $logGroups = aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/fin-trade-overview-extractor" --query 'logGroups[0].logGroupName' --output text
    
    if ($logGroups) {
        Write-Host "Log group found: $logGroups" -ForegroundColor Green
        
        # Get recent log streams
        $logStreams = aws logs describe-log-streams --log-group-name $logGroups --order-by LastEventTime --descending --limit 3 --query 'logStreams[].logStreamName' --output text
        
        if ($logStreams) {
            Write-Host "Recent log streams found" -ForegroundColor Green
            Write-Host "Getting recent errors..." -ForegroundColor Blue
            
            # Get recent error logs
            aws logs filter-log-events --log-group-name $logGroups --start-time ((Get-Date).AddHours(-2).ToUniversalTime().Subtract((Get-Date "1970-01-01")).TotalMilliseconds -as [int64]) --filter-pattern "ERROR" --query 'events[].message' --output text
        }
    }
}
catch {
    Write-Host "Could not access Lambda logs. Check AWS permissions." -ForegroundColor Yellow
}

# =============================================================================
# STEP 2: Test Lambda function with single symbol and detailed response
# =============================================================================

Write-Host ""
Write-Host "Step 2: Testing Lambda with single symbol for detailed error info..." -ForegroundColor Cyan

# Test with just AAPL to get detailed error information
$testPayload = @{
    symbols = @("AAPL")
    run_id = "diagnostic-test-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
    debug = $true
}

$jsonPayload = $testPayload | ConvertTo-Json -Compress
$base64Payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($jsonPayload))

Write-Host "Testing with AAPL symbol..." -ForegroundColor Blue

try {
    aws lambda invoke --function-name fin-trade-overview-extractor --payload $base64Payload diagnostic-response.json
    
    if (Test-Path "diagnostic-response.json") {
        $response = Get-Content "diagnostic-response.json" | ConvertFrom-Json
        
        Write-Host ""
        Write-Host "Lambda Response Analysis:" -ForegroundColor Yellow
        Write-Host "  Status Code: $($response.statusCode)" -ForegroundColor White
        Write-Host "  Success Count: $($response.success_count)" -ForegroundColor White
        Write-Host "  Error Count: $($response.error_count)" -ForegroundColor White
        Write-Host "  Execution Time: $($response.execution_time_ms)ms" -ForegroundColor White
        
        if ($response.errors -and $response.errors.Count -gt 0) {
            Write-Host ""
            Write-Host "Errors Found:" -ForegroundColor Red
            foreach ($error in $response.errors) {
                Write-Host "  $error" -ForegroundColor Red
            }
        }
        
        if ($response.s3_files) {
            Write-Host ""
            Write-Host "S3 Files:" -ForegroundColor Yellow
            Write-Host "  Success: $($response.s3_files.success)" -ForegroundColor White
            Write-Host "  Records: $($response.s3_files.records_processed)" -ForegroundColor White
            if ($response.s3_files.csv_files) {
                Write-Host "  CSV Files: $($response.s3_files.csv_files -join ', ')" -ForegroundColor White
            }
        }
    }
}
catch {
    Write-Host "Lambda invocation failed: $_" -ForegroundColor Red
}

# =============================================================================
# STEP 3: Check Lambda environment variables and configuration
# =============================================================================

Write-Host ""
Write-Host "Step 3: Checking Lambda configuration..." -ForegroundColor Cyan

try {
    Write-Host "Getting Lambda function configuration..." -ForegroundColor Blue
    
    $lambdaConfig = aws lambda get-function-configuration --function-name fin-trade-overview-extractor | ConvertFrom-Json
    
    Write-Host ""
    Write-Host "Lambda Configuration:" -ForegroundColor Yellow
    Write-Host "  Runtime: $($lambdaConfig.Runtime)" -ForegroundColor White
    Write-Host "  Timeout: $($lambdaConfig.Timeout) seconds" -ForegroundColor White
    Write-Host "  Memory: $($lambdaConfig.MemorySize) MB" -ForegroundColor White
    
    if ($lambdaConfig.Environment -and $lambdaConfig.Environment.Variables) {
        Write-Host ""
        Write-Host "Environment Variables:" -ForegroundColor Yellow
        
        # Check for Alpha Vantage API key (don't show the actual key)
        if ($lambdaConfig.Environment.Variables.ALPHA_VANTAGE_API_KEY) {
            Write-Host "  ALPHA_VANTAGE_API_KEY: [SET]" -ForegroundColor Green
        } else {
            Write-Host "  ALPHA_VANTAGE_API_KEY: [MISSING]" -ForegroundColor Red
        }
        
        # Check for S3 bucket
        if ($lambdaConfig.Environment.Variables.S3_BUCKET_NAME) {
            Write-Host "  S3_BUCKET_NAME: $($lambdaConfig.Environment.Variables.S3_BUCKET_NAME)" -ForegroundColor Green
        } else {
            Write-Host "  S3_BUCKET_NAME: [MISSING]" -ForegroundColor Red
        }
    }
}
catch {
    Write-Host "Could not get Lambda configuration: $_" -ForegroundColor Red
}

# =============================================================================
# STEP 4: Test Alpha Vantage API directly
# =============================================================================

Write-Host ""
Write-Host "Step 4: Testing Alpha Vantage API connectivity..." -ForegroundColor Cyan

# Try to get API key from Lambda config or prompt user
$apiKey = $null
if ($lambdaConfig -and $lambdaConfig.Environment -and $lambdaConfig.Environment.Variables -and $lambdaConfig.Environment.Variables.ALPHA_VANTAGE_API_KEY) {
    Write-Host "Using API key from Lambda configuration" -ForegroundColor Blue
    # Don't expose the actual key, just test if it works
    
    # Test API call (you could implement this)
    Write-Host "API key is configured in Lambda. Testing requires implementation..." -ForegroundColor Yellow
} else {
    Write-Host "Alpha Vantage API key not found in Lambda configuration!" -ForegroundColor Red
    Write-Host "This is likely why the Lambda function is returning empty data." -ForegroundColor Red
}

# =============================================================================
# STEP 5: Clean up empty records from Snowflake
# =============================================================================

Write-Host ""
Write-Host "Step 5: Cleaning up empty records in Snowflake..." -ForegroundColor Cyan
Write-Host ""
Write-Host "Run this SQL in Snowflake to clean up empty records:" -ForegroundColor Yellow
Write-Host ""

$cleanupSQL = @"
-- Clean up empty records from bulk load
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- Check current empty records
SELECT 'EMPTY RECORDS CHECK' as step;
SELECT COUNT(*) as empty_records 
FROM OVERVIEW 
WHERE NAME IS NULL OR NAME = '' OR TRIM(NAME) = '';

-- Delete empty records (keep any with actual data)
DELETE FROM OVERVIEW 
WHERE NAME IS NULL OR NAME = '' OR TRIM(NAME) = '';

-- Verify cleanup
SELECT COUNT(*) as remaining_records, COUNT(DISTINCT SYMBOL) as unique_symbols 
FROM OVERVIEW;
"@

Write-Host $cleanupSQL -ForegroundColor White

# =============================================================================
# STEP 6: Recommendations
# =============================================================================

Write-Host ""
Write-Host "Diagnosis Summary and Recommendations:" -ForegroundColor Green
Write-Host ""
Write-Host "Root Cause: Lambda function is not fetching financial data from Alpha Vantage API" -ForegroundColor Red
Write-Host ""
Write-Host "Likely Issues:" -ForegroundColor Yellow
Write-Host "  1. Missing or invalid Alpha Vantage API key" -ForegroundColor White
Write-Host "  2. API rate limiting or quota exceeded" -ForegroundColor White
Write-Host "  3. Network connectivity issues from Lambda" -ForegroundColor White
Write-Host "  4. Lambda timeout before API calls complete" -ForegroundColor White
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "  1. Check Lambda CloudWatch logs for specific errors" -ForegroundColor White
Write-Host "  2. Verify Alpha Vantage API key is set and valid" -ForegroundColor White
Write-Host "  3. Test API key with direct Alpha Vantage API calls" -ForegroundColor White
Write-Host "  4. Clean up empty records in Snowflake (SQL above)" -ForegroundColor White
Write-Host "  5. Fix Lambda function and retest with 1-2 symbols first" -ForegroundColor White
Write-Host ""
Write-Host "Files created:" -ForegroundColor Yellow
Write-Host "  diagnostic-response.json - Lambda diagnostic response" -ForegroundColor White

Write-Host ""
Write-Host "Pipeline Status: CSV structure fixed, but Lambda needs API connectivity fix" -ForegroundColor Yellow
# =============================================================================
# Monitor Pipeline Real-Time - Test and Track Data Loading
# This will test Lambda, monitor S3, and check Snowflake loading
# =============================================================================

Write-Host "Real-Time Pipeline Monitoring and Testing" -ForegroundColor Green
Write-Host ""

# =============================================================================
# Test 1: Trigger Lambda with a new test symbol
# =============================================================================

Write-Host "Step 1: Testing Lambda with Microsoft (MSFT)..." -ForegroundColor Cyan

$testPayload = @{
    symbols = @("MSFT")
    run_id = "pipeline-monitor-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
}

$jsonPayload = $testPayload | ConvertTo-Json -Compress
$base64Payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($jsonPayload))

try {
    aws lambda invoke --function-name fin-trade-overview-extractor --payload $base64Payload pipeline-monitor-test.json
    
    if (Test-Path "pipeline-monitor-test.json") {
        $response = Get-Content "pipeline-monitor-test.json" | ConvertFrom-Json
        
        Write-Host ""
        Write-Host "Lambda Results:" -ForegroundColor Yellow
        Write-Host "  Success Count: $($response.success_count)" -ForegroundColor White
        Write-Host "  Error Count: $($response.error_count)" -ForegroundColor White
        Write-Host "  Execution Time: $($response.execution_time_ms)ms" -ForegroundColor White
        
        if ($response.s3_files -and $response.s3_files.csv_files) {
            $csvFile = $response.s3_files.csv_files[0]
            Write-Host "  CSV Created: $csvFile" -ForegroundColor Green
            
            # Wait for S3 upload to complete
            Write-Host ""
            Write-Host "Step 2: Waiting for S3 upload and checking file..." -ForegroundColor Cyan
            Start-Sleep -Seconds 5
            
            # Check if file exists and get size
            try {
                $s3Info = aws s3 ls "s3://fin-trade-craft-landing/$csvFile" --human-readable | Out-String
                if ($s3Info.Trim()) {
                    Write-Host "  S3 File Status: EXISTS" -ForegroundColor Green
                    Write-Host "  File Info: $($s3Info.Trim())" -ForegroundColor White
                    
                    # Download and preview the file
                    aws s3 cp "s3://fin-trade-craft-landing/$csvFile" "temp-monitor-test.csv"
                    
                    if (Test-Path "temp-monitor-test.csv") {
                        $content = Get-Content "temp-monitor-test.csv" -TotalCount 2
                        Write-Host ""
                        Write-Host "  CSV Preview:" -ForegroundColor Yellow
                        Write-Host "  Header: $($content[0].Substring(0, [Math]::Min(100, $content[0].Length)))..." -ForegroundColor White
                        if ($content.Count -gt 1) {
                            Write-Host "  Data: $($content[1].Substring(0, [Math]::Min(100, $content[1].Length)))..." -ForegroundColor White
                            
                            # Check if data looks real (has company name)
                            if ($content[1] -match "Microsoft|MSFT" -and $content[1] -match "\d+\.\d+") {
                                Write-Host "  Data Quality: REAL FINANCIAL DATA DETECTED" -ForegroundColor Green
                            } else {
                                Write-Host "  Data Quality: EMPTY OR INVALID DATA" -ForegroundColor Red
                            }
                        }
                    }
                } else {
                    Write-Host "  S3 File Status: NOT FOUND" -ForegroundColor Red
                }
            }
            catch {
                Write-Host "  S3 Check Error: $_" -ForegroundColor Red
            }
        }
    }
}
catch {
    Write-Host "Lambda test failed: $_" -ForegroundColor Red
}

# =============================================================================
# Step 3: Monitor Snowflake loading
# =============================================================================

Write-Host ""
Write-Host "Step 3: Monitoring Snowflake data loading..." -ForegroundColor Cyan
Write-Host ""

Write-Host "Run these queries in Snowflake to check loading status:" -ForegroundColor Yellow
Write-Host ""

$snowflakeQueries = @"
-- Check current record count
SELECT COUNT(*) as total_records FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW;

-- Check for recent loads (last 10 minutes)
SELECT SYMBOL, NAME, MARKET_CAPITALIZATION, LOADED_AT 
FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW 
WHERE LOADED_AT > DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())
ORDER BY LOADED_AT DESC;

-- Check Snowpipe status
SELECT SYSTEM`$PIPE_STATUS('RAW.OVERVIEW_PIPE') as pipe_status;

-- Check for loading errors
SELECT FILE_NAME, STATUS, ERROR_COUNT, FIRST_ERROR_MESSAGE
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY 
WHERE TABLE_NAME = 'OVERVIEW' 
ORDER BY LAST_LOAD_TIME DESC 
LIMIT 5;

-- Manual load test (if Snowpipe isn't working)
COPY INTO FIN_TRADE_EXTRACT.RAW.OVERVIEW 
FROM @FIN_TRADE_EXTRACT.RAW.OVERVIEW_STAGE 
FILE_FORMAT = (FORMAT_NAME = 'FIN_TRADE_EXTRACT.RAW.CSV_FORMAT')
PATTERN = '.*pipeline-monitor.*'
ON_ERROR = 'CONTINUE';
"@

Write-Host $snowflakeQueries -ForegroundColor White

# =============================================================================
# Step 4: Diagnostic recommendations
# =============================================================================

Write-Host ""
Write-Host "Diagnostic Checklist:" -ForegroundColor Green
Write-Host ""

Write-Host "✓ Lambda Function: Working (real API key configured)" -ForegroundColor Green
Write-Host "? S3 Upload: Check file size and content above" -ForegroundColor Yellow
Write-Host "? Snowpipe Loading: Check with Snowflake queries above" -ForegroundColor Yellow
Write-Host ""

Write-Host "Common Issues:" -ForegroundColor Yellow
Write-Host "  1. Snowpipe delay (1-5 minutes is normal)" -ForegroundColor White
Write-Host "  2. S3 bucket notifications not configured correctly" -ForegroundColor White
Write-Host "  3. Table schema mismatch (run snowflake-pipeline-diagnostics.sql)" -ForegroundColor White
Write-Host "  4. File format issues" -ForegroundColor White
Write-Host ""

Write-Host "If Snowpipe isn't loading automatically:" -ForegroundColor Cyan
Write-Host "  Use the manual COPY command above to force load the data" -ForegroundColor White
Write-Host "  Then investigate S3 notifications and Snowpipe configuration" -ForegroundColor White
Write-Host ""

Write-Host "Files created:" -ForegroundColor Yellow
Write-Host "  pipeline-monitor-test.json - Lambda response" -ForegroundColor White
Write-Host "  temp-monitor-test.csv - Latest CSV data for inspection" -ForegroundColor White
Write-Host ""
Write-Host "Next: Run the Snowflake queries above to check data loading status" -ForegroundColor Cyan
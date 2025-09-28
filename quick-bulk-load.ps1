# =============================================================================
# Quick Bulk Load - Single Command Approach
# For immediate bulk loading of popular stocks
# =============================================================================

Write-Host "Quick Bulk Load: Loading 20 Popular Stocks" -ForegroundColor Green

# Define top 20 most popular stocks
$top20 = @("AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "JPM", "JNJ", "V", "PG", "UNH", "HD", "MA", "BAC", "PFE", "DIS", "KO", "ADBE", "CRM")

# Create single payload for all symbols
$payload = @{
    symbols = $top20
    run_id = "quick-bulk-load-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
}

$jsonPayload = $payload | ConvertTo-Json -Compress
$base64Payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($jsonPayload))

Write-Host "Symbols to process: $($top20 -join ', ')" -ForegroundColor Cyan
Write-Host ""

try {
    Write-Host "Invoking Lambda function for all 20 symbols..." -ForegroundColor Blue
    aws lambda invoke --function-name fin-trade-overview-extractor --payload $base64Payload quick-bulk-response.json
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Lambda invocation successful!" -ForegroundColor Green
        
        $response = Get-Content "quick-bulk-response.json" | ConvertFrom-Json
        Write-Host ""
        Write-Host "Results:" -ForegroundColor Yellow
        Write-Host "  Run ID: $($response.run_id)" -ForegroundColor White
        Write-Host "  Symbols processed: $($response.symbols_processed)" -ForegroundColor White  
        Write-Host "  Success count: $($response.success_count)" -ForegroundColor White
        Write-Host "  Error count: $($response.error_count)" -ForegroundColor White
        Write-Host "  Execution time: $($response.execution_time_ms)ms" -ForegroundColor White
        
        if ($response.s3_files -and $response.s3_files.success) {
            Write-Host "  CSV files created: $($response.s3_files.records_processed)" -ForegroundColor White
        }
    }
    else {
        Write-Host "Lambda invocation failed!" -ForegroundColor Red
    }
}
catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Green
Write-Host "1. Wait 2-5 minutes for Snowpipe to process the files" -ForegroundColor White
Write-Host "2. Run monitor-overview-loading.sql in Snowflake to check progress" -ForegroundColor White  
Write-Host "3. Expected result: ~20 companies in OVERVIEW table" -ForegroundColor White
Write-Host ""
Write-Host "Monitor with: SELECT COUNT(*) FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW;" -ForegroundColor Cyan
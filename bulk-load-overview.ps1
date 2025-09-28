# =============================================================================
# Bulk Load OVERVIEW Table - Multiple Stock Symbols
# Run this to populate the entire OVERVIEW table with many companies
# =============================================================================

Write-Host "Bulk Loading OVERVIEW Table with Multiple Stock Symbols..." -ForegroundColor Green

# =============================================================================
# Popular stock symbols to load (you can customize this list)
# =============================================================================

$symbols = @(
    # Tech Giants (FAANG+)
    "AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA",
    
    # Financial Services
    "JPM", "BAC", "WFC", "GS", "MS", "C",
    
    # Healthcare & Pharma
    "JNJ", "PFE", "ABT", "MRK", "UNH", "BMY",
    
    # Consumer Goods
    "PG", "KO", "PEP", "WMT", "HD", "MCD",
    
    # Energy & Utilities
    "XOM", "CVX", "NEE", "DUK",
    
    # Industrial & Defense
    "BA", "CAT", "GE", "LMT", "RTX",
    
    # Telecommunications
    "T", "VZ",
    
    # Entertainment & Media
    "DIS", "NFLX", "CMCSA"
)

Write-Host "Planning to process $($symbols.Count) stock symbols" -ForegroundColor Cyan

# =============================================================================
# Batch processing approach (recommended for rate limiting)
# =============================================================================

$batchSize = 5  # Process 5 symbols at a time to avoid rate limits
$delayBetweenBatches = 30  # Wait 30 seconds between batches

Write-Host "Processing in batches of $batchSize symbols with $delayBetweenBatches second delays" -ForegroundColor Yellow

for ($i = 0; $i -lt $symbols.Count; $i += $batchSize) {
    $batch = $symbols[$i..([Math]::Min($i + $batchSize - 1, $symbols.Count - 1))]
    $batchNumber = [Math]::Floor($i / $batchSize) + 1
    $totalBatches = [Math]::Ceiling($symbols.Count / $batchSize)
    
    Write-Host ""
    Write-Host "Batch $batchNumber of $totalBatches - Processing: $($batch -join ', ')" -ForegroundColor Cyan
    
    # Create JSON payload for Lambda
    $payload = @{
        symbols = $batch
        run_id = "bulk-load-batch-$(Get-Date -Format 'yyyyMMdd-HHmmss')-$batchNumber"
    }
    
    $jsonPayload = $payload | ConvertTo-Json -Compress
    $base64Payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($jsonPayload))
    
    try {
        # Invoke Lambda function
        Write-Host "Invoking Lambda function..." -ForegroundColor Blue
        $response = aws lambda invoke --function-name fin-trade-overview-extractor --payload $base64Payload "batch-$batchNumber-response.json"
        
        if ($LASTEXITCODE -eq 0) {
            $responseContent = Get-Content "batch-$batchNumber-response.json" | ConvertFrom-Json
            Write-Host "Batch $batchNumber completed successfully" -ForegroundColor Green
            Write-Host "  Symbols processed: $($responseContent.symbols_processed)" -ForegroundColor White
            Write-Host "  Success count: $($responseContent.success_count)" -ForegroundColor White
            Write-Host "  Error count: $($responseContent.error_count)" -ForegroundColor White
        }
        else {
            Write-Host "Batch $batchNumber failed with Lambda invocation error" -ForegroundColor Red
        }
    }
    catch {
        Write-Host "Error processing batch $batchNumber : $_" -ForegroundColor Red
    }
    
    # Wait between batches (except for the last batch)
    if ($i + $batchSize -lt $symbols.Count) {
        Write-Host "Waiting $delayBetweenBatches seconds before next batch..." -ForegroundColor Yellow
        Start-Sleep -Seconds $delayBetweenBatches
    }
}

# =============================================================================
# Monitor Snowflake loading progress
# =============================================================================

Write-Host ""
Write-Host "Bulk processing complete! Monitoring Snowflake data loading..." -ForegroundColor Green
Write-Host ""
Write-Host "Run these queries in Snowflake to monitor progress:" -ForegroundColor Cyan
Write-Host ""
Write-Host "-- Check current row count" -ForegroundColor White
Write-Host "SELECT COUNT(*) as total_rows FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW;" -ForegroundColor White
Write-Host ""
Write-Host "-- Check loading status by symbol" -ForegroundColor White
Write-Host "SELECT" -ForegroundColor White  
Write-Host "    SYMBOL," -ForegroundColor White
Write-Host "    COUNT(*) as row_count," -ForegroundColor White
Write-Host "    MAX(LOADED_AT) as last_loaded" -ForegroundColor White
Write-Host "FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW" -ForegroundColor White
Write-Host "GROUP BY SYMBOL" -ForegroundColor White
Write-Host "ORDER BY last_loaded DESC;" -ForegroundColor White
Write-Host ""
Write-Host "-- Check for any loading errors" -ForegroundColor White
Write-Host "SELECT" -ForegroundColor White
Write-Host "    FILE_NAME," -ForegroundColor White
Write-Host "    STATUS," -ForegroundColor White
Write-Host "    ERROR_COUNT," -ForegroundColor White
Write-Host "    FIRST_ERROR_MESSAGE" -ForegroundColor White
Write-Host "FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY" -ForegroundColor White
Write-Host "WHERE TABLE_NAME = 'OVERVIEW'" -ForegroundColor White
Write-Host "    AND STATUS != 'LOADED'" -ForegroundColor White
Write-Host "ORDER BY LAST_LOAD_TIME DESC;" -ForegroundColor White
Write-Host ""

# =============================================================================
# Expected results summary
# =============================================================================

Write-Host "Expected Results:" -ForegroundColor Green
Write-Host "- CSV files created in S3: s3://fin-trade-craft-landing/overview/" -ForegroundColor White
Write-Host "- Snowpipe will automatically load data (may take 1-5 minutes)" -ForegroundColor White
Write-Host "- Final table should have ~$($symbols.Count) rows (one per symbol)" -ForegroundColor White
Write-Host "- Check Snowflake in 5-10 minutes for complete data" -ForegroundColor White
Write-Host ""
Write-Host "Files created:" -ForegroundColor Yellow
Get-ChildItem -Name "batch-*-response.json" | ForEach-Object {
    Write-Host "  $_" -ForegroundColor White
}

Write-Host ""
Write-Host "Pipeline Status: Bulk Load → S3 → Snowpipe → Snowflake" -ForegroundColor Green
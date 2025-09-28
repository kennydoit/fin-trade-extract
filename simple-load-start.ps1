# Simple Data Loading Script - Using Direct AWS CLI
Write-Host "🚀 LOADING DATA - PROGRESSIVE BATCHES" -ForegroundColor Green

# Define symbol batches
$symbolBatch50 = @("AA", "AAPL", "ABBV", "ABT", "ADBE", "ADYEY", "AEP", "AIV", "AMAT", "AMD", "AMGN", "AMH", "AMT", "AMZN", "AON", "APD", "ATLASSIAN", "ATO", "AVB", "AVGO", "AWK", "AXP", "BA", "BAC", "BIIB", "BKR", "BLK", "BMY", "BNTX", "BRK.A", "BRK.B", "BSX", "BXP", "C", "CABO", "CAG", "CAT", "CB", "CCI", "CCL", "CE", "CHTR", "CL", "CLF", "CMCSA", "CME", "CMS", "COF", "COP", "COST")

function Invoke-LambdaWithSymbols {
    param(
        [string[]]$Symbols,
        [string]$RunId
    )
    
    Write-Host "   Loading $($Symbols.Count) symbols with run ID: $RunId" -ForegroundColor Cyan
    
    # Create JSON payload (simple structure)
    $jsonPayload = @{
        symbols = $Symbols
        run_id = $RunId
        batch_size = $Symbols.Count
    } | ConvertTo-Json -Compress
    
    # Convert to UTF8 bytes then base64
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($jsonPayload)
    $base64 = [System.Convert]::ToBase64String($bytes)
    
    # Invoke Lambda
    $result = aws lambda invoke --function-name fin-trade-extract-overview --payload $base64 --region us-east-2 "response-$RunId.json"
    
    if ($LASTEXITCODE -eq 0) {
        $response = Get-Content "response-$RunId.json" | ConvertFrom-Json
        if ($response.statusCode -eq 200) {
            Write-Host "   ✅ SUCCESS: $($response.success_count) symbols processed" -ForegroundColor Green
            Write-Host "      Files: $($response.s3_files.total_files), Records: $($response.s3_files.total_records)" -ForegroundColor White
            return $response
        } else {
            Write-Host "   ❌ ERROR: Lambda returned status $($response.statusCode)" -ForegroundColor Red
            return $null
        }
    } else {
        Write-Host "   ❌ ERROR: AWS CLI failed with exit code $LASTEXITCODE" -ForegroundColor Red
        return $null
    }
}

# Start loading
Write-Host "`n📊 PHASE 1: Loading first 50 symbols" -ForegroundColor Yellow

$runId = "batch-50-" + (Get-Date -Format "yyyyMMdd-HHmmss")
$result = Invoke-LambdaWithSymbols -Symbols $symbolBatch50 -RunId $runId

if ($result) {
    Write-Host "`n🎯 SUCCESS! Ready for next phase." -ForegroundColor Green
    Write-Host "   Rate limiter performance: $($result.rate_limiter_stats.throughput_improvement)" -ForegroundColor Cyan
    Write-Host "   Estimated throughput: $($result.rate_limiter_stats.estimated_symbols_per_hour) symbols/hour" -ForegroundColor Cyan
} else {
    Write-Host "`n❌ Phase 1 failed. Check Lambda logs for details." -ForegroundColor Red
}

# Clean up response files
Get-ChildItem -Path "response-*.json" -ErrorAction SilentlyContinue | Remove-Item -Force
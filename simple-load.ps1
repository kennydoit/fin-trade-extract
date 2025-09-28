# Simple Data Loading Script for Overview
# Loads data in small batches to test the pipeline

param(
    [int]$BatchSize = 50,
    [string]$BatchName = "test"
)

$LambdaFunctionName = "fin-trade-extract-overview"
$Region = "us-east-2"

# First 50 major stocks for testing
$TestSymbols = @(
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX", "ADBE", "CRM",
    "JPM", "BAC", "WFC", "GS", "MS", "C", "USB", "PNC", "TFC", "COF",
    "JNJ", "PFE", "UNH", "ABT", "TMO", "DHR", "BMY", "ABBV", "MRK", "LLY",
    "HD", "NKE", "MCD", "SBUX", "DIS", "CMCSA", "LOW", "TJX", "F", "GM", 
    "PG", "KO", "PEP", "WMT", "COST", "CL", "KMB", "GIS", "K", "HSY"
)

$SymbolsToLoad = $TestSymbols | Select-Object -First $BatchSize
$RunId = "load-$BatchName-$(Get-Date -Format 'yyyyMMdd-HHmmss')"

Write-Host "=" * 60 -ForegroundColor Green
Write-Host "LOADING $($SymbolsToLoad.Count) SYMBOLS" -ForegroundColor Yellow
Write-Host "Run ID: $RunId" -ForegroundColor Cyan
Write-Host "Symbols: $($SymbolsToLoad -join ', ')" -ForegroundColor White
Write-Host "=" * 60 -ForegroundColor Green

# Create simple JSON payload file
$PayloadObject = @{
    symbols = $SymbolsToLoad
    run_id = $RunId
    batch_size = $BatchSize
}

$JsonPayload = $PayloadObject | ConvertTo-Json -Compress
Write-Host "Payload: $JsonPayload" -ForegroundColor Gray

# Save payload to temp file for AWS CLI
$JsonPayload | Out-File -FilePath "temp-payload.json" -Encoding UTF8

Write-Host ""
Write-Host "Invoking Lambda function..." -ForegroundColor Blue

try {
    # Use file input for payload to avoid escaping issues
    $Response = aws lambda invoke `
        --function-name $LambdaFunctionName `
        --region $Region `
        --payload "file://temp-payload.json" `
        --cli-binary-format raw-in-base64-out `
        "response-$BatchName.json"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Lambda invocation successful!" -ForegroundColor Green
        
        # Read and display response
        $ResponseContent = Get-Content "response-$BatchName.json" | ConvertFrom-Json
        
        Write-Host ""
        Write-Host "RESULTS:" -ForegroundColor Yellow
        Write-Host "Status Code: $($ResponseContent.statusCode)" -ForegroundColor White
        
        if ($ResponseContent.statusCode -eq 200) {
            Write-Host "Symbols Processed: $($ResponseContent.symbols_processed)" -ForegroundColor Green
            Write-Host "Successful: $($ResponseContent.success_count)" -ForegroundColor Green
            Write-Host "Errors: $($ResponseContent.error_count)" -ForegroundColor $(if($ResponseContent.error_count -gt 0) {'Red'} else {'Green'})
            
            if ($ResponseContent.s3_files) {
                Write-Host ""
                Write-Host "S3 FILES:" -ForegroundColor Cyan
                Write-Host "Business: $($ResponseContent.s3_files.business_file)" -ForegroundColor White
                Write-Host "Landing: $($ResponseContent.s3_files.landing_file)" -ForegroundColor Gray
            }
            
            Write-Host ""
            Write-Host "SUCCESS! Data loaded to S3 and should appear in Snowflake shortly." -ForegroundColor Green
            Write-Host ""
            Write-Host "NEXT STEPS:" -ForegroundColor Yellow
            Write-Host "1. Check Snowflake: SELECT COUNT(*) FROM RAW.OVERVIEW;" -ForegroundColor Cyan
            Write-Host "2. View latest data: SELECT * FROM RAW.OVERVIEW ORDER BY LOAD_TIMESTAMP DESC LIMIT 10;" -ForegroundColor Cyan
            Write-Host "3. Run analytics: SELECT * FROM ANALYTICS.CURRENT_FINANCIAL_SNAPSHOT LIMIT 5;" -ForegroundColor Cyan
        } else {
            Write-Host "Error in Lambda execution:" -ForegroundColor Red
            Write-Host ($ResponseContent | ConvertTo-Json -Depth 10) -ForegroundColor Red
        }
        
    } else {
        Write-Host "AWS CLI invocation failed with exit code: $LASTEXITCODE" -ForegroundColor Red
    }
    
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
} finally {
    # Clean up temp files
    Remove-Item "temp-payload.json" -ErrorAction SilentlyContinue
    Remove-Item "response-$BatchName.json" -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "=" * 60 -ForegroundColor Green
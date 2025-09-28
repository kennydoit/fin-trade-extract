# ============================================================================
# ENTERPRISE DATA LOADING PIPELINE - Enhanced Local Edition
# Simplified PowerShell script that eliminates JSON encoding issues
# ============================================================================

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("test", "small", "medium", "large", "custom")]
    [string]$BatchSize = "test",
    
    [Parameter(Mandatory=$false)]
    [string[]]$CustomSymbols = @(),
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipConfirmation = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$Verbose = $false
)

# Color-coded output functions
function Write-Success($message) { Write-Host "✅ $message" -ForegroundColor Green }
function Write-Info($message) { Write-Host "ℹ️  $message" -ForegroundColor Cyan }
function Write-Warning($message) { Write-Host "⚠️  $message" -ForegroundColor Yellow }
function Write-Error($message) { Write-Host "❌ $message" -ForegroundColor Red }
function Write-Header($message) { 
    Write-Host "`n" + "="*80 -ForegroundColor DarkGray
    Write-Host $message -ForegroundColor Magenta
    Write-Host "="*80 -ForegroundColor DarkGray
}

# Lambda function configuration
$LAMBDA_FUNCTION = "fin-trade-extract-overview"
$AWS_REGION = "us-east-2"

# Symbol batches (pre-defined high-quality symbols)
$SymbolBatches = @{
    "test" = @("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA")
    "small" = @("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "JNJ", "V", 
                "PG", "UNH", "HD", "MA", "DIS", "ADBE", "NFLX", "CRM", "ACN", "TMO")
    "medium" = @("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "JNJ", "V",
                 "PG", "UNH", "HD", "MA", "DIS", "ADBE", "NFLX", "CRM", "ACN", "TMO",
                 "VZ", "KO", "PEP", "ABT", "COST", "AVGO", "WMT", "XOM", "LLY", "ABBV",
                 "MRK", "PFE", "CVX", "BAC", "ORCL", "WFC", "BRK.B", "ASML", "AZN", "TSM",
                 "NKE", "DHR", "MCD", "NEE", "BMY", "QCOM", "TXN", "IBM", "UPS", "HON")
    "large" = @() # Will be populated with 100+ symbols
}

# Add large batch symbols
$SymbolBatches["large"] = $SymbolBatches["medium"] + @(
    "LOW", "INTC", "AMD", "LIN", "PM", "UNP", "RTX", "SPGI", "CAT", "GS",
    "INTU", "ISRG", "BKNG", "SYK", "MDT", "ADP", "GILD", "AMT", "CVS", "VRTX",
    "LRCX", "MDLZ", "PLD", "CI", "ZTS", "FIS", "ATVI", "FISV", "CSX", "DUK",
    "MMM", "TJX", "AON", "ICE", "GPN", "USB", "PNC", "CL", "NSC", "BSX",
    "ITW", "ECL", "CME", "EMR", "APD", "COF", "FCX", "EOG", "HUM", "GD"
)

function Invoke-LambdaWithRetry {
    param(
        [string[]]$Symbols,
        [string]$RunId,
        [int]$MaxRetries = 3
    )
    
    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            Write-Info "Attempt $attempt/$MaxRetries - Loading $($Symbols.Count) symbols..."
            
            # Create properly formatted JSON payload
            $payload = @{
                symbols = $Symbols
                run_id = $RunId
                batch_size = $Symbols.Count
                debug = $Verbose.IsPresent
            }
            
            # Convert to JSON and handle encoding properly
            $jsonString = ($payload | ConvertTo-Json -Compress -Depth 10)
            
            # Use UTF8 encoding without BOM
            $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
            $bytes = $utf8NoBom.GetBytes($jsonString)
            $base64Payload = [Convert]::ToBase64String($bytes)
            
            # Invoke Lambda
            $result = aws lambda invoke `
                --function-name $LAMBDA_FUNCTION `
                --payload $base64Payload `
                --region $AWS_REGION `
                --output json `
                "response-$RunId.json" 2>&1
            
            if ($LASTEXITCODE -ne 0) {
                throw "AWS CLI failed with exit code $LASTEXITCODE. Error: $result"
            }
            
            # Parse response
            if (Test-Path "response-$RunId.json") {
                $response = Get-Content "response-$RunId.json" -Raw | ConvertFrom-Json
                
                if ($response.statusCode -eq 200) {
                    Write-Success "Batch completed successfully!"
                    Write-Info "  Processed: $($response.symbols_processed) symbols"
                    Write-Info "  Successful: $($response.success_count) symbols"
                    Write-Info "  Errors: $($response.error_count) symbols"
                    Write-Info "  Files created: $($response.s3_files.total_files)"
                    Write-Info "  Throughput: $($response.rate_limiter_stats.estimated_symbols_per_hour) symbols/hour"
                    
                    return $response
                } else {
                    throw "Lambda returned error status: $($response.statusCode)"
                }
            } else {
                throw "No response file created"
            }
            
        } catch {
            Write-Warning "Attempt $attempt failed: $($_.Exception.Message)"
            
            if ($attempt -eq $MaxRetries) {
                Write-Error "All attempts failed. Last error: $($_.Exception.Message)"
                return $null
            }
            
            Write-Info "Retrying in 5 seconds..."
            Start-Sleep 5
        }
    }
}

function Show-BatchSummary {
    param([string]$BatchType, [string[]]$Symbols)
    
    Write-Header "BATCH SUMMARY: $($BatchType.ToUpper())"
    Write-Info "Symbols to process: $($Symbols.Count)"
    Write-Info "Estimated time: $([math]::Round($Symbols.Count / 120, 1)) minutes" # ~120 symbols/min
    Write-Info "First 10 symbols: $($Symbols[0..9] -join ', ')"
    if ($Symbols.Count -gt 10) {
        Write-Info "...and $($Symbols.Count - 10) more"
    }
}

function Cleanup-ResponseFiles {
    Write-Info "Cleaning up response files..."
    Get-ChildItem -Path "response-*.json" -ErrorAction SilentlyContinue | Remove-Item -Force
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

Write-Header "🚀 ENHANCED FINANCIAL DATA LOADING PIPELINE"
Write-Info "Local VS Code optimized version with robust error handling"

# Determine symbols to process
$symbolsToProcess = @()
$batchDescription = ""

if ($CustomSymbols.Count -gt 0) {
    $symbolsToProcess = $CustomSymbols
    $batchDescription = "Custom ($($CustomSymbols.Count) symbols)"
} elseif ($SymbolBatches.ContainsKey($BatchSize)) {
    $symbolsToProcess = $SymbolBatches[$BatchSize]
    $batchDescription = "$($BatchSize.Substring(0,1).ToUpper())$($BatchSize.Substring(1)) batch"
} else {
    Write-Error "Invalid batch size: $BatchSize"
    exit 1
}

# Show batch summary
Show-BatchSummary -BatchType $batchDescription -Symbols $symbolsToProcess

# Confirmation prompt
if (-not $SkipConfirmation) {
    Write-Warning "This will make $($symbolsToProcess.Count) API calls to Alpha Vantage."
    $confirmation = Read-Host "Continue? (y/N)"
    if ($confirmation -ne 'y' -and $confirmation -ne 'Y') {
        Write-Info "Operation cancelled by user."
        exit 0
    }
}

# Execute the batch
$runId = "enhanced-$BatchSize-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
Write-Header "🎯 EXECUTING BATCH: $batchDescription"

$startTime = Get-Date
$result = Invoke-LambdaWithRetry -Symbols $symbolsToProcess -RunId $runId

if ($result) {
    $duration = (Get-Date) - $startTime
    
    Write-Header "🎉 BATCH COMPLETION SUMMARY"
    Write-Success "Batch completed in $([math]::Round($duration.TotalMinutes, 2)) minutes"
    Write-Success "Success rate: $([math]::Round(($result.success_count / $result.symbols_processed) * 100, 1))%"
    Write-Info "Run ID: $($result.run_id)"
    Write-Info "S3 Files: $($result.s3_files.total_files) files, $($result.s3_files.total_records) records"
    
    if ($result.rate_limiter_stats) {
        Write-Info "Performance: $($result.rate_limiter_stats.throughput_improvement) improvement"
        Write-Info "Current throughput: $($result.rate_limiter_stats.estimated_symbols_per_hour) symbols/hour"
    }
    
    Write-Header "📊 NEXT STEPS"
    Write-Info "1. Check Snowflake data: SELECT * FROM RAW.OVERVIEW ORDER BY LOAD_TIMESTAMP DESC LIMIT 10;"
    Write-Info "2. Run analytics: SELECT * FROM ANALYTICS.CURRENT_FINANCIAL_SNAPSHOT LIMIT 5;"
    Write-Info "3. Check watermarks: SELECT * FROM RAW.DATA_WATERMARKS ORDER BY LAST_RUN_TIMESTAMP DESC;"
    
} else {
    Write-Header "❌ BATCH FAILED"
    Write-Error "The batch processing failed. Check Lambda logs for details:"
    Write-Info "aws logs tail /aws/lambda/$LAMBDA_FUNCTION --region $AWS_REGION --follow"
}

# Cleanup
Cleanup-ResponseFiles

Write-Header "🏁 PIPELINE EXECUTION COMPLETE"
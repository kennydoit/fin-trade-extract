# =============================================================================
# PowerShell Script for Progressive Data Loading
# Loads overview data in batches: 50, 100, 500, 1000, then remaining symbols
# =============================================================================

# Configuration
$LambdaFunctionName = "fin-trade-extract-overview"
$Region = "us-east-2"

# Comprehensive list of S&P 500 and major stocks for testing
$AllSymbols = @(
    # Major Tech Stocks
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "TSLA", "META", "NVDA", "NFLX", "ADBE",
    "CRM", "ORCL", "INTC", "AMD", "IBM", "CSCO", "QCOM", "AMAT", "MU", "AVGO",
    
    # Financial Services
    "JPM", "BAC", "WFC", "GS", "MS", "C", "USB", "PNC", "TFC", "COF",
    "AXP", "BLK", "SCHW", "CB", "MMC", "AON", "SPGI", "ICE", "CME", "MCO",
    
    # Healthcare & Pharmaceuticals
    "JNJ", "PFE", "UNH", "ABT", "TMO", "DHR", "BMY", "ABBV", "MRK", "LLY",
    "GILD", "AMGN", "BIIB", "REGN", "VRTX", "ILMN", "ISRG", "SYK", "BSX", "MDT",
    
    # Consumer Discretionary
    "AMZN", "TSLA", "HD", "NKE", "MCD", "SBUX", "DIS", "CMCSA", "NFLX", "LOW",
    "TJX", "F", "GM", "MAR", "HLT", "CCL", "RCL", "NCLH", "LVS", "MGM",
    
    # Consumer Staples
    "PG", "KO", "PEP", "WMT", "COST", "CL", "KMB", "GIS", "K", "HSY",
    "MDLZ", "CPB", "CAG", "SJM", "HRL", "TSN", "KHC", "MNST", "KDP", "STZ",
    
    # Energy
    "XOM", "CVX", "COP", "EOG", "SLB", "PSX", "VLO", "MPC", "OXY", "HAL",
    "BKR", "WMB", "KMI", "OKE", "EPD", "ET", "MPLX", "PAGP", "EQT", "DVN",
    
    # Industrials
    "BA", "CAT", "DE", "GE", "MMM", "HON", "UPS", "RTX", "LMT", "NOC",
    "GD", "UNP", "NSC", "CSX", "FDX", "WM", "RSG", "EMR", "ETN", "ITW",
    
    # Materials & Basic Resources
    "LIN", "APD", "ECL", "FCX", "NEM", "DOW", "DD", "PPG", "SHW", "IFF",
    "LYB", "CE", "VMC", "MLM", "NUE", "STLD", "X", "CLF", "AA", "MP",
    
    # Real Estate
    "AMT", "PLD", "CCI", "EQIX", "PSA", "WELL", "DLR", "O", "SBAC", "EQR",
    "AVB", "ESS", "MAA", "UDR", "CPT", "AIV", "BXP", "VTR", "PEAK", "AMH",
    
    # Utilities
    "NEE", "DUK", "SO", "D", "AEP", "EXC", "SRE", "PEG", "XEL", "WEC",
    "PPL", "ES", "AWK", "ATO", "CMS", "DTE", "ED", "ETR", "FE", "NI",
    
    # Additional Growth Stocks
    "ZM", "SHOP", "SQ", "PYPL", "ROKU", "DOCU", "TWLO", "OKTA", "DDOG", "SNOW",
    "CRWD", "ZS", "NET", "FSLY", "MDB", "TEAM", "ATLASSIAN", "CZR", "DKNG", "PENN",
    
    # REITs
    "SPG", "REG", "MAC", "KIM", "FRT", "TCO", "SLG", "BXP", "VNO", "HIW",
    
    # Financial Technology
    "V", "MA", "PYPL", "SQ", "ADYEY", "FIS", "FISV", "GPN", "JKHY", "WEX",
    
    # Biotechnology
    "GILD", "BIIB", "AMGN", "REGN", "VRTX", "ILMN", "MRNA", "BNTX", "NVAX", "SGEN",
    
    # Telecommunications
    "VZ", "T", "TMUS", "CHTR", "DISH", "SIRI", "LUMN", "USM", "CABO", "GSAT",
    
    # Additional Major Stocks
    "BRK.A", "BRK.B", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"
)

# Remove duplicates and sort
$AllSymbols = $AllSymbols | Sort-Object | Get-Unique

Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "FINANCIAL DATA LOADING PIPELINE" -ForegroundColor Yellow
Write-Host "Progressive batch loading: 50, 100, 500, 1000, then remaining" -ForegroundColor Green
Write-Host "Total symbols available: $($AllSymbols.Count)" -ForegroundColor White
Write-Host "=" * 80 -ForegroundColor Cyan

# Function to invoke Lambda and monitor results
function Invoke-LambdaBatch {
    param(
        [string[]]$Symbols,
        [string]$BatchName,
        [int]$BatchSize = 100
    )
    
    $RunId = "batch-$BatchName-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
    $SymbolsToProcess = $Symbols | Select-Object -First $BatchSize
    
    Write-Host ""
    Write-Host "Starting BATCH: $BatchName" -ForegroundColor Yellow
    Write-Host "   Symbols to process: $($SymbolsToProcess.Count)" -ForegroundColor White
    Write-Host "   Run ID: $RunId" -ForegroundColor Gray
    Write-Host "   Symbols: $($SymbolsToProcess -join ', ')" -ForegroundColor Cyan
    
    # Create payload
    $Payload = @{
        symbols = $SymbolsToProcess
        run_id = $RunId
        batch_size = $BatchSize
    } | ConvertTo-Json -Compress
    
    Write-Host ""
    Write-Host "Invoking Lambda function..." -ForegroundColor Blue
    
    try {
        # Invoke Lambda function
        $Response = aws lambda invoke `
            --function-name $LambdaFunctionName `
            --region $Region `
            --payload $Payload `
            --cli-binary-format raw-in-base64-out `
            "response-$BatchName.json"
        
        if ($LASTEXITCODE -eq 0) {
            # Read and parse response
            $ResponseContent = Get-Content "response-$BatchName.json" | ConvertFrom-Json
            
            Write-Host "Lambda execution completed!" -ForegroundColor Green
            Write-Host ""
            Write-Host "RESULTS SUMMARY:" -ForegroundColor Yellow
            Write-Host "   Status Code: $($ResponseContent.statusCode)" -ForegroundColor $(if($ResponseContent.statusCode -eq 200) {'Green'} else {'Red'})
            Write-Host "   Symbols Processed: $($ResponseContent.symbols_processed)" -ForegroundColor White
            Write-Host "   Successful: $($ResponseContent.success_count)" -ForegroundColor Green
            Write-Host "   Errors: $($ResponseContent.error_count)" -ForegroundColor $(if($ResponseContent.error_count -gt 0) {'Red'} else {'Green'})
            
            if ($ResponseContent.s3_files) {
                Write-Host ""
                Write-Host "S3 FILES CREATED:" -ForegroundColor Blue
                if ($ResponseContent.s3_files.business_file) {
                    Write-Host "   Business Data: $($ResponseContent.s3_files.business_file)" -ForegroundColor White
                }
                if ($ResponseContent.s3_files.landing_file) {
                    Write-Host "   Landing Data: $($ResponseContent.s3_files.landing_file)" -ForegroundColor Gray
                }
            }
            
            if ($ResponseContent.rate_limiter_stats) {
                Write-Host ""
                Write-Host "RATE LIMITING STATS:" -ForegroundColor Magenta
                Write-Host "   API Calls: $($ResponseContent.rate_limiter_stats.calls)" -ForegroundColor White
                Write-Host "   Delays Applied: $($ResponseContent.rate_limiter_stats.delays)" -ForegroundColor Gray
            }
            
            return @{
                Success = $true
                ProcessedCount = $ResponseContent.symbols_processed
                SuccessCount = $ResponseContent.success_count
                ErrorCount = $ResponseContent.error_count
                RunId = $RunId
            }
        } else {
            Write-Host "Lambda invocation failed!" -ForegroundColor Red
            Write-Host "AWS CLI exit code: $LASTEXITCODE" -ForegroundColor Red
            return @{
                Success = $false
                ProcessedCount = 0
                SuccessCount = 0
                ErrorCount = $SymbolsToProcess.Count
                RunId = $RunId
            }
        }
        
    } catch {
        Write-Host "Error invoking Lambda: $_" -ForegroundColor Red
        return @{
            Success = $false
            ProcessedCount = 0
            SuccessCount = 0
            ErrorCount = $SymbolsToProcess.Count
            RunId = $RunId
        }
    }
}

# Function to wait between batches
function Wait-BetweenBatches {
    param([int]$Seconds = 30)
    
    Write-Host ""
    Write-Host "Waiting $Seconds seconds before next batch..." -ForegroundColor Yellow
    for ($i = $Seconds; $i -gt 0; $i--) {
        Write-Host "   $i seconds remaining..." -ForegroundColor Gray
        Start-Sleep 1
    }
}

# Track overall progress
$OverallStats = @{
    TotalProcessed = 0
    TotalSuccessful = 0
    TotalErrors = 0
    BatchResults = @()
}

# Batch 1: First 50 symbols
Write-Host ""
Write-Host "PHASE 1: Loading first 50 symbols" -ForegroundColor Magenta
$Result1 = Invoke-LambdaBatch -Symbols $AllSymbols -BatchName "50-symbols" -BatchSize 50
$OverallStats.BatchResults += $Result1
$OverallStats.TotalProcessed += $Result1.ProcessedCount
$OverallStats.TotalSuccessful += $Result1.SuccessCount
$OverallStats.TotalErrors += $Result1.ErrorCount

if ($Result1.Success) {
    Wait-BetweenBatches -Seconds 60
    
    # Batch 2: Next 50 symbols (51-100)
    Write-Host ""
    Write-Host "PHASE 2: Loading next 50 symbols (51-100)" -ForegroundColor Magenta
    $RemainingSymbols = $AllSymbols | Select-Object -Skip 50
    $Result2 = Invoke-LambdaBatch -Symbols $RemainingSymbols -BatchName "100-symbols" -BatchSize 50
    $OverallStats.BatchResults += $Result2
    $OverallStats.TotalProcessed += $Result2.ProcessedCount
    $OverallStats.TotalSuccessful += $Result2.SuccessCount
    $OverallStats.TotalErrors += $Result2.ErrorCount
    
    if ($Result2.Success) {
        Wait-BetweenBatches -Seconds 120
        
        # Batch 3: Next 400 symbols (101-500)
        Write-Host ""
        Write-Host "PHASE 3: Loading next 400 symbols (101-500)" -ForegroundColor Magenta
        $RemainingSymbols = $AllSymbols | Select-Object -Skip 100
        $Result3 = Invoke-LambdaBatch -Symbols $RemainingSymbols -BatchName "500-symbols" -BatchSize 400
        $OverallStats.BatchResults += $Result3
        $OverallStats.TotalProcessed += $Result3.ProcessedCount
        $OverallStats.TotalSuccessful += $Result3.SuccessCount
        $OverallStats.TotalErrors += $Result3.ErrorCount
        
        if ($Result3.Success) {
            Wait-BetweenBatches -Seconds 180
            
            # Batch 4: Next 500 symbols (501-1000)
            Write-Host ""
            Write-Host "PHASE 4: Loading next 500 symbols (501-1000)" -ForegroundColor Magenta
            $RemainingSymbols = $AllSymbols | Select-Object -Skip 500
            $Result4 = Invoke-LambdaBatch -Symbols $RemainingSymbols -BatchName "1000-symbols" -BatchSize 500
            $OverallStats.BatchResults += $Result4
            $OverallStats.TotalProcessed += $Result4.ProcessedCount
            $OverallStats.TotalSuccessful += $Result4.SuccessCount
            $OverallStats.TotalErrors += $Result4.ErrorCount
            
            if ($Result4.Success -and $AllSymbols.Count -gt 1000) {
                Wait-BetweenBatches -Seconds 240
                
                # Batch 5: Remaining symbols
                Write-Host ""
                Write-Host "PHASE 5: Loading remaining symbols (1000+)" -ForegroundColor Magenta
                $RemainingSymbols = $AllSymbols | Select-Object -Skip 1000
                if ($RemainingSymbols.Count -gt 0) {
                    $Result5 = Invoke-LambdaBatch -Symbols $RemainingSymbols -BatchName "remaining-symbols" -BatchSize $RemainingSymbols.Count
                    $OverallStats.BatchResults += $Result5
                    $OverallStats.TotalProcessed += $Result5.ProcessedCount
                    $OverallStats.TotalSuccessful += $Result5.SuccessCount
                    $OverallStats.TotalErrors += $Result5.ErrorCount
                }
            }
        }
    }
}

# Final Summary
Write-Host ""
Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "FINAL LOADING SUMMARY" -ForegroundColor Yellow
Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "Total Available Symbols: $($AllSymbols.Count)" -ForegroundColor White
Write-Host "Total Processed: $($OverallStats.TotalProcessed)" -ForegroundColor Blue
Write-Host "Total Successful: $($OverallStats.TotalSuccessful)" -ForegroundColor Green
Write-Host "Total Errors: $($OverallStats.TotalErrors)" -ForegroundColor $(if($OverallStats.TotalErrors -gt 0) {'Red'} else {'Green'})
Write-Host "Success Rate: $([math]::Round(($OverallStats.TotalSuccessful / [math]::Max($OverallStats.TotalProcessed, 1)) * 100, 2))%" -ForegroundColor Yellow

Write-Host ""
Write-Host "BATCH BREAKDOWN:" -ForegroundColor Blue
for ($i = 0; $i -lt $OverallStats.BatchResults.Count; $i++) {
    $result = $OverallStats.BatchResults[$i]
    $phase = $i + 1
    Write-Host "   Phase $phase - Run: $($result.RunId)" -ForegroundColor Gray
    Write-Host "            Processed: $($result.ProcessedCount), Success: $($result.SuccessCount), Errors: $($result.ErrorCount)" -ForegroundColor White
}

Write-Host ""
Write-Host "NEXT STEPS:" -ForegroundColor Green
Write-Host "   1. Check Snowflake for loaded data: SELECT * FROM RAW.OVERVIEW ORDER BY LOAD_TIMESTAMP DESC LIMIT 100" -ForegroundColor Cyan
Write-Host "   2. Run analytics: SELECT * FROM ANALYTICS.CURRENT_FINANCIAL_SNAPSHOT LIMIT 20" -ForegroundColor Cyan
Write-Host "   3. Test alerting: CALL RAW.RUN_ALL_ALERT_CHECKS();" -ForegroundColor Cyan
Write-Host "   4. Review watermarks: SELECT * FROM RAW.DATA_WATERMARKS ORDER BY LAST_RUN_TIMESTAMP DESC" -ForegroundColor Cyan

# Clean up response files
Write-Host ""
Write-Host "Cleaning up response files..." -ForegroundColor Gray
Get-ChildItem "response-*.json" -ErrorAction SilentlyContinue | Remove-Item -Force

Write-Host ""
Write-Host "Data loading pipeline completed!" -ForegroundColor Green
Write-Host "=" * 80 -ForegroundColor Cyan
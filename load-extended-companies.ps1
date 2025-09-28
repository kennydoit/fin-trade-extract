# Load Additional Companies - Extended Symbol Lists
# Adding more diverse company symbols for comprehensive coverage

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("tech", "finance", "healthcare", "energy", "mixed")]
    [string]$Sector = "mixed",
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipConfirmation = $false
)

$LAMBDA_FUNCTION = "fin-trade-extract-overview"
$AWS_REGION = "us-east-2"

# Extended symbol batches by sector
$SectorBatches = @{
    "tech" = @(
        "ADSK", "ANET", "ANSS", "APH", "ATUS", "AVGO", "CDNS", "CERN", "CHTR", "CRM",
        "CSCO", "CTSH", "DXCM", "EBAY", "ENPH", "FAST", "FSLR", "FTNT", "GPN", "HPE",
        "HPQ", "IBM", "INTC", "INTU", "ISRG", "JNPR", "KEYS", "KLAC", "LRCX", "MCHP",
        "MPWR", "MRVL", "MSFT", "MU", "NOW", "NVDA", "NXPI", "ORCL", "PAYC", "PAYX",
        "QCOM", "QRVO", "ROP", "SEDG", "SWKS", "TER", "TXN", "VRSN", "WDC", "XLNX"
    )
    "finance" = @(
        "AIG", "AJG", "ALL", "AMP", "AON", "AXP", "BAC", "BK", "BLK", "BRK.B",
        "C", "CB", "CMA", "CME", "COF", "DFS", "FITB", "GS", "HBAN", "HIG",
        "ICE", "JPM", "KEY", "L", "LNC", "MA", "MCO", "MET", "MMC", "MS",
        "NDAQ", "NTRS", "PFG", "PGR", "PNC", "PRU", "PYPL", "RF", "SCHW", "SIVB",
        "SPGI", "STT", "SYF", "TFC", "TRV", "USB", "V", "WFC", "ZION", "AXS"
    )
    "healthcare" = @(
        "A", "ABBV", "ABC", "ABMD", "ABT", "AGN", "ALGN", "ALXN", "AMGN", "ANTM",
        "BAX", "BIIB", "BMY", "BSX", "CAH", "CE", "CI", "CNC", "COO", "CVS",
        "DGX", "DHR", "DVA", "EW", "GILD", "HCA", "HOLX", "HUM", "IDXX", "IQV",
        "ISRG", "JNJ", "LH", "LLY", "MCK", "MDT", "MRK", "MTD", "PFE", "PKI",
        "REGN", "RMD", "STE", "SYK", "TMO", "UNH", "VAR", "VRTX", "WAT", "ZBH"
    )
    "energy" = @(
        "APA", "BKR", "COP", "CVX", "DVN", "EOG", "EQT", "FANG", "HAL", "HES",
        "KMI", "MPC", "MRO", "NOV", "OKE", "OXY", "PXD", "SLB", "VLO", "WMB",
        "XOM", "APC", "CHK", "CLR", "CXO", "ECA", "EIX", "EPD", "ET", "ETRN",
        "FCX", "FTI", "HP", "HPK", "KRP", "LNG", "MUR", "NBL", "NE", "NFG",
        "NRG", "OAS", "PDCE", "PE", "PER", "PSX", "QEP", "RRC", "SM", "SWN"
    )
    "mixed" = @(
        # Consumer Discretionary
        "AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX", "DIS", "NFLX", "TJX", "LOW",
        # Consumer Staples  
        "WMT", "PG", "KO", "PEP", "COST", "WBA", "CL", "KMB", "GIS", "K",
        # Industrials
        "BA", "HON", "UPS", "CAT", "MMM", "GE", "LMT", "RTX", "UNP", "FDX",
        # Materials
        "LIN", "APD", "ECL", "DD", "DOW", "FCX", "NEM", "PPG", "SHW", "VMC",
        # Utilities
        "NEE", "DUK", "SO", "D", "EXC", "AEP", "SRE", "PEG", "XEL", "ED"
    )
}

function Load-SectorBatch {
    param(
        [string]$SectorName,
        [string[]]$Symbols
    )
    
    Write-Host "`n=== LOADING $($SectorName.ToUpper()) SECTOR ===" -ForegroundColor Blue
    Write-Host "Symbols: $($Symbols.Count) companies" -ForegroundColor Cyan
    
    $runId = "$SectorName-batch-$(Get-Date -Format 'HHmmss')"
    
    $payload = @{
        symbols = $Symbols
        run_id = $runId
        batch_size = $Symbols.Count
    } | ConvertTo-Json -Compress
    
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($payload)
    $base64 = [Convert]::ToBase64String($bytes)
    
    Write-Host "Invoking Lambda function..." -ForegroundColor Yellow
    $result = aws lambda invoke --function-name $LAMBDA_FUNCTION --payload $base64 --region $AWS_REGION "result-$runId.json"
    
    if ($LASTEXITCODE -eq 0 -and (Test-Path "result-$runId.json")) {
        $response = Get-Content "result-$runId.json" -Raw | ConvertFrom-Json
        if ($response.statusCode -eq 200) {
            Write-Host "SUCCESS: $SectorName sector loaded!" -ForegroundColor Green
            Write-Host "  Processed: $($response.symbols_processed) symbols" -ForegroundColor White
            Write-Host "  Successful: $($response.success_count) symbols" -ForegroundColor White  
            Write-Host "  Success Rate: $([math]::Round(($response.success_count / $response.symbols_processed) * 100, 1))%" -ForegroundColor White
            Write-Host "  Throughput: $($response.rate_limiter_stats.estimated_symbols_per_hour) symbols/hour" -ForegroundColor White
        } else {
            Write-Host "ERROR: $SectorName sector failed with status $($response.statusCode)" -ForegroundColor Red
        }
        Remove-Item "result-$runId.json" -Force
    } else {
        Write-Host "ERROR: Lambda invocation failed for $SectorName sector" -ForegroundColor Red
    }
}

# Main execution
Write-Host "EXTENDED COMPANY LOADING - By Sector" -ForegroundColor Magenta
Write-Host "Target Sector: $($Sector.ToUpper())" -ForegroundColor White

if ($SectorBatches.ContainsKey($Sector)) {
    $symbols = $SectorBatches[$Sector]
    
    Write-Host "`nPlanning to load $($symbols.Count) companies from $($Sector.ToUpper()) sector" -ForegroundColor Yellow
    Write-Host "Sample symbols: $($symbols[0..4] -join ', ')..." -ForegroundColor Gray
    
    if (-not $SkipConfirmation) {
        $confirm = Read-Host "`nContinue with loading? (y/N)"
        if ($confirm -ne 'y' -and $confirm -ne 'Y') {
            Write-Host "Operation cancelled." -ForegroundColor Yellow
            exit 0
        }
    }
    
    Load-SectorBatch -SectorName $Sector -Symbols $symbols
    
} else {
    Write-Host "ERROR: Invalid sector '$Sector'" -ForegroundColor Red
    Write-Host "Available sectors: tech, finance, healthcare, energy, mixed" -ForegroundColor White
}

Write-Host "`n=== EXTENDED LOADING COMPLETE ===" -ForegroundColor Blue
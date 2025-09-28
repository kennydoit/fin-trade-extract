# Simple Lambda Testing and Data Loading Tools
# ASCII-only version to avoid Unicode issues

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("test", "load", "status", "logs")]
    [string]$Action = "test",
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("small", "medium", "large")]
    [string]$BatchSize = "small",
    
    [Parameter(Mandatory=$false)]
    [int]$LogLines = 20
)

$LAMBDA_FUNCTION = "fin-trade-extract-overview"
$AWS_REGION = "us-east-2"

# Simple symbol batches
$Batches = @{
    "small" = @("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA")
    "medium" = @("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "JNJ", "V", "PG", "UNH", "HD", "MA", "DIS", "ADBE", "NFLX", "CRM", "ACN", "TMO")
    "large" = @("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "JNJ", "V", "PG", "UNH", "HD", "MA", "DIS", "ADBE", "NFLX", "CRM", "ACN", "TMO", "VZ", "KO", "PEP", "ABT", "COST", "AVGO", "WMT", "XOM", "LLY", "ABBV", "MRK", "PFE", "CVX", "BAC", "ORCL", "WFC", "BRK.B", "ASML", "AZN", "TSM", "NKE", "DHR", "MCD", "NEE", "BMY", "QCOM", "TXN", "IBM", "UPS", "HON")
}

function Write-Success($msg) { Write-Host "SUCCESS: $msg" -ForegroundColor Green }
function Write-Info($msg) { Write-Host "INFO: $msg" -ForegroundColor Cyan }
function Write-Warning($msg) { Write-Host "WARNING: $msg" -ForegroundColor Yellow }
function Write-Error($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red }

function Test-Lambda {
    Write-Host "`n=== TESTING LAMBDA FUNCTION ===" -ForegroundColor Blue
    
    $testPayload = '{"symbols":["AAPL"],"run_id":"quick-test","batch_size":1}'
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($testPayload)
    $base64 = [Convert]::ToBase64String($bytes)
    
    Write-Info "Invoking Lambda function..."
    $result = aws lambda invoke --function-name $LAMBDA_FUNCTION --payload $base64 --region $AWS_REGION test-result.json
    
    if ($LASTEXITCODE -eq 0) {
        if (Test-Path "test-result.json") {
            $response = Get-Content "test-result.json" -Raw | ConvertFrom-Json
            if ($response.statusCode -eq 200) {
                Write-Success "Lambda function is working correctly"
                Write-Info "Processed: $($response.success_count) symbols"
                Write-Info "Throughput: $($response.rate_limiter_stats.estimated_symbols_per_hour) symbols/hour"
            } else {
                Write-Error "Lambda returned status: $($response.statusCode)"
            }
            Remove-Item "test-result.json" -Force
        }
    } else {
        Write-Error "Lambda invocation failed"
    }
}

function Load-Data {
    param([string]$Size)
    
    Write-Host "`n=== LOADING DATA BATCH: $Size ===" -ForegroundColor Blue
    
    if (-not $Batches.ContainsKey($Size)) {
        Write-Error "Invalid batch size: $Size"
        return
    }
    
    $symbols = $Batches[$Size]
    $runId = "simple-$Size-$(Get-Date -Format 'HHmmss')"
    
    Write-Info "Loading $($symbols.Count) symbols..."
    Write-Info "Symbols: $($symbols[0..4] -join ', ')..."
    
    $payload = @{
        symbols = $symbols
        run_id = $runId
        batch_size = $symbols.Count
    } | ConvertTo-Json -Compress
    
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($payload)
    $base64 = [Convert]::ToBase64String($bytes)
    
    Write-Info "Invoking Lambda function..."
    $result = aws lambda invoke --function-name $LAMBDA_FUNCTION --payload $base64 --region $AWS_REGION "load-result-$runId.json"
    
    if ($LASTEXITCODE -eq 0) {
        if (Test-Path "load-result-$runId.json") {
            $response = Get-Content "load-result-$runId.json" -Raw | ConvertFrom-Json
            if ($response.statusCode -eq 200) {
                Write-Success "Batch loading completed successfully"
                Write-Info "Processed: $($response.symbols_processed) symbols"
                Write-Info "Successful: $($response.success_count) symbols"
                Write-Info "Errors: $($response.error_count) symbols"
                Write-Info "Success Rate: $([math]::Round(($response.success_count / $response.symbols_processed) * 100, 1))%"
                Write-Info "Files Created: $($response.s3_files.total_files)"
                Write-Info "Throughput: $($response.rate_limiter_stats.estimated_symbols_per_hour) symbols/hour"
            } else {
                Write-Error "Loading failed with status: $($response.statusCode)"
            }
            Remove-Item "load-result-$runId.json" -Force
        }
    } else {
        Write-Error "Lambda invocation failed"
    }
}

function Get-Status {
    Write-Host "`n=== LAMBDA FUNCTION STATUS ===" -ForegroundColor Blue
    
    $info = aws lambda get-function --function-name $LAMBDA_FUNCTION --region $AWS_REGION --output json | ConvertFrom-Json
    
    if ($info) {
        Write-Success "Function is accessible"
        Write-Info "Function Name: $($info.Configuration.FunctionName)"
        Write-Info "Runtime: $($info.Configuration.Runtime)" 
        Write-Info "Memory: $($info.Configuration.MemorySize) MB"
        Write-Info "Timeout: $($info.Configuration.Timeout) seconds"
        Write-Info "Code Size: $([math]::Round($info.Configuration.CodeSize / 1024 / 1024, 2)) MB"
        Write-Info "Last Modified: $($info.Configuration.LastModified)"
        Write-Info "State: $($info.Configuration.State)"
    }
}

function Get-Logs {
    param([int]$Lines = 20)
    
    Write-Host "`n=== LAMBDA FUNCTION LOGS ===" -ForegroundColor Blue
    Write-Info "Getting last $Lines log entries..."
    
    aws logs tail "/aws/lambda/$LAMBDA_FUNCTION" --region $AWS_REGION --since "1h" | Select-Object -Last $Lines
}

# Main execution
Write-Host "LAMBDA TOOLKIT - Simple Edition" -ForegroundColor Magenta
Write-Host "Action: $Action | Function: $LAMBDA_FUNCTION | Region: $AWS_REGION" -ForegroundColor White

switch ($Action) {
    "test" { Test-Lambda }
    "load" { Load-Data -Size $BatchSize }
    "status" { Get-Status }
    "logs" { Get-Logs -Lines $LogLines }
    default { 
        Write-Error "Unknown action: $Action"
        Write-Info "Available actions: test, load, status, logs"
        Write-Info "Examples:"
        Write-Info "  .\simple-toolkit.ps1 test"
        Write-Info "  .\simple-toolkit.ps1 load -BatchSize medium"
        Write-Info "  .\simple-toolkit.ps1 status"
        Write-Info "  .\simple-toolkit.ps1 logs -LogLines 50"
    }
}

Write-Host "`n=== OPERATION COMPLETE ===" -ForegroundColor Blue
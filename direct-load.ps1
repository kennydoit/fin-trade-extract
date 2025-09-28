# Direct Lambda Invocation for Data Loading
# Simple approach to avoid encoding issues

Write-Host "STARTING DATA LOAD..." -ForegroundColor Green

# Define the symbols we want to load (first 10 for testing)
$Symbols = @("AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "BAC", "JNJ")
$RunId = "direct-load-$(Get-Date -Format 'yyyyMMdd-HHmmss')"

Write-Host "Loading $($Symbols.Count) symbols: $($Symbols -join ', ')" -ForegroundColor Cyan
Write-Host "Run ID: $RunId" -ForegroundColor Gray

# Create the JSON payload manually with proper escaping
$SymbolsJson = '"' + ($Symbols -join '","') + '"'
$Payload = "{`"symbols`":[$SymbolsJson],`"run_id`":`"$RunId`",`"batch_size`":10}"

Write-Host ""
Write-Host "Payload: $Payload" -ForegroundColor Yellow

# Invoke Lambda
Write-Host ""
Write-Host "Invoking Lambda function..." -ForegroundColor Blue

$Command = "aws lambda invoke --function-name fin-trade-extract-overview --payload '$Payload' --cli-binary-format raw-in-base64-out response-direct.json"
Write-Host "Command: $Command" -ForegroundColor Gray

Invoke-Expression $Command

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "SUCCESS! Lambda invoked successfully" -ForegroundColor Green
    
    # Read response
    $Response = Get-Content "response-direct.json" | ConvertFrom-Json
    Write-Host ""
    Write-Host "RESPONSE:" -ForegroundColor Yellow
    Write-Host ($Response | ConvertTo-Json -Depth 10) -ForegroundColor White
    
    # Clean up
    Remove-Item "response-direct.json" -ErrorAction SilentlyContinue
} else {
    Write-Host "FAILED! Exit code: $LASTEXITCODE" -ForegroundColor Red
}

Write-Host ""
Write-Host "COMPLETED!" -ForegroundColor Green
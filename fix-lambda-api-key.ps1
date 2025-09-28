# =============================================================================
# Fix Lambda Function Alpha Vantage API Key
# The Lambda is using "demo" key which doesn't work for real data
# =============================================================================

Write-Host "Lambda Function API Key Fix" -ForegroundColor Green
Write-Host ""

# Check current environment variables
Write-Host "Current Lambda Environment Variables:" -ForegroundColor Yellow
aws lambda get-function-configuration --function-name fin-trade-overview-extractor --query 'Environment.Variables' --output json

Write-Host ""
Write-Host "PROBLEM IDENTIFIED:" -ForegroundColor Red
Write-Host "Lambda is using API key: 'demo'" -ForegroundColor Red
Write-Host "The 'demo' key only works for Alpha Vantage documentation examples" -ForegroundColor Red
Write-Host "It will NOT return real financial data for actual stock symbols" -ForegroundColor Red
Write-Host ""

Write-Host "SOLUTION:" -ForegroundColor Green
Write-Host "1. Get a free Alpha Vantage API key from: https://www.alphavantage.co/support/#api-key" -ForegroundColor Cyan
Write-Host "2. Update Lambda environment variable with real API key" -ForegroundColor Cyan
Write-Host ""

# Prompt for API key
$apiKey = Read-Host "Enter your Alpha Vantage API key (or press Enter to skip for now)"

if ($apiKey -and $apiKey -ne "demo" -and $apiKey.Length -gt 5) {
    Write-Host ""
    Write-Host "Updating Lambda environment variable..." -ForegroundColor Blue
    
    try {
        # Update Lambda environment variable
        aws lambda update-function-configuration --function-name fin-trade-overview-extractor --environment "Variables={ALPHA_VANTAGE_API_KEY=$apiKey}"
        
        Write-Host "Lambda environment variable updated successfully!" -ForegroundColor Green
        
        # Wait a moment for the update to take effect
        Write-Host "Waiting 10 seconds for Lambda update to take effect..." -ForegroundColor Yellow
        Start-Sleep -Seconds 10
        
        # Test with single symbol
        Write-Host ""
        Write-Host "Testing Lambda with real API key..." -ForegroundColor Blue
        
        $testPayload = @{
            symbols = @("AAPL")
            run_id = "api-key-test-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
        }
        
        $jsonPayload = $testPayload | ConvertTo-Json -Compress
        $base64Payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($jsonPayload))
        
        aws lambda invoke --function-name fin-trade-overview-extractor --payload $base64Payload api-key-test.json
        
        if (Test-Path "api-key-test.json") {
            $response = Get-Content "api-key-test.json" | ConvertFrom-Json
            
            Write-Host ""
            Write-Host "Test Results:" -ForegroundColor Yellow
            Write-Host "  Success Count: $($response.success_count)" -ForegroundColor White
            Write-Host "  Error Count: $($response.error_count)" -ForegroundColor White
            Write-Host "  API Calls Made: $($response.rate_limiter_stats.calls)" -ForegroundColor White
            
            if ($response.success_count -gt 0) {
                Write-Host ""
                Write-Host "SUCCESS! Lambda is now fetching real financial data!" -ForegroundColor Green
                Write-Host "You can now run bulk loads to populate your Snowflake table." -ForegroundColor Green
            } elseif ($response.rate_limiter_stats.calls -gt 0) {
                Write-Host ""
                Write-Host "API calls are being made but still getting errors." -ForegroundColor Yellow
                Write-Host "Check CloudWatch logs for specific API response issues." -ForegroundColor Yellow
            } else {
                Write-Host ""
                Write-Host "Still no API calls being made. There may be other configuration issues." -ForegroundColor Red
            }
        }
    }
    catch {
        Write-Host "Error updating Lambda: $_" -ForegroundColor Red
    }
} else {
    Write-Host ""
    Write-Host "NEXT STEPS:" -ForegroundColor Yellow
    Write-Host "1. Go to: https://www.alphavantage.co/support/#api-key" -ForegroundColor White
    Write-Host "2. Enter your email to get a free API key" -ForegroundColor White
    Write-Host "3. Re-run this script with your real API key" -ForegroundColor White
    Write-Host ""
    Write-Host "Manual Lambda Update Command:" -ForegroundColor Cyan
    Write-Host "aws lambda update-function-configuration --function-name fin-trade-overview-extractor --environment `"Variables={ALPHA_VANTAGE_API_KEY=YOUR_ACTUAL_KEY}`"" -ForegroundColor White
}

Write-Host ""
Write-Host "Current Status:" -ForegroundColor Yellow
Write-Host "  Snowflake Pipeline: WORKING (CSV structure fixed)" -ForegroundColor Green
Write-Host "  Lambda Function: NEEDS REAL API KEY" -ForegroundColor Red
Write-Host "  Next: Update API key, then test with 1-2 symbols before bulk load" -ForegroundColor Cyan
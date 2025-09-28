# Test if the API key is accessible via Parameter Store
$apiKeyName = "/fin-trade-craft/alpha_vantage_key"

Write-Host "Testing Parameter Store access..."
Write-Host "Looking for parameter: $apiKeyName"

try {
    # Get the API key value
    $result = aws ssm get-parameter --name $apiKeyName --with-decryption --region us-east-2 --output json | ConvertFrom-Json
    
    if ($result.Parameter.Value) {
        $keyLength = $result.Parameter.Value.Length
        $maskedKey = $result.Parameter.Value.Substring(0, [Math]::Min(4, $keyLength)) + "..." + 
                     $result.Parameter.Value.Substring([Math]::Max(0, $keyLength - 4))
        
        Write-Host "✅ API Key found successfully!" -ForegroundColor Green
        Write-Host "   Key length: $keyLength characters"
        Write-Host "   Masked key: $maskedKey"
        Write-Host "   Type: $($result.Parameter.Type)"
        Write-Host "   Last modified: $($result.Parameter.LastModifiedDate)"
    } else {
        Write-Host "❌ API Key parameter exists but has no value" -ForegroundColor Red
    }
} catch {
    Write-Host "❌ Failed to access API key: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "`nNow testing Lambda function with real API call..."
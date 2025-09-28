# Lambda Function Redeployment Script
# This script redeploys the overview-extractor Lambda function with latest code

Write-Host "🚀 Redeploying Lambda Function: fin-trade-extract-overview" -ForegroundColor Green

# Step 1: Navigate to the function directory
$functionPath = "lambda-functions\overview-extractor"
Push-Location $functionPath

try {
    # Step 2: Create deployment package
    Write-Host "📦 Creating deployment package..." -ForegroundColor Yellow
    
    # Remove old zip if exists
    if (Test-Path "function.zip") {
        Remove-Item "function.zip" -Force
    }
    
    # Add lambda function files to zip
    Compress-Archive -Path "lambda_function.py", "__init__.py" -DestinationPath "function.zip" -Force
    
    # Add common module files
    if (Test-Path "..\common\*.py") {
        Write-Host "📁 Adding common modules..." -ForegroundColor Yellow
        
        # Create temporary directory structure
        New-Item -ItemType Directory -Path "temp\common" -Force | Out-Null
        Copy-Item -Path "..\common\*.py" -Destination "temp\common\" -Force
        Copy-Item -Path "lambda_function.py", "__init__.py" -Destination "temp\" -Force
        
        # Remove old zip and create new one with proper structure
        Remove-Item "function.zip" -Force -ErrorAction SilentlyContinue
        
        Push-Location "temp"
        Compress-Archive -Path "*" -DestinationPath "..\function.zip" -Force
        Pop-Location
        
        # Clean up temp directory
        Remove-Item -Path "temp" -Recurse -Force
    }
    
    # Step 3: Update Lambda function code
    Write-Host "☁️ Uploading new code to AWS Lambda..." -ForegroundColor Yellow
    
    $result = aws lambda update-function-code --function-name "fin-trade-extract-overview" --zip-file "fileb://function.zip" --region "us-east-2" --output json
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Lambda function updated successfully!" -ForegroundColor Green
        
        # Parse and display result
        $resultJson = $result | ConvertFrom-Json
        Write-Host "   Function: $($resultJson.FunctionName)" -ForegroundColor Cyan
        Write-Host "   Runtime: $($resultJson.Runtime)" -ForegroundColor Cyan
        Write-Host "   Last Modified: $($resultJson.LastModified)" -ForegroundColor Cyan
        Write-Host "   Version: $($resultJson.Version)" -ForegroundColor Cyan
    } else {
        Write-Host "❌ Failed to update Lambda function" -ForegroundColor Red
        Write-Host "Error output: $result" -ForegroundColor Red
    }
    
    # Clean up deployment package
    Remove-Item "function.zip" -Force -ErrorAction SilentlyContinue
    
} catch {
    Write-Host "❌ Error during deployment: $($_.Exception.Message)" -ForegroundColor Red
} finally {
    Pop-Location
}

Write-Host "`n🎯 Next: Test the updated function" -ForegroundColor Magenta
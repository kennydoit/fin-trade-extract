# Lambda Function Redeployment Script - Simple Version
Write-Host "🚀 Redeploying Lambda Function: fin-trade-extract-overview" -ForegroundColor Green

# Navigate to function directory
Push-Location "lambda-functions\overview-extractor"

# Create deployment package
Write-Host "📦 Creating deployment package..." -ForegroundColor Yellow
Remove-Item "function.zip" -Force -ErrorAction SilentlyContinue

# Create temp directory with proper structure
New-Item -ItemType Directory -Path "temp\common" -Force | Out-Null
Copy-Item -Path "..\common\*.py" -Destination "temp\common\" -Force
Copy-Item -Path "lambda_function.py", "__init__.py" -Destination "temp\" -Force

# Create zip from temp directory
Push-Location "temp"
Compress-Archive -Path "*" -DestinationPath "..\function.zip" -Force
Pop-Location

# Clean up temp
Remove-Item -Path "temp" -Recurse -Force

# Upload to AWS
Write-Host "☁️ Uploading to AWS Lambda..." -ForegroundColor Yellow
aws lambda update-function-code --function-name "fin-trade-extract-overview" --zip-file "fileb://function.zip" --region "us-east-2"

# Clean up
Remove-Item "function.zip" -Force -ErrorAction SilentlyContinue
Pop-Location

Write-Host "✅ Deployment complete! Testing function..." -ForegroundColor Green
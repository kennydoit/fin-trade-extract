# Deploy Lambda with Dependencies
Write-Host "Deploying Lambda function with dependencies..." -ForegroundColor Green

$functionPath = "lambda-functions\overview-extractor"
$tempPath = "lambda-deploy-temp"

# Clean up any existing temp directory
if (Test-Path $tempPath) {
    Remove-Item -Path $tempPath -Recurse -Force
}

# Create temp deployment directory
New-Item -ItemType Directory -Path $tempPath -Force | Out-Null
New-Item -ItemType Directory -Path "$tempPath\common" -Force | Out-Null

# Copy function files
Copy-Item -Path "$functionPath\lambda_function.py" -Destination $tempPath -Force
Copy-Item -Path "$functionPath\__init__.py" -Destination $tempPath -Force

# Copy common modules
Copy-Item -Path "lambda-functions\common\*.py" -Destination "$tempPath\common\" -Force

# Install dependencies
Write-Host "Installing Python dependencies..." -ForegroundColor Yellow
pip install -r lambda-functions\requirements.txt -t $tempPath --no-deps --implementation cp --python-version 3.9 --only-binary=:all:

# Create deployment zip
Push-Location $tempPath
Compress-Archive -Path "*" -DestinationPath "..\function-with-deps.zip" -Force
Pop-Location

# Deploy to AWS
Write-Host "Deploying to AWS Lambda..." -ForegroundColor Yellow
aws lambda update-function-code --function-name "fin-trade-extract-overview" --zip-file "fileb://function-with-deps.zip" --region "us-east-2"

# Clean up
Remove-Item -Path $tempPath -Recurse -Force
Remove-Item -Path "function-with-deps.zip" -Force

Write-Host "Deployment completed!" -ForegroundColor Green
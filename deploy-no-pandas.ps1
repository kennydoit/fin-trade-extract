# Test Lambda without pandas - simple version
Write-Host "Creating Lambda deployment without pandas dependency..." -ForegroundColor Green

# Remove pandas import from s3_data_writer temporarily
$s3WriterPath = "lambda-functions\common\s3_data_writer.py"
$content = Get-Content $s3WriterPath -Raw

# Create a backup
Copy-Item $s3WriterPath "$s3WriterPath.backup" -Force

# Remove pandas import line
$modifiedContent = $content -replace "import pandas as pd`n", ""

# Write modified content
Set-Content -Path $s3WriterPath -Value $modifiedContent

# Create deployment
New-Item -ItemType Directory -Path "lambda-no-pandas" -Force | Out-Null
python -m pip install boto3 requests python-dateutil -t lambda-no-pandas

# Add function files
New-Item -ItemType Directory -Path "lambda-no-pandas\common" -Force | Out-Null
Copy-Item -Path "lambda-functions\overview-extractor\lambda_function.py" -Destination "lambda-no-pandas\" -Force
Copy-Item -Path "lambda-functions\overview-extractor\__init__.py" -Destination "lambda-no-pandas\" -Force  
Copy-Item -Path "lambda-functions\common\*.py" -Destination "lambda-no-pandas\common\" -Force

# Create zip
Push-Location "lambda-no-pandas"
Compress-Archive -Path "*" -DestinationPath "..\lambda-no-pandas.zip" -Force
Pop-Location

# Deploy
aws lambda update-function-code --function-name "fin-trade-extract-overview" --zip-file "fileb://lambda-no-pandas.zip" --region "us-east-2"

# Restore backup
Move-Item "$s3WriterPath.backup" $s3WriterPath -Force

# Clean up
Remove-Item -Path "lambda-no-pandas" -Recurse -Force
Remove-Item -Path "lambda-no-pandas.zip" -Force

Write-Host "Deployment complete - testing without pandas" -ForegroundColor Green
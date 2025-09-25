# AWS Lambda Deployment Scripts for Fin-Trade-Extract

## Prerequisites

1. **AWS CLI configured** with appropriate credentials for account `441691361831`
2. **PowerShell 5.1+** (Windows)
3. **Python 3.9+** installed
4. **AWS Tools for PowerShell** (optional but recommended)

## Account Configuration

- **AWS Account ID**: 441691361831
- **Region**: us-east-2
- **User**: SpecialKayEveryDay
- **S3 Bucket**: fin-trade-craft-landing

## Deployment Steps

### 1. Create IAM Execution Role

```powershell
# Create the IAM role for Lambda execution
aws iam create-role `
  --role-name FinTradeExtractLambdaRole `
  --assume-role-policy-document file://deployment/lambda-trust-policy.json `
  --description "Execution role for Fin Trade Extract Lambda functions"

# Attach the custom policy
aws iam put-role-policy `
  --role-name FinTradeExtractLambdaRole `
  --policy-name FinTradeExtractLambdaPolicy `
  --policy-document file://deployment/lambda-execution-role-policy.json

# Wait for role to be available
Start-Sleep -Seconds 10
```

### 2. Create Deployment Package

```powershell
# Navigate to the lambda functions directory
cd lambda-functions

# Create deployment package directory
if (Test-Path overview-extractor-deploy) { Remove-Item overview-extractor-deploy -Recurse -Force }
New-Item -ItemType Directory -Name overview-extractor-deploy

# Install dependencies
pip install -r requirements.txt --target overview-extractor-deploy/

# Copy function code
Copy-Item -Path overview-extractor/* -Destination overview-extractor-deploy/ -Recurse
Copy-Item -Path common/* -Destination overview-extractor-deploy/common/ -Recurse

# Create deployment ZIP
Compress-Archive -Path overview-extractor-deploy/* -DestinationPath overview-extractor-deploy.zip -Force

# Clean up temporary directory
Remove-Item overview-extractor-deploy -Recurse -Force
```

### 3. Create Lambda Function

```powershell
# Create the Lambda function
aws lambda create-function `
  --function-name fin-trade-extract-overview `
  --runtime python3.9 `
  --role arn:aws:iam::441691361831:role/FinTradeExtractLambdaRole `
  --handler lambda_function.lambda_handler `
  --zip-file fileb://overview-extractor-deploy.zip `
  --description "Extract Alpha Vantage company overview data to S3" `
  --timeout 900 `
  --memory-size 512 `
  --environment Variables='{AWS_DEFAULT_REGION=us-east-2}'
```

### 4. Configure Function Settings

```powershell
# Update function configuration if needed
aws lambda update-function-configuration `
  --function-name fin-trade-extract-overview `
  --timeout 900 `
  --memory-size 512 `
  --environment Variables='{AWS_DEFAULT_REGION=us-east-2,PYTHONPATH=/var/task}'
```

### 5. Set up EventBridge Rule (Optional)

```powershell
# Create EventBridge rule for scheduled execution
aws events put-rule `
  --name fin-trade-extract-overview-schedule `
  --schedule-expression "rate(1 hour)" `
  --description "Hourly trigger for overview extraction" `
  --state ENABLED

# Add Lambda permission for EventBridge
aws lambda add-permission `
  --function-name fin-trade-extract-overview `
  --statement-id allow-eventbridge `
  --action lambda:InvokeFunction `
  --principal events.amazonaws.com `
  --source-arn arn:aws:events:us-east-2:441691361831:rule/fin-trade-extract-overview-schedule

# Add Lambda target to the rule
aws events put-targets `
  --rule fin-trade-extract-overview-schedule `
  --targets Id=1,Arn=arn:aws:lambda:us-east-2:441691361831:function:fin-trade-extract-overview,Input='{\"symbols\":[\"AAPL\",\"MSFT\",\"GOOGL\",\"AMZN\",\"TSLA\"],\"batch_size\":50}'
```

## Update Deployment

### Update Function Code

```powershell
# Update the Lambda function code
cd lambda-functions

# Recreate deployment package
if (Test-Path overview-extractor-deploy.zip) { Remove-Item overview-extractor-deploy.zip }
if (Test-Path overview-extractor-deploy) { Remove-Item overview-extractor-deploy -Recurse -Force }

New-Item -ItemType Directory -Name overview-extractor-deploy
pip install -r requirements.txt --target overview-extractor-deploy/
Copy-Item -Path overview-extractor/* -Destination overview-extractor-deploy/ -Recurse
Copy-Item -Path common/* -Destination overview-extractor-deploy/common/ -Recurse
Compress-Archive -Path overview-extractor-deploy/* -DestinationPath overview-extractor-deploy.zip -Force
Remove-Item overview-extractor-deploy -Recurse -Force

# Update function
aws lambda update-function-code `
  --function-name fin-trade-extract-overview `
  --zip-file fileb://overview-extractor-deploy.zip
```

## Testing

### Test Locally (if needed)

```powershell
# Install requirements locally
pip install -r lambda-functions/requirements.txt

# Set environment variables for local testing
$env:AWS_DEFAULT_REGION = "us-east-2"

# Run test
cd lambda-functions/overview-extractor
python lambda_function.py
```

### Test Lambda Function

```powershell
# Test with AWS CLI
aws lambda invoke `
  --function-name fin-trade-extract-overview `
  --payload '{"symbols":["AAPL","MSFT"],"batch_size":10}' `
  response.json

# Check response
Get-Content response.json | ConvertFrom-Json | ConvertTo-Json -Depth 10
```

## Monitoring

### CloudWatch Logs

```powershell
# View recent logs
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/fin-trade-extract-overview"

# Get latest log stream
$logGroup = "/aws/lambda/fin-trade-extract-overview"
$latestStream = aws logs describe-log-streams --log-group-name $logGroup --order-by LastEventTime --descending --max-items 1 --query 'logStreams[0].logStreamName' --output text

# View logs
aws logs get-log-events --log-group-name $logGroup --log-stream-name $latestStream
```

### Function Metrics

```powershell
# Get function configuration
aws lambda get-function-configuration --function-name fin-trade-extract-overview

# Get function statistics
aws cloudwatch get-metric-statistics `
  --namespace AWS/Lambda `
  --metric-name Duration `
  --dimensions Name=FunctionName,Value=fin-trade-extract-overview `
  --start-time (Get-Date).AddDays(-1) `
  --end-time (Get-Date) `
  --period 3600 `
  --statistics Average,Maximum
```

## Cleanup

### Remove Resources

```powershell
# Delete Lambda function
aws lambda delete-function --function-name fin-trade-extract-overview

# Delete EventBridge rule and targets (if created)
aws events remove-targets --rule fin-trade-extract-overview-schedule --ids 1
aws events delete-rule --name fin-trade-extract-overview-schedule

# Delete IAM role and policy
aws iam delete-role-policy --role-name FinTradeExtractLambdaRole --policy-name FinTradeExtractLambdaPolicy
aws iam delete-role --role-name FinTradeExtractLambdaRole
```

## Cost Optimization Notes

- **Memory**: 512 MB should be sufficient for most batch sizes (adjust based on monitoring)
- **Timeout**: 15 minutes (900 seconds) allows processing ~1000 symbols with rate limiting
- **Frequency**: Hourly runs keep within Alpha Vantage limits (75 calls/min premium)
- **Batch Size**: 50-100 symbols per execution optimizes file sizes and Lambda costs

## Security Best Practices

1. **Least Privilege**: IAM role has minimal required permissions
2. **Encrypted Storage**: Parameter Store uses AWS SSM default encryption
3. **VPC**: Not required for this use case (public API access)
4. **Environment Variables**: Sensitive data stored in Parameter Store, not env vars
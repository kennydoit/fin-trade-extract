# Fin-Trade-Extract: AWS Lambda Functions

This repository contains AWS Lambda functions for extracting financial data from Alpha Vantage API and storing it in S3 for Snowflake ingestion via Snowpipe.

## Architecture Overview

The system follows a cost-optimized serverless architecture:

```
EventBridge (Schedule) → Lambda (Extract) → S3 (Landing) → Snowpipe → Snowflake
```

### Key Components

1. **AWS Lambda**: Serverless extraction functions with adaptive rate limiting
2. **Amazon S3**: Transient landing zone for compressed Parquet/JSON files  
3. **EventBridge**: Scheduled triggers for batch processing
4. **Parameter Store**: Secure API key management with KMS encryption
5. **Snowpipe**: Auto-ingestion from S3 to Snowflake tables

### Cost Target
- **AWS**: $0-$3/month (within Lambda free tier)
- **Snowflake**: $2-$5/month (storage + Snowpipe)
- **Total**: ~$3-$8/month (excluding Alpha Vantage subscription)

## Available Extractors

### ✅ Overview Extractor (`overview-extractor/`)
- **Function**: Company overview and fundamental metrics
- **API Endpoint**: Alpha Vantage OVERVIEW
- **Output**: Business data (Parquet) + audit trail (JSON)
- **Status**: Production ready

### 🚧 Planned Extractors (Future)
- Income Statement (`income-statement-extractor/`)
- Balance Sheet (`balance-sheet-extractor/`)  
- Cash Flow (`cash-flow-extractor/`)
- Time Series Daily (`time-series-extractor/`)
- Economic Indicators (`economic-indicators-extractor/`)

## Project Structure

```
lambda-functions/
├── common/                          # Shared utilities
│   ├── adaptive_rate_limiter.py     # Smart rate limiting
│   ├── parameter_store.py           # AWS SSM integration  
│   ├── s3_data_writer.py           # S3 output utilities
│   └── symbol_id_calculator.py     # Deterministic symbol IDs
├── overview-extractor/              # Overview extractor function
│   ├── lambda_function.py          # Main Lambda handler
│   ├── test-events.json            # Test event templates
│   └── README.md                   # Function documentation
├── requirements.txt                 # Python dependencies
└── __init__.py                     # Module initialization

deployment/                         # Deployment automation
├── deploy-overview-extractor.ps1   # PowerShell deployment script
├── lambda-execution-role-policy.json # IAM policy
├── lambda-trust-policy.json       # IAM trust relationship
└── README.md                      # Deployment guide
```

## Quick Start

### Prerequisites
- AWS CLI configured for account `441691361831` (us-east-2)
- PowerShell 5.1+ (Windows)
- Python 3.9+
- Alpha Vantage API key stored in Parameter Store

### 1. Store API Key
```powershell
aws ssm put-parameter `
  --name "/fin-trade-craft/alpha_vantage_key" `
  --value "YOUR_ALPHA_VANTAGE_KEY" `
  --type "SecureString" `
  --description "Alpha Vantage API key for fin-trade-extract"
```

### 2. Deploy Overview Extractor
```powershell
# Initial deployment
.\deployment\deploy-overview-extractor.ps1

# Update existing function  
.\deployment\deploy-overview-extractor.ps1 -Update

# Deploy and test
.\deployment\deploy-overview-extractor.ps1 -Update -Test
```

### 3. Test Function
```powershell
aws lambda invoke `
  --function-name fin-trade-extract-overview `
  --payload '{"symbols":["AAPL","MSFT","GOOGL"],"batch_size":10}' `
  response.json
```

## Configuration

### AWS Account Settings
- **Account ID**: 441691361831
- **Region**: us-east-2  
- **S3 Bucket**: fin-trade-craft-landing
- **Parameter**: `/fin-trade-craft/alpha_vantage_key`

### Lambda Configuration
- **Runtime**: Python 3.9
- **Memory**: 512 MB (cost-optimized)
- **Timeout**: 15 minutes (900 seconds)
- **Concurrent Executions**: Default (no reservation needed)

### Rate Limiting
- **Alpha Vantage Premium**: 75 calls/minute
- **Adaptive Delays**: 0.4-2.0 seconds based on processing overhead
- **Batch Size**: 50-100 symbols per execution (configurable)

## Security

### Least Privilege IAM
Lambda functions use minimal required permissions:
- CloudWatch Logs (create/write)
- S3 bucket access (read/write to `fin-trade-craft-landing`)
- Parameter Store (read `/fin-trade-craft/alpha_vantage_key`)
- KMS decrypt (SSM default key only)

### Data Protection  
- API keys stored encrypted in Parameter Store
- S3 data encrypted at rest (default)
- No sensitive data in environment variables
- VPC not required (public API access)

## Data Pipeline

### Input: Event Trigger
```json
{
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "batch_size": 100,
  "run_id": "optional-custom-id"
}
```

### Processing: Lambda Function
1. Retrieve API key from Parameter Store
2. Process symbols with adaptive rate limiting
3. Transform API responses to business schema
4. Create audit trail records

### Output: S3 Files
```
s3://fin-trade-craft-landing/
├── business_data/overview/YYYYMMDD_HHMMSS_runid.parquet
└── landing_data/overview/YYYYMMDD_HHMMSS_runid.json.gz
```

### Ingestion: Snowpipe (External)
Snowflake Snowpipe monitors S3 and auto-ingests new files into tables.

## Monitoring

### CloudWatch Metrics
- Lambda duration, memory usage, errors
- Custom metrics for API success rates
- S3 write success/failure events

### CloudWatch Logs
- Detailed processing logs per symbol
- Rate limiting adjustments
- Error details with full context

### Cost Monitoring
- Lambda GB-seconds and request count
- S3 storage and request charges
- Parameter Store access costs

## Development

### Local Testing
```powershell
# Set environment
$env:AWS_DEFAULT_REGION = "us-east-2"

# Install dependencies
pip install -r lambda-functions/requirements.txt

# Run test
cd lambda-functions/overview-extractor
python lambda_function.py
```

### Adding New Extractors
1. Copy `overview-extractor/` as template
2. Modify `lambda_function.py` for new API endpoint
3. Update business schema transformation
4. Create deployment script
5. Test with sample events

## Troubleshooting

### Common Issues

**API Key Access Denied**
```powershell
# Verify parameter exists
aws ssm describe-parameters --filters "Key=Name,Values=/fin-trade-craft/alpha_vantage_key"

# Test access
aws ssm get-parameter --name "/fin-trade-craft/alpha_vantage_key" --with-decryption
```

**S3 Write Failures** 
```powershell
# Verify bucket access
aws s3 ls s3://fin-trade-craft-landing/

# Test write permissions
aws s3 cp test.txt s3://fin-trade-craft-landing/_test/
```

**Lambda Timeouts**
- Reduce batch size in event payload
- Monitor CloudWatch logs for bottlenecks  
- Increase memory if needed (cost impact)

**Rate Limiting Issues**
- Check Alpha Vantage subscription limits
- Review rate limiter logs
- Adjust delay configuration if needed

## Cost Optimization

### Lambda Optimization
- Use 512 MB memory (optimal price/performance)
- Batch processing reduces invocation costs
- Stay within free tier (400k GB-seconds/month)

### S3 Optimization  
- Lifecycle rules delete files after 7-30 days
- Compressed formats reduce storage/transfer
- Batch writes minimize request charges

### Snowflake Optimization
- Large batch files reduce Snowpipe file processing costs
- Parquet format optimizes compression and query performance

## Support

### Documentation
- Function-specific README files
- Deployment guides with examples
- Architecture diagrams in prompts/

### Monitoring Tools
- CloudWatch dashboards
- Lambda insights for performance
- S3 access logs

### Error Handling
- Comprehensive exception handling
- Retry logic for transient failures
- Dead letter queues (future enhancement)

---

**Next Steps**: 
1. Deploy overview extractor and validate end-to-end pipeline
2. Set up Snowflake Snowpipe for auto-ingestion  
3. Create EventBridge schedule for regular execution
4. Add additional extractors (income statement, balance sheet, etc.)
5. Implement monitoring dashboard and alerts
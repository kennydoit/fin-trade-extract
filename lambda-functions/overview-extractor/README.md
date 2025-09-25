# Overview Extractor Lambda Function

AWS Lambda function that extracts company overview data from Alpha Vantage API and stores it in S3 for Snowflake ingestion via Snowpipe.

## Architecture

This Lambda function follows the cost-optimized pipeline architecture:

1. **EventBridge** triggers Lambda on schedule
2. **Lambda** processes symbols in batches, calling Alpha Vantage API
3. **S3** stores results as compressed Parquet (business data) and JSON (audit trail)
4. **Snowpipe** auto-ingests S3 files into Snowflake tables

## Features

### Data Processing
- **Batch Processing**: Processes multiple symbols per execution (configurable batch size)
- **Adaptive Rate Limiting**: Optimizes API call timing based on processing overhead
- **Data Transformation**: Maps Alpha Vantage fields to standardized business schema
- **Error Handling**: Robust error handling with status tracking

### AWS Integrations
- **Parameter Store**: Secure API key retrieval with KMS decryption
- **S3 Storage**: Compressed Parquet files for business data, JSON for audit trail
- **CloudWatch**: Comprehensive logging and monitoring
- **IAM**: Least-privilege security model

### Output Formats

#### Business Data (Parquet)
- Location: `s3://fin-trade-craft-landing/business_data/overview/`
- Format: Compressed Parquet files
- Schema: Standardized business fields for Snowflake ingestion

#### Landing Data (JSON)
- Location: `s3://fin-trade-craft-landing/landing_data/overview/`  
- Format: Compressed JSON files
- Content: Raw API responses for audit trail and reprocessing

## Event Format

```json
{
  "symbols": ["AAPL", "MSFT", "GOOGL"],  // Required: list of stock symbols
  "run_id": "custom-run-id-123",        // Optional: custom run identifier
  "batch_size": 100                      // Optional: maximum symbols to process
}
```

## Response Format

```json
{
  "statusCode": 200,
  "run_id": "uuid-generated-run-id",
  "symbols_processed": 3,
  "success_count": 2,
  "error_count": 1,
  "s3_files": {
    "business_data": {
      "files_written": 1,
      "total_records": 2,
      "files": [/* file details */]
    },
    "landing_data": {
      "files_written": 1,
      "total_records": 3,
      "files": [/* file details */]
    }
  },
  "rate_limiter_stats": {
    "total_calls": 3,
    "success_rate": "66.7%",
    "current_delay": "0.45s"
  }
}
```

## Configuration

### Environment Variables
- `AWS_DEFAULT_REGION`: AWS region (us-east-2)
- `PYTHONPATH`: Python module path (/var/task)

### Function Settings
- **Runtime**: Python 3.9
- **Memory**: 512 MB (optimized for batch processing)
- **Timeout**: 15 minutes (900 seconds)
- **Handler**: `lambda_function.lambda_handler`

### IAM Permissions
The Lambda execution role requires:
- CloudWatch Logs (create/write)
- S3 (read/write to fin-trade-craft-landing bucket)
- Systems Manager Parameter Store (read alpha_vantage_key)
- KMS (decrypt SSM parameters)

## Deployment

### Quick Deployment
```powershell
# Deploy new function
.\deployment\deploy-overview-extractor.ps1

# Update existing function
.\deployment\deploy-overview-extractor.ps1 -Update

# Deploy and test
.\deployment\deploy-overview-extractor.ps1 -Update -Test
```

### Manual Testing
```powershell
# Test specific event
aws lambda invoke `
  --function-name fin-trade-extract-overview `
  --payload '{"symbols":["AAPL","MSFT"],"batch_size":10}' `
  response.json
```

## Cost Analysis

Based on pipeline guidelines targeting $3-8/month total cost:

### Lambda Costs (Minimal)
- **Compute**: ~18 runs/day × 15 min/run × 512 MB = 270k GB-seconds/month
- **Free Tier**: Covers 400k GB-seconds → $0/month
- **Requests**: ~540 requests/month (covered by free tier)

### Data Transfer & Storage
- **S3**: Transient storage, lifecycle managed → pennies/month
- **API Calls**: Covered by Alpha Vantage subscription
- **Snowflake**: Storage and Snowpipe ingestion as per pipeline

## Monitoring

### CloudWatch Metrics
- Duration, Memory utilization, Error rate
- Custom metrics for API success rates
- S3 file creation events

### Logging
- Detailed processing logs per symbol
- Rate limiting adjustments
- S3 write confirmations
- Error details with context

## Symbol Processing

### Data Coverage
The function processes company overview data including:
- **Basic Info**: Name, description, sector, industry
- **Financial Metrics**: Market cap, P/E ratio, EPS, dividend yield
- **Technical Indicators**: Moving averages, 52-week high/low, beta
- **Analyst Data**: Target price, ratings

### Status Tracking
Each symbol gets a status:
- `success`: Data successfully extracted and transformed
- `empty`: No data returned from API
- `rate_limited`: API rate limit encountered
- `error`: Request or processing error
- `timeout`: Request timeout

## Error Handling

### API Errors
- Request timeouts with exponential backoff
- Rate limit detection and adaptive delay adjustment
- JSON parsing error recovery
- Invalid response handling

### AWS Errors
- S3 write failures with retry logic
- Parameter Store access errors
- IAM permission issues

### Data Quality
- Schema validation for business records
- Content hash calculation for change detection
- Landing record creation for audit trail

## Integration with Snowflake

### File Format
Parquet files are optimized for Snowpipe:
- Compressed with gzip
- Proper column types for Snowflake ingestion
- Batched to optimize file sizes (target 1-100MB)

### Snowpipe Setup (External)
Files written to S3 trigger Snowpipe ingestion:
```sql
-- Example Snowpipe (configured separately)
CREATE PIPE overview_pipe 
AUTO_INGEST = TRUE
AS COPY INTO overview_table
FROM @overview_stage
FILE_FORMAT = (TYPE = PARQUET COMPRESSION = GZIP);
```

## Performance Optimization

### Rate Limiting
- Adaptive delays based on processing overhead
- Automatic recovery from rate limits
- Per-extractor type optimization

### Batch Processing
- Configurable batch sizes (default 100 symbols)
- Memory-efficient processing
- Parallel-safe operations

### S3 Optimization
- Single write per batch (no individual file per symbol)
- Compressed formats reduce storage and transfer costs
- Proper metadata for Snowpipe processing

## Future Enhancements

1. **Multi-Table Support**: Extend to other Alpha Vantage endpoints
2. **Dead Letter Queue**: Handle persistent failures
3. **Metrics Dashboard**: Custom CloudWatch dashboard
4. **Auto-scaling**: Dynamic batch size based on remaining execution time
5. **Symbol Universe**: Integration with listing status for symbol discovery
# AWS Lambda Deployment Commands

## 🚀 Deploy Overview Extractor

### 1. Deploy Lambda Function
```bash
aws lambda create-function \
  --function-name fin-trade-overview-extractor \
  --runtime python3.9 \
  --role arn:aws:iam::901792597085:role/lambda-execution-role \
  --handler overview-extractor/lambda_function.lambda_handler \
  --zip-file fileb://overview-extractor.zip \
  --timeout 300 \
  --memory-size 512 \
  --environment Variables='{ALPHA_VANTAGE_API_KEY_PARAM="/fin-trade-extract/alpha-vantage-api-key"}'
```

### 2. Test in AWS Console
**Test Event JSON:**
```json
{
  "symbols": ["AAPL", "MSFT"],
  "run_id": "aws-test-001"
}
```

### 3. Expected Response
```json
{
  "statusCode": 200,
  "symbols_processed": 2,
  "success_count": 2,
  "s3_files": {
    "total_files": 4,
    "total_records": 2,
    "business_data": {
      "files": [
        {
          "bucket": "fin-trade-craft-landing",
          "key": "overview/overview_20250926_174032_6355aacc.csv",
          "record_count": 2,
          "file_size_bytes": 3918
        }
      ]
    }
  }
}
```

## 📋 Next Steps After Deployment

### 1. Check S3 Files
```bash
aws s3 ls s3://fin-trade-craft-landing/overview/
```

### 2. Configure S3 Bucket Notifications
Get Snowpipe SQS ARN from Snowflake:
```sql
DESCRIBE PIPE FIN_TRADE_EXTRACT.RAW.PIPE_OVERVIEW;
```

Update `aws/s3/bucket-notification-config.json` with the SQS ARN, then:
```bash
aws s3api put-bucket-notification-configuration \
  --bucket fin-trade-craft-landing \
  --notification-configuration file://../aws/s3/bucket-notification-config.json
```

### 3. Verify Snowflake Data Ingestion
```sql
-- Check data loaded
SELECT COUNT(*), MAX(CREATED_AT) 
FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW;

-- Test analytics view
SELECT * FROM FIN_TRADE_EXTRACT.ANALYTICS.VW_FINANCIAL_SUMMARY 
WHERE SYMBOL IN ('AAPL', 'MSFT')
LIMIT 5;
```

## 🎯 Success Criteria
- ✅ Lambda function executes without errors
- ✅ CSV files appear in S3 with correct format
- ✅ Snowpipe ingests data automatically
- ✅ Analytics views show real financial data

## ⚡ Ready to Go Live!
Your local tests passed perfectly. The pipeline is ready for production deployment!
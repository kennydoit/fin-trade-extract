# 🚀 End-to-End Testing Guide

## Prerequisites Checklist
- ✅ Snowflake infrastructure deployed (database, tables, Snowpipes)
- ✅ AWS S3 bucket created (`fin-trade-craft-landing`)
- ✅ AWS IAM role configured (`SnowflakeS3AccessRole`)
- ✅ Lambda functions updated to CSV format
- ✅ Alpha Vantage API key stored in Parameter Store

## Phase 1: Local Testing (Optional but Recommended)

### 1. Test Lambda Functions Locally
```bash
cd C:\Users\Kenrm\repositories\fin-trade-extract
python test_lambda_functions.py
```

**Expected Output:**
- ✅ Overview Extractor test passes
- ✅ Time Series Extractor test passes  
- ✅ Financial Statements Extractor test passes

## Phase 2: AWS Lambda Deployment

### 1. Package Overview Extractor
```bash
cd lambda-functions
# Create deployment package
tar -czf overview-extractor.zip overview-extractor/ common/ requirements.txt
```

### 2. Deploy to AWS Lambda
```bash
aws lambda create-function \
  --function-name fin-trade-overview-extractor \
  --runtime python3.9 \
  --role arn:aws:iam::901792597085:role/lambda-execution-role \
  --handler overview-extractor/lambda_function.lambda_handler \
  --zip-file fileb://overview-extractor.zip \
  --timeout 300 \
  --memory-size 512
```

### 3. Test in AWS Console
**Test Event JSON:**
```json
{
  "symbols": ["AAPL", "MSFT"],
  "run_id": "aws-test-001"
}
```

**Expected Lambda Response:**
```json
{
  "statusCode": 200,
  "symbols_processed": 2,
  "success_count": 2,
  "s3_files": {
    "total_files": 2,
    "total_records": 2
  }
}
```

## Phase 3: Verify S3 → Snowpipe → Snowflake Pipeline

### 1. Check S3 Files Created
```bash
aws s3 ls s3://fin-trade-craft-landing/overview/
```

**Expected Output:**
```
2025-09-26 14:30:52       1234 overview_20250926_143052_abc123.csv
```

### 2. Get Snowpipe SQS Queue ARN
```sql
-- In Snowflake, get the SQS queue ARN for notifications
DESCRIBE PIPE FIN_TRADE_EXTRACT.RAW.PIPE_OVERVIEW;
```

**Copy the `notification_channel` value (SQS ARN)**

### 3. Configure S3 Bucket Notifications
```bash
# Update bucket-notification-config.json with actual SQS ARN
# Then apply the configuration:
aws s3api put-bucket-notification-configuration \
  --bucket fin-trade-craft-landing \
  --notification-configuration file://aws/s3/bucket-notification-config.json
```

### 4. Verify Snowflake Data Ingestion
```sql
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- Check if data loaded into OVERVIEW table
SELECT 
    COUNT(*) as RECORD_COUNT,
    MAX(CREATED_AT) as LATEST_LOAD,
    COUNT(DISTINCT SYMBOL) as UNIQUE_SYMBOLS
FROM OVERVIEW
WHERE CREATED_AT >= CURRENT_DATE();

-- Sample data check
SELECT SYMBOL, NAME, SECTOR, MARKET_CAPITALIZATION 
FROM OVERVIEW 
WHERE SYMBOL IN ('AAPL', 'MSFT')
ORDER BY SYMBOL;
```

**Expected Results:**
- `RECORD_COUNT`: 2 (or number of symbols tested)
- `LATEST_LOAD`: Recent timestamp
- Data for AAPL and MSFT visible

## Phase 4: Test Analytics Views

### 1. Check Financial Summary View
```sql
USE SCHEMA ANALYTICS;

-- Test main analytics view
SELECT 
    SYMBOL,
    COMPANY_NAME,
    SECTOR,
    MARKET_CAPITALIZATION,
    PE_RATIO,
    DIVIDEND_YIELD
FROM VW_FINANCIAL_SUMMARY
WHERE SYMBOL IN ('AAPL', 'MSFT')
ORDER BY SYMBOL;
```

### 2. Check Sector Performance
```sql
SELECT 
    SECTOR,
    COMPANY_COUNT,
    AVG_MARKET_CAP,
    TOTAL_SECTOR_VALUE
FROM VW_SECTOR_PERFORMANCE
WHERE COMPANY_COUNT > 0
ORDER BY TOTAL_SECTOR_VALUE DESC
LIMIT 5;
```

## Phase 5: Deploy Additional Extractors

Once overview extractor works end-to-end:

### 1. Deploy Time Series Extractor
```bash
# Package and deploy time-series-extractor
# Test with same process
```

### 2. Deploy Financial Statements Extractor
```bash
# Package and deploy financial-statements-extractor  
# Test with: {"symbols": ["AAPL"], "statement_type": "INCOME_STATEMENT"}
```

## Success Criteria ✅

**Data Pipeline Working When:**
1. ✅ Lambda functions execute without errors
2. ✅ CSV files appear in S3 bucket with correct structure
3. ✅ Snowpipe ingests files automatically (check `COPY_HISTORY`)
4. ✅ Raw tables contain data with correct formatting
5. ✅ Analytics views return meaningful business insights

## Troubleshooting Common Issues

### Lambda Function Errors
- Check CloudWatch logs for detailed error messages
- Verify Alpha Vantage API key in Parameter Store
- Ensure IAM permissions for S3 write access

### Snowpipe Not Triggering
- Verify S3 bucket notifications are configured correctly
- Check SQS queue ARN matches Snowpipe notification channel
- Monitor Snowpipe status: `SELECT SYSTEM$PIPE_STATUS('PIPE_NAME');`

### No Data in Tables
- Check `COPY_HISTORY` for load errors
- Verify CSV file format matches table schema
- Check file paths match Snowpipe stage definitions

## Monitoring Commands

```sql
-- Monitor Snowpipe status
SELECT SYSTEM$PIPE_STATUS('PIPE_OVERVIEW');

-- Check recent copy operations
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'OVERVIEW',
  START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));

-- View table statistics
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'RAW'
ORDER BY TABLE_NAME;
```
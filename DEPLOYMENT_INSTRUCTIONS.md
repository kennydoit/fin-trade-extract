# Deployment Instructions for Lambda Functions

## 1. Package and Deploy Overview Extractor

### Create deployment package:
```bash
cd lambda-functions/
zip -r overview-extractor.zip overview-extractor/ common/ requirements.txt
```

### Deploy to AWS Lambda:
```bash
aws lambda create-function \
  --function-name fin-trade-overview-extractor \
  --runtime python3.9 \
  --role arn:aws:iam::YOUR_ACCOUNT:role/lambda-execution-role \
  --handler overview-extractor.lambda_function.lambda_handler \
  --zip-file fileb://overview-extractor.zip \
  --timeout 300 \
  --memory-size 512
```

## 2. Test Lambda Function

### Test Event (JSON):
```json
{
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "run_id": "test-run-001"
}
```

### Expected Results:
- CSV files created in `s3://fin-trade-craft-landing/overview/`
- Snowpipe automatically ingests CSV files
- Data appears in `FIN_TRADE_EXTRACT.RAW.OVERVIEW` table
- Analytics views populate with data

## 3. Verify Data Pipeline

### Check S3 Bucket:
```bash
aws s3 ls s3://fin-trade-craft-landing/overview/
```

### Check Snowflake Tables:
```sql
-- Check if data loaded
SELECT COUNT(*), MAX(CREATED_AT) 
FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW;

-- Test analytics view
SELECT * FROM FIN_TRADE_EXTRACT.ANALYTICS.VW_FINANCIAL_SUMMARY 
LIMIT 5;
```

## 4. Deploy Additional Extractors

Once overview extractor works:
- Deploy `time-series-extractor` 
- Deploy `financial-statements-extractor`
- Test with different statement types: INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW

## 5. Monitor Snowpipe Status

```sql
-- Check Snowpipe ingestion status
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'OVERVIEW',
  START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));
```
# Snowflake Setup Guide for fin-trade-extract Pipeline

Complete step-by-step guide to configure Snowflake for your serverless data pipeline.

## Prerequisites

- Snowflake account with ACCOUNTADMIN privileges
- AWS CLI configured with access to your account (441691361831)
- S3 bucket `fin-trade-craft-landing` already created

## Step 1: Initial Snowflake Setup

### 1.1 Run Database Setup
```sql
-- Execute in Snowflake Web UI or SnowSQL
-- File: snowflake/schema/01_database_setup.sql
```

This creates:
- `FIN_TRADE_EXTRACT` database
- `RAW`, `PROCESSED`, `ANALYTICS` schemas
- `FIN_TRADE_WH` warehouse (X-Small, cost-optimized)
- File formats for Parquet, JSON, and CSV

### 1.2 Create Tables
```sql
-- Execute: snowflake/schema/02_tables.sql
```

Creates all tables for:
- Company metadata (`LISTING_STATUS`, `OVERVIEW`)
- Financial statements (`INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW`)
- Market data (`TIME_SERIES_DAILY_ADJUSTED`)
- Alternative data (`COMMODITIES`, `ECONOMIC_INDICATORS`, `INSIDER_TRANSACTIONS`, `EARNINGS_CALL_TRANSCRIPTS`)

## Step 2: AWS IAM Setup for Snowflake

### 2.1 Create Snowflake Storage Integration (Placeholder)
```sql
-- Execute: snowflake/schema/03_storage_integration.sql
-- This will initially fail - that's expected
```

### 2.2 Get Snowflake IAM Requirements
```sql
-- In Snowflake, run:
DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;
```

Note down:
- `STORAGE_AWS_IAM_USER_ARN`
- `STORAGE_AWS_EXTERNAL_ID`

### 2.3 Create IAM Role in AWS
```bash
# Step 1: Create trust policy file
cat > /tmp/snowflake-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "YOUR_STORAGE_AWS_IAM_USER_ARN_FROM_ABOVE"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "YOUR_STORAGE_AWS_EXTERNAL_ID_FROM_ABOVE"
                }
            }
        }
    ]
}
EOF

# Step 2: Create the IAM role
aws iam create-role \
    --role-name SnowflakeS3AccessRole \
    --assume-role-policy-document file:///tmp/snowflake-trust-policy.json

# Step 3: Attach S3 access policy
aws iam put-role-policy \
    --role-name SnowflakeS3AccessRole \
    --policy-name SnowflakeS3Access \
    --policy-document file://deployment/snowflake-s3-policy.json

# Step 4: Get the role ARN
aws iam get-role --role-name SnowflakeS3AccessRole --query 'Role.Arn' --output text
```

### 2.4 Update Storage Integration
```sql
-- Update the storage integration with the actual IAM role ARN
ALTER STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::441691361831:role/SnowflakeS3AccessRole';
```

## Step 3: Configure Snowpipe

### 3.1 Create Snowpipes
```sql
-- Execute: snowflake/schema/04_snowpipe.sql
```

### 3.2 Get SQS Queue Information
```sql
SHOW PIPES IN SCHEMA RAW;
-- Note down the NOTIFICATION_CHANNEL for each pipe
```

### 3.3 Configure S3 Bucket Notifications
For each Snowpipe, configure S3 bucket notifications:

```bash
# Create S3 notification configuration
cat > /tmp/s3-notification-config.json << EOF
{
    "QueueConfigurations": [
        {
            "Id": "OverviewPipeNotification",
            "QueueArn": "YOUR_OVERVIEW_PIPE_NOTIFICATION_CHANNEL",
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {
                            "Name": "prefix",
                            "Value": "overview/"
                        }
                    ]
                }
            }
        },
        {
            "Id": "TimeSeriesPipeNotification",
            "QueueArn": "YOUR_TIME_SERIES_PIPE_NOTIFICATION_CHANNEL",
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {
                            "Name": "prefix",
                            "Value": "time-series/"
                        }
                    ]
                }
            }
        }
    ]
}
EOF

# Apply the notification configuration
aws s3api put-bucket-notification-configuration \
    --bucket fin-trade-craft-landing \
    --notification-configuration file:///tmp/s3-notification-config.json
```

## Step 4: Create Analytical Views
```sql
-- Execute: snowflake/schema/05_views.sql
```

This creates business intelligence views:
- `COMPANIES`: Master company view
- `FINANCIAL_SUMMARY`: Key financial metrics
- `SECTOR_PERFORMANCE`: Industry analysis
- `STOCK_PERFORMANCE`: Price performance metrics
- `INSIDER_TRADING_SUMMARY`: Insider activity analysis
- `DATA_FRESHNESS`: Data monitoring

## Step 5: Validation and Testing

### 5.1 Test Storage Integration
```sql
-- List files in S3 (should work after Lambda generates data)
LIST @OVERVIEW_STAGE;
LIST @TIME_SERIES_STAGE;
```

### 5.2 Test Manual Data Loading
```sql
-- Test copying data manually (after Lambda generates files)
COPY INTO OVERVIEW
FROM @OVERVIEW_STAGE
FILE_FORMAT = PARQUET_FORMAT
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';
```

### 5.3 Run Data Quality Checks
```sql
-- Execute: snowflake/maintenance/data_quality_checks.sql
```

### 5.4 Monitor Performance
```sql
-- Execute: snowflake/maintenance/performance_monitoring.sql
```

## Step 6: Cost Optimization

### 6.1 Configure Auto-Suspend (Already Done)
- Warehouse auto-suspends after 60 seconds
- Uses X-Small warehouse (1 credit/hour when active)

### 6.2 Monitor Costs
```sql
-- Weekly cost monitoring
SELECT 
    'Weekly Cost Summary' AS metric,
    SUM(credits_used) * 2.0 AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP());
```

### 6.3 Set Resource Monitors (Optional)
```sql
-- Create resource monitor for cost control
CREATE RESOURCE MONITOR FIN_TRADE_MONITOR WITH
    CREDIT_QUOTA = 10              -- $20/month limit
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80 PERCENT DO SUSPEND_IMMEDIATE
        ON 90 PERCENT DO NOTIFY;

-- Apply to warehouse
ALTER WAREHOUSE FIN_TRADE_WH SET RESOURCE_MONITOR = 'FIN_TRADE_MONITOR';
```

## Troubleshooting

### Common Issues

1. **Storage Integration Fails**
   - Verify IAM role ARN is correct
   - Check trust policy includes correct Snowflake IAM user
   - Ensure S3 bucket policy allows Snowflake access

2. **Snowpipe Not Loading Data**
   - Check S3 bucket notifications are configured
   - Verify file formats match Lambda output
   - Check Snowpipe error logs: `SELECT * FROM TABLE(VALIDATE_PIPE_LOAD(...))`

3. **High Costs**
   - Check warehouse auto-suspend settings
   - Monitor query performance
   - Review Snowpipe file sizes (larger files are more efficient)

### Monitoring Commands

```sql
-- Check Snowpipe status
SELECT SYSTEM$PIPE_STATUS('FIN_TRADE_EXTRACT.RAW.OVERVIEW_PIPE');

-- View recent loads
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE PIPE_NAME LIKE 'FIN_TRADE_EXTRACT.RAW.%'
ORDER BY LAST_LOAD_TIME DESC
LIMIT 10;

-- Monitor warehouse usage
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'FIN_TRADE_WH'
ORDER BY START_TIME DESC
LIMIT 10;
```

## Expected Costs

Based on the $3-8/month target:

- **Compute (Warehouse)**: ~$1-3/month (X-Small, auto-suspend)
- **Snowpipe**: ~$1-2/month (pay-per-use ingestion)
- **Storage**: ~$1-3/month (depends on data volume)
- **Total**: $3-8/month target ✅

## Next Steps

1. Complete AWS IAM role setup
2. Run all SQL scripts in order
3. Test with Lambda function data
4. Set up monitoring alerts
5. Configure resource monitors for cost control

The Snowflake component is now fully configured for your serverless data pipeline!
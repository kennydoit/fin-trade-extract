# Step 3: Configure S3 Storage Integration

This step connects Snowflake to your S3 bucket for automated data ingestion.

## Prerequisites
- AWS CLI installed and configured
- Snowflake account with ACCOUNTADMIN privileges

## Step-by-Step Process

### 1. Run Snowflake Storage Integration Script
Copy and paste the contents of `snowflake/schema/03_storage_integration.sql` into your Snowflake worksheet and run it.

**Important**: The storage integration will initially fail because the IAM role doesn't exist yet.

### 2. Get Snowflake IAM Details
After running the script, execute this command in Snowflake:
```sql
DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;
```

Look for these two values in the output:
- `STORAGE_AWS_IAM_USER_ARN`
- `STORAGE_AWS_EXTERNAL_ID`

### 3. Update Trust Policy File
Edit `aws/iam/snowflake-trust-policy.json` and replace:
- `SNOWFLAKE_USER_ARN` with the `STORAGE_AWS_IAM_USER_ARN` value
- `SNOWFLAKE_EXTERNAL_ID` with the `STORAGE_AWS_EXTERNAL_ID` value

### 4. Create IAM Role in AWS
Run these AWS CLI commands:

```bash
# Create the IAM role
aws iam create-role --role-name SnowflakeS3AccessRole --assume-role-policy-document file://aws/iam/snowflake-trust-policy.json

# Attach the S3 access policy
aws iam put-role-policy --role-name SnowflakeS3AccessRole --policy-name SnowflakeS3Access --policy-document file://aws/iam/snowflake-s3-policy.json

# Get the role ARN (you'll need this)
aws iam get-role --role-name SnowflakeS3AccessRole --query 'Role.Arn' --output text
```

### 5. Update Storage Integration
Update the storage integration in Snowflake with the correct role ARN:
```sql
ALTER STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::441691361831:role/SnowflakeS3AccessRole';
```

### 6. Verify Configuration
Test the storage integration:
```sql
DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;
SHOW STAGES IN SCHEMA RAW;
```

## What This Creates
- **Storage Integration**: Connects Snowflake to your S3 bucket
- **9 External Stages**: One for each data type (overview, time-series, financials, etc.)
- **IAM Role**: AWS role that allows Snowflake to access your S3 bucket

## Cost Impact
- **Storage Integration**: Free
- **External Stages**: Free
- **IAM Role**: Free

## Troubleshooting
- If you get "Access Denied" errors, verify the IAM role trust policy
- If stages don't work, check the S3 bucket permissions
- Make sure your AWS CLI is configured for account 441691361831
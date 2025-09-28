#!/bin/bash
# =============================================================================
# Snowflake CSV Data Loading Setup Script
# Run this step by step to set up the complete data pipeline
# =============================================================================

echo "🚀 Setting up Snowflake CSV Data Loading Pipeline..."

# =============================================================================
# STEP 1: Verify prerequisites
# =============================================================================

echo "Step 1: Verifying prerequisites..."

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI not found. Please install AWS CLI and configure credentials."
    exit 1
fi

echo "✅ AWS CLI found"

# Check S3 bucket access
if aws s3 ls s3://fin-trade-craft-landing/ &> /dev/null; then
    echo "✅ S3 bucket access confirmed"
else
    echo "❌ Cannot access S3 bucket fin-trade-craft-landing"
    exit 1
fi

# =============================================================================
# STEP 2: Manual Snowflake Setup Instructions
# =============================================================================

echo ""
echo "📋 MANUAL STEPS - Please execute these in Snowflake Web UI:"
echo ""
echo "1. Log in to your Snowflake account as ACCOUNTADMIN"
echo "2. Execute the following SQL files in order:"
echo "   a. snowflake/schema/01_database_setup_csv.sql"
echo "   b. snowflake/schema/02_tables_csv.sql" 
echo "   c. snowflake/schema/03_storage_integration_csv.sql"
echo ""
echo "3. After step 2c, run this command to get AWS credentials:"
echo "   DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;"
echo ""
echo "4. Note down these values:"
echo "   - STORAGE_AWS_IAM_USER_ARN"
echo "   - STORAGE_AWS_EXTERNAL_ID"
echo ""

read -p "Press Enter after you've completed the Snowflake setup and noted the credentials..."

# =============================================================================
# STEP 3: Get Snowflake credentials from user
# =============================================================================

echo ""
echo "Please provide the Snowflake storage integration credentials:"
read -p "Enter STORAGE_AWS_IAM_USER_ARN: " STORAGE_AWS_IAM_USER_ARN
read -p "Enter STORAGE_AWS_EXTERNAL_ID: " STORAGE_AWS_EXTERNAL_ID

# =============================================================================
# STEP 4: Create AWS IAM Role
# =============================================================================

echo ""
echo "Step 4: Creating AWS IAM Role for Snowflake..."

# Update trust policy with actual values
sed "s|REPLACE_WITH_STORAGE_AWS_IAM_USER_ARN|$STORAGE_AWS_IAM_USER_ARN|g" deployment/snowflake-trust-policy.json > deployment/snowflake-trust-policy-updated.json
sed -i "s|REPLACE_WITH_STORAGE_AWS_EXTERNAL_ID|$STORAGE_AWS_EXTERNAL_ID|g" deployment/snowflake-trust-policy-updated.json

# Create IAM role
echo "Creating IAM role..."
aws iam create-role \
    --role-name SnowflakeS3AccessRole \
    --assume-role-policy-document file://deployment/snowflake-trust-policy-updated.json \
    --description "Role for Snowflake to access S3 bucket fin-trade-craft-landing"

if [ $? -eq 0 ]; then
    echo "✅ IAM role created successfully"
else
    echo "⚠️  Role may already exist, continuing..."
fi

# Attach policy to role
echo "Attaching S3 access policy..."
aws iam put-role-policy \
    --role-name SnowflakeS3AccessRole \
    --policy-name SnowflakeS3Access \
    --policy-document file://deployment/snowflake-s3-policy.json

if [ $? -eq 0 ]; then
    echo "✅ S3 policy attached successfully"
else
    echo "❌ Failed to attach S3 policy"
    exit 1
fi

# =============================================================================
# STEP 5: Update Snowflake Storage Integration
# =============================================================================

ROLE_ARN="arn:aws:iam::441691361831:role/SnowflakeS3AccessRole"

echo ""
echo "Step 5: Update Snowflake Storage Integration"
echo "Run this SQL command in Snowflake:"
echo ""
echo "ALTER STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION"
echo "SET STORAGE_AWS_ROLE_ARN = '$ROLE_ARN';"
echo ""

read -p "Press Enter after updating the storage integration in Snowflake..."

# =============================================================================
# STEP 6: Create Snowpipes
# =============================================================================

echo ""
echo "Step 6: Create Snowpipes in Snowflake"
echo "Execute this SQL file in Snowflake:"
echo "   snowflake/schema/04_snowpipes_csv.sql"
echo ""
echo "After creating pipes, run this query to get notification channels:"
echo ""
echo "SELECT PIPE_NAME, NOTIFICATION_CHANNEL"
echo "FROM TABLE(INFORMATION_SCHEMA.PIPES)"
echo "WHERE PIPE_SCHEMA = 'RAW'"
echo "ORDER BY PIPE_NAME;"
echo ""

read -p "Press Enter after creating Snowpipes and noting the notification channels..."

# =============================================================================
# STEP 7: Get Snowpipe notification channels
# =============================================================================

echo ""
echo "Please provide the Snowpipe notification channel ARNs:"
read -p "Enter OVERVIEW_PIPE notification channel: " OVERVIEW_PIPE_ARN
read -p "Enter TIME_SERIES_PIPE notification channel: " TIME_SERIES_PIPE_ARN
read -p "Enter INCOME_STATEMENT_PIPE notification channel: " INCOME_STATEMENT_PIPE_ARN
read -p "Enter BALANCE_SHEET_PIPE notification channel: " BALANCE_SHEET_PIPE_ARN
read -p "Enter CASH_FLOW_PIPE notification channel: " CASH_FLOW_PIPE_ARN

# =============================================================================
# STEP 8: Create S3 Bucket Notification Configuration
# =============================================================================

echo ""
echo "Step 8: Creating S3 bucket notification configuration..."

# Create bucket notification configuration file
cat > deployment/bucket-notifications.json << EOF
{
    "QueueConfigurations": [
        {
            "Id": "overview-pipe-notification",
            "QueueArn": "$OVERVIEW_PIPE_ARN",
            "Events": ["s3:ObjectCreated:Put", "s3:ObjectCreated:Post"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {"Name": "prefix", "Value": "overview/"},
                        {"Name": "suffix", "Value": ".csv"}
                    ]
                }
            }
        },
        {
            "Id": "time-series-pipe-notification", 
            "QueueArn": "$TIME_SERIES_PIPE_ARN",
            "Events": ["s3:ObjectCreated:Put", "s3:ObjectCreated:Post"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {"Name": "prefix", "Value": "time-series/"},
                        {"Name": "suffix", "Value": ".csv"}
                    ]
                }
            }
        },
        {
            "Id": "income-statement-pipe-notification",
            "QueueArn": "$INCOME_STATEMENT_PIPE_ARN",
            "Events": ["s3:ObjectCreated:Put", "s3:ObjectCreated:Post"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {"Name": "prefix", "Value": "income-statement/"},
                        {"Name": "suffix", "Value": ".csv"}
                    ]
                }
            }
        },
        {
            "Id": "balance-sheet-pipe-notification",
            "QueueArn": "$BALANCE_SHEET_PIPE_ARN",
            "Events": ["s3:ObjectCreated:Put", "s3:ObjectCreated:Post"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {"Name": "prefix", "Value": "balance-sheet/"},
                        {"Name": "suffix", "Value": ".csv"}
                    ]
                }
            }
        },
        {
            "Id": "cash-flow-pipe-notification",
            "QueueArn": "$CASH_FLOW_PIPE_ARN",
            "Events": ["s3:ObjectCreated:Put", "s3:ObjectCreated:Post"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {"Name": "prefix", "Value": "cash-flow/"},
                        {"Name": "suffix", "Value": ".csv"}
                    ]
                }
            }
        }
    ]
}
EOF

# Apply notification configuration
echo "Applying S3 bucket notification configuration..."
aws s3api put-bucket-notification-configuration \
    --bucket fin-trade-craft-landing \
    --notification-configuration file://deployment/bucket-notifications.json

if [ $? -eq 0 ]; then
    echo "✅ S3 bucket notifications configured successfully"
else
    echo "❌ Failed to configure S3 bucket notifications"
    exit 1
fi

# =============================================================================
# STEP 9: Test the Pipeline
# =============================================================================

echo ""
echo "Step 9: Testing the pipeline..."

# Test Lambda function
echo "Running Lambda function to generate test data..."
aws lambda invoke \
    --function-name fin-trade-overview-extractor \
    --payload '{"symbols": ["MSFT"], "run_id": "snowflake-test-001"}' \
    test-response.json

if [ $? -eq 0 ]; then
    echo "✅ Lambda function executed"
    echo "Response:"
    cat test-response.json
    echo ""
else
    echo "❌ Lambda function failed"
fi

# =============================================================================
# FINAL INSTRUCTIONS
# =============================================================================

echo ""
echo "🎉 Setup Complete! Next Steps:"
echo ""
echo "1. Check Snowflake for automatic data loading:"
echo "   SELECT COUNT(*) FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW;"
echo ""
echo "2. Monitor pipe status:"
echo "   SELECT SYSTEM\$PIPE_STATUS('OVERVIEW_PIPE');"
echo ""
echo "3. View loaded data:"
echo "   SELECT * FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW ORDER BY LOADED_AT DESC LIMIT 5;"
echo ""
echo "4. Set up additional Lambda functions for time-series, financial statements"
echo ""
echo "5. Create analytics views for business intelligence"
echo ""
echo "📋 Files created:"
echo "   - deployment/snowflake-trust-policy-updated.json"
echo "   - deployment/bucket-notifications.json"
echo "   - test-response.json"
echo ""
echo "✅ Pipeline Status: Lambda → S3 → Snowpipe → Snowflake"
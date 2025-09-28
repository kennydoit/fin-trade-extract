# =============================================================================
# Snowflake CSV Data Loading Setup Script (PowerShell) - Fixed Version
# Run this step by step to set up the complete data pipeline
# =============================================================================

Write-Host "Setting up Snowflake CSV Data Loading Pipeline..." -ForegroundColor Green

# =============================================================================
# STEP 1: Verify prerequisites
# =============================================================================

Write-Host "Step 1: Verifying prerequisites..." -ForegroundColor Yellow

# Check AWS CLI
try {
    aws --version | Out-Null
    Write-Host "AWS CLI found" -ForegroundColor Green
}
catch {
    Write-Host "AWS CLI not found. Please install AWS CLI and configure credentials." -ForegroundColor Red
    exit 1
}

# Check S3 bucket access
try {
    aws s3 ls s3://fin-trade-craft-landing/ | Out-Null
    Write-Host "S3 bucket access confirmed" -ForegroundColor Green
}
catch {
    Write-Host "Cannot access S3 bucket fin-trade-craft-landing" -ForegroundColor Red
    exit 1
}

# =============================================================================
# STEP 2: Manual Snowflake Setup Instructions
# =============================================================================

Write-Host ""
Write-Host "MANUAL STEPS - Please execute these in Snowflake Web UI:" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Log in to your Snowflake account as ACCOUNTADMIN"
Write-Host "2. Execute the following SQL files in order:"
Write-Host "   a. snowflake/schema/01_database_setup_csv.sql"
Write-Host "   b. snowflake/schema/02_tables_csv.sql" 
Write-Host "   c. snowflake/schema/03_storage_integration_csv.sql"
Write-Host ""
Write-Host "3. After step 2c, run this command to get AWS credentials:"
Write-Host "   DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;" -ForegroundColor White
Write-Host ""
Write-Host "4. Note down these values:" -ForegroundColor Cyan
Write-Host "   - STORAGE_AWS_IAM_USER_ARN"
Write-Host "   - STORAGE_AWS_EXTERNAL_ID"
Write-Host ""

Read-Host "Press Enter after you've completed the Snowflake setup and noted the credentials"

# =============================================================================
# STEP 3: Get Snowflake credentials from user
# =============================================================================

Write-Host ""
Write-Host "Please provide the Snowflake storage integration credentials:" -ForegroundColor Yellow
$STORAGE_AWS_IAM_USER_ARN = Read-Host "Enter STORAGE_AWS_IAM_USER_ARN"
$STORAGE_AWS_EXTERNAL_ID = Read-Host "Enter STORAGE_AWS_EXTERNAL_ID"

# =============================================================================
# STEP 4: Create AWS IAM Role
# =============================================================================

Write-Host ""
Write-Host "Step 4: Creating AWS IAM Role for Snowflake..." -ForegroundColor Yellow

# Update trust policy with actual values
$trustPolicyContent = Get-Content "deployment/snowflake-trust-policy.json" -Raw
$trustPolicyContent = $trustPolicyContent -replace "REPLACE_WITH_STORAGE_AWS_IAM_USER_ARN", $STORAGE_AWS_IAM_USER_ARN
$trustPolicyContent = $trustPolicyContent -replace "REPLACE_WITH_STORAGE_AWS_EXTERNAL_ID", $STORAGE_AWS_EXTERNAL_ID
$trustPolicyContent | Out-File "deployment/snowflake-trust-policy-updated.json" -Encoding utf8

# Create IAM role
Write-Host "Creating IAM role..." -ForegroundColor Blue
try {
    aws iam create-role --role-name SnowflakeS3AccessRole --assume-role-policy-document file://deployment/snowflake-trust-policy-updated.json --description "Role for Snowflake to access S3 bucket fin-trade-craft-landing"
    Write-Host "IAM role created successfully" -ForegroundColor Green
}
catch {
    Write-Host "Role may already exist, continuing..." -ForegroundColor Yellow
}

# Attach policy to role
Write-Host "Attaching S3 access policy..." -ForegroundColor Blue
try {
    aws iam put-role-policy --role-name SnowflakeS3AccessRole --policy-name SnowflakeS3Access --policy-document file://deployment/snowflake-s3-policy.json
    Write-Host "S3 policy attached successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to attach S3 policy" -ForegroundColor Red
    exit 1
}

# =============================================================================
# STEP 5: Update Snowflake Storage Integration
# =============================================================================

$ROLE_ARN = "arn:aws:iam::441691361831:role/SnowflakeS3AccessRole"

Write-Host ""
Write-Host "Step 5: Update Snowflake Storage Integration" -ForegroundColor Yellow
Write-Host "Run this SQL command in Snowflake:" -ForegroundColor Cyan
Write-Host ""
Write-Host "ALTER STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION" -ForegroundColor White
Write-Host "SET STORAGE_AWS_ROLE_ARN = '$ROLE_ARN';" -ForegroundColor White
Write-Host ""

Read-Host "Press Enter after updating the storage integration in Snowflake"

# =============================================================================
# STEP 6: Create Snowpipes
# =============================================================================

Write-Host ""
Write-Host "Step 6: Create Snowpipes in Snowflake" -ForegroundColor Yellow
Write-Host "Execute this SQL file in Snowflake:" -ForegroundColor Cyan
Write-Host "   snowflake/schema/04_snowpipes_csv.sql" -ForegroundColor White
Write-Host ""
Write-Host "After creating pipes, run these commands to get notification channels:" -ForegroundColor Cyan
Write-Host ""
Write-Host "DESC PIPE OVERVIEW_PIPE;" -ForegroundColor White
Write-Host "DESC PIPE TIME_SERIES_PIPE;" -ForegroundColor White
Write-Host "DESC PIPE INCOME_STATEMENT_PIPE;" -ForegroundColor White
Write-Host "DESC PIPE BALANCE_SHEET_PIPE;" -ForegroundColor White
Write-Host "DESC PIPE CASH_FLOW_PIPE;" -ForegroundColor White
Write-Host ""
Write-Host "Look for the 'notification_channel' field in each output" -ForegroundColor Cyan
Write-Host ""

Read-Host "Press Enter after creating Snowpipes and noting the notification channels"

# =============================================================================
# STEP 7: Get Snowpipe notification channels
# =============================================================================

Write-Host ""
Write-Host "Please provide the Snowpipe notification channel ARNs:" -ForegroundColor Yellow
$OVERVIEW_PIPE_ARN = Read-Host "Enter OVERVIEW_PIPE notification channel"
$TIME_SERIES_PIPE_ARN = Read-Host "Enter TIME_SERIES_PIPE notification channel (or press Enter to skip)"
$INCOME_STATEMENT_PIPE_ARN = Read-Host "Enter INCOME_STATEMENT_PIPE notification channel (or press Enter to skip)"
$BALANCE_SHEET_PIPE_ARN = Read-Host "Enter BALANCE_SHEET_PIPE notification channel (or press Enter to skip)"
$CASH_FLOW_PIPE_ARN = Read-Host "Enter CASH_FLOW_PIPE notification channel (or press Enter to skip)"

# =============================================================================
# STEP 8: Create S3 Bucket Notification Configuration
# =============================================================================

Write-Host ""
Write-Host "Step 8: Creating S3 bucket notification configuration..." -ForegroundColor Yellow

# Build notification configurations array
$queueConfigurations = @()

# Always add overview pipe
$queueConfigurations += @"
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
        }
"@

# Add other pipes if provided
if ($TIME_SERIES_PIPE_ARN) {
    $queueConfigurations += @"
        ,{
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
        }
"@
}

# Create bucket notification configuration file
$notificationConfig = @"
{
    "QueueConfigurations": [
$($queueConfigurations -join "")
    ]
}
"@

$notificationConfig | Out-File "deployment/bucket-notifications.json" -Encoding utf8

Write-Host "Created notification configuration file: deployment/bucket-notifications.json"

# Apply notification configuration
Write-Host "Applying S3 bucket notification configuration..." -ForegroundColor Blue
try {
    aws s3api put-bucket-notification-configuration --bucket fin-trade-craft-landing --notification-configuration file://deployment/bucket-notifications.json
    Write-Host "S3 bucket notifications configured successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to configure S3 bucket notifications" -ForegroundColor Red
    Write-Host "Check the notification configuration file and try again" -ForegroundColor Yellow
    exit 1
}

# =============================================================================
# STEP 9: Test the Pipeline
# =============================================================================

Write-Host ""
Write-Host "Step 9: Testing the pipeline..." -ForegroundColor Yellow

# Test Lambda function
Write-Host "Running Lambda function to generate test data..." -ForegroundColor Blue
try {
    $json = '{"symbols": ["MSFT"], "run_id": "snowflake-test-001"}'
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
    $base64 = [System.Convert]::ToBase64String($bytes)
    aws lambda invoke --function-name fin-trade-overview-extractor --payload $base64 test-response.json
    Write-Host "Lambda function executed" -ForegroundColor Green
    Write-Host "Response:" -ForegroundColor Cyan
    Get-Content test-response.json | Write-Host
    Write-Host ""
}
catch {
    Write-Host "Lambda function failed" -ForegroundColor Red
}

# =============================================================================
# FINAL INSTRUCTIONS
# =============================================================================

Write-Host ""
Write-Host "Setup Complete! Next Steps:" -ForegroundColor Green
Write-Host ""
Write-Host "1. Check Snowflake for automatic data loading:" -ForegroundColor Cyan
Write-Host "   SELECT COUNT(*) FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW;" -ForegroundColor White
Write-Host ""
Write-Host "2. Monitor pipe status:" -ForegroundColor Cyan
Write-Host "   SELECT SYSTEM`$PIPE_STATUS('OVERVIEW_PIPE');" -ForegroundColor White
Write-Host ""
Write-Host "3. View loaded data:" -ForegroundColor Cyan
Write-Host "   SELECT * FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW ORDER BY LOADED_AT DESC LIMIT 5;" -ForegroundColor White
Write-Host ""
Write-Host "4. Set up additional Lambda functions for time-series, financial statements" -ForegroundColor Cyan
Write-Host ""
Write-Host "5. Create analytics views for business intelligence" -ForegroundColor Cyan
Write-Host ""
Write-Host "Files created:" -ForegroundColor Yellow
Write-Host "   - deployment/snowflake-trust-policy-updated.json"
Write-Host "   - deployment/bucket-notifications.json"
Write-Host "   - test-response.json"
Write-Host ""
Write-Host "Pipeline Status: Lambda -> S3 -> Snowpipe -> Snowflake" -ForegroundColor Green
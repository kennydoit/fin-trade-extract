-- ============================================================================
-- Snowflake Storage Integration Setup for S3
-- Connects Snowflake to fin-trade-craft-landing S3 bucket
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: CREATE STORAGE INTEGRATION
-- ============================================================================

-- Create storage integration to connect to S3 bucket
CREATE OR REPLACE STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::441691361831:role/SnowflakeS3AccessRole'  -- Will be created
STORAGE_ALLOWED_LOCATIONS = ('s3://fin-trade-craft-landing/')
STORAGE_BLOCKED_LOCATIONS = ()  -- None blocked
COMMENT = 'Integration for accessing fin-trade-craft-landing S3 bucket';

-- Show the storage integration to get the IAM role details
DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;

-- ============================================================================
-- STEP 2: CREATE EXTERNAL STAGES FOR DIFFERENT DATA TYPES
-- ============================================================================

-- Overview data stage
CREATE OR REPLACE STAGE OVERVIEW_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/overview/'
FILE_FORMAT = (
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
)
COMMENT = 'Stage for overview data from Lambda function';

-- Time series data stage
CREATE OR REPLACE STAGE TIME_SERIES_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/time-series/'
FILE_FORMAT = (
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
)
COMMENT = 'Stage for time series data from Lambda function';

-- Income statement stage
CREATE OR REPLACE STAGE INCOME_STATEMENT_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/income-statement/'
FILE_FORMAT = (
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
)
COMMENT = 'Stage for income statement data from Lambda function';

-- Balance sheet stage
CREATE OR REPLACE STAGE BALANCE_SHEET_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/balance-sheet/'
FILE_FORMAT = (
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
)
COMMENT = 'Stage for balance sheet data from Lambda function';

-- Cash flow stage
CREATE OR REPLACE STAGE CASH_FLOW_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/cash-flow/'
FILE_FORMAT = (
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
)
COMMENT = 'Stage for cash flow data from Lambda function';

-- Commodities stage
CREATE OR REPLACE STAGE COMMODITIES_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/commodities/'
FILE_FORMAT = (
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
)
COMMENT = 'Stage for commodities data from Lambda function';

-- Economic indicators stage
CREATE OR REPLACE STAGE ECONOMIC_INDICATORS_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/economic-indicators/'
FILE_FORMAT = (
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
)
COMMENT = 'Stage for economic indicators data from Lambda function';

-- Insider transactions stage
CREATE OR REPLACE STAGE INSIDER_TRANSACTIONS_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/insider-transactions/'
FILE_FORMAT = (
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
)
COMMENT = 'Stage for insider transactions data from Lambda function';

-- Earnings call transcripts stage
CREATE OR REPLACE STAGE EARNINGS_CALL_TRANSCRIPTS_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/earnings-calls/'
FILE_FORMAT = (
    TYPE = 'JSON'
    COMPRESSION = 'GZIP'
)
COMMENT = 'Stage for earnings call transcripts data from Lambda function';

-- ============================================================================
-- STEP 3: TEST STAGES (OPTIONAL - AFTER DATA IS LOADED)
-- ============================================================================

-- Test listing files in stages (run after Lambda functions have generated data)
-- LIST @OVERVIEW_STAGE;
-- LIST @TIME_SERIES_STAGE;

-- ============================================================================
-- STEP 4: SHOW CREATED OBJECTS
-- ============================================================================

-- Show storage integration
SHOW STORAGE INTEGRATIONS;

-- Show stages
SHOW STAGES IN SCHEMA RAW;

-- Show stage details
SELECT 
    STAGE_NAME,
    STAGE_URL,
    STAGE_TYPE,
    COMMENT
FROM INFORMATION_SCHEMA.STAGES 
WHERE STAGE_SCHEMA = 'RAW'
ORDER BY STAGE_NAME;

-- ============================================================================
-- NOTES FOR AWS SETUP
-- ============================================================================

/*
IMPORTANT: After creating the storage integration, you need to:

1. Get the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from:
   DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;

2. Create an IAM role in AWS named 'SnowflakeS3AccessRole' with:
   - Trust policy allowing Snowflake to assume the role
   - Policy granting access to s3://fin-trade-craft-landing/

3. Update the STORAGE_AWS_ROLE_ARN in the storage integration above

Example AWS CLI commands to create the role:
aws iam create-role --role-name SnowflakeS3AccessRole --assume-role-policy-document file://snowflake-trust-policy.json
aws iam put-role-policy --role-name SnowflakeS3AccessRole --policy-name SnowflakeS3Access --policy-document file://snowflake-s3-policy.json

The JSON policy files will be created in the deployment directory.
*/
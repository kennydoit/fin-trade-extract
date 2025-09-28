-- ============================================================================
-- Snowflake Storage Integration Setup for S3 CSV Files
-- Connects Snowflake to fin-trade-craft-landing S3 bucket for CSV data
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: CREATE FILE FORMAT FOR CSV DATA
-- ============================================================================

-- Create file format for CSV data from Lambda functions
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
REPLACE_INVALID_CHARACTERS = TRUE
EMPTY_FIELD_AS_NULL = TRUE
NULL_IF = ('', 'NULL', 'null', 'None')
COMMENT = 'File format for Lambda-generated CSV files';

-- ============================================================================
-- STEP 2: CREATE STORAGE INTEGRATION
-- ============================================================================

-- Create storage integration to connect to S3 bucket
CREATE OR REPLACE STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::441691361831:role/SnowflakeS3AccessRole'  -- Will be created
STORAGE_ALLOWED_LOCATIONS = ('s3://fin-trade-craft-landing/')
COMMENT = 'Integration for accessing fin-trade-craft-landing S3 bucket CSV files';

-- Show the storage integration to get the IAM role details
DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;

-- ============================================================================
-- STEP 3: CREATE EXTERNAL STAGES FOR CSV DATA TYPES
-- ============================================================================

-- Overview data stage (CSV format)
CREATE OR REPLACE STAGE OVERVIEW_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/overview/'
FILE_FORMAT = CSV_FORMAT
COMMENT = 'Stage for overview CSV data from Lambda function';

-- Time series data stage (CSV format)
CREATE OR REPLACE STAGE TIME_SERIES_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/time-series/'
FILE_FORMAT = CSV_FORMAT
COMMENT = 'Stage for time series CSV data from Lambda function';

-- Income statement stage (CSV format)
CREATE OR REPLACE STAGE INCOME_STATEMENT_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/income-statement/'
FILE_FORMAT = CSV_FORMAT
COMMENT = 'Stage for income statement CSV data from Lambda function';

-- Balance sheet stage (CSV format)
CREATE OR REPLACE STAGE BALANCE_SHEET_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/balance-sheet/'
FILE_FORMAT = CSV_FORMAT
COMMENT = 'Stage for balance sheet CSV data from Lambda function';

-- Cash flow stage (CSV format)
CREATE OR REPLACE STAGE CASH_FLOW_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/cash-flow/'
FILE_FORMAT = CSV_FORMAT
COMMENT = 'Stage for cash flow CSV data from Lambda function';

-- ============================================================================
-- STEP 4: TEST STAGES (OPTIONAL - AFTER DATA IS LOADED)
-- ============================================================================

-- Test listing files in stages (run after Lambda functions have generated data)
-- LIST @OVERVIEW_STAGE;
-- LIST @TIME_SERIES_STAGE;

-- Test loading a sample file to verify CSV format
-- SELECT $1, $2, $3, $4, $5 FROM @OVERVIEW_STAGE/aws-test-005_2025-09-26T18:35:00Z.csv LIMIT 5;

-- ============================================================================
-- STEP 5: SHOW CREATED OBJECTS
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
    AND STAGE_NAME IN ('OVERVIEW_STAGE', 'TIME_SERIES_STAGE', 'INCOME_STATEMENT_STAGE', 'BALANCE_SHEET_STAGE', 'CASH_FLOW_STAGE')
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
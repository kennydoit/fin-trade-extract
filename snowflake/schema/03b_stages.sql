-- ============================================================================
-- Snowflake External Stages for S3 Data Sources
-- Creates stages for each data type using the storage integration
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- ============================================================================
-- EXTERNAL STAGES FOR DIFFERENT DATA TYPES
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
-- VERIFICATION
-- ============================================================================

-- Show all stages
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

-- Test one stage (will be empty until Lambda writes data)
-- LIST @OVERVIEW_STAGE;
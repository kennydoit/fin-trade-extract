-- ============================================================================
-- Snowpipe Configuration for Automated Data Loading
-- Automatically loads data from S3 to Snowflake tables when files arrive
-- ============================================================================

-- Make sure we're in the right context
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;

-- Verify prerequisites exist
SHOW STORAGE INTEGRATIONS LIKE 'FIN_TRADE_S3_INTEGRATION';
SHOW STAGES IN SCHEMA RAW;

-- ============================================================================
-- OVERVIEW DATA SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE OVERVIEW_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO OVERVIEW
FROM @OVERVIEW_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';  -- Continue processing even if some records fail

-- ============================================================================
-- TIME SERIES DATA SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE TIME_SERIES_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO TIME_SERIES_DAILY_ADJUSTED
FROM @TIME_SERIES_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- INCOME STATEMENT SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE INCOME_STATEMENT_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO INCOME_STATEMENT
FROM @INCOME_STATEMENT_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- BALANCE SHEET SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE BALANCE_SHEET_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO BALANCE_SHEET
FROM @BALANCE_SHEET_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- CASH FLOW SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE CASH_FLOW_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO CASH_FLOW
FROM @CASH_FLOW_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- COMMODITIES SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE COMMODITIES_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO COMMODITIES
FROM @COMMODITIES_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- ECONOMIC INDICATORS SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE ECONOMIC_INDICATORS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO ECONOMIC_INDICATORS
FROM @ECONOMIC_INDICATORS_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- INSIDER TRANSACTIONS SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE INSIDER_TRANSACTIONS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO INSIDER_TRANSACTIONS
FROM @INSIDER_TRANSACTIONS_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- EARNINGS CALL TRANSCRIPTS SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE EARNINGS_CALL_TRANSCRIPTS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO EARNINGS_CALL_TRANSCRIPTS
FROM @EARNINGS_CALL_TRANSCRIPTS_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- SHOW CREATED SNOWPIPES
-- ============================================================================

SHOW PIPES IN SCHEMA RAW;

-- Get Snowpipe notification channels (for SQS setup)
-- Note: Run SHOW PIPES first, then use this query to get the notification channels
SELECT 
    "name" as PIPE_NAME,
    "notification_channel" as NOTIFICATION_CHANNEL
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ============================================================================
-- SNOWPIPE MANAGEMENT COMMANDS
-- ============================================================================

-- Check Snowpipe status
-- SELECT SYSTEM$PIPE_STATUS('FIN_TRADE_EXTRACT.RAW.OVERVIEW_PIPE');

-- Refresh pipe manually if needed
-- ALTER PIPE OVERVIEW_PIPE REFRESH;

-- Show pipe history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
--     DATE_RANGE_START=>DATEADD('day',-7,CURRENT_DATE()),
--     DATE_RANGE_END=>CURRENT_DATE(),
--     PIPE_NAME=>'FIN_TRADE_EXTRACT.RAW.OVERVIEW_PIPE'
-- ));

-- ============================================================================
-- NOTES FOR AWS SQS SETUP
-- ============================================================================

/*
IMPORTANT: After creating the Snowpipes, you need to:

1. Get the notification channels from the pipes above
2. Configure S3 bucket notifications to send events to those SQS queues
3. Set up S3 event notifications for each prefix:
   - s3://fin-trade-craft-landing/overview/
   - s3://fin-trade-craft-landing/time-series/
   - s3://fin-trade-craft-landing/income-statement/
   - etc.

Example AWS CLI command to add S3 bucket notification:
aws s3api put-bucket-notification-configuration --bucket fin-trade-craft-landing --notification-configuration file://s3-notification-config.json

This will automatically trigger Snowpipe when Lambda functions write files to S3.
*/
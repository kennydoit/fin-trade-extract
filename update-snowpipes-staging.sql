-- ============================================================================
-- Update Snowpipes to use Staging Tables and Watermarking
-- This replaces the direct loading with proper staging pipeline
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA STAGING;

-- ============================================================================
-- 1. DROP OLD PIPES (they load directly to production)
-- ============================================================================

-- First, stop the old pipes
USE SCHEMA RAW;
ALTER PIPE OVERVIEW_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;

-- Drop old pipe that loads directly to production
DROP PIPE IF EXISTS OVERVIEW_PIPE;

-- ============================================================================
-- 2. CREATE NEW STAGING PIPE
-- ============================================================================

USE SCHEMA STAGING;

-- Create new pipe that loads to staging table
CREATE OR REPLACE PIPE OVERVIEW_STAGING_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO OVERVIEW_STAGING (
    SYMBOL_ID, SYMBOL, ASSET_TYPE, NAME, DESCRIPTION, CIK, EXCHANGE, 
    CURRENCY, COUNTRY, SECTOR, INDUSTRY, ADDRESS, OFFICIAL_SITE, 
    FISCAL_YEAR_END, MARKET_CAPITALIZATION, EBITDA, PE_RATIO, PEG_RATIO, 
    BOOK_VALUE, DIVIDEND_PER_SHARE, DIVIDEND_YIELD, EPS, REVENUE_PER_SHARE_TTM, 
    PROFIT_MARGIN, OPERATING_MARGIN_TTM, RETURN_ON_ASSETS_TTM, RETURN_ON_EQUITY_TTM, 
    REVENUE_TTM, GROSS_PROFIT_TTM, DILUTED_EPS_TTM, QUARTERLY_EARNINGS_GROWTH_YOY, 
    QUARTERLY_REVENUE_GROWTH_YOY, ANALYST_TARGET_PRICE, TRAILING_PE, FORWARD_PE, 
    PRICE_TO_SALES_RATIO_TTM, PRICE_TO_BOOK_RATIO, EV_TO_REVENUE, EV_TO_EBITDA, 
    BETA, WEEK_52_HIGH, WEEK_52_LOW, DAY_50_MOVING_AVERAGE, DAY_200_MOVING_AVERAGE, 
    SHARES_OUTSTANDING, DIVIDEND_DATE, EX_DIVIDEND_DATE, API_RESPONSE_STATUS, 
    CREATED_AT, UPDATED_AT,
    FILE_NAME,    -- Metadata: capture source file name
    BATCH_ID      -- Metadata: extract batch ID from filename
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 
        $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, 
        $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, 
        $45, $46, $47, $48, $49, $50,
        METADATA$FILENAME as FILE_NAME,
        REGEXP_SUBSTR(METADATA$FILENAME, '[^/]+_([^_]+)_', 1, 1, 'e', 1) as BATCH_ID
    FROM @RAW.OVERVIEW_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
ON_ERROR = 'CONTINUE'
COMMENT = 'Staging pipe for OVERVIEW data - loads to staging with metadata';

-- ============================================================================
-- 3. GET NEW PIPE NOTIFICATION CHANNEL
-- ============================================================================

-- Get the notification channel for S3 setup
DESC PIPE OVERVIEW_STAGING_PIPE;

SELECT 'UPDATE S3 NOTIFICATIONS' as instruction,
       'Use the notification_channel from DESC PIPE above' as note,
       'Replace the old OVERVIEW_PIPE ARN in S3 bucket notifications' as action;

-- ============================================================================
-- 4. CREATE PROCESSING TASK (automated staging to production)
-- ============================================================================

-- Task to automatically process staging data every 5 minutes
CREATE OR REPLACE TASK PROCESS_OVERVIEW_TASK
WAREHOUSE = 'FIN_TRADE_WH'
SCHEDULE = '5 MINUTE'
AS
DECLARE
    batch_id VARCHAR DEFAULT 'AUTO_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD_HH24-MI-SS');
BEGIN
    -- Check if there's new data in staging
    LET new_records := (SELECT COUNT(*) FROM STAGING.OVERVIEW_STAGING 
                        WHERE INGESTED_AT > (SELECT LAST_PROCESSED_TIMESTAMP 
                                           FROM STAGING.DATA_WATERMARKS 
                                           WHERE TABLE_NAME = 'OVERVIEW'));
    
    -- Process if there are new records
    IF (new_records > 0) THEN
        -- Update batch ID in staging for tracking
        UPDATE STAGING.OVERVIEW_STAGING 
        SET BATCH_ID = :batch_id 
        WHERE BATCH_ID IS NULL;
        
        -- Call processing procedure
        CALL STAGING.SP_PROCESS_OVERVIEW_STAGING(:batch_id);
        
        -- Log the processing
        INSERT INTO STAGING.PROCESSING_LOG (TABLE_NAME, BATCH_ID, RECORDS_PROCESSED, PROCESSED_AT)
        VALUES ('OVERVIEW', :batch_id, :new_records, CURRENT_TIMESTAMP());
    END IF;
END;

-- ============================================================================
-- 5. CREATE PROCESSING LOG TABLE
-- ============================================================================

CREATE OR REPLACE TABLE PROCESSING_LOG (
    LOG_ID                              NUMBER AUTOINCREMENT,
    TABLE_NAME                          VARCHAR(100),
    BATCH_ID                            VARCHAR(100),
    RECORDS_PROCESSED                   NUMBER,
    PROCESSING_START_TIME               TIMESTAMP_LTZ,
    PROCESSING_END_TIME                 TIMESTAMP_LTZ,
    PROCESSED_AT                        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    STATUS                              VARCHAR(50) DEFAULT 'SUCCESS',
    ERROR_MESSAGE                       VARCHAR(16777216)
)
COMMENT = 'Log of all data processing batches';

-- ============================================================================
-- 6. START THE AUTOMATED PROCESSING
-- ============================================================================

-- Resume the task (it will run every 5 minutes)
ALTER TASK PROCESS_OVERVIEW_TASK RESUME;

-- ============================================================================
-- 7. VERIFICATION QUERIES
-- ============================================================================

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('STAGING.OVERVIEW_STAGING_PIPE') as staging_pipe_status;

-- Check task status  
SHOW TASKS;

-- Check current watermark
SELECT * FROM DATA_WATERMARKS WHERE TABLE_NAME = 'OVERVIEW';

-- Summary of setup
SELECT 'ENTERPRISE PIPELINE SETUP COMPLETE' as status,
       'Data flow: S3 → STAGING.OVERVIEW_STAGING → RAW.OVERVIEW' as pipeline,
       'Automated processing every 5 minutes' as automation,
       'Deduplication and watermarking enabled' as features;
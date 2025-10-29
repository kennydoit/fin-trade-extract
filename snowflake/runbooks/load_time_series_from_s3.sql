-- ============================================================================
-- Load Time Series Daily Adjusted Data from S3 to Snowflake
-- 
-- Purpose: Loads daily stock price data with adjustments for dividends and splits
-- Source: Alpha Vantage TIME_SERIES_DAILY_ADJUSTED API endpoint
-- Destination: FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED
-- Update Strategy: MERGE (upsert based on SYMBOL + DATE)
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Step 1: Create external stage pointing to S3 time series folder
CREATE OR REPLACE STAGE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGE
  URL='s3://fin-trade-craft-landing/time_series_daily_adjusted/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
  FILE_FORMAT = (
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '', 'None')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ENCODING = 'UTF8'
  );

-- Step 2: List files in stage to verify content
-- LIST @TIME_SERIES_STAGE;

-- Step 3: Create the target table if it doesn't exist
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED (
    SYMBOL VARCHAR(20) NOT NULL,
    DATE DATE NOT NULL,
    OPEN NUMBER(18, 4),
    HIGH NUMBER(18, 4),
    LOW NUMBER(18, 4),
    CLOSE NUMBER(18, 4),
    ADJUSTED_CLOSE NUMBER(18, 4),
    VOLUME NUMBER(20, 0),
    DIVIDEND_AMOUNT NUMBER(18, 6),
    SPLIT_COEFFICIENT NUMBER(18, 6),
    
    -- Metadata
    SYMBOL_ID NUMBER(38, 0),
    LOAD_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    PRIMARY KEY (SYMBOL, DATE)
)
COMMENT = 'Daily adjusted stock prices from Alpha Vantage API - watermark based ETL';

-- Step 4: Create transient staging table (all VARCHAR for initial load)
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING (
    SYMBOL VARCHAR(20),
    TIMESTAMP VARCHAR(50),
    OPEN VARCHAR(50),
    HIGH VARCHAR(50),
    LOW VARCHAR(50),
    CLOSE VARCHAR(50),
    ADJUSTED_CLOSE VARCHAR(50),
    VOLUME VARCHAR(50),
    DIVIDEND_AMOUNT VARCHAR(50),
    SPLIT_COEFFICIENT VARCHAR(50)
);

-- Step 5: Copy data from S3 into staging table
COPY INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING
FROM @TIME_SERIES_STAGE
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    NULL_IF = ('NULL', 'null', '', 'None')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
PURGE = FALSE
ON_ERROR = 'CONTINUE';


-- Step 9: MERGE staging data into target table
MERGE INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED AS target
USING (
    SELECT 
        s.SYMBOL,
        TRY_TO_DATE(s.TIMESTAMP) AS DATE,
        TRY_TO_NUMBER(s.OPEN, 18, 4) AS OPEN,
        TRY_TO_NUMBER(s.HIGH, 18, 4) AS HIGH,
        TRY_TO_NUMBER(s.LOW, 18, 4) AS LOW,
        TRY_TO_NUMBER(s.CLOSE, 18, 4) AS CLOSE,
        TRY_TO_NUMBER(s.ADJUSTED_CLOSE, 18, 4) AS ADJUSTED_CLOSE,
        TRY_TO_NUMBER(s.VOLUME, 20, 0) AS VOLUME,
        TRY_TO_NUMBER(s.DIVIDEND_AMOUNT, 18, 6) AS DIVIDEND_AMOUNT,
        TRY_TO_NUMBER(s.SPLIT_COEFFICIENT, 18, 6) AS SPLIT_COEFFICIENT,
        NULL AS SYMBOL_ID,  -- Will be populated later when SYMBOL table is available
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM TIME_SERIES_STAGING s
    WHERE TRY_TO_DATE(s.TIMESTAMP) IS NOT NULL
      AND s.SYMBOL IS NOT NULL
      AND TRY_TO_NUMBER(s.CLOSE, 18, 4) > 0
) AS source
ON target.SYMBOL = source.SYMBOL 
   AND target.DATE = source.DATE
WHEN MATCHED THEN
    UPDATE SET
        target.OPEN = source.OPEN,
        target.HIGH = source.HIGH,
        target.LOW = source.LOW,
        target.CLOSE = source.CLOSE,
        target.ADJUSTED_CLOSE = source.ADJUSTED_CLOSE,
        target.VOLUME = source.VOLUME,
        target.DIVIDEND_AMOUNT = source.DIVIDEND_AMOUNT,
        target.SPLIT_COEFFICIENT = source.SPLIT_COEFFICIENT,
        target.SYMBOL_ID = source.SYMBOL_ID,
        target.LOAD_DATE = source.LOAD_DATE
WHEN NOT MATCHED THEN
    INSERT (
        SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, ADJUSTED_CLOSE,
        VOLUME, DIVIDEND_AMOUNT, SPLIT_COEFFICIENT,
        SYMBOL_ID, LOAD_DATE
    )
    VALUES (
        source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW,
        source.CLOSE, source.ADJUSTED_CLOSE, source.VOLUME,
        source.DIVIDEND_AMOUNT, source.SPLIT_COEFFICIENT,
        source.SYMBOL_ID, source.LOAD_DATE
    );

-- Step 13: Cleanup staging table
DROP TABLE IF EXISTS TIME_SERIES_STAGING;

SELECT '✅ Time Series data load completed successfully!' as status;

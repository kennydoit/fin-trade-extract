-- ============================================================================
-- Load Insider Transactions Data from S3 to Snowflake
--
-- Purpose: Loads insider trading data from S3 staged files
-- Source: Alpha Vantage INSIDER_TRADING API endpoint
-- Destination: FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS
-- Update Strategy: MERGE (upsert based on a composite key)
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Step 1: Create external stage pointing to S3 insider transactions folder
CREATE OR REPLACE STAGE FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGE
  URL='s3://fin-trade-craft-landing/insider_transactions/'
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
LIST @INSIDER_TRANSACTIONS_STAGE;

-- Step 3: Create the target table if it doesn't exist
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS (
    SYMBOL VARCHAR(20) NOT NULL,
    TRANSACTION_DATE DATE NOT NULL,
    TRANSACTION_TYPE VARCHAR(50),
    SHARES NUMBER(38, 2),
    PRICE_PER_SHARE NUMBER(38, 4),
    TOTAL_VALUE NUMBER(38, 2),
    INSIDER_NAME VARCHAR(255),
    INSIDER_TITLE VARCHAR(255),
    TRANSACTION_CODE VARCHAR(10),
    FILING_DATE DATE NOT NULL,
    SYMBOL_ID NUMBER(38, 0),
    LOAD_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Define a composite primary key to uniquely identify each transaction
    PRIMARY KEY (SYMBOL, FILING_DATE, TRANSACTION_DATE, INSIDER_NAME, TRANSACTION_TYPE, SHARES, PRICE_PER_SHARE)
)
COMMENT = 'Insider trading data from Alpha Vantage API - watermark based ETL';

-- Step 4: Create a transient staging table for loading data from S3
-- This structure matches the 8 columns from the Alpha Vantage API response
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING (
    TRANSACTION_DATE VARCHAR(50),
    TICKER VARCHAR(20),
    EXECUTIVE VARCHAR(255),
    EXECUTIVE_TITLE VARCHAR(255),
    SECURITY_TYPE VARCHAR(255),
    ACQUISITION_OR_DISPOSAL VARCHAR(5),
    SHARES VARCHAR(50),
    SHARE_PRICE VARCHAR(50)
);

-- Step 5: Copy data from S3 into staging table, mapping columns explicitly
COPY INTO FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING (
    TRANSACTION_DATE,
    TICKER,
    EXECUTIVE,
    EXECUTIVE_TITLE,
    SECURITY_TYPE,
    ACQUISITION_OR_DISPOSAL,
    SHARES,
    SHARE_PRICE
)
FROM @INSIDER_TRANSACTIONS_STAGE
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    NULL_IF = ('NULL', 'null', '', 'None')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
)
ON_ERROR = 'CONTINUE';

-- Step 6: MERGE staging data into target table
MERGE INTO FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS target
USING (
    SELECT
        TICKER AS SYMBOL,
        TRY_TO_DATE(TRANSACTION_DATE) AS TRANSACTION_DATE,
        ACQUISITION_OR_DISPOSAL AS TRANSACTION_TYPE,
        TRY_TO_NUMBER(SHARES, 38, 2) AS SHARES,
        TRY_TO_NUMBER(SHARE_PRICE, 38, 4) AS PRICE_PER_SHARE,
        (SHARES * PRICE_PER_SHARE) AS TOTAL_VALUE,
        EXECUTIVE AS INSIDER_NAME,
        EXECUTIVE_TITLE AS INSIDER_TITLE,
        NULL AS TRANSACTION_CODE, -- Not provided by this API endpoint
        -- Use TRANSACTION_DATE for FILING_DATE as it's the closest available date
        TRY_TO_DATE(TRANSACTION_DATE) AS FILING_DATE,
        NULL AS SYMBOL_ID, -- To be populated later
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
    WHERE TRY_TO_DATE(TRANSACTION_DATE) IS NOT NULL
      AND TICKER IS NOT NULL
) source
ON target.SYMBOL = source.SYMBOL
   AND target.FILING_DATE = source.FILING_DATE
   AND target.TRANSACTION_DATE = source.TRANSACTION_DATE
   AND target.INSIDER_NAME = source.INSIDER_NAME
   AND target.TRANSACTION_TYPE = source.TRANSACTION_TYPE
   AND target.SHARES = source.SHARES
   AND target.PRICE_PER_SHARE = source.PRICE_PER_SHARE

WHEN NOT MATCHED THEN
    INSERT (
        SYMBOL, TRANSACTION_DATE, TRANSACTION_TYPE, SHARES, PRICE_PER_SHARE,
        TOTAL_VALUE, INSIDER_NAME, INSIDER_TITLE, TRANSACTION_CODE, FILING_DATE,
        SYMBOL_ID, LOAD_DATE
    )
    VALUES (
        source.SYMBOL, source.TRANSACTION_DATE, source.TRANSACTION_TYPE, source.SHARES, source.PRICE_PER_SHARE,
        source.TOTAL_VALUE, source.INSIDER_NAME, source.INSIDER_TITLE, source.TRANSACTION_CODE, source.FILING_DATE,
        source.SYMBOL_ID, source.LOAD_DATE
    );

-- Step 7: Cleanup staging table
DROP TABLE IF EXISTS INSIDER_TRANSACTIONS_STAGING;

SELECT '✅ Insider Transactions data load completed successfully!' as status;

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

-- Step 3: Create the target table with a simplified schema
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS (
    SYMBOL VARCHAR(20) NOT NULL,
    TRANSACTION_DATE DATE NOT NULL,
    TRANSACTION_TYPE VARCHAR(50),
    SHARES NUMBER(38, 2),
    PRICE_PER_SHARE NUMBER(38, 4),
    INSIDER_NAME VARCHAR(255),
    INSIDER_TITLE VARCHAR(255),
    SYMBOL_ID NUMBER(38, 0),
    LOAD_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Define a simplified composite primary key
    PRIMARY KEY (SYMBOL, TRANSACTION_DATE, INSIDER_NAME, TRANSACTION_TYPE)
)
COMMENT = 'Insider trading data from Alpha Vantage API - watermark based ETL';

-- Add comment to table
COMMENT ON TABLE FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS IS 'Insider trading data from Alpha Vantage API - watermark based ETL';

-- Step 4: Create a transient staging table that matches the CSV structure
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING (
    transaction_date VARCHAR(50),
    ticker VARCHAR(50),
    executive VARCHAR(255),
    executive_title VARCHAR(255),
    security_type VARCHAR(255),
    acquisition_or_disposal VARCHAR(50),
    shares VARCHAR(50),
    share_price VARCHAR(50)
);

-- Defensively truncate the staging table to ensure it's empty
TRUNCATE TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING;

-- Step 5: Copy data from S3 into staging table
COPY INTO FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
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

-- Step 6: MERGE staging data into target table (simplified)
MERGE INTO FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS target
USING (
    SELECT
        ticker AS SYMBOL,
        TRY_TO_DATE(transaction_date) AS TRANSACTION_DATE,
        acquisition_or_disposal AS TRANSACTION_TYPE,
        TRY_TO_NUMBER(shares, 38, 2) AS SHARES,
        TRY_TO_NUMBER(share_price, 38, 4) AS PRICE_PER_SHARE,
        executive AS INSIDER_NAME,
        executive_title AS INSIDER_TITLE,
    ABS(HASH(ticker)) % 1000000000 AS SYMBOL_ID,
    CURRENT_DATE() AS LOAD_DATE
    FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
        WHERE TRY_TO_DATE(transaction_date) IS NOT NULL
            AND ticker IS NOT NULL
            AND acquisition_or_disposal IS NOT NULL
) source
ON target.SYMBOL = source.SYMBOL
   AND target.TRANSACTION_DATE = source.TRANSACTION_DATE
   AND target.INSIDER_NAME = source.INSIDER_NAME
   AND target.TRANSACTION_TYPE = source.TRANSACTION_TYPE

WHEN NOT MATCHED THEN
    INSERT (
        SYMBOL, TRANSACTION_DATE, TRANSACTION_TYPE, SHARES, PRICE_PER_SHARE,
        INSIDER_NAME, INSIDER_TITLE, SYMBOL_ID, LOAD_DATE
    )
    VALUES (
        source.SYMBOL, source.TRANSACTION_DATE, source.TRANSACTION_TYPE, source.SHARES, source.PRICE_PER_SHARE,
        source.INSIDER_NAME, source.INSIDER_TITLE, source.SYMBOL_ID, source.LOAD_DATE
    );

-- Step 7: Cleanup staging table
DROP TABLE IF EXISTS INSIDER_TRANSACTIONS_STAGING;

SELECT '✅ Insider Transactions data load completed successfully!' as status;

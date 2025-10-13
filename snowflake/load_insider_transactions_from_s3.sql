-- ============================================================================
-- Load Insider Transactions Data from S3 to Snowflake
-- 
-- Purpose: Loads insider trading transaction data from S3 staged files
-- Source: Alpha Vantage INSIDER_TRANSACTIONS API endpoint
-- Destination: FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS
-- Update Strategy: MERGE (upsert based on natural key)
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Step 1: Create external stage pointing to S3 insider_transactions folder
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
    FILING_DATE DATE,
    OWNER_NAME VARCHAR(255) NOT NULL,
    OWNER_TITLE VARCHAR(255),
    TRANSACTION_TYPE VARCHAR(100),
    ACQUISITION_DISPOSITION VARCHAR(20),  -- 'A' (acquisition) or 'D' (disposition)
    SECURITY_NAME VARCHAR(255),
    SECURITY_TYPE VARCHAR(100),
    SHARES NUMBER(38, 4),
    VALUE NUMBER(38, 4),
    SHARES_OWNED_FOLLOWING NUMBER(38, 4),
    
    -- Metadata
    SYMBOL_ID NUMBER(38, 0),  -- Consistent hash for joins
    LOAD_DATE DATE DEFAULT CURRENT_DATE(),
    
    -- Natural key constraint (upsert on these columns)
    -- Multiple transactions on same date by same person in same security are possible
    CONSTRAINT PK_INSIDER_TRANSACTIONS PRIMARY KEY (SYMBOL, TRANSACTION_DATE, OWNER_NAME, SECURITY_TYPE)
);

-- Add comment to table
COMMENT ON TABLE FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS IS 
'Insider trading transactions from Alpha Vantage. Tracks buy/sell activity by company insiders (executives, directors, major shareholders). Updated via watermark-based ETL.';

-- Step 4: Create a transient staging table for loading data from S3
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING (
    SYMBOL VARCHAR(20),
    TRANSACTION_DATE VARCHAR(50),
    FILING_DATE VARCHAR(50),
    OWNER_NAME VARCHAR(255),
    OWNER_TITLE VARCHAR(255),
    TRANSACTION_TYPE VARCHAR(100),
    ACQUISITION_DISPOSITION VARCHAR(20),
    SECURITY_NAME VARCHAR(255),
    SECURITY_TYPE VARCHAR(100),
    SHARES VARCHAR(50),
    VALUE VARCHAR(50),
    SHARES_OWNED_FOLLOWING VARCHAR(50)
);

-- Step 5: Copy data from S3 to staging table
COPY INTO FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
FROM @FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGE
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('None', 'null', '', 'N/A')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE
)
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Step 6: Show staging statistics
SELECT 
    'Staging Statistics' as step,
    COUNT(*) as total_rows,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    COUNT(DISTINCT OWNER_NAME) as unique_owners,
    COUNT(CASE WHEN ACQUISITION_DISPOSITION = 'A' THEN 1 END) as acquisitions,
    COUNT(CASE WHEN ACQUISITION_DISPOSITION = 'D' THEN 1 END) as dispositions,
    MIN(TRY_TO_DATE(TRANSACTION_DATE, 'YYYY-MM-DD')) as earliest_transaction,
    MAX(TRY_TO_DATE(TRANSACTION_DATE, 'YYYY-MM-DD')) as latest_transaction
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING;

-- Step 7: Data quality checks
SELECT 'Data Quality Check - Missing Required Fields' as check_type,
       COUNT(*) as issues
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
WHERE SYMBOL IS NULL 
   OR TRANSACTION_DATE IS NULL
   OR OWNER_NAME IS NULL
   OR SECURITY_TYPE IS NULL;

SELECT 'Data Quality Check - Invalid Transaction Dates' as check_type,
       COUNT(*) as issues
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
WHERE TRY_TO_DATE(TRANSACTION_DATE, 'YYYY-MM-DD') IS NULL;

SELECT 'Data Quality Check - Invalid Filing Dates' as check_type,
       COUNT(*) as issues
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
WHERE FILING_DATE IS NOT NULL
  AND TRY_TO_DATE(FILING_DATE, 'YYYY-MM-DD') IS NULL;

SELECT 'Data Quality Check - Invalid Acquisition/Disposition Codes' as check_type,
       COUNT(*) as issues
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
WHERE ACQUISITION_DISPOSITION IS NOT NULL
  AND ACQUISITION_DISPOSITION NOT IN ('A', 'D', 'a', 'd');

SELECT 'Data Quality Check - Invalid Share Amounts' as check_type,
       COUNT(*) as issues
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
WHERE SHARES IS NOT NULL
  AND TRY_TO_NUMBER(SHARES, 38, 4) IS NULL;

-- Step 8: Show sample of staged data
SELECT 'Sample Staged Data' as step,
       SYMBOL,
       TRANSACTION_DATE,
       OWNER_NAME,
       OWNER_TITLE,
       TRANSACTION_TYPE,
       ACQUISITION_DISPOSITION,
       SHARES,
       VALUE,
       SECURITY_TYPE
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
LIMIT 5;

-- Step 9: Merge staged data into target table
MERGE INTO FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS target
USING (
    SELECT 
        SYMBOL,
        TRY_TO_DATE(TRANSACTION_DATE, 'YYYY-MM-DD') as TRANSACTION_DATE,
        TRY_TO_DATE(FILING_DATE, 'YYYY-MM-DD') as FILING_DATE,
        OWNER_NAME,
        OWNER_TITLE,
        TRANSACTION_TYPE,
        UPPER(ACQUISITION_DISPOSITION) as ACQUISITION_DISPOSITION,  -- Normalize to uppercase
        SECURITY_NAME,
        SECURITY_TYPE,
        TRY_TO_NUMBER(NULLIF(SHARES, ''), 38, 4) as SHARES,
        TRY_TO_NUMBER(NULLIF(VALUE, ''), 38, 4) as VALUE,
        TRY_TO_NUMBER(NULLIF(SHARES_OWNED_FOLLOWING, ''), 38, 4) as SHARES_OWNED_FOLLOWING,
        ABS(HASH(SYMBOL)) % 1000000000 as SYMBOL_ID,  -- Consistent with LISTING_STATUS
        CURRENT_DATE() as LOAD_DATE
    FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING
    WHERE SYMBOL IS NOT NULL
      AND TRY_TO_DATE(TRANSACTION_DATE, 'YYYY-MM-DD') IS NOT NULL
      AND OWNER_NAME IS NOT NULL
      AND SECURITY_TYPE IS NOT NULL
) source
ON target.SYMBOL = source.SYMBOL
   AND target.TRANSACTION_DATE = source.TRANSACTION_DATE
   AND target.OWNER_NAME = source.OWNER_NAME
   AND target.SECURITY_TYPE = source.SECURITY_TYPE
WHEN MATCHED THEN
    UPDATE SET
        target.FILING_DATE = source.FILING_DATE,
        target.OWNER_TITLE = source.OWNER_TITLE,
        target.TRANSACTION_TYPE = source.TRANSACTION_TYPE,
        target.ACQUISITION_DISPOSITION = source.ACQUISITION_DISPOSITION,
        target.SECURITY_NAME = source.SECURITY_NAME,
        target.SHARES = source.SHARES,
        target.VALUE = source.VALUE,
        target.SHARES_OWNED_FOLLOWING = source.SHARES_OWNED_FOLLOWING,
        target.SYMBOL_ID = source.SYMBOL_ID,
        target.LOAD_DATE = source.LOAD_DATE
WHEN NOT MATCHED THEN
    INSERT (
        SYMBOL, TRANSACTION_DATE, FILING_DATE, OWNER_NAME, OWNER_TITLE,
        TRANSACTION_TYPE, ACQUISITION_DISPOSITION, SECURITY_NAME, SECURITY_TYPE,
        SHARES, VALUE, SHARES_OWNED_FOLLOWING,
        SYMBOL_ID, LOAD_DATE
    )
    VALUES (
        source.SYMBOL, source.TRANSACTION_DATE, source.FILING_DATE, source.OWNER_NAME, source.OWNER_TITLE,
        source.TRANSACTION_TYPE, source.ACQUISITION_DISPOSITION, source.SECURITY_NAME, source.SECURITY_TYPE,
        source.SHARES, source.VALUE, source.SHARES_OWNED_FOLLOWING,
        source.SYMBOL_ID, source.LOAD_DATE
    );

-- Step 10: Final statistics
SELECT 
    'Final Table Statistics' as step,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    COUNT(DISTINCT OWNER_NAME) as unique_insiders,
    COUNT(CASE WHEN ACQUISITION_DISPOSITION = 'A' THEN 1 END) as acquisitions,
    COUNT(CASE WHEN ACQUISITION_DISPOSITION = 'D' THEN 1 END) as dispositions,
    MIN(TRANSACTION_DATE) as earliest_transaction,
    MAX(TRANSACTION_DATE) as latest_transaction,
    MAX(LOAD_DATE) as last_load_date
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS;

-- Step 11: Summary by symbol (top 10 most active)
SELECT 
    'Most Active Symbols (by transaction count)' as step,
    SYMBOL,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT OWNER_NAME) as unique_insiders,
    COUNT(CASE WHEN ACQUISITION_DISPOSITION = 'A' THEN 1 END) as buys,
    COUNT(CASE WHEN ACQUISITION_DISPOSITION = 'D' THEN 1 END) as sells,
    SUM(SHARES) as total_shares_transacted,
    MIN(TRANSACTION_DATE) as earliest_date,
    MAX(TRANSACTION_DATE) as latest_date
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS
GROUP BY SYMBOL
ORDER BY total_transactions DESC
LIMIT 10;

-- Step 12: Recent activity (last 30 days)
SELECT 
    'Recent Insider Activity (Last 30 Days)' as step,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    SUM(CASE WHEN ACQUISITION_DISPOSITION = 'A' THEN 1 ELSE 0 END) as buys,
    SUM(CASE WHEN ACQUISITION_DISPOSITION = 'D' THEN 1 ELSE 0 END) as sells,
    SUM(SHARES) as total_shares
FROM FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS
WHERE TRANSACTION_DATE >= DATEADD(day, -30, CURRENT_DATE());

-- Step 13: Clean up staging table
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS_STAGING;

SELECT '✅ Insider transactions load complete!' as status;

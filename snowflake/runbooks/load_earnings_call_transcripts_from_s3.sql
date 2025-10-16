-- ============================================================================
-- Load Earnings Call Transcripts Data from S3 to Snowflake
--
-- Purpose: Loads earnings call transcript data from S3 into Snowflake
-- Source: Alpha Vantage EARNINGS_CALL_TRANSCRIPT API endpoint
-- Destination: FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPTS
-- Update Strategy: MERGE (upsert based on SYMBOL + QUARTER)
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Step 1: Create external stage pointing to S3 earnings call transcripts folder
CREATE OR REPLACE STAGE FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPTS_STAGE
  URL='s3://fin-trade-craft-landing/earnings_call_transcripts/'
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
LIST @EARNINGS_CALL_TRANSCRIPTS_STAGE;

-- Step 3: Create the target table if it doesn't exist
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPTS (
    SYMBOL VARCHAR(20) NOT NULL,
    QUARTER VARCHAR(8) NOT NULL,
    TRANSCRIPT_DATE DATE,
    TRANSCRIPT_TEXT STRING,
    SPEAKER STRING,
    ROLE STRING,
    LOAD_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Step 4: Create transient staging table (all VARCHAR for initial load)
CREATE OR REPLACE TRANSIENT TABLE EARNINGS_CALL_TRANSCRIPTS_STAGING AS
SELECT * FROM FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPTS WHERE 1=0;

-- Step 5: Copy data from S3 into staging table
COPY INTO EARNINGS_CALL_TRANSCRIPTS_STAGING
FROM @EARNINGS_CALL_TRANSCRIPTS_STAGE
FILE_FORMAT = (TYPE = 'CSV');

-- Step 6: MERGE staging data into target table
MERGE INTO FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPTS AS target
USING (
    SELECT 
        s.SYMBOL,
        s.QUARTER,
        TRY_TO_DATE(s.TRANSCRIPT_DATE) AS TRANSCRIPT_DATE,
        s.TRANSCRIPT_TEXT,
        s.SPEAKER,
        s.ROLE,
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM EARNINGS_CALL_TRANSCRIPTS_STAGING s
    WHERE s.SYMBOL IS NOT NULL AND s.QUARTER IS NOT NULL
) AS source
ON target.SYMBOL = source.SYMBOL AND target.QUARTER = source.QUARTER
WHEN MATCHED THEN
    UPDATE SET
        target.TRANSCRIPT_DATE = source.TRANSCRIPT_DATE,
        target.TRANSCRIPT_TEXT = source.TRANSCRIPT_TEXT,
        target.SPEAKER = source.SPEAKER,
        target.ROLE = source.ROLE,
        target.LOAD_DATE = source.LOAD_DATE
WHEN NOT MATCHED THEN
    INSERT (
        SYMBOL, QUARTER, TRANSCRIPT_DATE, TRANSCRIPT_TEXT, SPEAKER, ROLE, LOAD_DATE
    )
    VALUES (
        source.SYMBOL, source.QUARTER, source.TRANSCRIPT_DATE, source.TRANSCRIPT_TEXT, source.SPEAKER, source.ROLE, source.LOAD_DATE
    );

-- Step 7: Cleanup staging table
DROP TABLE IF EXISTS EARNINGS_CALL_TRANSCRIPTS_STAGING;

-- Step 8: Show summary statistics
SELECT 
    'Total Earnings Call Transcripts' as metric,
    CAST(COUNT(*) AS VARCHAR) as value
FROM EARNINGS_CALL_TRANSCRIPTS
UNION ALL
SELECT 
    'Unique Symbols',
    CAST(COUNT(DISTINCT SYMBOL) AS VARCHAR)
FROM EARNINGS_CALL_TRANSCRIPTS;

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

CREATE OR REPLACE STAGE FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPT_STAGE
    URL='s3://fin-trade-craft-landing/earnings_call_transcript/'
    STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
    FILE_FORMAT = (
        TYPE = 'JSON'
        COMPRESSION = 'AUTO'
    );

-- LIST @FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPT_STAGE;


DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPT_UNSPLIT;
CREATE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPT_UNSPLIT (
    TRANSCRIPT_DATA VARIANT
);

COPY INTO FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPT_UNSPLIT
FROM (
    SELECT $1 FROM @FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPT_STAGE
)
FILE_FORMAT = (TYPE = 'JSON');

CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPT (
    SYMBOL_ID NUMBER(38, 0),
    QUARTER VARCHAR(20),
    SYMBOL VARCHAR(20),
    SENTIMENT FLOAT,
    TRANSCRIPT_TEXT STRING
);

-- Create the final split-out table
INSERT INTO FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPT
SELECT
    ABS(HASH(TRANSCRIPT_DATA:symbol::STRING)) % 1000000000 AS symbol_id,
    TRANSCRIPT_DATA:quarter::STRING AS quarter,
    TRANSCRIPT_DATA:symbol::STRING AS symbol,
    MAX(transcript_entry.value:sentiment::FLOAT) AS sentiment,
    ARRAY_TO_STRING(ARRAY_AGG(transcript_entry.value:content::STRING), ' ') AS transcript_text
FROM FIN_TRADE_EXTRACT.RAW.EARNINGS_CALL_TRANSCRIPT_UNSPLIT,
     LATERAL FLATTEN(INPUT => TRANSCRIPT_DATA:transcript) transcript_entry
GROUP BY TRANSCRIPT_DATA:quarter, TRANSCRIPT_DATA:symbol;

-- name: Decode Snowflake private key
      run: |
        echo "${{ secrets.SNOWFLAKE_PRIVATE_KEY_DER_B64 }}" | base64 -d > snowflake_rsa_key.der

with open("snowflake_rsa_key.der", "rb") as key_file:
    private_key = key_file.read()

conn = snowflake.connector.connect(
    user='ETL_USER',
    account='GIDLNKY-MBC80835',  # add region if needed
    private_key=private_key,
    warehouse='FIN_TRADE_WH',
    database='FIN_TRADE_EXTRACT',
    schema='RAW'
)

-- name: Clean up private key
      run: rm -f snowflake_rsa_key.der



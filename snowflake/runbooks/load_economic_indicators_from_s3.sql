-- ============================================================================ 
-- Load Economic Indicators Data from S3 to Snowflake (Incremental Upsert)
-- ============================================================================ 

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;

USE ROLE ETL_ROLE;

-- Create the economic indicators table if it doesn't exist
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.ECONOMIC_INDICATORS (
    INDICATOR_NAME STRING NOT NULL,
    FUNCTION_NAME STRING NOT NULL,
    MATURITY STRING,
    DATE DATE,
    INTERVAL STRING NOT NULL,
    UNIT STRING,
    VALUE FLOAT,
    NAME STRING,
    RUN_ID STRING NOT NULL,
    LOAD_DATE DATE DEFAULT CURRENT_DATE(),
    UNIQUE (INDICATOR_NAME, FUNCTION_NAME, MATURITY, DATE, INTERVAL)
);

-- Step 1: Create external stage for economic indicators
CREATE OR REPLACE STAGE FIN_TRADE_EXTRACT.RAW.ECONOMIC_INDICATORS_STAGE
  URL = 's3://fin-trade-craft-landing/economic_indicators/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.ECONOMIC_INDICATORS_STAGING
(
    INDICATOR_NAME STRING,
    FUNCTION_NAME STRING,
    MATURITY STRING,
    DATE DATE,
    INTERVAL STRING,
    UNIT STRING,
    VALUE FLOAT,
    NAME STRING,
    RUN_ID STRING
);

-- Step 3: Load all CSVs from S3 into staging table
COPY INTO FIN_TRADE_EXTRACT.RAW.ECONOMIC_INDICATORS_STAGING
FROM @ECONOMIC_INDICATORS_STAGE
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*\\.csv'
ON_ERROR = 'CONTINUE';

-- Step 4: Merge staged data into target table
MERGE INTO FIN_TRADE_EXTRACT.RAW.ECONOMIC_INDICATORS tgt
USING (
    SELECT DISTINCT
        indicator_name, function_name, maturity, date, interval, unit, value, name, run_id
    FROM FIN_TRADE_EXTRACT.RAW.ECONOMIC_INDICATORS_STAGING
) src
ON tgt.indicator_name = src.indicator_name
   AND tgt.function_name = src.function_name
   AND (tgt.maturity = src.maturity OR (tgt.maturity IS NULL AND src.maturity IS NULL))
   AND tgt.date = src.date
   AND tgt.interval = src.interval
WHEN MATCHED THEN UPDATE SET
    unit = src.unit,
    value = src.value,
    name = src.name,
    run_id = src.run_id,
    load_date = CURRENT_DATE()
WHEN NOT MATCHED THEN INSERT (
    indicator_name, function_name, maturity, date, interval, unit, value, name, run_id
) VALUES (
    src.indicator_name, src.function_name, src.maturity, src.date, src.interval, src.unit, src.value, src.name, src.run_id
);

-- Step 5: Cleanup staging table
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.ECONOMIC_INDICATORS_STAGING;

SELECT '✅ Economic indicators data load complete!' as status;

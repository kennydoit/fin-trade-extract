-- Load FRED Commodities from S3 into Snowflake
-- This script is intended to be run in Snowflake Worksheets or via a Python orchestrator.
-- It assumes the S3 files have already been uploaded by the fetch ETL step.

-- Set variables as needed
set s3_bucket = 'fin-trade-craft-landing';
set s3_prefix = 'commodities/';

-- List of commodity tables to refresh
set commodities = array_construct('WTI', 'BRENT', 'NATURAL_GAS', 'COPPER', 'ALUMINUM', 'WHEAT', 'CORN', 'COTTON', 'SUGAR', 'ALL_COMMODITIES');

-- Loop over each commodity and load the latest file
-- (Replace this with a Python orchestrator for more advanced logic)
-- Example for one commodity:
--
-- CREATE TABLE IF NOT EXISTS FRED_COMMODITIES_WTI (
--   COMMODITY STRING,
--   DATE DATE,
--   VALUE FLOAT
-- );
--
-- REMOVE old data (full refresh)
-- DELETE FROM FRED_COMMODITIES_WTI;
--
-- COPY INTO FRED_COMMODITIES_WTI
-- FROM 's3://fin-trade-craft-landing/commodities/WTI_20251028T153331Z.csv'
-- FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
-- FORCE=TRUE;

-- Repeat for each commodity as needed.
-- For automation, use a Python script to enumerate S3 and generate the correct COPY INTO statements.

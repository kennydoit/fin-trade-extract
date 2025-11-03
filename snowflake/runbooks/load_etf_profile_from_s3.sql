-- Load ETF Profile Data from S3 to Snowflake
-- Source: Alpha Vantage ETF_PROFILE API endpoint
-- Destination: FIN_TRADE_EXTRACT.RAW.ETF_PROFILE

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

CREATE OR REPLACE STAGE FIN_TRADE_EXTRACT.RAW.ETF_PROFILE_STAGE
  URL='s3://fin-trade-craft-landing/etf_profile/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
  FILE_FORMAT = (TYPE = 'JSON');

-- Create target table if not exists
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.ETF_PROFILE (
    SYMBOL VARCHAR(20) PRIMARY KEY,
    PROFILE_DATA VARIANT,
    LOAD_DATE DATE DEFAULT CURRENT_DATE()
);

-- Load data from S3
COPY INTO FIN_TRADE_EXTRACT.RAW.ETF_PROFILE
FROM (
    SELECT
      SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '.', 1)::STRING AS SYMBOL,
      $1 AS PROFILE_DATA,
      CURRENT_DATE() AS LOAD_DATE
    FROM @FIN_TRADE_EXTRACT.RAW.ETF_PROFILE_STAGE
)
FILE_FORMAT = (TYPE = 'JSON');


-- Show load results
SELECT COUNT(*) AS total_records FROM FIN_TRADE_EXTRACT.RAW.ETF_PROFILE;

-- Parse all top-level fields from PROFILE_DATA
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.RAW.ETF_PROFILE_PARSED AS
SELECT
  SYMBOL,
  PROFILE_DATA:"net_assets"::STRING AS net_assets,
  PROFILE_DATA:"net_expense_ratio"::STRING AS net_expense_ratio,
  PROFILE_DATA:"portfolio_turnover"::STRING AS portfolio_turnover,
  PROFILE_DATA:"dividend_yield"::STRING AS dividend_yield,
  PROFILE_DATA:"inception_date"::STRING AS inception_date,
  PROFILE_DATA:"leveraged"::STRING AS leveraged,
  PROFILE_DATA:"sectors" AS sectors,
  PROFILE_DATA:"holdings" AS holdings,
  LOAD_DATE
FROM FIN_TRADE_EXTRACT.RAW.ETF_PROFILE;

-- Flatten sectors array
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.RAW.ETF_PROFILE_SECTORS AS
SELECT
  SYMBOL,
  sector.value:sector::STRING AS sector,
  sector.value:weight::STRING AS weight,
  LOAD_DATE
FROM FIN_TRADE_EXTRACT.RAW.ETF_PROFILE,
  LATERAL FLATTEN(input => PROFILE_DATA:"sectors") sector;

-- Flatten holdings array
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.RAW.ETF_PROFILE_HOLDINGS AS
SELECT
  SYMBOL,
  holding.value:symbol::STRING AS holding_symbol,
  holding.value:description::STRING AS description,
  holding.value:weight::STRING AS weight,
  LOAD_DATE
FROM FIN_TRADE_EXTRACT.RAW.ETF_PROFILE,
  LATERAL FLATTEN(input => PROFILE_DATA:"holdings") holding;

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


-- Create parsed target table for ETF Profile
CREATE OR REPLACE TABLE FIN_TRADE_EXTRACT.RAW.ETF_PROFILE (
  SYMBOL_ID NUMBER PRIMARY KEY,
  SYMBOL VARCHAR(20),
  NET_ASSETS VARCHAR,
  NET_EXPENSE_RATIO VARCHAR,
  PORTFOLIO_TURNOVER VARCHAR,
  DIVIDEND_YIELD VARCHAR,
  INCEPTION_DATE VARCHAR,
  LEVERAGED VARCHAR,
  SECTORS ARRAY,
  HOLDINGS ARRAY,
  LOAD_DATE DATE DEFAULT CURRENT_DATE()
);

-- Load and parse data from S3 directly into the parsed table
INSERT OVERWRITE INTO FIN_TRADE_EXTRACT.RAW.ETF_PROFILE
SELECT
  ABS(HASH(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '.', 1))) AS SYMBOL_ID,
  SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '.', 1)::STRING AS SYMBOL,
  $1:"net_assets"::STRING AS NET_ASSETS,
  $1:"net_expense_ratio"::STRING AS NET_EXPENSE_RATIO,
  $1:"portfolio_turnover"::STRING AS PORTFOLIO_TURNOVER,
  $1:"dividend_yield"::STRING AS DIVIDEND_YIELD,
  $1:"inception_date"::STRING AS INCEPTION_DATE,
  $1:"leveraged"::STRING AS LEVERAGED,
  $1:"sectors" AS SECTORS,
  $1:"holdings" AS HOLDINGS,
  CURRENT_DATE() AS LOAD_DATE
FROM @FIN_TRADE_EXTRACT.RAW.ETF_PROFILE_STAGE;

-- Show load results
SELECT COUNT(*) AS total_records FROM FIN_TRADE_EXTRACT.RAW.ETF_PROFILE;
SELECT SYMBOL, SYMBOL_ID FROM FIN_TRADE_EXTRACT.RAW.ETF_PROFILE LIMIT 10;



-- Flatten sectors array (one record per entry, with renamed columns and holding_sector)
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.RAW.ETF_PROFILE_SECTORS AS
SELECT
  SYMBOL_ID,
  SYMBOL,
  sector.value:sector::STRING AS sector_name,
  sector.value:weight::STRING AS sector_weight,
  'SECTORS' AS holding_sector,
  LOAD_DATE
FROM FIN_TRADE_EXTRACT.RAW.ETF_PROFILE,
  LATERAL FLATTEN(input => SECTORS) sector;




-- Create a single final detail table with one record per sector or holding
CREATE OR REPLACE TABLE FIN_TRADE_EXTRACT.RAW.ETF_PROFILE_DETAIL AS
SELECT
  SYMBOL_ID,
  SYMBOL,
  NULL AS holding_symbol,
  NULL AS holding_description,
  NULL AS holding_weight,
  sector.value:sector::STRING AS sector_name,
  sector.value:weight::STRING AS sector_weight,
  'SECTORS' AS holding_sector,
  LOAD_DATE
FROM FIN_TRADE_EXTRACT.RAW.ETF_PROFILE,
  LATERAL FLATTEN(input => SECTORS) sector
UNION ALL
SELECT
  SYMBOL_ID,
  SYMBOL,
  holding.value:symbol::STRING AS holding_symbol,
  holding.value:description::STRING AS holding_description,
  holding.value:weight::STRING AS holding_weight,
  NULL AS sector_name,
  NULL AS sector_weight,
  'HOLDINGS' AS holding_sector,
  LOAD_DATE
FROM FIN_TRADE_EXTRACT.RAW.ETF_PROFILE,
  LATERAL FLATTEN(input => HOLDINGS) holding;

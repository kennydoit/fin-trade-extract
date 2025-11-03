-- Create a view to extract all top-level fields from ETF_PROFILE, and flatten sectors and holdings arrays

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

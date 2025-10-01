USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;

-- List matching files in the stage for 2025-09-29
LIST @OVERVIEW_STAGE pattern='.*overview_20250929.*\\.csv';

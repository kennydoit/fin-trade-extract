
-- Manual update of watermarks for any table_name in ETL_WATERMARKS

-- Parameters needed: 
--    :table_name - name of the table in ETL_WATERMARKS to update, e.g., 'CASH_FLOW'
--    :api_eligible - new value for API_ELIGIBLE column, e.g., 'YES', 'NO', 'DEL'
--    :date_column - name of the date column in Alpha Vantage data, e.g., 'QUARTER', 'FISCAL_DATE_ENDING'

-- Special conditions:
--    if date_column is 'QUARTER', then the FIRST_FISCAL_DATE and LAST_FISCAL_DATE values
--    need to be converted from 'YYYYQX' format to date format 'YYYY-MM-DD' using the following logic:
--       Q1 -> '03-31'
--       Q2 -> '06-30'
--       Q3 -> '09-30'
--       Q4 -> '12-31'

-- Actions:
--    1. query the raw table name for the min and max of the specified date_column for each symbol:
--         e.g., SELECT SYMBOL,MIN(TO_DATE(FISCAL_DATE_ENDING)), MAX(TO_DATE(FISCAL_DATE_ENDING)) FROM RAW.INCOME_STATEMENT GROUP BY SYMBOL;
--    2. join the min and max dates by table name and symbol to ETL_WATERMARKS and update the API_ELIGIBLE, FIRST_FISCAL_DATE, LAST_FISCAL_DATE columns

-- Set parameters
SET table_name = 'CASH_FLOW'; -- Change as needed
SET api_eligible = 'YES';     -- Change as needed
SET date_column = 'FISCAL_DATE_ENDING'; -- Or 'QUARTER'

-- For FISCAL_DATE_ENDING (standard date column)
MERGE INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS target
USING (
    SELECT
        SYMBOL,
        MIN(TO_DATE(FISCAL_DATE_ENDING)) AS FIRST_FISCAL_DATE,
        MAX(TO_DATE(FISCAL_DATE_ENDING)) AS LAST_FISCAL_DATE
    FROM FIN_TRADE_EXTRACT.RAW.CASH_FLOW -- Change to your raw table
    GROUP BY SYMBOL
) source
ON target.TABLE_NAME = $table_name AND target.SYMBOL = source.SYMBOL
WHEN MATCHED THEN UPDATE SET
    FIRST_FISCAL_DATE = source.FIRST_FISCAL_DATE,
    LAST_FISCAL_DATE = source.LAST_FISCAL_DATE,
    API_ELIGIBLE = $api_eligible,
    UPDATED_AT = CURRENT_TIMESTAMP();

-- For QUARTER (e.g., '2022Q3' format)
MERGE INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS target
USING (
    SELECT
        SYMBOL,
        MIN(TO_DATE(
            LEFT(QUARTER, 4) || '-' ||
            CASE RIGHT(QUARTER, 2)
                WHEN 'Q1' THEN '03-31'
                WHEN 'Q2' THEN '06-30'
                WHEN 'Q3' THEN '09-30'
                WHEN 'Q4' THEN '12-31'
            END
        )) AS FIRST_FISCAL_DATE,
        MAX(TO_DATE(
            LEFT(QUARTER, 4) || '-' ||
            CASE RIGHT(QUARTER, 2)
                WHEN 'Q1' THEN '03-31'
                WHEN 'Q2' THEN '06-30'
                WHEN 'Q3' THEN '09-30'
                WHEN 'Q4' THEN '12-31'
            END
        )) AS LAST_FISCAL_DATE
    FROM FIN_TRADE_EXTRACT.RAW.CASH_FLOW -- Change to your raw table
    GROUP BY SYMBOL
) source
ON target.TABLE_NAME = $table_name AND target.SYMBOL = source.SYMBOL
WHEN MATCHED THEN UPDATE SET
    FIRST_FISCAL_DATE = source.FIRST_FISCAL_DATE,
    LAST_FISCAL_DATE = source.LAST_FISCAL_DATE,
    API_ELIGIBLE = $api_eligible,
    UPDATED_AT = CURRENT_TIMESTAMP();
-- ============================================================================
-- Initialize ETL Watermarks for Cash Flow Data
-- 
-- Purpose: Creates watermark records for all eligible symbols
-- Table: CASH_FLOW
-- Source: ETL_WATERMARKS table
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Insert watermark records for all active stocks not already tracked for CASH_FLOW
INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS (
    TABLE_NAME,
    SYMBOL,
    API_ELIGIBLE,
    EXCHANGE,
    ASSET_TYPE,
    STATUS,
    DELISTING_DATE,
    FIRST_FISCAL_DATE,
    LAST_FISCAL_DATE,
    LAST_SUCCESSFUL_RUN,
    CONSECUTIVE_FAILURES,
    CREATED_AT,
    UPDATED_AT
)
SELECT 
    'CASH_FLOW' as TABLE_NAME,
    SYMBOL,
    CASE 
        WHEN DELISTING_DATE IS NOT NULL AND DELISTING_DATE <= CURRENT_DATE() THEN 'DEL'
        WHEN ASSET_TYPE = 'Stock' AND STATUS = 'Active' THEN 'YES'
        ELSE 'NO'
    END as API_ELIGIBLE,
    EXCHANGE,
    ASSET_TYPE,
    STATUS,
    DELISTING_DATE,
    NULL as FIRST_FISCAL_DATE,      -- Will be set during first successful extraction
    NULL as LAST_FISCAL_DATE,        -- Will be set during first successful extraction
    NULL as LAST_SUCCESSFUL_RUN,     -- Will be set after first run
    0 as CONSECUTIVE_FAILURES,
    CURRENT_TIMESTAMP() as CREATED_AT,
    CURRENT_TIMESTAMP() as UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.SYMBOL
WHERE NOT EXISTS (
    SELECT 1 
    FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
    WHERE w.TABLE_NAME = 'CASH_FLOW'
      AND w.SYMBOL = SYMBOL.SYMBOL
);

SELECT '
====================================================================
CASH_FLOW WATERMARKS INITIALIZED

Next steps:
1. Run the GitHub Actions workflow:
   .github/workflows/cash_flow_watermark_etl.yml

2. Test with a small batch first:
   - Set max_symbols: 5
   - Leave exchange_filter blank
   - Monitor execution

3. After successful test:
   - Remove max_symbols limit
   - Let it process all eligible symbols
   - Future runs will use 135-day staleness logic

4. Check watermark updates:
   SELECT * FROM ETL_WATERMARKS 
   WHERE TABLE_NAME = ''CASH_FLOW'' 
   ORDER BY LAST_SUCCESSFUL_RUN DESC LIMIT 20;

5. Verify data loaded:
   SELECT COUNT(*), MIN(FISCAL_DATE_ENDING), MAX(FISCAL_DATE_ENDING)
   FROM CASH_FLOW;
====================================================================
' as instructions;

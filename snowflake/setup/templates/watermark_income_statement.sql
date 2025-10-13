-- ============================================================================
-- Initialize ETL Watermarks for Income Statement Data
-- 
-- Purpose: Creates watermark records for all eligible symbols
-- Table: INCOME_STATEMENT
-- Source: ETL_WATERMARKS table
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Insert watermark records for all active stocks not already tracked for INCOME_STATEMENT
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
    'INCOME_STATEMENT' as TABLE_NAME,
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
    WHERE w.TABLE_NAME = 'INCOME_STATEMENT'
      AND w.SYMBOL = SYMBOL.SYMBOL
);

-- Show summary of created records
SELECT 
    '✅ Income Statement Watermarks Created' as status,
    COUNT(*) as total_records,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as eligible_for_api,
    COUNT(CASE WHEN API_ELIGIBLE = 'NO' THEN 1 END) as not_eligible,
    COUNT(CASE WHEN API_ELIGIBLE = 'DEL' THEN 1 END) as delisted,
    COUNT(DISTINCT EXCHANGE) as unique_exchanges
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'INCOME_STATEMENT';

-- Show breakdown by exchange
SELECT 
    'Breakdown by Exchange' as info,
    EXCHANGE,
    COUNT(*) as total,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as eligible
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'INCOME_STATEMENT'
GROUP BY EXCHANGE
ORDER BY total DESC;

SELECT '
====================================================================
INCOME_STATEMENT WATERMARKS INITIALIZED

Next steps:
1. Run the GitHub Actions workflow:
   .github/workflows/income_statement_watermark_etl.yml

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
   WHERE TABLE_NAME = ''INCOME_STATEMENT'' 
   ORDER BY LAST_SUCCESSFUL_RUN DESC LIMIT 20;

5. Verify data loaded:
   SELECT COUNT(*), MIN(FISCAL_DATE_ENDING), MAX(FISCAL_DATE_ENDING)
   FROM INCOME_STATEMENT;
====================================================================
' as instructions;

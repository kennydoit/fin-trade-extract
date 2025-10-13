-- ============================================================================
-- Initialize ETL Watermarks for Insider Transactions Data
-- 
-- Purpose: Creates watermark records for all eligible symbols
-- Table: INSIDER_TRANSACTIONS
-- Source: ETL_WATERMARKS table (or SYMBOL table if first time)
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Insert watermark records for all active stocks not already tracked for INSIDER_TRANSACTIONS
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
    'INSIDER_TRANSACTIONS' as TABLE_NAME,
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
    NULL as FIRST_FISCAL_DATE,      -- Will be set to earliest transaction date
    NULL as LAST_FISCAL_DATE,        -- Will be set to latest transaction date
    NULL as LAST_SUCCESSFUL_RUN,     -- Will be set after first run
    0 as CONSECUTIVE_FAILURES,
    CURRENT_TIMESTAMP() as CREATED_AT,
    CURRENT_TIMESTAMP() as UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.SYMBOL
WHERE NOT EXISTS (
    SELECT 1 
    FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
    WHERE w.TABLE_NAME = 'INSIDER_TRANSACTIONS'
      AND w.SYMBOL = SYMBOL.SYMBOL
);

-- Show summary of created records
SELECT 
    '✅ Insider Transactions Watermarks Created' as status,
    COUNT(*) as total_records,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as eligible_for_api,
    COUNT(CASE WHEN API_ELIGIBLE = 'NO' THEN 1 END) as not_eligible,
    COUNT(CASE WHEN API_ELIGIBLE = 'DEL' THEN 1 END) as delisted,
    COUNT(DISTINCT EXCHANGE) as unique_exchanges
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'INSIDER_TRANSACTIONS';

-- Show breakdown by exchange
SELECT 
    'Breakdown by Exchange' as info,
    EXCHANGE,
    COUNT(*) as total,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as eligible
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'INSIDER_TRANSACTIONS'
GROUP BY EXCHANGE
ORDER BY total DESC;

SELECT '
====================================================================
INSIDER_TRANSACTIONS WATERMARKS INITIALIZED

Key differences from fundamentals ETL:
- NO staleness check (insider trading is sporadic and unpredictable)
- Companies may have zero transactions for months (this is normal)
- Some companies have frequent insider activity, others have none
- Run monthly to capture recent insider trading activity

Next steps:
1. Run the GitHub Actions workflow:
   .github/workflows/insider_transactions_watermark_etl.yml

2. Test with a small batch first:
   - Set max_symbols: 10
   - Leave exchange_filter blank
   - Monitor execution

3. Expected behavior:
   - Many symbols will have "no data" (normal for insider transactions)
   - Some symbols will have transactions (executives buying/selling)
   - Watermarks track LAST_FISCAL_DATE = latest transaction date
   - No staleness check (always re-fetch when run)

4. After successful test:
   - Remove max_symbols limit
   - Run monthly via schedule or manually
   - Use skip_recent_hours to avoid re-processing same day

5. Check watermark updates:
   SELECT * FROM ETL_WATERMARKS 
   WHERE TABLE_NAME = ''INSIDER_TRANSACTIONS'' 
   ORDER BY LAST_SUCCESSFUL_RUN DESC LIMIT 20;

6. Verify data loaded:
   SELECT 
       COUNT(*) as total_transactions,
       COUNT(DISTINCT SYMBOL) as symbols_with_activity,
       MIN(TRANSACTION_DATE) as earliest_transaction,
       MAX(TRANSACTION_DATE) as latest_transaction
   FROM INSIDER_TRANSACTIONS;

7. View recent insider activity:
   SELECT SYMBOL, TRANSACTION_DATE, OWNER_NAME, 
          ACQUISITION_DISPOSITION, SHARES, VALUE
   FROM INSIDER_TRANSACTIONS
   WHERE TRANSACTION_DATE >= DATEADD(day, -30, CURRENT_DATE())
   ORDER BY TRANSACTION_DATE DESC
   LIMIT 20;
====================================================================
' as instructions;

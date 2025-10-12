-- ============================================================================
-- Diagnose BALANCE_SHEET Watermark Staleness Issue
-- 
-- Problem: 135-day staleness logic not working - re-pulling symbols
-- Possible causes:
-- 1. LAST_FISCAL_DATE not being set during watermark updates
-- 2. LAST_FISCAL_DATE is NULL for all symbols
-- 3. Date comparison logic issue
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- STEP 1: Check if LAST_FISCAL_DATE is being set
SELECT 
    '1. LAST_FISCAL_DATE Population Check' as diagnostic_step,
    COUNT(*) as total_watermarks,
    COUNT(LAST_FISCAL_DATE) as has_last_fiscal_date,
    COUNT(FIRST_FISCAL_DATE) as has_first_fiscal_date,
    COUNT(LAST_SUCCESSFUL_RUN) as has_last_run,
    ROUND(COUNT(LAST_FISCAL_DATE) * 100.0 / COUNT(*), 2) as pct_with_last_fiscal_date
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
  AND API_ELIGIBLE = 'YES';

-- STEP 2: Show sample watermarks with dates
SELECT 
    '2. Sample Watermarks with Dates' as diagnostic_step,
    SYMBOL,
    FIRST_FISCAL_DATE,
    LAST_FISCAL_DATE,
    LAST_SUCCESSFUL_RUN,
    DATEDIFF(day, LAST_FISCAL_DATE, CURRENT_DATE()) as days_since_last_fiscal,
    CASE 
        WHEN LAST_FISCAL_DATE IS NULL THEN 'NULL - Will be pulled'
        WHEN LAST_FISCAL_DATE < DATEADD(day, -135, CURRENT_DATE()) THEN 'OLD - Will be pulled'
        ELSE 'RECENT - Will be skipped'
    END as staleness_status
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
  AND API_ELIGIBLE = 'YES'
ORDER BY LAST_FISCAL_DATE DESC NULLS FIRST
LIMIT 20;

-- STEP 3: Count symbols by staleness
SELECT 
    '3. Staleness Distribution' as diagnostic_step,
    CASE 
        WHEN LAST_FISCAL_DATE IS NULL THEN '1. NULL (never pulled)'
        WHEN LAST_FISCAL_DATE < DATEADD(day, -135, CURRENT_DATE()) THEN '2. STALE (>135 days old)'
        ELSE '3. FRESH (<135 days old)'
    END as category,
    COUNT(*) as symbol_count
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
  AND API_ELIGIBLE = 'YES'
GROUP BY category
ORDER BY category;

-- STEP 4: Show what would be pulled with current logic
SELECT 
    '4. Symbols That Would Be Pulled' as diagnostic_step,
    SYMBOL,
    LAST_FISCAL_DATE,
    LAST_SUCCESSFUL_RUN,
    DATEDIFF(day, LAST_FISCAL_DATE, CURRENT_DATE()) as days_old
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
  AND API_ELIGIBLE = 'YES'
  AND (LAST_FISCAL_DATE IS NULL 
       OR LAST_FISCAL_DATE < DATEADD(day, -135, CURRENT_DATE()))
ORDER BY LAST_FISCAL_DATE NULLS FIRST
LIMIT 20;

-- STEP 5: Check for recent updates that shouldn't be pulled again
SELECT 
    '5. Recently Updated Symbols (should NOT be pulled)' as diagnostic_step,
    SYMBOL,
    LAST_FISCAL_DATE,
    LAST_SUCCESSFUL_RUN,
    DATEDIFF(day, LAST_FISCAL_DATE, CURRENT_DATE()) as days_since_last_fiscal,
    DATEDIFF(hour, LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) as hours_since_last_run
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
  AND API_ELIGIBLE = 'YES'
  AND LAST_SUCCESSFUL_RUN IS NOT NULL
  AND LAST_SUCCESSFUL_RUN > DATEADD(day, -7, CURRENT_TIMESTAMP())  -- Updated in last 7 days
ORDER BY LAST_SUCCESSFUL_RUN DESC
LIMIT 20;

-- STEP 6: Check the actual BALANCE_SHEET table data
SELECT 
    '6. Latest Data in BALANCE_SHEET Table' as diagnostic_step,
    SYMBOL,
    MAX(FISCAL_DATE_ENDING) as latest_fiscal_date,
    COUNT(*) as record_count,
    MAX(LOAD_DATE) as last_loaded
FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET
GROUP BY SYMBOL
ORDER BY latest_fiscal_date DESC
LIMIT 20;

-- STEP 7: Cross-check watermarks vs actual data
SELECT 
    '7. Watermark vs Actual Data Mismatch' as diagnostic_step,
    w.SYMBOL,
    w.LAST_FISCAL_DATE as watermark_last_fiscal,
    MAX(b.FISCAL_DATE_ENDING) as actual_last_fiscal,
    DATEDIFF(day, MAX(b.FISCAL_DATE_ENDING), w.LAST_FISCAL_DATE) as date_diff_days,
    CASE 
        WHEN w.LAST_FISCAL_DATE IS NULL THEN 'WATERMARK NULL'
        WHEN MAX(b.FISCAL_DATE_ENDING) IS NULL THEN 'NO DATA LOADED'
        WHEN w.LAST_FISCAL_DATE != MAX(b.FISCAL_DATE_ENDING) THEN 'MISMATCH!'
        ELSE 'MATCH'
    END as status
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
LEFT JOIN FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET b ON w.SYMBOL = b.SYMBOL
WHERE w.TABLE_NAME = 'BALANCE_SHEET'
  AND w.API_ELIGIBLE = 'YES'
  AND w.LAST_SUCCESSFUL_RUN IS NOT NULL
GROUP BY w.SYMBOL, w.LAST_FISCAL_DATE
HAVING (CASE 
        WHEN w.LAST_FISCAL_DATE IS NULL THEN 'WATERMARK NULL'
        WHEN MAX(b.FISCAL_DATE_ENDING) IS NULL THEN 'NO DATA LOADED'
        WHEN w.LAST_FISCAL_DATE != MAX(b.FISCAL_DATE_ENDING) THEN 'MISMATCH!'
        ELSE 'MATCH'
    END) != 'MATCH'
LIMIT 20;

-- STEP 8: Show the exact SQL logic used in Python script
SELECT 
    '8. Test the Exact WHERE Clause from Python' as diagnostic_step,
    COUNT(*) as symbols_to_process
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
  AND API_ELIGIBLE = 'YES'
  AND (LAST_FISCAL_DATE IS NULL 
       OR LAST_FISCAL_DATE < DATEADD(day, -135, CURRENT_DATE()));

SELECT '
====================================================================
DIAGNOSTICS COMPLETE

Review the results above to identify the issue:

1. If "pct_with_last_fiscal_date" is 0% or low:
   → LAST_FISCAL_DATE is not being set during watermark updates
   → Check the bulk_update_watermarks() function
   
2. If Step 7 shows MISMATCHES:
   → Watermarks are not reflecting actual loaded data
   → The MERGE statement may not be executing properly
   
3. If Step 5 shows recently updated symbols:
   → But they still appear in Step 4 as "would be pulled"
   → The staleness logic is broken
   
4. Expected behavior after first successful run:
   → Step 3 should show mostly "3. FRESH (<135 days old)"
   → Step 4 should show very few or zero symbols
   → Step 8 count should be close to 0 (until 135 days pass)

Next Actions:
- If LAST_FISCAL_DATE is NULL for all: Check watermark update code
- If dates are set but symbols still pulled: Check WHERE clause logic
- If mismatches found: Manual UPDATE to fix watermarks
====================================================================
' as instructions;

-- ============================================================================
-- Reset Balance Sheet Watermarks and Table
-- 
-- Use this script to clean up balance sheet data and watermarks, then re-initialize
-- them using the add_data_source_watermarks.yml workflow.
-- 
-- When to use:
-- - After testing with old fetch_balance_sheet_bulk.py (has load_date column)
-- - When you need to clear watermark state (dates, run timestamps)
-- - Before switching to production watermark-based ETL
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- STEP 1: Show current state
SELECT 
    'Before cleanup' as status,
    COUNT(*) as watermark_count,
    COUNT(CASE WHEN LAST_SUCCESSFUL_RUN IS NOT NULL THEN 1 END) as symbols_with_runs,
    COUNT(CASE WHEN FIRST_FISCAL_DATE IS NOT NULL THEN 1 END) as symbols_with_data
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET';

SELECT 
    'Before cleanup' as status,
    COUNT(*) as balance_sheet_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    MIN(FISCAL_DATE_ENDING) as earliest_date,
    MAX(FISCAL_DATE_ENDING) as latest_date
FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET;

-- STEP 2: Delete all BALANCE_SHEET watermarks
DELETE FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
WHERE TABLE_NAME = 'BALANCE_SHEET';

SELECT 
    '✅ Deleted BALANCE_SHEET watermarks' as result,
    COUNT(*) as remaining_watermarks
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
WHERE TABLE_NAME = 'BALANCE_SHEET';

-- STEP 3: Drop the BALANCE_SHEET table (will be recreated by load script)
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET;

SELECT '✅ BALANCE_SHEET table dropped' as result;

-- STEP 4: Verify cleanup
SELECT 
    'After cleanup' as status,
    COUNT(*) as watermark_count
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET';

-- STEP 5: Instructions
SELECT '
====================================================================
✅ Cleanup Complete!

Next Steps:
1. Go to GitHub Actions → Add Data Source Watermarks workflow
2. Select "BALANCE_SHEET" from the dropdown
3. Run the workflow to recreate fresh watermarks

This will:
- Create watermarks for all active common stocks
- Set API_ELIGIBLE = YES for active stocks
- Set API_ELIGIBLE = NO for delisted/non-stock symbols
- Initialize all watermark fields to NULL (ready for first run)

Then run:
- balance_sheet_watermark_etl.yml with max_symbols: 5 (test)
- Verify data loads correctly
- Run full extraction
====================================================================
' as instructions;

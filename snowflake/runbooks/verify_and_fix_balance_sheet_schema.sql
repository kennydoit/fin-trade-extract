-- ============================================================================
-- Verify and Fix BALANCE_SHEET Schema - LOAD_DATE Issue
-- 
-- Problem: Table was created with CREATED_AT before we fixed the schema
-- Solution: Drop and let it recreate with correct LOAD_DATE column
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- STEP 1: Check current schema
SELECT 'Current BALANCE_SHEET schema:' as status;

DESCRIBE TABLE FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET;

-- STEP 2: Look for CREATED_AT column (should NOT exist!)
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'RAW'
  AND TABLE_NAME = 'BALANCE_SHEET'
  AND COLUMN_NAME IN ('CREATED_AT', 'LOAD_DATE')
ORDER BY COLUMN_NAME;

-- STEP 3: Show current data count
SELECT 
    'Current data count' as status,
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    MIN(FISCAL_DATE_ENDING) as earliest_date,
    MAX(FISCAL_DATE_ENDING) as latest_date
FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET;

-- STEP 4: Show current watermark count
SELECT 
    'Current watermark count' as status,
    COUNT(*) as watermark_count,
    COUNT(CASE WHEN LAST_SUCCESSFUL_RUN IS NOT NULL THEN 1 END) as symbols_with_runs,
    COUNT(CASE WHEN FIRST_FISCAL_DATE IS NOT NULL THEN 1 END) as symbols_with_data
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET';

-- STEP 5: Delete all BALANCE_SHEET watermarks
DELETE FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
WHERE TABLE_NAME = 'BALANCE_SHEET';

SELECT 
    '✅ Deleted BALANCE_SHEET watermarks' as result,
    COUNT(*) as remaining_watermarks
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
WHERE TABLE_NAME = 'BALANCE_SHEET';
-- Should return 0

-- STEP 6: Drop the table (will be recreated with correct schema)
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET;

SELECT '✅ BALANCE_SHEET table dropped' as result;

-- STEP 7: Verify table is gone
SELECT 'Verify table dropped:' as status;

SELECT COUNT(*) as table_exists
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'RAW'
  AND TABLE_NAME = 'BALANCE_SHEET';
-- Should return 0

SELECT '
====================================================================
✅ BALANCE_SHEET cleanup complete!

What was done:
1. Deleted all BALANCE_SHEET watermarks from ETL_WATERMARKS table
2. Dropped BALANCE_SHEET table (had wrong schema with CREATED_AT)

Next Steps:
1. Go to GitHub Actions → Add Data Source Watermarks workflow
2. Select "BALANCE_SHEET" from the dropdown
3. Run the workflow to recreate fresh watermarks

This will:
- Create watermarks for all active common stocks
- Set API_ELIGIBLE = YES for active stocks
- Set API_ELIGIBLE = NO for delisted/non-stock symbols
- Initialize all watermark fields to NULL (ready for first run)

4. Run balance_sheet_watermark_etl.yml with max_symbols: 5 (test)
5. The load_balance_sheet_from_s3.sql will CREATE the table with:
   - LOAD_DATE DATE DEFAULT CURRENT_DATE()  ✅ CORRECT
   - NOT CREATED_AT TIMESTAMP_NTZ           ❌ OLD/WRONG

The latest code in load_balance_sheet_from_s3.sql (line 95) now has:
   LOAD_DATE DATE DEFAULT CURRENT_DATE()

This ensures consistency across all tables!
====================================================================
' as instructions;

-- ============================================================================
-- Fix COMPANY_OVERVIEW LOAD_DATE Column
-- 
-- Changes LOAD_DATE from VARCHAR(50) to DATE to match standardization
-- 
-- When to use:
-- - After updating load_company_overview_from_s3.sql to use DATE type
-- - To fix existing data with VARCHAR LOAD_DATE values
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- STEP 1: Show current state
SELECT 
    'Before fix' as status,
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    TYPEOF(LOAD_DATE) as load_date_type
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW
LIMIT 1;

-- STEP 2: Check sample LOAD_DATE values
SELECT 
    'Sample LOAD_DATE values' as status,
    SYMBOL,
    LOAD_DATE,
    TYPEOF(LOAD_DATE) as current_type
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW
LIMIT 10;

-- STEP 3: Drop and recreate table with correct schema
-- WARNING: This will delete all existing data!
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW;

SELECT '✅ Old COMPANY_OVERVIEW table dropped' as result;

-- STEP 4: Table will be recreated by load_company_overview_from_s3.sql
-- with correct LOAD_DATE DATE column

SELECT '
====================================================================
✅ COMPANY_OVERVIEW table dropped!

Next Steps:
1. Re-run the company overview extraction workflow:
   - Go to GitHub Actions → overview_full_universe.yml
   - Run the workflow to extract and load fresh data
   
2. OR run the SQL loader manually:
   - Execute: snowflake/runbooks/load_company_overview_from_s3.sql
   - This will CREATE the table with correct schema
   - Then COPY and MERGE data from S3

The new table will have:
- LOAD_DATE DATE DEFAULT CURRENT_DATE()
- Consistent with TIME_SERIES, BALANCE_SHEET, LISTING_STATUS

Note: No watermarks need to be recreated (COMPANY_OVERVIEW watermarks 
are separate from the table itself)
====================================================================
' as instructions;

-- ============================================================================
-- Clean Up Existing Duplicates and Migrate to New Pipeline
-- Run this to clean existing data and set up the new enterprise pipeline
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;

-- ============================================================================
-- STEP 1: Clean up existing duplicate AAPL records
-- ============================================================================

USE SCHEMA RAW;

-- Check current duplicates
SELECT 'CURRENT DUPLICATES' as step;
SELECT 
    SYMBOL,
    COUNT(*) as duplicate_count,
    MIN(LOADED_AT) as first_loaded,
    MAX(LOADED_AT) as last_loaded
FROM OVERVIEW 
GROUP BY SYMBOL
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Keep only the most recent record per symbol
SELECT 'REMOVING DUPLICATES' as step;

-- Create temp table with deduplicated data
CREATE OR REPLACE TEMPORARY TABLE OVERVIEW_DEDUP AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY LOADED_AT DESC, 
                             CASE WHEN API_RESPONSE_STATUS = 'success' THEN 1 ELSE 2 END,
                             CASE WHEN NAME IS NOT NULL THEN 1 ELSE 2 END) as rn
    FROM OVERVIEW
) 
WHERE rn = 1;

-- Check deduplication results
SELECT 'DEDUPLICATION RESULTS' as step;
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    SUM(CASE WHEN API_RESPONSE_STATUS = 'success' THEN 1 ELSE 0 END) as successful_records
FROM OVERVIEW_DEDUP;

-- Replace current table with deduplicated data
TRUNCATE TABLE OVERVIEW;

INSERT INTO OVERVIEW 
SELECT * FROM OVERVIEW_DEDUP;

-- Verify cleanup
SELECT 'CLEANUP VERIFICATION' as step;
SELECT 
    SYMBOL,
    NAME,
    API_RESPONSE_STATUS,
    LOADED_AT
FROM OVERVIEW
ORDER BY SYMBOL;

-- ============================================================================
-- STEP 2: Set up the new enterprise pipeline
-- ============================================================================

SELECT 'SETTING UP ENTERPRISE PIPELINE' as step;

-- Execute the enterprise pipeline setup
-- (This would normally be done by running enterprise-overview-pipeline.sql)

-- For now, let's create the essential staging components
CREATE SCHEMA IF NOT EXISTS STAGING;

-- ============================================================================
-- STEP 3: Manual bulk load with deduplication
-- ============================================================================

-- Create a manual deduplication procedure for immediate use
CREATE OR REPLACE PROCEDURE DEDUPLICATE_OVERVIEW()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Create temp table with latest record per symbol
    CREATE OR REPLACE TEMPORARY TABLE OVERVIEW_LATEST AS
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY SYMBOL 
                   ORDER BY 
                       SOURCE_CREATED_AT DESC NULLS LAST,
                       LOADED_AT DESC,
                       CASE WHEN API_RESPONSE_STATUS = 'success' THEN 1 ELSE 2 END,
                       CASE WHEN NAME IS NOT NULL AND NAME != '' THEN 1 ELSE 2 END
               ) as rn
        FROM OVERVIEW
    ) 
    WHERE rn = 1;
    
    -- Replace table contents
    DELETE FROM OVERVIEW;
    
    INSERT INTO OVERVIEW 
    SELECT * FROM OVERVIEW_LATEST;
    
    RETURN 'Deduplication completed. Kept latest record per symbol.';
END;
$$;

-- Run deduplication
CALL DEDUPLICATE_OVERVIEW();

-- ============================================================================
-- STEP 4: Enhanced bulk loading with deduplication
-- ============================================================================

-- Create procedure for bulk loading with automatic deduplication
CREATE OR REPLACE PROCEDURE BULK_LOAD_WITH_DEDUP(SYMBOLS_LIST ARRAY)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Convert array to string for Lambda payload
    var symbols = SYMBOLS_LIST;
    var batch_id = 'bulk_' + new Date().toISOString().replace(/[:.]/g, '-');
    
    // This would trigger Lambda function (simulated here)
    // In practice, you'd call this from PowerShell/Python
    
    return "Batch " + batch_id + " prepared for " + symbols.length + " symbols. " +
           "Run PowerShell script to execute Lambda and then call DEDUPLICATE_OVERVIEW().";
$$;

-- ============================================================================
-- STEP 5: Create monitoring views
-- ============================================================================

-- View for current overview status
CREATE OR REPLACE VIEW OVERVIEW_SUMMARY AS
SELECT 
    COUNT(*) as total_companies,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    COUNT(CASE WHEN API_RESPONSE_STATUS = 'success' THEN 1 END) as successful_loads,
    COUNT(CASE WHEN NAME IS NOT NULL AND NAME != '' THEN 1 END) as companies_with_names,
    MAX(LOADED_AT) as latest_load_time,
    AVG(CASE WHEN MARKET_CAPITALIZATION > 0 THEN MARKET_CAPITALIZATION END) as avg_market_cap
FROM OVERVIEW;

-- View for data quality
CREATE OR REPLACE VIEW OVERVIEW_DATA_QUALITY AS
SELECT 
    SYMBOL,
    NAME,
    API_RESPONSE_STATUS,
    CASE 
        WHEN API_RESPONSE_STATUS = 'success' AND NAME IS NOT NULL AND NAME != '' 
             AND MARKET_CAPITALIZATION IS NOT NULL THEN 'HIGH'
        WHEN API_RESPONSE_STATUS = 'success' AND NAME IS NOT NULL THEN 'MEDIUM'
        WHEN API_RESPONSE_STATUS = 'success' THEN 'LOW'
        ELSE 'FAILED'
    END as data_quality,
    MARKET_CAPITALIZATION,
    LOADED_AT
FROM OVERVIEW
ORDER BY 
    CASE data_quality 
        WHEN 'HIGH' THEN 1 
        WHEN 'MEDIUM' THEN 2 
        WHEN 'LOW' THEN 3 
        ELSE 4 
    END,
    SYMBOL;

-- ============================================================================
-- VERIFICATION AND NEXT STEPS
-- ============================================================================

-- Check current status
SELECT 'CURRENT STATUS' as step;
SELECT * FROM OVERVIEW_SUMMARY;

SELECT 'DATA QUALITY BREAKDOWN' as step;
SELECT 
    data_quality,
    COUNT(*) as count
FROM OVERVIEW_DATA_QUALITY
GROUP BY data_quality
ORDER BY count DESC;

SELECT 'NEXT STEPS' as step,
       '1. Run bulk load PowerShell script for more symbols' as action_1,
       '2. Call DEDUPLICATE_OVERVIEW() after each bulk load' as action_2,
       '3. Set up enterprise pipeline with staging tables' as action_3,
       '4. Monitor with OVERVIEW_SUMMARY and OVERVIEW_DATA_QUALITY views' as action_4;
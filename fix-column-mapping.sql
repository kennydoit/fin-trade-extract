-- ============================================================================
-- FIX COLUMN MAPPING ISSUE
-- The CSV has 50 columns but we need to map only specific ones to our 17-column table
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- Check current data
SELECT COUNT(*) as current_records FROM RAW.OVERVIEW;
SELECT SYMBOL, NAME, SECTOR, STATUS FROM RAW.OVERVIEW LIMIT 5;

-- Clear the table since the data mapping was wrong
TRUNCATE TABLE RAW.OVERVIEW;

-- Create a proper mapping using TRANSFORM clause to select specific columns
-- From our CSV analysis: columns 1-14 are company descriptors, 15-47 are financials, 48-50 are status/dates
COPY INTO RAW.OVERVIEW (
    SYMBOL_ID, SYMBOL, ASSETTYPE, NAME, DESCRIPTION, CIK, EXCHANGE, 
    CURRENCY, COUNTRY, SECTOR, INDUSTRY, ADDRESS, OFFICIALSITE, 
    FISCALYEAREND, STATUS, CREATED_AT, UPDATED_AT
)
FROM (
    SELECT 
        t.$1,   -- SYMBOL_ID
        t.$2,   -- SYMBOL  
        t.$3,   -- ASSET_TYPE
        t.$4,   -- NAME
        t.$5,   -- DESCRIPTION
        t.$6,   -- CIK
        t.$7,   -- EXCHANGE
        t.$8,   -- CURRENCY
        t.$9,   -- COUNTRY
        t.$10,  -- SECTOR
        t.$11,  -- INDUSTRY
        t.$12,  -- ADDRESS
        t.$13,  -- OFFICIAL_SITE
        t.$14,  -- FISCAL_YEAR_END
        t.$48,  -- API_RESPONSE_STATUS (skip columns 15-47)
        t.$49,  -- CREATED_AT
        t.$50   -- UPDATED_AT
    FROM @OVERVIEW_STAGE t
    WHERE METADATA$FILENAME = 'overview_20250928_035238_aad23b30.csv'
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Update audit fields
UPDATE RAW.OVERVIEW 
SET BATCH_ID = 'MANUAL_LOAD_20250928', 
    SOURCE_FILE_NAME = 'overview_20250928_035238_aad23b30.csv'
WHERE BATCH_ID IS NULL;

-- Check if data looks good now
SELECT COUNT(*) as records_loaded FROM RAW.OVERVIEW;
SELECT SYMBOL, NAME, SECTOR, EXCHANGE, STATUS FROM RAW.OVERVIEW WHERE NAME IS NOT NULL LIMIT 10;

-- If first file looks good, load the rest with same mapping
COPY INTO RAW.OVERVIEW (
    SYMBOL_ID, SYMBOL, ASSETTYPE, NAME, DESCRIPTION, CIK, EXCHANGE, 
    CURRENCY, COUNTRY, SECTOR, INDUSTRY, ADDRESS, OFFICIALSITE, 
    FISCALYEAREND, STATUS, CREATED_AT, UPDATED_AT
)
FROM (
    SELECT 
        t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, 
        t.$11, t.$12, t.$13, t.$14, t.$48, t.$49, t.$50
    FROM @OVERVIEW_STAGE t
    WHERE METADATA$FILENAME IN (
        'overview_20250928_035431_559ca0e6.csv',
        'overview_20250928_035537_6cbdb789.csv',
        'overview_20250928_035702_49a938d1.csv',
        'overview_20250928_125259_5c81f779.csv',
        'overview_20250928_125427_6e8be71b.csv',
        'overview_20250928_125458_6d0eb07b.csv',
        'overview_20250928_125529_aca3aa9e.csv'
    )
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Update audit fields for all remaining records
UPDATE RAW.OVERVIEW 
SET BATCH_ID = 'MANUAL_LOAD_20250928'
WHERE BATCH_ID IS NULL;

-- Final verification - should now have proper company data
SELECT COUNT(*) as total_records FROM RAW.OVERVIEW;
SELECT COUNT(*) as records_with_names FROM RAW.OVERVIEW WHERE NAME IS NOT NULL;

-- Show sample of properly loaded data
SELECT 
    SYMBOL, 
    NAME, 
    SECTOR, 
    INDUSTRY, 
    EXCHANGE,
    STATUS
FROM RAW.OVERVIEW 
WHERE NAME IS NOT NULL
ORDER BY SYMBOL 
LIMIT 20;

-- Summary by sector
SELECT 
    SECTOR,
    COUNT(*) as company_count
FROM RAW.OVERVIEW 
WHERE NAME IS NOT NULL AND SECTOR IS NOT NULL
GROUP BY SECTOR 
ORDER BY company_count DESC;
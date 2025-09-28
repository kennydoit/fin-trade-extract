-- ============================================================================
-- Fix SYMBOL_ID Format Issues
-- Update existing records and prepare for proper SYMBOL_ID generation
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: Check current SYMBOL_ID format
-- ============================================================================

SELECT 'CURRENT SYMBOL_ID FORMAT' as step;

SELECT 
    SYMBOL,
    SYMBOL_ID,
    CASE 
        WHEN SYMBOL_ID LIKE '%_ID' THEN 'SIMPLE_CONCATENATION'
        WHEN REGEXP_LIKE(SYMBOL_ID, '^[0-9]+$') THEN 'NUMERIC_ID'
        ELSE 'OTHER_FORMAT'
    END as current_format
FROM OVERVIEW
ORDER BY SYMBOL;

-- ============================================================================
-- STEP 2: Generate proper SYMBOL_IDs
-- ============================================================================

SELECT 'GENERATING PROPER SYMBOL_IDs' as step;

-- Create a mapping of proper SYMBOL_IDs
-- Using a hash-based approach for consistent numeric IDs
SELECT 
    SYMBOL,
    SYMBOL_ID as old_symbol_id,
    -- Generate a consistent numeric ID based on symbol
    ABS(HASH(SYMBOL)) % 100000000 as new_symbol_id,
    -- Alternative: sequence-based ID
    ROW_NUMBER() OVER (ORDER BY SYMBOL) + 10000000 as sequential_id
FROM OVERVIEW
ORDER BY SYMBOL;

-- ============================================================================
-- STEP 3: Update existing records with proper SYMBOL_IDs
-- ============================================================================

SELECT 'UPDATING SYMBOL_IDs' as step;

-- Update SYMBOL_ID to use hash-based numeric IDs
UPDATE OVERVIEW 
SET SYMBOL_ID = ABS(HASH(SYMBOL)) % 100000000;

-- Verify the update
SELECT 'UPDATED SYMBOL_IDs' as step;
SELECT 
    SYMBOL,
    SYMBOL_ID,
    'NUMERIC_ID' as format_type
FROM OVERVIEW
ORDER BY SYMBOL;

-- ============================================================================
-- STEP 4: Create a reference table for consistent SYMBOL_IDs
-- ============================================================================

-- Create a lookup table for consistent SYMBOL_ID mapping
CREATE OR REPLACE TABLE SYMBOL_ID_MAPPING (
    SYMBOL          VARCHAR(20) PRIMARY KEY,
    SYMBOL_ID       NUMBER(20,0) NOT NULL,
    CREATED_AT      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate with current mappings
INSERT INTO SYMBOL_ID_MAPPING (SYMBOL, SYMBOL_ID)
SELECT DISTINCT 
    SYMBOL,
    CAST(SYMBOL_ID AS NUMBER)
FROM OVERVIEW
WHERE SYMBOL_ID IS NOT NULL;

-- Show the mapping
SELECT 'SYMBOL_ID MAPPING TABLE' as step;
SELECT * FROM SYMBOL_ID_MAPPING ORDER BY SYMBOL;

-- ============================================================================
-- STEP 5: Create function for consistent SYMBOL_ID generation
-- ============================================================================

-- Create a function to generate consistent SYMBOL_IDs for new symbols
CREATE OR REPLACE FUNCTION GET_SYMBOL_ID(symbol_input VARCHAR)
RETURNS NUMBER
LANGUAGE JAVASCRIPT
AS $$
    // Generate consistent numeric ID from symbol
    var hash = 0;
    var symbol = SYMBOL_INPUT.toString();
    
    if (symbol.length === 0) return 0;
    
    for (var i = 0; i < symbol.length; i++) {
        var char = symbol.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    
    // Return positive number between 10000000 and 99999999
    return Math.abs(hash % 90000000) + 10000000;
$$;

-- Test the function
SELECT 'SYMBOL_ID FUNCTION TEST' as step;
SELECT 
    SYMBOL,
    GET_SYMBOL_ID(SYMBOL) as generated_id,
    SYMBOL_ID as current_id,
    CASE WHEN GET_SYMBOL_ID(SYMBOL) = SYMBOL_ID THEN 'MATCH' ELSE 'DIFFERENT' END as comparison
FROM OVERVIEW
LIMIT 5;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'FINAL VERIFICATION' as step;

-- Verify all SYMBOL_IDs are now numeric
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN REGEXP_LIKE(SYMBOL_ID::VARCHAR, '^[0-9]+$') THEN 1 END) as numeric_ids,
    COUNT(CASE WHEN SYMBOL_ID LIKE '%_ID' THEN 1 END) as old_format_ids
FROM OVERVIEW;

-- Show sample of corrected data
SELECT 
    SYMBOL,
    SYMBOL_ID,
    NAME,
    MARKET_CAPITALIZATION
FROM OVERVIEW
ORDER BY SYMBOL
LIMIT 10;
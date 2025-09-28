-- ============================================================================
-- SIMPLE TABLE STRUCTURE CHECK
-- Check the actual OVERVIEW table structure to fix column mapping
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- Show the exact structure of the OVERVIEW table
DESC TABLE RAW.OVERVIEW;

-- Show all column names in the table in order
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'RAW' 
AND TABLE_NAME = 'OVERVIEW'
ORDER BY ORDINAL_POSITION;

-- Count total columns in the table
SELECT COUNT(*) as table_column_count 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'RAW' 
AND TABLE_NAME = 'OVERVIEW';

-- Check a few sample rows to see existing data structure
SELECT * FROM RAW.OVERVIEW LIMIT 3;
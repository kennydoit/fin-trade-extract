-- ============================================================================
-- Initialize ETL Watermarks for Time Series Daily Adjusted Data
-- 
-- Purpose: Creates watermark records for all eligible symbols
-- Table: TIME_SERIES_DAILY_ADJUSTED
-- Source: ETL_WATERMARKS table
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Insert watermark records for all active tradable securities (stocks AND ETFs)
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
    'TIME_SERIES_DAILY_ADJUSTED' as TABLE_NAME,
    SYMBOL,
    CASE 
        WHEN DELISTING_DATE IS NOT NULL AND DELISTING_DATE <= CURRENT_DATE() THEN 'DEL'
        WHEN STATUS = 'Active' THEN 'YES'  -- All active securities (stocks AND ETFs)
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
    WHERE w.TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
      AND w.SYMBOL = SYMBOL.SYMBOL
);

SELECT '
====================================================================
TIME_SERIES_DAILY_ADJUSTED WATERMARKS INITIALIZED

Important Differences from Fundamentals:
- Includes BOTH stocks AND ETFs (all tradable securities)
- Uses 5-day staleness check (more frequent updates than fundamentals)
- Compact mode: Only recent data (100 days)
- Full mode: Complete history (20+ years)

Next steps:
1. Run the GitHub Actions workflow:
   .github/workflows/time_series_watermark_etl.yml

2. Test with a small batch first:
   - Set max_symbols: 5
   - Leave exchange_filter blank
   - Use compact mode (faster, recent data only)

3. After successful test:
   - Remove max_symbols limit
   - Let it process all eligible symbols
   - Consider full mode for backfill (takes much longer)

4. Check watermark updates:
   SELECT * FROM ETL_WATERMARKS 
   WHERE TABLE_NAME = ''TIME_SERIES_DAILY_ADJUSTED'' 
   ORDER BY LAST_SUCCESSFUL_RUN DESC LIMIT 20;

5. Verify data loaded:
   SELECT 
       COUNT(*) as total_records,
       COUNT(DISTINCT SYMBOL) as symbols,
       MIN(DATE) as earliest_date,
       MAX(DATE) as latest_date
   FROM TIME_SERIES_DAILY_ADJUSTED;

Recommended Schedule:
- Weekly: Run with skip_recent_hours=168 (7 days)
- Daily (optional): Run with skip_recent_hours=24
====================================================================
' as instructions;

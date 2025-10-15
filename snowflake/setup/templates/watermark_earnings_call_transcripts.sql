-- ============================================================================
-- Initialize ETL Watermarks for Earnings Call Transcripts Data
-- 
-- Purpose: Creates watermark records for all eligible symbols
-- Table: EARNINGS_CALL_TRANSCRIPT
-- Source: ETL_WATERMARKS table (or SYMBOL table if first time)
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Insert watermark records for all active stocks not already tracked for EARNINGS_CALL_TRANSCRIPT
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
    'EARNINGS_CALL_TRANSCRIPT' as TABLE_NAME,
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
    NULL as FIRST_FISCAL_DATE,      -- Will be set to earliest transcript date
    NULL as LAST_FISCAL_DATE,        -- Will be set to latest transcript date
    NULL as LAST_SUCCESSFUL_RUN,     -- Will be set after first run
    0 as CONSECUTIVE_FAILURES,
    CURRENT_TIMESTAMP() as CREATED_AT,
    CURRENT_TIMESTAMP() as UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.SYMBOL
WHERE NOT EXISTS (
    SELECT 1 
    FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
    WHERE w.TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
      AND w.SYMBOL = SYMBOL.SYMBOL
);

-- Show summary of created records
SELECT 
    '✅ Earnings Call Transcript Watermarks Created' as status,
    COUNT(*) as total_records,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as eligible_for_api,
    COUNT(CASE WHEN API_ELIGIBLE = 'NO' THEN 1 END) as not_eligible,
    COUNT(CASE WHEN API_ELIGIBLE = 'DEL' THEN 1 END) as delisted,
    COUNT(DISTINCT EXCHANGE) as unique_exchanges
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT';

-- Show breakdown by exchange
SELECT 
    'Breakdown by Exchange' as info,
    EXCHANGE,
    COUNT(*) as total,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as eligible
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
GROUP BY EXCHANGE
ORDER BY total DESC;

SELECT '
====================================================================
EARNINGS_CALL_TRANSCRIPT WATERMARKS INITIALIZED

Key differences from other ETLs:
- One API call per symbol per quarter (not just per symbol)
- Only fetch for valid fiscal quarters (2010 Q1 onward, or first full quarter after IPO)
- Only process active stocks for now
- If no data for all quarters, set API_ELIGIBLE = ''SUS''
- Watermarks track FIRST_FISCAL_DATE and LAST_FISCAL_DATE for transcript data

Next steps:
1. Run the GitHub Actions workflow:
   .github/workflows/earnings_call_transcripts_watermark_etl.yml

2. Test with a small batch first:
   - Set max_symbols: 10
   - Leave exchange_filter blank
   - Monitor execution

3. Expected behavior:
   - Many symbols/quarters will have "no data" (normal for transcripts)
   - Some symbols will have transcripts
   - Watermarks track LAST_FISCAL_DATE = latest transcript date

4. After successful test:
   - Remove max_symbols limit
   - Run monthly via schedule or manually

5. Check watermark updates:
   SELECT * FROM ETL_WATERMARKS 
   WHERE TABLE_NAME = ''EARNINGS_CALL_TRANSCRIPT'' 
   ORDER BY LAST_SUCCESSFUL_RUN DESC LIMIT 20;

6. Verify data loaded:
   SELECT 
       COUNT(*) as total_transcripts,
       COUNT(DISTINCT SYMBOL) as symbols_with_transcripts,
       MIN(TRANSCRIPT_DATE) as earliest_transcript,
       MAX(TRANSCRIPT_DATE) as latest_transcript
   FROM EARNINGS_CALL_TRANSCRIPTS;

7. View recent transcripts:
   SELECT SYMBOL, TRANSCRIPT_DATE, FISCAL_QUARTER, CEO_NAME, 
          TITLE, SUMMARY
   FROM EARNINGS_CALL_TRANSCRIPTS
   WHERE TRANSCRIPT_DATE >= DATEADD(day, -90, CURRENT_DATE())
   ORDER BY TRANSCRIPT_DATE DESC
   LIMIT 20;
====================================================================
' as instructions;

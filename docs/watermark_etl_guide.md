# Watermark-Based ETL Setup Guide

## Overview

This guide explains how to use the new watermark-based ETL system for TIME_SERIES_DAILY_ADJUSTED data extraction.

## Key Differences from Old System

### Old Approach:
- ❌ Queried LISTING_STATUS table directly
- ❌ No tracking of what was already processed
- ❌ Always fetched full history (inefficient)
- ❌ No failure tracking

### New Watermark Approach:
- ✅ Queries ETL_WATERMARKS table for processing status
- ✅ Tracks first and last fiscal dates for each symbol
- ✅ Smart mode selection (full vs compact)
- ✅ Tracks consecutive failures
- ✅ Updates watermarks after successful processing

## Prerequisites

**You must complete these steps first:**

1. **Create base watermarks table:**
   - Workflow: "Create ETL Watermarks Table"
   - Creates ~20,000 LISTING_STATUS watermarks
   - Sets `API_ELIGIBLE = 'NO'` for all

2. **Create TIME_SERIES_DAILY_ADJUSTED watermarks:**
   - Workflow: "Add Data Source Watermarks"
   - Select: "TIME_SERIES_DAILY_ADJUSTED"
   - Creates ~20,000 TIME_SERIES watermarks
   - Sets `API_ELIGIBLE = 'YES'` for all symbols

## Watermark Logic

The ETL process uses the following logic to determine processing mode:

### Full Refresh Mode (outputsize=full)
Fetches complete history when:
- `FIRST_FISCAL_DATE IS NULL` AND `LAST_FISCAL_DATE IS NULL` (never processed)
- `LAST_FISCAL_DATE` is older than STALENESS_DAYS (default: 5 days)

### Compact Mode (outputsize=compact)
Fetches last 100 days only when:
- `FIRST_FISCAL_DATE IS NOT NULL` AND `LAST_FISCAL_DATE IS NOT NULL`
- `LAST_FISCAL_DATE` is within STALENESS_DAYS (data is fresh)

## Workflow: Time Series ETL - Watermark Based

### Inputs:

**1. exchange_filter** (optional)
- Options: NYSE, NASDAQ, AMEX, or blank for all
- Filters which exchange symbols to process
- Example: Select "NYSE" to process only NYSE-listed symbols

**2. max_symbols** (optional)
- Limits number of symbols to process (for testing)
- Leave blank to process all eligible symbols
- Example: Set to `10` for a quick test run

**3. staleness_days** (default: 5)
- Number of days before data is considered stale
- If `LAST_FISCAL_DATE` older than this → full refresh
- If within this period → compact mode
- Example: `7` = refresh if data is more than a week old

**4. batch_size** (default: 50)
- Number of symbols to process per batch
- Affects memory usage and commit frequency
- Example: `100` for faster processing (if memory allows)

### Example Usage:

**First-Time Bootstrap (All Symbols):**
```
exchange_filter: (blank - process all)
max_symbols: (blank - process all)
staleness_days: 5
batch_size: 50
```

**Test Run (10 NYSE symbols):**
```
exchange_filter: NYSE
max_symbols: 10
staleness_days: 5
batch_size: 10
```

**Daily Refresh (NASDAQ only):**
```
exchange_filter: NASDAQ
max_symbols: (blank)
staleness_days: 1
batch_size: 50
```

## Watermark Updates

After successful processing, the workflow updates these fields:

```sql
FIRST_FISCAL_DATE       -- Set to earliest date (if previously NULL)
LAST_FISCAL_DATE        -- Updated to most recent date in data
LAST_SUCCESSFUL_RUN     -- Set to current timestamp
CONSECUTIVE_FAILURES    -- Reset to 0
UPDATED_AT              -- Set to current timestamp
```

After failed processing:

```sql
CONSECUTIVE_FAILURES    -- Incremented by 1
UPDATED_AT              -- Set to current timestamp
```

## Monitoring Progress

### During Execution:
- Watch workflow logs for real-time progress
- Shows full vs compact mode for each symbol
- Displays success/failure counts

### After Completion:
- Download the `watermark-etl-results-*.json` artifact
- Contains detailed processing results
- Shows mode breakdown and efficiency metrics

### Query Watermarks:
```sql
-- See processing status
SELECT 
    EXCHANGE,
    COUNT(*) as TOTAL,
    COUNT(CASE WHEN LAST_SUCCESSFUL_RUN IS NOT NULL THEN 1 END) as PROCESSED,
    COUNT(CASE WHEN CONSECUTIVE_FAILURES > 0 THEN 1 END) as FAILURES,
    AVG(DATEDIFF('day', LAST_FISCAL_DATE, CURRENT_DATE())) as AVG_DAYS_STALE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND API_ELIGIBLE = 'YES'
GROUP BY EXCHANGE;

-- Find symbols needing refresh
SELECT 
    SYMBOL,
    EXCHANGE,
    LAST_FISCAL_DATE,
    DATEDIFF('day', LAST_FISCAL_DATE, CURRENT_DATE()) as DAYS_STALE,
    CONSECUTIVE_FAILURES
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND API_ELIGIBLE = 'YES'
  AND (LAST_FISCAL_DATE IS NULL 
       OR DATEDIFF('day', LAST_FISCAL_DATE, CURRENT_DATE()) > 5)
ORDER BY DAYS_STALE DESC NULLS FIRST
LIMIT 20;
```

## Benefits

### Efficiency:
- ⚡ Compact mode only fetches ~100 days vs full history
- 📊 Processes only eligible symbols (skips API_ELIGIBLE = 'NO')
- 🎯 Can filter by exchange for parallel processing

### Reliability:
- 🔄 Tracks failures and retries automatically
- ✅ Updates watermarks only on success
- 📝 Maintains audit trail of processing history

### Flexibility:
- 🎚️ Configurable staleness threshold
- 🔧 Exchange-based filtering for targeted updates
- 🧪 Test mode with symbol limits

## Next Steps

After running the TIME_SERIES_DAILY_ADJUSTED ETL:

1. Verify watermarks were updated:
   ```sql
   SELECT COUNT(*) FROM ETL_WATERMARKS 
   WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
     AND LAST_SUCCESSFUL_RUN IS NOT NULL;
   ```

2. Check data in TIME_SERIES_DAILY_ADJUSTED table

3. Set up additional data sources (COMPANY_OVERVIEW, etc.)

4. Schedule regular refreshes using GitHub Actions schedules

# ETL Operations Guide - fin-trade-extract

## Overview
This document outlines the rules, best practices, gotchas, and performance optimizations for the fin-trade-extract ETL pipeline. This is the authoritative reference for all ETL operations in this repository.

**Last Updated:** October 12, 2025

---

## Table of Contents
1. [Watermark-Based ETL System](#watermark-based-etl-system)
2. [Critical Rules & Gotchas](#critical-rules--gotchas)
3. [Performance Optimizations](#performance-optimizations)
4. [Data Source Workflows](#data-source-workflows)
5. [Snowflake Cost Management](#snowflake-cost-management)
6. [Data Integrity Rules](#data-integrity-rules)
7. [Troubleshooting Guide](#troubleshooting-guide)

---

## Watermark-Based ETL System

### 🎯 Critical Concept: Watermarks Drive Everything

**⚠️ THE MOST IMPORTANT RULE:**

The `ETL_WATERMARKS` table is **THE SINGLE SOURCE OF TRUTH** for all ETL processing. 

**Understanding this is critical:**
- ✅ If a symbol exists in `ETL_WATERMARKS` with `API_ELIGIBLE='YES'` → It will be processed
- ❌ If a symbol does NOT exist in `ETL_WATERMARKS` → It will be **completely ignored**
- ❌ If `API_ELIGIBLE='NO'` or `'DEL'` → Symbol is **skipped** (even if data exists)

**Before running ANY extraction workflow:**
```sql
-- ALWAYS verify watermarks exist first!
SELECT COUNT(*) 
FROM ETL_WATERMARKS 
WHERE TABLE_NAME = 'YOUR_DATA_SOURCE' 
  AND API_ELIGIBLE = 'YES';

-- If count = 0, you MUST create watermarks first:
-- Go to GitHub Actions → Run add_data_source_watermarks.yml workflow
```

### ETL_WATERMARKS Table Structure

The `ETL_WATERMARKS` table is the **single source of truth** for all ETL processing state.

**Key Columns:**
```sql
TABLE_NAME                  VARCHAR(100)    -- Data source (e.g., 'TIME_SERIES_DAILY_ADJUSTED', 'BALANCE_SHEET')
SYMBOL_ID                   NUMBER(38,0)    -- Hash-based ID: ABS(HASH(symbol)) % 1000000000
SYMBOL                      VARCHAR(20)     -- Stock ticker
NAME                        VARCHAR(255)    -- Company name
EXCHANGE                    VARCHAR(64)     -- NYSE, NASDAQ, etc.
ASSET_TYPE                  VARCHAR(64)     -- Stock, ETF, etc.
STATUS                      VARCHAR(12)     -- Active, Delisted
API_ELIGIBLE                VARCHAR(3)      -- 'YES', 'NO', or 'DEL'
IPO_DATE                    DATE            -- From LISTING_STATUS
DELISTING_DATE              DATE            -- From LISTING_STATUS
FIRST_FISCAL_DATE           DATE            -- Earliest data point collected
LAST_FISCAL_DATE            DATE            -- Most recent data point collected
LAST_SUCCESSFUL_RUN         TIMESTAMP_NTZ   -- Last successful ETL timestamp
CONSECUTIVE_FAILURES        NUMBER(5,0)     -- Failure counter
CREATED_AT                  TIMESTAMP_NTZ   -- Record creation time
UPDATED_AT                  TIMESTAMP_NTZ   -- Last update time
```

**Primary Key:** `(TABLE_NAME, SYMBOL_ID)`

### Watermark Lifecycle

#### 1. Initialization (First Time)
```sql
-- Watermarks are created from LISTING_STATUS table
INSERT INTO ETL_WATERMARKS (TABLE_NAME, SYMBOL_ID, SYMBOL, ...)
SELECT 'TIME_SERIES_DAILY_ADJUSTED', ABS(HASH(symbol)) % 1000000000, symbol, ...
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'LISTING_STATUS';
```

**Initial State:**
- `FIRST_FISCAL_DATE = NULL`
- `LAST_FISCAL_DATE = NULL`
- `LAST_SUCCESSFUL_RUN = NULL`
- `API_ELIGIBLE = 'YES'` (for eligible symbols)

#### 2. First Extraction (Full Mode)
- Script queries: `WHERE API_ELIGIBLE = 'YES' AND LAST_FISCAL_DATE IS NULL`
- Processing mode: **FULL** (complete history from IPO)
- After success: Updates `FIRST_FISCAL_DATE`, `LAST_FISCAL_DATE`, `LAST_SUCCESSFUL_RUN`

#### 3. Incremental Extractions (Compact Mode)
- Script queries: `WHERE API_ELIGIBLE = 'YES'`
- Processing mode decision:
  ```python
  days_since_last = (today - LAST_FISCAL_DATE).days
  if days_since_last < staleness_days:  # Default: 5 days
      mode = 'compact'  # Latest 100 days only
  else:
      mode = 'full'     # Complete refresh
  ```

#### 4. Delisting Detection
- On successful extraction, checks `DELISTING_DATE`:
  ```sql
  UPDATE ETL_WATERMARKS
  SET API_ELIGIBLE = CASE 
      WHEN DELISTING_DATE IS NOT NULL AND DELISTING_DATE <= CURRENT_DATE() 
      THEN 'DEL'  -- Permanently exclude from future runs
      ELSE API_ELIGIBLE 
  END
  ```

### API_ELIGIBLE States

| State | Meaning | Included in ETL? | Use Case |
|-------|---------|------------------|----------|
| `'YES'` | Active, eligible for API calls | ✅ Yes | Active stocks, normal processing |
| `'NO'` | Not eligible (wrong asset type) | ❌ No | ETFs for stock-only endpoints |
| `'DEL'` | Delisted, final data captured | ❌ No | Delisted stocks, saves API quota |

**CRITICAL:** Once `API_ELIGIBLE = 'DEL'`, the symbol is **permanently excluded** unless manually reset.

### Incremental Processing with skip_recent_hours

**Purpose:** Avoid re-processing symbols that were recently updated.

**Example:**
```yaml
skip_recent_hours: 12
```

**Query Logic:**
```sql
WHERE API_ELIGIBLE = 'YES'
  AND (LAST_SUCCESSFUL_RUN IS NULL 
       OR LAST_SUCCESSFUL_RUN < DATEADD(hour, -12, CURRENT_TIMESTAMP()))
```

**Use Cases:**
- Daily runs: `skip_recent_hours: 24` (skip symbols processed in last 24h)
- Intraday runs: `skip_recent_hours: 6` (refresh only stale symbols)
- Full universe refresh: `skip_recent_hours: null` (process everything)

---

## Critical Rules & Gotchas

### 🚨 RULE 1: Always Close Snowflake Connections When Not Needed

**THE PROBLEM:**
Keeping a Snowflake connection open keeps the warehouse running, even if no queries are executing.

**BAD PATTERN (Costs $18-20/day):**
```python
# Open connection
watermark_manager = WatermarkETLManager(config)
watermark_manager.connect()

# Query watermarks (5-10 seconds)
symbols = watermark_manager.get_symbols_to_process()

# Extract from API (1-2 HOURS - warehouse running idle!)
for symbol in symbols:
    data = fetch_from_api(symbol)
    upload_to_s3(data)

# Update watermarks (5-10 seconds)
watermark_manager.update_watermarks(symbols)
watermark_manager.close()
```

**GOOD PATTERN (Costs $0.01):**
```python
# STEP 1: Query watermarks, then CLOSE
watermark_manager = WatermarkETLManager(config)
try:
    symbols = watermark_manager.get_symbols_to_process()
finally:
    watermark_manager.close()  # ← CRITICAL: Close immediately!

# STEP 2: Extract from API (NO connection open)
for symbol in symbols:
    data = fetch_from_api(symbol)
    upload_to_s3(data)

# STEP 3: Open NEW connection for updates
watermark_manager = WatermarkETLManager(config)
watermark_manager.connect()
try:
    watermark_manager.bulk_update_watermarks(results)
finally:
    watermark_manager.close()
```

**Cost Impact:** ~$18-20/day savings per workflow run!

### 🚨 RULE 2: Use Bulk Operations, Not Row-by-Row Updates

**THE PROBLEM:**
Individual UPDATE statements are extremely slow in Snowflake.

**BAD PATTERN (960 symbols = 13 minutes):**
```python
for symbol_data in results:
    cursor.execute(f"""
        UPDATE ETL_WATERMARKS 
        SET LAST_FISCAL_DATE = '{symbol_data['date']}'
        WHERE SYMBOL = '{symbol_data['symbol']}'
    """)
    connection.commit()  # ← 960 individual commits!
```

**GOOD PATTERN (960 symbols = 10 seconds):**
```python
# Create temp table
cursor.execute("CREATE TEMP TABLE WATERMARK_UPDATES (SYMBOL VARCHAR, FIRST_DATE DATE, LAST_DATE DATE)")

# Build bulk INSERT
values_list = [f"('{u['symbol']}', '{u['first_date']}', '{u['last_date']}')" 
               for u in results]
cursor.execute(f"INSERT INTO WATERMARK_UPDATES VALUES {','.join(values_list)}")

# Single MERGE statement
cursor.execute("""
    MERGE INTO ETL_WATERMARKS target
    USING WATERMARK_UPDATES source
    ON target.SYMBOL = source.SYMBOL AND target.TABLE_NAME = 'TIME_SERIES'
    WHEN MATCHED THEN UPDATE SET
        LAST_FISCAL_DATE = source.LAST_DATE,
        LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP()
""")

# Single commit
connection.commit()
```

**Performance Impact:** 100x speedup, ~$0.50 savings per run!

### 🚨 RULE 3: NEVER Drop Tables in Production

**THE PROBLEM:**
`DROP TABLE IF EXISTS` destroys all historical data.

**BAD PATTERN:**
```sql
DROP TABLE IF EXISTS TIME_SERIES_DAILY_ADJUSTED;  -- ❌ DELETES ALL DATA!
CREATE TABLE TIME_SERIES_DAILY_ADJUSTED (...);
COPY INTO TIME_SERIES_DAILY_ADJUSTED FROM @STAGE;
```

**GOOD PATTERN:**
```sql
-- Comment out DROP, use CREATE IF NOT EXISTS
-- DROP TABLE IF EXISTS TIME_SERIES_DAILY_ADJUSTED;  -- DANGER: Would delete all data!
CREATE TABLE IF NOT EXISTS TIME_SERIES_DAILY_ADJUSTED (...);

-- Use MERGE to upsert
MERGE INTO TIME_SERIES_DAILY_ADJUSTED target
USING staging_table source
ON target.symbol = source.symbol AND target.date = source.date
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

**Data Integrity:** Preserves historical data, enables true incremental loading.

### 🚨 RULE 4: SYMBOL_ID Must Use Snowflake HASH(), Not Python hash()

**THE PROBLEM:**
Python's `hash()` and Snowflake's `HASH()` produce different values.

**CORRECT PATTERN (All code must use this):**
```sql
-- In SQL (Snowflake)
SYMBOL_ID = ABS(HASH(symbol)) % 1000000000

-- In Python - DON'T calculate SYMBOL_ID!
-- Let Snowflake calculate it during load
```

**Why This Matters:**
- Watermarks from different sources must join correctly
- `SYMBOL_ID` is used across all tables for consistency
- Mismatched IDs cause duplicate records and broken joins

**Affected Components:**
- ✅ `LISTING_STATUS` table creation
- ✅ All watermark creation scripts
- ✅ All SQL loaders (TIME_SERIES, BALANCE_SHEET, COMPANY_OVERVIEW)
- ❌ Python extraction scripts (don't calculate SYMBOL_ID)

### 🚨 RULE 5: PARALLEL Parameter Only Works on Internal Stages

**THE PROBLEM:**
`PARALLEL` parameter is invalid for external (S3) stages.

**WRONG:**
```sql
COPY INTO table
FROM @S3_EXTERNAL_STAGE
PARALLEL = 16;  -- ❌ ERROR: invalid parameter 'PARALLEL'
```

**CORRECT:**
```sql
COPY INTO table
FROM @S3_EXTERNAL_STAGE;
-- Note: Snowflake automatically parallelizes S3 loads based on file count
```

**Why It Doesn't Matter:**
- Snowflake auto-parallelizes external stage loads
- Based on number of files in S3
- 100 files = up to 100 parallel threads automatically

### 🚨 RULE 6: Always Clean Up S3 Before Extraction

**THE PROBLEM:**
Snowflake's `COPY FROM s3://bucket/prefix/*.csv` loads **ALL** files matching the pattern. If old files remain from previous runs, they get loaded multiple times, causing duplicates.

**CRITICAL PATTERN (All extraction scripts MUST do this):**
```python
def cleanup_s3_bucket(bucket: str, prefix: str, s3_client) -> int:
    """
    Delete ALL existing files in the S3 prefix before new extraction.
    Handles >1000 files using pagination.
    
    Returns: Number of files deleted
    """
    logger.info(f"🧹 Cleaning up S3 bucket: s3://{bucket}/{prefix}")
    
    deleted_count = 0
    continuation_token = None
    
    while True:
        # List objects (handles pagination for >1000 files)
        list_params = {'Bucket': bucket, 'Prefix': prefix}
        if continuation_token:
            list_params['ContinuationToken'] = continuation_token
        
        response = s3_client.list_objects_v2(**list_params)
        
        # Delete objects if any exist
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )
            deleted_count += len(objects_to_delete)
            logger.info(f"   Deleted {len(objects_to_delete)} files (total: {deleted_count})")
        
        # Check if there are more objects to list
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    
    logger.info(f"✅ S3 cleanup complete: {deleted_count} files deleted")
    return deleted_count

# STEP 1 of EVERY extraction workflow (before API calls)
cleanup_s3_bucket(s3_bucket, s3_prefix, s3_client)
```

**Why Pagination Matters:**
- S3 `list_objects_v2()` returns max 1,000 objects per call
- Without pagination, files 1001+ won't be deleted
- Old files = duplicate data in Snowflake

**Standard S3 Prefixes:**
- Time series: `time_series_daily_adjusted/`
- Balance sheet: `balance_sheet/`
- Company overview: `company_overview/`
- Listing status: `listing_status/`

### 🚨 RULE 7: Watermarks Table Drives ALL ETL Processing

**⚠️ CRITICAL UNDERSTANDING:**

The `ETL_WATERMARKS` table is **NOT** just metadata - it is the **gatekeeper** for the entire ETL system.

**If a (TABLE_NAME, SYMBOL) combination does NOT exist in ETL_WATERMARKS:**
- ❌ The symbol will NOT be extracted
- ❌ No API calls will be made
- ❌ No data will be loaded
- ❌ The symbol is invisible to all workflows

**Watermark Creation is MANDATORY:**

```bash
# Step 1: Create watermarks FIRST (before any extraction)
# Run GitHub workflow: add_data_source_watermarks.yml
# Select: BALANCE_SHEET (or your target data source)

# Step 2: Verify watermarks were created
SELECT COUNT(*) 
FROM ETL_WATERMARKS 
WHERE TABLE_NAME = 'BALANCE_SHEET' AND API_ELIGIBLE = 'YES';
-- Should return ~2000-3000 symbols (active stocks)

# Step 3: NOW you can run extraction
# Run GitHub workflow: balance_sheet_watermark_etl.yml
```

**Common Mistake:**
```python
# ❌ WRONG: Running extraction without watermarks
# Result: "No symbols to process" - workflows complete with 0 data

# ✅ CORRECT: Always ensure watermarks exist first
# Check: SELECT COUNT(*) FROM ETL_WATERMARKS WHERE TABLE_NAME = 'YOUR_DATA_SOURCE'
# If count = 0, run add_data_source_watermarks.yml workflow first!
```

**Watermark Prerequisites by Data Source:**

| Data Source | Prerequisite | API_ELIGIBLE Logic |
|-------------|--------------|-------------------|
| `LISTING_STATUS` | None (seed table) | All symbols = 'YES' |
| `TIME_SERIES_DAILY_ADJUSTED` | LISTING_STATUS must exist | All symbols = 'YES' |
| `COMPANY_OVERVIEW` | LISTING_STATUS must exist | Only ASSET_TYPE='Stock' = 'YES' |
| `BALANCE_SHEET` | LISTING_STATUS must exist | Only ASSET_TYPE='Stock' AND STATUS='Active' = 'YES' |
| `CASH_FLOW` | LISTING_STATUS must exist | Only ASSET_TYPE='Stock' AND STATUS='Active' = 'YES' |
| `INCOME_STATEMENT` | LISTING_STATUS must exist | Only ASSET_TYPE='Stock' AND STATUS='Active' = 'YES' |

**Watermark Lifecycle Summary:**
1. **Create** watermarks from LISTING_STATUS (one-time setup per data source)
2. **Query** watermarks to get symbols to process (every ETL run)
3. **Update** watermarks after successful extraction (bulk MERGE, not row-by-row)
4. **Mark delisted** symbols as `API_ELIGIBLE='DEL'` to save API quota

**Re-processing Symbols:**
```sql
-- To re-process specific symbols, reset their watermarks:
UPDATE ETL_WATERMARKS
SET LAST_SUCCESSFUL_RUN = NULL,
    FIRST_FISCAL_DATE = NULL,
    LAST_FISCAL_DATE = NULL,
    CONSECUTIVE_FAILURES = 0
WHERE TABLE_NAME = 'BALANCE_SHEET' 
  AND SYMBOL IN ('AAPL', 'MSFT', 'GOOGL');

-- To re-process ALL symbols for a data source:
UPDATE ETL_WATERMARKS
SET LAST_SUCCESSFUL_RUN = NULL,
    FIRST_FISCAL_DATE = NULL,
    LAST_FISCAL_DATE = NULL,
    CONSECUTIVE_FAILURES = 0
WHERE TABLE_NAME = 'BALANCE_SHEET';
```

---

## Performance Optimizations

### Optimization 1: Connection Management
**Savings:** $18-20/day per workflow  
**Implementation:** Close connections immediately after querying, reopen only for updates  
**Files Changed:**
- `scripts/github_actions/fetch_time_series_watermark.py`
- `scripts/github_actions/fetch_balance_sheet_watermark.py`

### Optimization 2: Bulk MERGE for Watermarks
**Savings:** ~$0.50/run, 100x speedup (13 min → 10 sec for 960 symbols)  
**Implementation:** Single temp table + MERGE instead of individual UPDATEs  
**Method:** `bulk_update_watermarks(successful_updates, failed_symbols)`

### Optimization 3: Incremental Processing (skip_recent_hours)
**Savings:** 50-90% reduction in API calls and processing time  
**Implementation:** Workflow parameter filters recently processed symbols  
**Example:** `skip_recent_hours: 24` for daily runs

### Optimization 4: Smart Mode Switching (Full vs Compact)
**Savings:** 1 API call vs 1 API call, but Compact is lighter payload  
**Logic:**
- Fresh data (<5 days): Compact mode (latest 100 days)
- Stale data (≥5 days): Full mode (complete history)
- Never processed: Full mode

### Optimization 5: Automatic Delisting Detection
**Savings:** Eliminates wasteful API calls for delisted symbols  
**Implementation:** Sets `API_ELIGIBLE = 'DEL'` after successful final extraction  
**Impact:** 500+ delisted symbols excluded = 500+ fewer API calls per run

### Optimization 6: S3 as Staging Layer
**Savings:** No Snowflake compute during API extraction  
**Pattern:** Extract → S3 → Snowflake (warehouse only active during load)  
**Benefit:** Clean separation, easy debugging, cost isolation

### Optimization 7: External Stages (No Data Egress)
**Savings:** No charges for moving data between S3 and Snowflake  
**Implementation:** All stages use S3 integration  
**Benefit:** Data stays in AWS, no cross-cloud transfer fees

### Cost Summary

| Optimization | Type | Savings/Impact |
|--------------|------|----------------|
| Connection Management | Compute | $18-20/day |
| Bulk MERGE | Compute | $0.50/run |
| skip_recent_hours | API + Compute | 50-90% reduction |
| Smart Mode Switching | API | Efficient payload size |
| Delisting Detection | API | 500+ calls saved |
| S3 Staging | Architecture | Clean separation |
| External Stages | Data Transfer | $0 egress fees |

**Total Monthly Savings:** ~$500-600 (from $600/month → $6-10/month)

---

## Data Source Workflows

### TIME_SERIES_DAILY_ADJUSTED

**Workflow:** `.github/workflows/time_series_watermark_etl.yml`

**ETL Steps:**
1. **Cleanup S3:** Delete old files from `s3://fin-trade-craft-landing/time_series_daily_adjusted/`
2. **Extract:** `scripts/github_actions/fetch_time_series_watermark.py`
   - Query watermarks where `API_ELIGIBLE = 'YES'`
   - Apply `skip_recent_hours` filter
   - Fetch from Alpha Vantage API (full or compact mode)
   - Upload CSV files to S3 (one file per symbol)
3. **Load:** `snowflake/runbooks/load_time_series_from_s3.sql`
   - COPY INTO staging table from S3
   - Extract symbol from filename using regex
   - MERGE into TIME_SERIES_DAILY_ADJUSTED (deduplication)
4. **Update Watermarks:** Bulk MERGE in STEP 4

**Key Parameters:**
- `exchange_filter`: NYSE, NASDAQ, AMEX, or blank for all
- `max_symbols`: Limit number of symbols (for testing)
- `staleness_days`: Days before full refresh (default: 5)
- `skip_recent_hours`: Skip recently processed symbols

**API Endpoint:** `TIME_SERIES_DAILY_ADJUSTED`  
**API Rate Limit:** 75 calls/minute  
**File Pattern:** `time_series_daily_adjusted/{SYMBOL}_{TIMESTAMP}.csv`

### BALANCE_SHEET

**Workflow:** `.github/workflows/balance_sheet_watermark_etl.yml`

**ETL Steps:**
1. **Cleanup S3:** Delete old files from `s3://fin-trade-craft-landing/balance_sheet/`
2. **Extract:** `scripts/github_actions/fetch_balance_sheet_watermark.py`
   - Query watermarks where `API_ELIGIBLE = 'YES'` AND `ASSET_TYPE = 'Stock'`
   - Fetch annual AND quarterly reports
   - Flatten nested JSON to CSV (38 columns)
   - Upload to S3
3. **Load:** `snowflake/runbooks/load_balance_sheet_from_s3.sql`
   - COPY INTO staging table
   - Calculate SYMBOL_ID using HASH(symbol)
   - MERGE by (symbol, report_type, fiscal_date_ending)
4. **Update Watermarks:** Bulk MERGE in STEP 4

**Key Parameters:**
- `exchange_filter`: NYSE, NASDAQ, or blank
- `max_symbols`: Limit for testing
- `skip_recent_hours`: Skip recent symbols

**API Endpoint:** `BALANCE_SHEET`  
**File Pattern:** `balance_sheet/{SYMBOL}_{TIMESTAMP}.csv`

### COMPANY_OVERVIEW

**Workflow:** `.github/workflows/overview_full_universe.yml`

**ETL Steps:**
1. **Extract:** `scripts/github_actions/fetch_company_overview_bulk.py`
   - Fetch overview data for all symbols
   - Upload to S3 (one file per symbol)
2. **Load:** `snowflake/runbooks/load_company_overview_from_s3.sql`
   - COPY INTO staging using MATCH_BY_COLUMN_NAME
   - Calculate SYMBOL_ID using HASH(Symbol)
   - MERGE by symbol

**CRITICAL:** No SYMBOL_ID in CSV - calculated in SQL to match LISTING_STATUS

**API Endpoint:** `OVERVIEW`  
**File Pattern:** `overview_{SYMBOL}_processed.csv`

### LISTING_STATUS

**Workflow:** `.github/workflows/listing_status_etl.yml`

**ETL Steps:**
1. **Extract:** Fetch active and delisted listings
2. **Load:** `snowflake/runbooks/load_listing_status_from_s3_simple.sql`
   - COPY both active and delisted files
   - Deduplicate (prioritize active file)
   - MERGE into LISTING_STATUS

**Special Notes:**
- This is the **source of truth** for all symbols
- All watermarks inherit `SYMBOL_ID`, `NAME`, `EXCHANGE`, etc. from here
- Must be run first before creating watermarks

**File Pattern:** `listing_status_*.csv`

---

## Snowflake Cost Management

### Warehouse Auto-Suspend

**Configuration:**
```sql
ALTER WAREHOUSE FIN_TRADE_WH SET AUTO_SUSPEND = 60;  -- 60 seconds
```

**How It Works:**
- Warehouse auto-suspends after 60 seconds of inactivity
- Prevents charges during idle time
- **CRITICAL:** Only works if connection is closed!

### Cost Per Hour (X-SMALL Warehouse)

| Size | Credits/Hour | Cost/Hour (Standard) |
|------|-------------|---------------------|
| X-SMALL | 1 | $2.30 |
| SMALL | 2 | $4.60 |
| MEDIUM | 4 | $9.20 |

**Calculation Example:**
- 5 minutes of active warehouse time
- X-SMALL warehouse
- Cost: (5/60) × $2.30 = **$0.19**

### Cost Monitoring Queries

**Daily Warehouse Usage:**
```sql
SELECT 
    DATE_TRUNC('day', start_time) as day,
    warehouse_name,
    SUM(DATEDIFF('second', start_time, end_time)) / 3600.0 as hours_used,
    hours_used * 1 * 2.30 as estimated_cost  -- X-SMALL rate
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
```

**Long-Running Sessions (Warning Sign):**
```sql
SELECT 
    session_id,
    user_name,
    warehouse_name,
    start_time,
    end_time,
    DATEDIFF('minute', start_time, end_time) as duration_minutes
FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS
WHERE warehouse_name = 'FIN_TRADE_WH'
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
  AND DATEDIFF('minute', start_time, end_time) > 30
ORDER BY duration_minutes DESC;
```

**If you see sessions >30 minutes:** Connection was left open during API extraction! Fix the code.

### Resource Monitors (Optional)

```sql
-- Create monitor with $50/month limit
CREATE RESOURCE MONITOR ETL_COST_MONITOR WITH
    CREDIT_QUOTA = 22  -- $50 / $2.30 per credit
    TRIGGERS 
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Apply to warehouse
ALTER WAREHOUSE FIN_TRADE_WH SET RESOURCE_MONITOR = ETL_COST_MONITOR;
```

---

## Data Integrity Rules

### Rule 1: Primary Keys and Deduplication

**TIME_SERIES_DAILY_ADJUSTED:**
- Natural Key: `(symbol, date)`
- MERGE logic: ON target.symbol = source.symbol AND target.date = source.date

**BALANCE_SHEET:**
- Natural Key: `(symbol, report_type, fiscal_date_ending)`
- Handles both annual and quarterly reports

**COMPANY_OVERVIEW:**
- Natural Key: `(symbol)`
- One record per symbol (latest overwrites)

**LISTING_STATUS:**
- Natural Key: `(symbol)`
- Deduplication prioritizes active file over delisted file

### Rule 2: NULL Handling

**TRY_TO_NUMBER / TRY_TO_DATE:**
```sql
-- Use TRY_TO_* functions to handle invalid data gracefully
TRY_TO_NUMBER(market_cap) as MARKET_CAP,
TRY_TO_DATE(fiscal_year_end, 'YYYY-MM-DD') as FISCAL_YEAR_END
```

**Benefit:** Invalid data becomes NULL instead of causing load failure

### Rule 3: ON_ERROR = CONTINUE

**All COPY statements:**
```sql
COPY INTO table
FROM @stage
ON_ERROR = CONTINUE;  -- Skip bad files, don't fail entire load
```

**Monitoring:** Check `COPY_HISTORY` for rejected rows

### Rule 4: Filename Metadata

**Always capture source filename:**
```sql
COPY INTO staging_table (..., source_filename)
FROM (
    SELECT $1, $2, ..., METADATA$FILENAME
    FROM @stage
)
```

**Benefit:** Debugging, audit trail, deduplication logic

---

## Troubleshooting Guide

### Issue 1: Warehouse Costs Spiking

**Symptoms:**
- Unexpected high bills
- Sessions running >30 minutes
- Warehouse active during API extraction

**Diagnosis:**
```sql
-- Find long-running sessions
SELECT session_id, user_name, start_time, end_time,
       DATEDIFF('minute', start_time, end_time) as duration_min
FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS
WHERE warehouse_name = 'FIN_TRADE_WH'
  AND duration_min > 30
ORDER BY start_time DESC;
```

**Solution:**
- Check Python scripts for missing `connection.close()`
- Verify connection closed BEFORE API extraction
- Add `try/finally` blocks to ensure cleanup

### Issue 2: Watermark Updates Taking >10 Minutes

**Symptoms:**
- STEP 4 hanging at "Updating watermarks..."
- Progress logs showing 80 seconds per 100 symbols

**Diagnosis:**
- Using old code with individual UPDATEs
- Not using `bulk_update_watermarks()` method

**Solution:**
- Verify using latest code with bulk MERGE
- Cancel workflow and restart with updated code
- Should complete in ~10 seconds for 1000 symbols

### Issue 3: Duplicate Symbols in Watermarks

**Symptoms:**
- Same symbol appearing multiple times
- Different SYMBOL_ID for same symbol

**Diagnosis:**
```sql
-- Find duplicates
SELECT symbol, COUNT(*) as count, LISTAGG(SYMBOL_ID, ', ') as ids
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
GROUP BY symbol
HAVING COUNT(*) > 1;
```

**Root Cause:** SYMBOL_ID calculated inconsistently (Python hash vs Snowflake HASH)

**Solution:**
- Delete bad records
- Verify all SQL uses: `ABS(HASH(symbol)) % 1000000000`
- Verify Python does NOT calculate SYMBOL_ID

### Issue 4: Historical Data Missing

**Symptoms:**
- Table only has recent data
- Old data disappeared after ETL run

**Diagnosis:**
```sql
-- Check table row counts over time
SELECT COUNT(*) as total_rows,
       MIN(date) as earliest_date,
       MAX(date) as latest_date
FROM TIME_SERIES_DAILY_ADJUSTED;
```

**Root Cause:** `DROP TABLE IF EXISTS` in SQL loader

**Solution:**
- Search for `DROP TABLE` statements
- Comment them out
- Use `CREATE TABLE IF NOT EXISTS` instead
- Verify MERGE logic preserves historical data

### Issue 5: API Rate Limit Errors

**Symptoms:**
- "API rate limit exceeded" errors
- Many symbols marked as failed

**Diagnosis:**
- Check `CONSECUTIVE_FAILURES` in watermarks
- Review workflow logs for rate limit messages

**Solution:**
- Reduce `max_symbols` parameter
- Increase delay between API calls
- Split large runs across multiple days
- Use `skip_recent_hours` to avoid re-processing

### Issue 6: Invalid PARALLEL Parameter Error

**Symptoms:**
```
SQL compilation error: invalid parameter 'PARALLEL'
```

**Root Cause:** Using PARALLEL on external (S3) stage

**Solution:**
- Remove `PARALLEL = 16` from COPY statements
- Snowflake auto-parallelizes S3 loads
- PARALLEL only works on internal stages

---

## Workflow Best Practices

### For Daily Production Runs

**TIME_SERIES_DAILY_ADJUSTED:**
```yaml
exchange_filter: ''          # All exchanges
max_symbols: null            # All symbols
staleness_days: 5            # Default
skip_recent_hours: 24        # Skip symbols processed in last 24h
```

**BALANCE_SHEET:**
```yaml
exchange_filter: ''          # All exchanges  
max_symbols: null            # All symbols
skip_recent_hours: 168       # 7 days (quarterly updates)
```

### For Testing/Development

```yaml
exchange_filter: 'NYSE'      # Limit to one exchange
max_symbols: 10              # Small batch
staleness_days: 5
skip_recent_hours: null      # Process all (ignore last run)
```

### For Initial Universe Load

```yaml
exchange_filter: ''          # All exchanges
max_symbols: null            # All symbols
staleness_days: 5
skip_recent_hours: null      # Process everything first time
```

### For Incremental Refresh

```yaml
exchange_filter: ''
max_symbols: null
staleness_days: 5
skip_recent_hours: 6         # Only refresh stale symbols
```

---

## Quick Reference Commands

### Check Watermark Status
```sql
-- Overall summary
SELECT 
    TABLE_NAME,
    API_ELIGIBLE,
    COUNT(*) as symbol_count,
    COUNT(LAST_SUCCESSFUL_RUN) as processed_count,
    COUNT(LAST_SUCCESSFUL_RUN) * 100.0 / COUNT(*) as pct_processed
FROM ETL_WATERMARKS
GROUP BY TABLE_NAME, API_ELIGIBLE
ORDER BY TABLE_NAME, API_ELIGIBLE;

-- Symbols needing processing
SELECT SYMBOL, TABLE_NAME, LAST_FISCAL_DATE, LAST_SUCCESSFUL_RUN
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND API_ELIGIBLE = 'YES'
  AND (LAST_SUCCESSFUL_RUN IS NULL 
       OR LAST_SUCCESSFUL_RUN < DATEADD('day', -7, CURRENT_TIMESTAMP()))
ORDER BY SYMBOL
LIMIT 20;
```

### Reset Symbol for Re-Processing
```sql
-- Clear watermark to force full refresh
UPDATE ETL_WATERMARKS
SET FIRST_FISCAL_DATE = NULL,
    LAST_FISCAL_DATE = NULL,
    LAST_SUCCESSFUL_RUN = NULL,
    CONSECUTIVE_FAILURES = 0
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND SYMBOL = 'AAPL';
```

### Re-Enable Delisted Symbol
```sql
UPDATE ETL_WATERMARKS
SET API_ELIGIBLE = 'YES'
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND SYMBOL = 'XYZ'
  AND API_ELIGIBLE = 'DEL';
```

### Check for Stuck Warehouse Sessions
```sql
-- Currently active sessions
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE WAREHOUSE_NAME = 'FIN_TRADE_WH'
  AND END_TIME IS NULL
ORDER BY START_TIME DESC;
```

---

## Version History

**v1.0 - October 12, 2025**
- Initial documentation
- Documented all critical optimizations (connection mgmt, bulk MERGE)
- Captured all gotchas and rules
- Added troubleshooting guide

**Key Contributors:**
- Connection management optimization: Saves $18-20/day
- Bulk MERGE optimization: 100x speedup, saves $0.50/run
- SYMBOL_ID consistency fix: Prevents duplicate records
- DROP TABLE prevention: Preserves historical data

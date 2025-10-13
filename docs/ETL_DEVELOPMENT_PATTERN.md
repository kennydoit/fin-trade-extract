# ETL Development Pattern - 5-File Standard

## Overview

Every data source in the fin-trade-extract system follows a **consistent 5-file pattern**. This standardization ensures:
- Predictable project structure
- Easier maintenance and debugging
- Consistent development workflow
- Clear documentation for each data source
- Reusable code patterns across all ETLs

## The 5-File Pattern

For each data source, you must create exactly **5 files** following this naming convention and structure:

### 1. Python Extraction Script
**Location:** `scripts/github_actions/fetch_{source}_watermark.py`

**Purpose:** Extracts data from Alpha Vantage API and uploads to S3

**Key Components:**
```python
class WatermarkETLManager:
    - connect() - Snowflake connection
    - close() - Close connection
    - get_symbols_to_process() - Query ETL_WATERMARKS
    - bulk_update_watermarks() - Update watermarks after extraction

def cleanup_s3_bucket() - Remove old files (prevents duplicates)
def fetch_{source}() - Call Alpha Vantage API
def upload_to_s3() - Write JSON to S3 with explicit field mapping
def main() - Orchestrate: cleanup → query → extract → update
```

**Critical Patterns:**
- ✅ Connection management: close after query, reopen for updates
- ✅ Bulk operations: MERGE with temp table, not row-by-row
- ✅ S3 cleanup before extraction (pagination for >1000 files)
- ✅ Explicit field mapping to snake_case
- ✅ Error handling with retries for API calls
- ✅ Watermark updates even on "no data" responses

**Staleness Check (conditionally required):**
- **Include for:** Quarterly fundamentals (balance_sheet, cash_flow, income_statement)
  - Use 135-day staleness check (quarterly reporting cycle)
- **Exclude for:** Sporadic data (insider_transactions, earnings)
  - No staleness check (data is unpredictable)

---

### 2. SQL Load Script
**Location:** `snowflake/load_{source}_from_s3.sql`

**Purpose:** Load data from S3 into Snowflake tables

**Structure (13 steps):**
```sql
-- Step 1-2: Create/Replace STAGE (external S3)
CREATE OR REPLACE STAGE {SOURCE}_STAGE ...

-- Step 3: Create final table (if not exists)
CREATE TABLE IF NOT EXISTS {SOURCE} (
    -- Data columns (proper types)
    -- SYMBOL_ID NUMBER (foreign key)
    -- LOAD_DATE TIMESTAMP_NTZ (audit)
    PRIMARY KEY (natural_key_columns)
);

-- Step 4: Create STAGING table (transient, all VARCHAR)
CREATE OR REPLACE TRANSIENT TABLE {SOURCE}_STAGING (
    -- All VARCHAR columns for initial load
);

-- Step 5: COPY INTO staging from S3
COPY INTO {SOURCE}_STAGING FROM @{SOURCE}_STAGE ...

-- Step 6: Show row counts
SELECT COUNT(*) FROM {SOURCE}_STAGING;

-- Step 7: Data Quality Checks (5-7 validations)
-- Missing required fields
-- Invalid dates
-- Invalid numeric values
-- Duplicate keys
-- Referential integrity

-- Step 8: Preview staging data
SELECT * FROM {SOURCE}_STAGING LIMIT 10;

-- Step 9: MERGE into final table
MERGE INTO {SOURCE} AS target
USING (
    SELECT 
        -- TRY_TO_DATE() for dates
        -- TRY_TO_NUMBER() for numbers
        -- UPPER() for codes
        -- COALESCE() for defaults
    FROM {SOURCE}_STAGING
) AS source
ON (natural_key_match)
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- Step 10-12: Summary queries
-- Total records
-- Most active symbols
-- Recent activity
-- Exchange breakdown

-- Step 13: Cleanup staging
DROP TABLE {SOURCE}_STAGING;
```

**Critical Patterns:**
- ✅ STAGE → STAGING → final table flow
- ✅ All VARCHAR in STAGING (handles bad data)
- ✅ TRY_TO_* functions in MERGE (type conversions)
- ✅ Natural key for MERGE (not surrogate key)
- ✅ Data quality checks before MERGE
- ✅ Summary queries for validation

---

### 3. GitHub Actions Workflow
**Location:** `.github/workflows/{source}_watermark_etl.yml`

**Purpose:** Automated ETL orchestration

**Structure:**
```yaml
name: {Source} ETL - Watermark Based

on:
  workflow_dispatch:
    inputs:
      exchange_filter:
        description: 'Filter by exchange (e.g., NYSE, NASDAQ)'
        required: false
        default: ''
      max_symbols:
        description: 'Maximum symbols to process (leave empty for all)'
        required: false
        default: ''
      skip_recent_hours:
        description: 'Skip symbols processed in last N hours'
        required: false
        default: ''
      batch_size:
        description: 'API batch size'
        required: false
        default: '50'  # Or 100 for less frequent sources
  
  schedule:
    # Quarterly: '0 2 15 1,4,7,10 *' (fundamentals)
    # Monthly: '0 4 1 * *' (insider transactions, earnings)
    # Weekly: '0 3 * * 1' (time series)
    - cron: 'appropriate_schedule'

jobs:
  extract_and_load:
    runs-on: ubuntu-22.04
    timeout-minutes: 360
    
    permissions:
      id-token: write
      contents: read
    
    steps:
      - name: Checkout code
      - name: Set up Python 3.11
      - name: Install dependencies
      - name: Configure AWS credentials (OIDC)
      - name: Extract data (Python script)
      - name: Load data (Snowflake SQL)
      - name: Upload artifacts
      - name: Generate summary
```

**Critical Patterns:**
- ✅ 4 standard inputs (exchange_filter, max_symbols, skip_recent_hours, batch_size)
- ✅ Appropriate cron schedule for data frequency
- ✅ Python 3.11, ubuntu-22.04
- ✅ OIDC authentication (not static keys)
- ✅ Timeout: 360 minutes (6 hours)
- ✅ Artifacts upload for debugging
- ✅ Summary report generation

**Schedule Guidelines:**
- **Quarterly:** Fundamentals (balance_sheet, cash_flow, income_statement)
- **Monthly:** Insider transactions, earnings
- **Weekly:** Time series data
- **Daily:** Real-time data (if applicable)

---

### 4. Watermark Initialization Template
**Location:** `snowflake/setup/templates/watermark_{source}.sql`

**Purpose:** Initialize ETL_WATERMARKS for new data source

**Structure:**
```sql
-- ============================================
-- Watermark Initialization: {SOURCE}
-- ============================================
-- Purpose: Initialize ETL_WATERMARKS table for {source} data extraction
-- Table: ETL_WATERMARKS
-- Target: {SOURCE} table
-- Run: Once before first ETL execution

-- Step 1: Insert watermark records
INSERT INTO ETL_WATERMARKS (
    TABLE_NAME,
    SYMBOL_ID,
    FIRST_FISCAL_DATE,
    LAST_FISCAL_DATE,
    LAST_SUCCESSFUL_RUN,
    LAST_UPDATED,
    API_ELIGIBLE
)
SELECT 
    '{SOURCE}' AS TABLE_NAME,
    SYMBOL_ID,
    NULL AS FIRST_FISCAL_DATE,  -- Will be populated on first run
    NULL AS LAST_FISCAL_DATE,   -- Will be populated on first run
    NULL AS LAST_SUCCESSFUL_RUN,
    CURRENT_TIMESTAMP() AS LAST_UPDATED,
    CASE 
        -- API_ELIGIBLE logic (source-specific)
        WHEN ASSET_TYPE = 'Stock' AND STATUS = 'Active' THEN 'YES'
        WHEN STATUS IN ('Delisted', 'Inactive') THEN 'DEL'
        ELSE 'NO'
    END AS API_ELIGIBLE
FROM SYMBOL
WHERE NOT EXISTS (
    SELECT 1 FROM ETL_WATERMARKS 
    WHERE TABLE_NAME = '{SOURCE}' 
    AND SYMBOL_ID = SYMBOL.SYMBOL_ID
);

-- Step 2: Verify record counts
SELECT 
    API_ELIGIBLE,
    COUNT(*) as count
FROM ETL_WATERMARKS
WHERE TABLE_NAME = '{SOURCE}'
GROUP BY API_ELIGIBLE
ORDER BY API_ELIGIBLE;

-- Step 3: Show sample records
SELECT * FROM ETL_WATERMARKS
WHERE TABLE_NAME = '{SOURCE}'
LIMIT 10;

-- Next Steps:
-- 1. Verify counts match expectations
-- 2. Test with small batch (max_symbols: 10)
-- 3. Check watermark updates after test run
-- 4. Run full production ETL
```

**API_ELIGIBLE Logic by Source:**
- **Stock Data (fundamentals, insider):** Active stocks only
- **Time Series:** All tradable securities (stocks + ETFs)
- **Company Overview:** All symbols (one-time load)

---

### 5. ETL Guide Documentation
**Location:** `docs/{SOURCE}_ETL_GUIDE.md`

**Purpose:** Comprehensive setup and operations guide

**Structure:**
```markdown
# {Source} ETL Guide - Watermark Based

## Table of Contents
1. Overview
2. Prerequisites
3. Data Structure
4. Initial Setup
5. Running the ETL
6. Monitoring & Validation
7. Troubleshooting
8. Maintenance

## 1. Overview
- Data source description
- Update frequency
- API endpoint details
- Key differences from other ETLs

## 2. Prerequisites
### GitHub Secrets Required (9-11 secrets)
- AWS_ROLE_TO_ASSUME
- AWS_REGION
- S3_BUCKET_NAME
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA
- SNOWFLAKE_WAREHOUSE
- ALPHA_VANTAGE_API_KEY
- [Source-specific secrets]

### Snowflake Tables Required
- ETL_WATERMARKS (initialized)
- SYMBOL (populated)
- {SOURCE} (created by SQL script)

## 3. Data Structure
### Table: {SOURCE}
- Column definitions (name, type, description)
- Primary key definition
- Natural key explanation
- Foreign keys
- Metadata columns

### Sample Data
[Example rows showing data format]

## 4. Initial Setup

### Step 1: Initialize Watermarks
```sql
-- Run: snowflake/setup/templates/watermark_{source}.sql
-- Expected: ~2000-3000 records for stocks
-- Verify: API_ELIGIBLE distribution
```

### Step 2: Test with Small Batch
```yaml
# GitHub Actions → {Source} ETL
exchange_filter: NYSE
max_symbols: 10
skip_recent_hours: (blank)
batch_size: 50
```

### Step 3: Verify Results
```sql
-- Check loaded data
-- Check watermark updates
-- Validate data quality
```

### Step 4: Run Full Production ETL
[Full production instructions]

## 5. Running the ETL

### Manual Trigger (GitHub Actions)
[Step-by-step instructions]

### Scheduled Execution
[Cron schedule explanation]

### Parameters Explained
- exchange_filter: [usage]
- max_symbols: [usage]
- skip_recent_hours: [usage]
- batch_size: [usage]

## 6. Monitoring & Validation

### Data Quality Checks
```sql
-- 5-7 validation queries
-- Expected results
-- Alert thresholds
```

### Success Criteria
- Initial setup: [criteria]
- Production runs: [criteria]
- Monthly maintenance: [criteria]

## 7. Troubleshooting

### Issue: No Symbols Selected
**Symptoms:** [description]
**Causes:** [list]
**Solutions:** [step-by-step]

### Issue: API Rate Limits
[Similar structure for 5-7 common issues]

## 8. Maintenance

### Weekly Tasks
[If applicable]

### Monthly Tasks
- Review data quality metrics
- Check for stale watermarks
- Validate recent loads

### Quarterly Tasks
[If applicable]

## Important Differences
[Source-specific variations from standard pattern]

## File Locations
1. Python script: [path]
2. SQL script: [path]
3. Workflow: [path]
4. Watermark template: [path]
5. This guide: [path]
```

---

## Current Implementation Status

### ✅ Complete Sources (5/5 files)
| Source | Python | SQL | Workflow | Watermark | Guide |
|--------|--------|-----|----------|-----------|-------|
| **Income Statement** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Cash Flow** | ✅ | ✅ | ✅ | ❌ | ✅ |
| **Insider Transactions** | ✅ | ✅ | ✅ | ✅ | ✅ |

### ⚠️ Incomplete Sources
| Source | Python | SQL | Workflow | Watermark | Guide |
|--------|--------|-----|----------|-----------|-------|
| **Balance Sheet** | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Company Overview** | ✅ | ❌ | ✅ | ✅ | ✅ |
| **Time Series** | ✅ | ❌ | ✅ | ❌ | ❌ |

---

## Development Workflow

When adding a **new data source**, follow this process:

### Phase 1: Analysis
1. Review API documentation (Alpha Vantage)
2. Identify natural key columns
3. Determine update frequency (schedule)
4. Check if staleness check is needed
5. Define API_ELIGIBLE logic

### Phase 2: Implementation (5 files)

**Order matters for efficiency:**

#### Step 1: Create Python Extraction Script
- Copy from similar source (e.g., cash_flow_watermark.py)
- Update API endpoint and function name
- Modify field mapping for specific source
- Adjust staleness check (or remove if not needed)
- Test locally with small batch

#### Step 2: Create SQL Load Script
- Copy from similar source (e.g., load_cash_flow_from_s3.sql)
- Define table schema with proper data types
- Update natural key in PRIMARY KEY and MERGE
- Create 5-7 data quality checks
- Add source-specific summary queries

#### Step 3: Create GitHub Actions Workflow
- Copy from similar source workflow
- Update schedule (cron) based on data frequency
- Adjust default batch_size if needed
- Update workflow name and descriptions

#### Step 4: Create Watermark Template
- Copy from similar source (e.g., watermark_income_statement.sql)
- Update TABLE_NAME throughout
- Adjust API_ELIGIBLE logic for source type
- Update expected counts in comments

#### Step 5: Create ETL Guide
- Copy from similar source (e.g., CASH_FLOW_ETL_GUIDE.md)
- Update all source references
- Document source-specific differences
- Add relevant troubleshooting scenarios
- Update file paths

### Phase 3: Testing
1. **Watermark Initialization:** Run template SQL, verify counts
2. **Small Batch Test:** Run workflow with max_symbols: 10
3. **Data Validation:** Run quality checks, verify data loaded
4. **Watermark Updates:** Confirm LAST_SUCCESSFUL_RUN updated
5. **Full Production:** Run without limits or wait for schedule

### Phase 4: Documentation
- Update this file's status table
- Add to main README.md
- Document any unique troubleshooting findings

---

## File Naming Conventions

**Strict naming required for automation:**

| File Type | Pattern | Example |
|-----------|---------|---------|
| Python Script | `fetch_{source}_watermark.py` | `fetch_balance_sheet_watermark.py` |
| SQL Load | `load_{source}_from_s3.sql` | `load_balance_sheet_from_s3.sql` |
| Workflow | `{source}_watermark_etl.yml` | `balance_sheet_watermark_etl.yml` |
| Watermark Template | `watermark_{source}.sql` | `watermark_balance_sheet.sql` |
| ETL Guide | `{SOURCE}_ETL_GUIDE.md` | `BALANCE_SHEET_ETL_GUIDE.md` |

**Case Conventions:**
- Python/SQL/Workflow files: `snake_case`
- Documentation files: `UPPER_CASE` for source name
- Table names in SQL: `UPPER_CASE`
- Column names in SQL: `UPPER_CASE`
- JSON keys from API: `snake_case`

---

## Key Differences by Data Type

### Quarterly Fundamentals (Balance Sheet, Cash Flow, Income Statement)
- **Schedule:** Quarterly - `cron: '0 2 15 1,4,7,10 *'`
- **Staleness Check:** ✅ Required (135 days)
- **Batch Size:** 50 (default)
- **Natural Key:** `(SYMBOL, FISCAL_DATE_ENDING, REPORT_TYPE)`
- **API_ELIGIBLE:** Active stocks only

### Monthly Sporadic (Insider Transactions, Earnings)
- **Schedule:** Monthly - `cron: '0 4 1 * *'`
- **Staleness Check:** ❌ Not needed (unpredictable data)
- **Batch Size:** 100 (higher, less frequent)
- **Natural Key:** Source-specific (may include additional fields)
- **Expected Behavior:** Many "no data" responses are NORMAL

### Weekly Time Series (Daily/Intraday Prices)
- **Schedule:** Weekly - `cron: '0 3 * * 1'`
- **Staleness Check:** ✅ Required (7-14 days)
- **Batch Size:** 50
- **Natural Key:** `(SYMBOL, DATE)`
- **API_ELIGIBLE:** Stocks AND ETFs (all tradable)

### One-Time Reference (Company Overview)
- **Schedule:** Manual or very infrequent
- **Staleness Check:** ✅ Required (365 days, stale if no update in year)
- **Batch Size:** 100
- **Natural Key:** `(SYMBOL)` - one record per symbol
- **API_ELIGIBLE:** All symbols

---

## Best Practices

### Code Quality
- ✅ Follow existing patterns exactly (copy from working source)
- ✅ Include comprehensive error handling
- ✅ Log all major operations (API calls, S3 uploads, DB updates)
- ✅ Use bulk operations (never row-by-row in loops)
- ✅ Close connections immediately after use

### Testing
- ✅ Always test with max_symbols: 10 first
- ✅ Verify watermarks update even on "no data"
- ✅ Run all data quality checks before production
- ✅ Test with different exchanges (NYSE, NASDAQ, etc.)
- ✅ Validate natural key prevents duplicates

### Documentation
- ✅ Document source-specific differences prominently
- ✅ Include expected behavior (e.g., "no data is normal")
- ✅ Provide copy-paste ready SQL queries
- ✅ Add troubleshooting for observed issues
- ✅ Keep file location paths updated

### Maintenance
- ✅ Review execution logs monthly
- ✅ Monitor API rate limit usage
- ✅ Check for stale watermarks quarterly
- ✅ Update documentation as patterns evolve
- ✅ Archive old data per retention policy

---

## Common Pitfalls to Avoid

### ❌ Don't: Use row-by-row watermark updates
**Problem:** Extremely slow (10,000+ individual UPDATEs)
**Solution:** Use bulk MERGE with temp table

### ❌ Don't: Skip S3 cleanup
**Problem:** Duplicate data loaded, file clutter
**Solution:** Always cleanup_s3_bucket() before extraction

### ❌ Don't: Keep connection open entire process
**Problem:** Connection timeouts, resource waste
**Solution:** Close after query, reopen for updates

### ❌ Don't: Assume all sources need staleness check
**Problem:** Skips valid symbols with sporadic data
**Solution:** Only use for predictable quarterly data

### ❌ Don't: Use literal SQL dates in Python
**Problem:** Hard to maintain, easy to forget to update
**Solution:** Use DATEADD() functions in SQL queries

### ❌ Don't: Skip data quality checks
**Problem:** Bad data silently loaded
**Solution:** Run 5-7 validations before MERGE

### ❌ Don't: Use different natural keys in SQL vs business logic
**Problem:** Unexpected duplicates or update failures
**Solution:** Document and consistently use same natural key

---

## Migration from Old Incremental ETL

If you have **pre-watermark ETL code**, follow this migration:

### Old Pattern (Deprecated)
```python
# incremental_etl.py - Generic module
# PostgreSQL for tracking
# Content hashing for change detection
# Row-by-row processing
# No S3 cleanup
```

### New Pattern (Current)
```python
# fetch_{source}_watermark.py - Source-specific
# Snowflake ETL_WATERMARKS for tracking
# Watermark dates for change detection
# Bulk operations with temp tables
# S3 cleanup before extraction
```

### Migration Steps
1. ✅ Create all 5 new files (don't modify old code)
2. ✅ Initialize watermarks in Snowflake
3. ✅ Test new ETL with small batch
4. ✅ Verify data matches old system
5. ✅ Run new ETL in parallel for 1-2 cycles
6. ✅ Decommission old ETL
7. ✅ Archive old code (don't delete, for reference)

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-10-12 | Initial 5-file pattern documentation |

---

## Related Documentation

- **ETL Operations Guide:** `docs/ETL_OPERATIONS_GUIDE.md` - Watermark system rules
- **Individual ETL Guides:** `docs/{SOURCE}_ETL_GUIDE.md` - Source-specific setup
- **Workflow Examples:** `.github/workflows/` - All workflows
- **SQL Templates:** `snowflake/setup/templates/` - Initialization scripts

---

## Support & Troubleshooting

**For issues with:**
- **Pattern understanding:** Review this document + working examples (INCOME_STATEMENT)
- **Source-specific problems:** Check `docs/{SOURCE}_ETL_GUIDE.md`
- **Watermark system:** Review `docs/ETL_OPERATIONS_GUIDE.md`
- **GitHub Actions:** Check workflow logs and artifacts
- **Data quality:** Run validation queries in source ETL guide

**Quick Reference:**
- Working example (complete): INCOME_STATEMENT, INSIDER_TRANSACTIONS
- Working example (partial): CASH_FLOW, COMPANY_OVERVIEW
- Copy these as templates for new sources

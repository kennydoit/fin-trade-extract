# Data Source Watermarks Setup Guide

## Overview

This guide explains how to set up watermarks for different data sources in the ETL pipeline.

## Two-Step Process

### Step 1: Create Base ETL_WATERMARKS Table

**Workflow:** `Create ETL Watermarks Table`

This creates the base watermarking table and populates it with all symbols from LISTING_STATUS.

**What it does:**
- Drops existing ETL_WATERMARKS table (if exists)
- Creates new table with complete schema
- Inserts ~20,000 symbols with `TABLE_NAME = 'LISTING_STATUS'`
- Sets `API_ELIGIBLE = 'NO'` for all base records

**Run this once before creating data source watermarks.**

### Step 2: Add Data Source Watermarks

**Workflow:** `Add Data Source Watermarks`

This copies base watermarks and creates data source-specific entries with appropriate filtering.

**What it creates:**

#### TIME_SERIES_DAILY_ADJUSTED
- **Eligibility:** All symbols (both active and delisted)
- **API_ELIGIBLE:** 'YES' for all symbols
- **Purpose:** Historical price data
- **Count:** ~20,000 symbols

#### COMPANY_OVERVIEW
- **Eligibility:** Common stocks only
- **API_ELIGIBLE:** 'YES' if `ASSET_TYPE = 'Stock'`, otherwise 'NO'
- **Purpose:** Company fundamental information
- **Count:** ~15,000-18,000 symbols (stocks only)

#### BALANCE_SHEET
- **Eligibility:** Active common stocks only
- **API_ELIGIBLE:** 'YES' if `ASSET_TYPE = 'Stock'` AND `STATUS = 'Active'`
- **Purpose:** Balance sheet financial data
- **Count:** ~8,000-10,000 symbols

#### CASH_FLOW
- **Eligibility:** Active common stocks only
- **API_ELIGIBLE:** 'YES' if `ASSET_TYPE = 'Stock'` AND `STATUS = 'Active'`
- **Purpose:** Cash flow statement data
- **Count:** ~8,000-10,000 symbols

#### INCOME_STATEMENT
- **Eligibility:** Active common stocks only
- **API_ELIGIBLE:** 'YES' if `ASSET_TYPE = 'Stock'` AND `STATUS = 'Active'`
- **Purpose:** Income statement financial data
- **Count:** ~8,000-10,000 symbols

#### INSIDER_TRANSACTIONS
- **Eligibility:** Active common stocks only
- **API_ELIGIBLE:** 'YES' if `ASSET_TYPE = 'Stock'` AND `STATUS = 'Active'`
- **Purpose:** Insider trading activity
- **Count:** ~8,000-10,000 symbols

#### EARNINGS_CALL_TRANSCRIPT
- **Eligibility:** Active common stocks only
- **API_ELIGIBLE:** 'YES' if `ASSET_TYPE = 'Stock'` AND `STATUS = 'Active'`
- **Purpose:** Earnings call transcripts
- **Count:** ~8,000-10,000 symbols

## Usage Instructions

### Initial Setup (Run Once)

1. **Create base watermarking table:**
   - Go to: GitHub Actions → "Create ETL Watermarks Table"
   - Click: "Run workflow"
   - Wait for completion
   - Verify: ~20,000 records with `TABLE_NAME = 'LISTING_STATUS'`

2. **Add data source watermarks:**
   - Go to: GitHub Actions → "Add Data Source Watermarks"
   - Select: "ALL_SOURCES" (or specific data source)
   - Click: "Run workflow"
   - Wait for completion
   - Verify: Multiple TABLE_NAME entries (LISTING_STATUS, TIME_SERIES_DAILY_ADJUSTED, etc.)

### Verify Results

Check the workflow output for summary tables showing:
- Total symbols per data source
- API eligible vs not eligible counts
- Percentage of eligible symbols
- Breakdown by exchange and asset type

### Query Watermarks

```sql
-- See all data sources
SELECT 
    TABLE_NAME,
    COUNT(*) as TOTAL,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as ELIGIBLE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

-- Get symbols ready for TIME_SERIES_DAILY_ADJUSTED extraction
SELECT SYMBOL, EXCHANGE, ASSET_TYPE, STATUS
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND API_ELIGIBLE = 'YES'
ORDER BY SYMBOL;

-- Get symbols ready for COMPANY_OVERVIEW extraction
SELECT SYMBOL, NAME, EXCHANGE, STATUS
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'COMPANY_OVERVIEW'
  AND API_ELIGIBLE = 'YES'
ORDER BY SYMBOL;
```

## Next Steps

After watermarks are created:

1. **TIME_SERIES_DAILY_ADJUSTED:** Run incremental ETL for time series data
2. **COMPANY_OVERVIEW:** Run company overview extraction
3. **Financial Statements:** Run balance sheet, cash flow, income statement extractions
4. **Advanced Data:** Run insider transactions and earnings call transcript extractions

Each ETL process will:
- Query watermarks where `API_ELIGIBLE = 'YES'`
- Process symbols in batches
- Update `FIRST_FISCAL_DATE`, `LAST_FISCAL_DATE`, `LAST_SUCCESSFUL_RUN`
- Track failures with `CONSECUTIVE_FAILURES`
- Update `UPDATED_AT` timestamp

## Table Schema

```sql
TABLE_NAME                  VARCHAR(100) NOT NULL     -- Data source identifier
SYMBOL_ID                   NUMBER(38,0) NOT NULL     -- Hash-based symbol ID
SYMBOL                      VARCHAR(20) NOT NULL      -- Stock ticker
NAME                        VARCHAR(255)              -- Company name
EXCHANGE                    VARCHAR(64)               -- NYSE, NASDAQ, etc.
ASSET_TYPE                  VARCHAR(64)               -- Stock, ETF, etc.
STATUS                      VARCHAR(12)               -- Active, Delisted
API_ELIGIBLE                VARCHAR(3)                -- YES or NO
IPO_DATE                    DATE                      -- IPO date
DELISTING_DATE              DATE                      -- Delisting date
FIRST_FISCAL_DATE           DATE                      -- First data point
LAST_FISCAL_DATE            DATE                      -- Most recent data point
LAST_SUCCESSFUL_RUN         TIMESTAMP_NTZ             -- Last successful extraction
CONSECUTIVE_FAILURES        NUMBER(5,0)               -- Failure tracking
CREATED_AT                  TIMESTAMP_NTZ             -- Creation timestamp
UPDATED_AT                  TIMESTAMP_NTZ             -- Last update timestamp

Primary Key: (TABLE_NAME, SYMBOL_ID)
```

## Troubleshooting

**Issue:** Watermarks not created for a data source
- **Solution:** Run "Add Data Source Watermarks" workflow again (uses MERGE logic)

**Issue:** Too many/too few eligible symbols
- **Solution:** Check LISTING_STATUS data quality and filtering logic in SQL

**Issue:** Duplicate symbols
- **Solution:** Primary key (TABLE_NAME, SYMBOL_ID) prevents duplicates automatically

**Issue:** Need to reset watermarks
- **Solution:** Re-run "Create ETL Watermarks Table" (drops and recreates everything)

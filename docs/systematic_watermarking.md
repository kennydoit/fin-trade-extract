# Systematic ETL Watermarking System

## Overview

A systematic approach to ETL watermarking that provides proper tracking and incremental processing for financial data extraction. This system establishes watermarks **before** data loading to ensure reliable incremental ETL operations.

## Architecture

### **Core Components**

1. **ETL_WATERMARKS Table**: Central tracking table for all data processing status
2. **Watermarking Workflow**: Manual workflow to initialize watermarks for new data types
3. **Data Type Configurations**: Predefined settings for each supported data type
4. **Status Tracking**: Comprehensive processing status management

### **Supported Data Types**

| Data Type | Table Name | Refresh Frequency | Priority | Description |
|-----------|------------|-------------------|----------|-------------|
| `time_series_daily_adjusted` | `TIME_SERIES_DAILY_ADJUSTED` | 1 day | High (1) | Daily stock prices and volume |
| `company_overview` | `COMPANY_OVERVIEW` | 30 days | Medium (3) | Company fundamental information |
| `listing_status` | `LISTING_STATUS` | 7 days | High (2) | Stock listing and exchange info |
| `balance_sheet` | `BALANCE_SHEET` | 90 days | Lower (4) | Company balance sheet data |

## Watermarking Workflow Process

### **Step 1: Manual Watermark Initialization**

**Before loading any new data type**, run the watermarking workflow:

#### **Via GitHub Actions (Recommended)**
1. Go to Actions tab in GitHub repository
2. Select "Create ETL Watermarks" workflow
3. Click "Run workflow"
4. Select table name (e.g., `TIME_SERIES_DAILY_ADJUSTED`)
5. Choose options:
   - `create_table_only: false` - Create table AND initialize watermarks
   - `create_table_only: true` - Only create the watermarks table

#### **Via Python Script (Local)**
```bash
# Initialize watermarks for time series data
python scripts/watermarking/create_watermarks.py --table-name TIME_SERIES_DAILY_ADJUSTED

# Only create the table structure
python scripts/watermarking/create_watermarks.py --table-name COMPANY_OVERVIEW --create-table-only
```

#### **Via Snowflake SQL (Manual)**
```sql
-- Run the complete setup script
@snowflake/watermarking/create_watermarks_table.sql
```

### **Step 2: Validate Watermark Setup**

After initialization, validate the setup:

```sql
-- Check watermark summary
SELECT 
    DATA_TYPE,
    PROCESSING_STATUS,
    COUNT(*) as COUNT
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
GROUP BY DATA_TYPE, PROCESSING_STATUS
ORDER BY DATA_TYPE, PROCESSING_STATUS;
```

Expected results:
- `time_series_daily_adjusted | pending | 1000` (or number of active symbols)

### **Step 3: Run Data Extraction**

Now your data extractors will work with proper incremental logic:

```bash
# This will now process only symbols marked as 'pending' or stale
python scripts/github_actions/fetch_time_series_bulk.py
```

## ETL_WATERMARKS Table Schema

```sql
CREATE TABLE ETL_WATERMARKS (
    WATERMARK_ID NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    SYMBOL VARCHAR(20) NOT NULL,
    DATA_TYPE VARCHAR(50) NOT NULL,
    PROCESSING_STATUS VARCHAR(20) NOT NULL,
    LAST_PROCESSED_DATE DATE,
    LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    ERROR_MESSAGE VARCHAR(2000),
    RETRY_COUNT NUMBER(10,0) DEFAULT 0,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT UK_ETL_WATERMARKS_SYMBOL_DATA_TYPE UNIQUE (SYMBOL, DATA_TYPE)
);
```

### **Processing Status Values**

| Status | Meaning | Next Action |
|--------|---------|-------------|
| `pending` | Never processed before | Will be selected for processing |
| `completed` | Successfully processed | Will be skipped until refresh needed |
| `failed` | Processing failed | Will be retried |
| `processing` | Currently being processed | Will be skipped |
| `skipped` | Intentionally skipped (no data) | Will be skipped |

## Incremental Processing Logic

### **Symbol Selection Logic**
1. **Never Processed**: Symbols with no watermark entry or `pending` status
2. **Stale Data**: Symbols where `LAST_UPDATED` > `refresh_frequency_days` ago
3. **Priority Order**: Never processed first, then stale data
4. **Limits Applied**: Respects `MAX_SYMBOLS` environment variable

### **Processing Flow**
1. **Get Symbols**: Query watermarks for symbols needing processing
2. **Mark Processing**: Update status to `processing` (prevents duplicate work)
3. **Extract Data**: Call Alpha Vantage API and upload to S3
4. **Update Status**: Mark as `completed` or `failed` based on result
5. **Next Run**: Selects different symbols based on updated watermarks

## Benefits of Systematic Approach

### **✅ Reliability**
- No duplicate processing of same symbols
- Clear tracking of what was processed when
- Automatic retry logic for failed symbols

### **✅ Cost Efficiency**
- Only processes symbols that need updating
- Respects refresh frequencies (daily vs monthly vs quarterly)
- Prevents wasted API calls on already-processed symbols

### **✅ Scalability**
- Handles thousands of symbols systematically
- Prioritizes never-processed symbols first
- Batch processing with configurable limits

### **✅ Visibility**
- Clear status tracking for all symbols
- Error message capture for failed processing
- Comprehensive reporting and monitoring

## Usage Examples

### **Fresh Start (No Existing Data)**
```bash
# 1. Drop all existing tables (if any)
# Done manually in Snowflake

# 2. Initialize watermarks for time series
python scripts/watermarking/create_watermarks.py --table-name TIME_SERIES_DAILY_ADJUSTED

# 3. Run time series extraction (will process first batch of pending symbols)
python scripts/github_actions/fetch_time_series_bulk.py

# 4. Initialize watermarks for company overview
python scripts/watermarking/create_watermarks.py --table-name COMPANY_OVERVIEW

# 5. Run company overview extraction
python scripts/github_actions/fetch_company_overview_bulk.py
```

### **Adding New Data Type**
```bash
# Initialize watermarks for balance sheet data
python scripts/watermarking/create_watermarks.py --table-name BALANCE_SHEET

# Run balance sheet extraction
python scripts/github_actions/fetch_balance_sheet_bulk.py
```

### **Monitoring Progress**
```sql
-- Check overall progress
SELECT 
    DATA_TYPE,
    PROCESSING_STATUS,
    COUNT(*) as COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATA_TYPE), 1) as PCT
FROM ETL_WATERMARKS
GROUP BY DATA_TYPE, PROCESSING_STATUS
ORDER BY DATA_TYPE, PROCESSING_STATUS;

-- Check recent activity
SELECT 
    DATA_TYPE,
    COUNT(*) as PROCESSED_TODAY
FROM ETL_WATERMARKS
WHERE DATE(LAST_UPDATED) = CURRENT_DATE()
  AND PROCESSING_STATUS = 'completed'
GROUP BY DATA_TYPE;
```

## Migration from Previous System

### **If You Have Existing Data**
1. **Backup existing tables** (optional)
2. **Run watermarking initialization** - will create watermarks for existing symbols
3. **Existing extractors will work** - they'll use the new watermarking system
4. **Monitor first runs** - ensure incremental logic works as expected

### **If Starting Fresh**
1. **Drop all existing tables** (manually in Snowflake)
2. **Initialize watermarks** for your first data type
3. **Run extraction** - will systematically process all symbols
4. **Add more data types** as needed

This systematic approach ensures reliable, cost-efficient, and scalable ETL operations!